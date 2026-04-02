# Databricks notebook source
# FinBank S.A. — Notificación de Resumen Diario del Pipeline
# ==========================================================
# Se ejecuta como última tarea del workflow (on_success).
# Envía un correo con el resumen completo de la ejecución:
#   - Registros procesados por capa
#   - Tiempo total de ejecución
#   - Número de alertas de calidad generadas
#   - Alerta de anomalía de volumen (si aplica)

# COMMAND ----------

import json
import smtplib
import logging
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("notification")

spark = SparkSession.builder.getOrCreate()

def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope="finbank-kv", key=key)

STORAGE_ACCOUNT  = get_secret("storage-account-name")
BRONZE_ROOT      = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ERRORS_ROOT      = f"abfss://errors@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ALERT_EMAIL_TO   = get_secret("alert-email-to")
SMTP_HOST        = get_secret("smtp-host")
SMTP_PORT        = int(get_secret("smtp-port"))
SMTP_USER        = get_secret("smtp-user")
SMTP_PASSWORD    = get_secret("smtp-password")

# ─────────────────────────────────────────────────────
# Leer outputs de tareas anteriores
# ─────────────────────────────────────────────────────

def parse_task_output(widget_name: str) -> dict:
    try:
        raw = dbutils.widgets.get(widget_name)
        return json.loads(raw) if raw else {}
    except Exception:
        return {}

bronze_out  = parse_task_output("bronze_output")
silver_out  = parse_task_output("silver_output")
gold_out    = parse_task_output("gold_output")
quality_out = parse_task_output("quality_output")

batch_id    = bronze_out.get("batch_id", "DESCONOCIDO")

# ─────────────────────────────────────────────────────
# Verificación de anomalía de volumen
# (±30% respecto al promedio de las últimas 7 ejecuciones)
# ─────────────────────────────────────────────────────

def check_volume_anomaly(current_records: int) -> dict:
    """
    Compara el volumen actual con el promedio de las últimas 7 ejecuciones.
    Si difiere más de 30%, devuelve alerta con detalle.
    """
    result = {
        "anomaly_detected": False,
        "current": current_records,
        "avg_7d": 0,
        "pct_diff": 0.0,
    }
    try:
        log_path = f"{BRONZE_ROOT}/_control/ingestion_log"
        df_log = (
            spark.read.format("delta").load(log_path)
            .filter(F.col("status") == "SUCCESS")
            .filter(F.col("batch_id") != batch_id)
            .groupBy("batch_id")
            .agg(F.sum("n_records").alias("total_records"))
            .orderBy(F.col("batch_id").desc())
            .limit(7)
        )
        rows = df_log.collect()
        if len(rows) >= 3:
            avg_7d  = sum(r["total_records"] for r in rows) / len(rows)
            pct_diff = abs(current_records - avg_7d) / avg_7d * 100 if avg_7d > 0 else 0
            result["avg_7d"]  = round(avg_7d, 0)
            result["pct_diff"] = round(pct_diff, 2)
            if pct_diff > 30:
                result["anomaly_detected"] = True
                log.warning(
                    f"ALERTA DE VOLUMEN: {current_records:,} registros vs promedio "
                    f"7d de {avg_7d:,.0f} ({pct_diff:.1f}% de diferencia)"
                )
    except Exception as e:
        log.warning(f"No se pudo calcular anomalía de volumen: {e}")
    return result


# ─────────────────────────────────────────────────────
# Construcción del correo HTML
# ─────────────────────────────────────────────────────

def build_email_body(bronze_out: dict, silver_out: dict,
                     gold_out: dict, quality_out: dict,
                     volume_check: dict) -> str:

    now_str     = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    total_bronze = bronze_out.get("total_records", 0)
    total_silver = silver_out.get("total_records_silver", 0)
    total_gold   = gold_out.get("total_records_gold", 0)
    q_total      = quality_out.get("total_checks", 0)
    q_passed     = quality_out.get("passed", 0)
    q_failed     = quality_out.get("failed", 0)
    failed_checks = quality_out.get("failed_checks", [])

    volume_section = ""
    if volume_check["anomaly_detected"]:
        volume_section = f"""
        <tr style="background:#fff3cd;">
          <td colspan="2" style="padding:8px;color:#856404;font-weight:bold;">
            ⚠️ ALERTA DE VOLUMEN: {volume_check['current']:,} registros procesados vs
            promedio 7d de {volume_check['avg_7d']:,.0f}
            ({volume_check['pct_diff']:.1f}% de diferencia — umbral: 30%)
          </td>
        </tr>"""

    quality_rows = ""
    if failed_checks:
        quality_rows = "".join(
            f'<tr><td style="padding:4px 8px;color:#dc3545;">✗ {c}</td></tr>'
            for c in failed_checks
        )
    else:
        quality_rows = '<tr><td style="padding:4px 8px;color:#198754;">✓ Todos los checks pasaron</td></tr>'

    status_color = "#198754" if q_failed == 0 else "#dc3545"
    status_label = "EXITOSO" if q_failed == 0 else f"ALERTAS ({q_failed} checks fallidos)"

    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#212529;">
    <h2 style="color:#0d6efd;">FinBank S.A. — Reporte Diario del Pipeline de Datos</h2>
    <p><strong>Fecha de ejecución:</strong> {now_str}</p>
    <p><strong>Batch ID:</strong> <code>{batch_id}</code></p>
    <p><strong>Estado:</strong>
       <span style="color:{status_color};font-weight:bold;">{status_label}</span>
    </p>

    <h3>Registros procesados por capa</h3>
    <table border="1" cellpadding="6" cellspacing="0"
           style="border-collapse:collapse;width:100%;max-width:600px;">
      <thead style="background:#0d6efd;color:white;">
        <tr>
          <th>Capa</th>
          <th>Registros</th>
        </tr>
      </thead>
      <tbody>
        <tr><td>🥉 Bronze</td><td style="text-align:right;">{total_bronze:,}</td></tr>
        <tr><td>🥈 Silver</td><td style="text-align:right;">{total_silver:,}</td></tr>
        <tr><td>🥇 Gold</td> <td style="text-align:right;">{total_gold:,}</td></tr>
        {volume_section}
      </tbody>
    </table>

    <h3>Calidad de datos</h3>
    <table border="1" cellpadding="6" cellspacing="0"
           style="border-collapse:collapse;width:100%;max-width:600px;">
      <thead style="background:#6c757d;color:white;">
        <tr><th>Check</th></tr>
      </thead>
      <tbody>
        <tr>
          <td style="padding:6px 8px;">
            Total: {q_total} | Pasaron: {q_passed} |
            Fallaron: <strong style="color:{status_color};">{q_failed}</strong>
          </td>
        </tr>
        {quality_rows}
      </tbody>
    </table>

    <br>
    <p style="font-size:12px;color:#6c757d;">
      Este mensaje es generado automáticamente por el pipeline de datos de FinBank S.A.<br>
      Para acceder al dashboard de monitoreo, ingrese al workspace de Databricks.
    </p>
    </body></html>
    """


def send_email(subject: str, body_html: str):
    """Envía el correo de resumen usando SMTP autenticado."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_EMAIL_TO

    msg.attach(MIMEText(body_html, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, [ALERT_EMAIL_TO], msg.as_string())
        log.info(f"Correo enviado a {ALERT_EMAIL_TO}")
    except Exception as e:
        log.error(f"Error enviando correo: {e}")
        raise


# ─────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────

# COMMAND ----------

total_records = bronze_out.get("total_records", 0)
volume_check  = check_volume_anomaly(total_records)

# Si hay anomalía de volumen, enviar alerta diferenciada ANTES del resumen
if volume_check["anomaly_detected"]:
    send_email(
        subject=f"⚠️ [FinBank Pipeline] ALERTA DE VOLUMEN — Batch {batch_id}",
        body_html=f"""
        <html><body>
        <h2 style="color:#856404;">Alerta de Anomalía de Volumen</h2>
        <p>El batch <strong>{batch_id}</strong> procesó
           <strong>{total_records:,}</strong> registros, lo que representa una
           diferencia del <strong>{volume_check['pct_diff']:.1f}%</strong>
           respecto al promedio de las últimas 7 ejecuciones
           ({volume_check['avg_7d']:,.0f} registros).</p>
        <p>El pipeline continuó su ejecución. Revisar los logs para determinar
           si la variación es esperada o indica un problema en la fuente.</p>
        </body></html>
        """
    )

# Reporte diario de resumen
q_failed = quality_out.get("failed", 0)
status_label = "EXITOSO" if q_failed == 0 else f"CON ALERTAS DE CALIDAD"
email_body = build_email_body(bronze_out, silver_out, gold_out, quality_out, volume_check)

send_email(
    subject=f"[FinBank Pipeline] Reporte Diario — {status_label} — {batch_id}",
    body_html=email_body
)

log.info("Notificación enviada correctamente.")

dbutils.notebook.exit(json.dumps({
    "batch_id":       batch_id,
    "email_sent_to":  ALERT_EMAIL_TO,
    "volume_anomaly": volume_check["anomaly_detected"],
    "status":         "SUCCESS",
}))
