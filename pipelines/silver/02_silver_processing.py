# Databricks notebook source
# FinBank S.A. — Capa Silver: Limpieza y conformación
# ====================================================
# Consume Bronze, aplica limpieza completa, valida integridad
# referencial, enmascara PII, calcula ind_sospechoso y
# genera reporte de calidad por ejecución.

# COMMAND ----------

import json
import hashlib
import logging
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, DateType, TimestampType
)
from delta.tables import DeltaTable

# ─────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────

def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope="finbank-kv", key=key)

STORAGE_ACCOUNT = get_secret("storage-account-name")
BRONZE_ROOT  = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_ROOT  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ERRORS_ROOT  = f"abfss://errors@{STORAGE_ACCOUNT}.dfs.core.windows.net"

QUALITY_LOG_PATH = f"{SILVER_ROOT}/_control/quality_log"
ERROR_RECORDS_PATH = f"{ERRORS_ROOT}/referential_integrity_errors"

# Columnas PII que se enmascaran con SHA-256
PII_COLUMNS = {
    "TB_CLIENTES_CORE": ["nomb_cli", "apell_cli", "num_doc"],
}

# Fecha de corte de negocio para validar fechas fuera de rango
DATE_MIN = date(2000, 1, 1)
DATE_MAX = date(2030, 12, 31)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver")

spark = SparkSession.builder.getOrCreate()

# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────

def sha256_udf(col_value):
    """UDF para enmascarar PII con SHA-256."""
    if col_value is None:
        return None
    return hashlib.sha256(str(col_value).encode()).hexdigest()

sha256_spark = F.udf(sha256_udf, StringType())


def read_bronze(table_name: str) -> DataFrame:
    """Lee la última versión de una tabla Bronze."""
    path = f"{BRONZE_ROOT}/{table_name}"
    return spark.read.format("delta").load(path)


def write_silver(df: DataFrame, table_name: str, pk: str,
                 batch_id: str) -> int:
    """Escribe en Silver con MERGE para idempotencia."""
    target_path = f"{SILVER_ROOT}/{table_name}"

    df_with_meta = (
        df
        .withColumn("_silver_timestamp", F.current_timestamp())
        .withColumn("_batch_id_silver",  F.lit(batch_id))
    )

    if DeltaTable.isDeltaTable(spark, target_path):
        (
            DeltaTable.forPath(spark, target_path)
            .alias("tgt")
            .merge(df_with_meta.alias("src"), f"tgt.{pk} = src.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df_with_meta.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )

    return spark.read.format("delta").load(target_path).count()


def send_to_error_table(df_rejected: DataFrame, table_name: str,
                        reason: str, batch_id: str):
    """Envía registros rechazados a la tabla de errores con motivo documentado."""
    if df_rejected.rdd.isEmpty():
        return
    n = df_rejected.count()
    (
        df_rejected
        .withColumn("_source_table", F.lit(table_name))
        .withColumn("_rejection_reason", F.lit(reason))
        .withColumn("_rejected_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .write
        .format("delta")
        .mode("append")
        .save(ERROR_RECORDS_PATH)
    )
    log.info(f"  [{table_name}] {n:,} registros enviados a errores: {reason}")


def log_quality_report(table_name: str, batch_id: str, n_input: int,
                       n_output: int, n_rejected: int,
                       null_pct_by_col: dict, duration_sec: float):
    """Persiste el reporte de calidad de datos por ejecución."""
    schema = (
        "table_name STRING, batch_id STRING, n_input LONG, n_output LONG, "
        "n_rejected LONG, pct_conformes DOUBLE, null_summary STRING, "
        "duration_sec DOUBLE, logged_at TIMESTAMP"
    )
    pct_conformes = round((n_output / n_input * 100) if n_input > 0 else 0.0, 2)
    null_summary  = json.dumps(null_pct_by_col)
    row_df = spark.createDataFrame(
        [(table_name, batch_id, n_input, n_output, n_rejected,
          pct_conformes, null_summary, duration_sec, datetime.utcnow())],
        schema=schema
    )
    row_df.write.format("delta").mode("append").save(QUALITY_LOG_PATH)
    log.info(
        f"  [{table_name}] Calidad: {n_input:,} entrada | "
        f"{n_output:,} conformes ({pct_conformes}%) | {n_rejected:,} rechazados"
    )


def compute_null_pct(df: DataFrame) -> dict:
    """Calcula el porcentaje de nulos por columna."""
    n = df.count()
    if n == 0:
        return {}
    null_counts = df.select(
        [F.round(F.sum(F.col(c).isNull().cast("int")) / n * 100, 2)
         .alias(c) for c in df.columns]
    ).collect()[0].asDict()
    return {k: v for k, v in null_counts.items() if v and v > 0}


# ─────────────────────────────────────────────────────
# Transformaciones por tabla
# ─────────────────────────────────────────────────────

def process_clientes(batch_id: str) -> dict:
    t_start = datetime.utcnow()
    table   = "TB_CLIENTES_CORE"
    log.info(f"Procesando Silver: {table}")

    df = read_bronze(table)
    n_input = df.count()

    # 1. Eliminar duplicados exactos
    df = df.dropDuplicates(["id_cli"])

    # 2. Filtrar campos obligatorios nulos
    required = ["id_cli", "tip_doc", "fec_alta", "cod_segmento", "depto_res", "estado_cli"]
    df_valid, df_invalid = (
        df.filter(F.col("id_cli").isNotNull() &
                  F.col("tip_doc").isNotNull() &
                  F.col("fec_alta").isNotNull() &
                  F.col("cod_segmento").isNotNull()),
        df.filter(F.col("id_cli").isNull() |
                  F.col("tip_doc").isNull() |
                  F.col("fec_alta").isNull() |
                  F.col("cod_segmento").isNull())
    )
    send_to_error_table(df_invalid, table, "CAMPO_OBLIGATORIO_NULO", batch_id)

    # 3. Estandarizar tipos de datos
    df_valid = (
        df_valid
        .withColumn("fec_nac",    F.to_date("fec_nac",  "yyyy-MM-dd"))
        .withColumn("fec_alta",   F.to_date("fec_alta", "yyyy-MM-dd"))
        .withColumn("score_buro", F.col("score_buro").cast(IntegerType()))
        .withColumn("cod_segmento", F.upper(F.trim("cod_segmento")))
        .withColumn("estado_cli",   F.upper(F.trim("estado_cli")))
        .withColumn("tip_doc",      F.upper(F.trim("tip_doc")))
    )

    # 4. Validar rango de fechas
    df_valid = df_valid.filter(
        F.col("fec_nac").isNull() |
        (F.col("fec_nac").between(F.lit(str(DATE_MIN)), F.lit(str(DATE_MAX))))
    )

    # 5. Enmascarar PII con SHA-256
    for col_name in PII_COLUMNS.get(table, []):
        if col_name in df_valid.columns:
            df_valid = df_valid.withColumn(col_name, sha256_spark(F.col(col_name)))

    # 6. Imputar nulos no críticos
    df_valid = (
        df_valid
        .fillna({"score_buro": 0, "ciudad_res": "DESCONOCIDO",
                 "canal_adquis": "NO_INFORMADO"})
    )

    null_pct  = compute_null_pct(df_valid)
    n_output  = write_silver(df_valid, table, "id_cli", batch_id)
    n_rejected = n_input - df_valid.count()
    duration   = (datetime.utcnow() - t_start).total_seconds()
    log_quality_report(table, batch_id, n_input, n_output, n_rejected,
                       null_pct, duration)
    return {"table": table, "status": "SUCCESS", "records": n_output}


def process_productos(batch_id: str) -> dict:
    t_start = datetime.utcnow()
    table   = "TB_PRODUCTOS_CAT"
    log.info(f"Procesando Silver: {table}")

    df = read_bronze(table)
    n_input = df.count()

    df = df.dropDuplicates(["cod_prod"])
    df_valid = df.filter(
        F.col("cod_prod").isNotNull() & F.col("tip_prod").isNotNull()
    )
    df_invalid = df.subtract(df_valid)
    send_to_error_table(df_invalid, table, "CAMPO_OBLIGATORIO_NULO", batch_id)

    df_valid = (
        df_valid
        .withColumn("tasa_ea",         F.col("tasa_ea").cast(DoubleType()))
        .withColumn("plazo_max_meses",  F.col("plazo_max_meses").cast(IntegerType()))
        .withColumn("cuota_min",        F.col("cuota_min").cast(DoubleType()))
        .withColumn("comision_admin",   F.col("comision_admin").cast(DoubleType()))
        .withColumn("tip_prod",         F.upper(F.trim("tip_prod")))
        .withColumn("estado_prod",      F.upper(F.trim("estado_prod")))
        .fillna({"cuota_min": 0.0, "comision_admin": 0.0})
    )

    null_pct   = compute_null_pct(df_valid)
    n_output   = write_silver(df_valid, table, "cod_prod", batch_id)
    n_rejected = n_input - df_valid.count()
    duration   = (datetime.utcnow() - t_start).total_seconds()
    log_quality_report(table, batch_id, n_input, n_output, n_rejected,
                       null_pct, duration)
    return {"table": table, "status": "SUCCESS", "records": n_output}


def process_sucursales(batch_id: str) -> dict:
    t_start = datetime.utcnow()
    table   = "TB_SUCURSALES_RED"
    log.info(f"Procesando Silver: {table}")

    df = read_bronze(table)
    n_input = df.count()

    df = df.dropDuplicates(["cod_suc"])
    df_valid = df.filter(F.col("cod_suc").isNotNull() & F.col("ciudad").isNotNull())
    df_invalid = df.subtract(df_valid)
    send_to_error_table(df_invalid, table, "CAMPO_OBLIGATORIO_NULO", batch_id)

    df_valid = (
        df_valid
        .withColumn("latitud",   F.col("latitud").cast(DoubleType()))
        .withColumn("longitud",  F.col("longitud").cast(DoubleType()))
        .withColumn("activo",    F.col("activo").cast(IntegerType()))
        .withColumn("ciudad",    F.upper(F.trim("ciudad")))
        .withColumn("depto",     F.upper(F.trim("depto")))
        .withColumn("tip_punto", F.upper(F.trim("tip_punto")))
        .fillna({"activo": 0})
    )

    null_pct   = compute_null_pct(df_valid)
    n_output   = write_silver(df_valid, table, "cod_suc", batch_id)
    n_rejected = n_input - df_valid.count()
    duration   = (datetime.utcnow() - t_start).total_seconds()
    log_quality_report(table, batch_id, n_input, n_output, n_rejected,
                       null_pct, duration)
    return {"table": table, "status": "SUCCESS", "records": n_output}


def process_obligaciones(batch_id: str, clientes_ids: list,
                         productos_ids: list) -> dict:
    t_start = datetime.utcnow()
    table   = "TB_OBLIGACIONES"
    log.info(f"Procesando Silver: {table}")

    df = read_bronze(table)
    n_input = df.count()

    # 1. Duplicados
    df = df.dropDuplicates(["id_oblig"])

    # 2. Campos obligatorios nulos
    df_valid = df.filter(
        F.col("id_oblig").isNotNull() &
        F.col("id_cli").isNotNull() &
        F.col("cod_prod").isNotNull() &
        F.col("vr_desembolsado").isNotNull()
    )
    df_invalid = df.subtract(df_valid)
    send_to_error_table(df_invalid, table, "CAMPO_OBLIGATORIO_NULO", batch_id)

    # 3. Integridad referencial — id_cli debe existir en TB_CLIENTES_CORE
    cli_df  = spark.createDataFrame([(c,) for c in clientes_ids], ["id_cli_ref"])
    prod_df = spark.createDataFrame([(p,) for p in productos_ids], ["cod_prod_ref"])

    df_ri_valid = df_valid.join(
        cli_df, df_valid["id_cli"] == cli_df["id_cli_ref"], "inner"
    ).drop("id_cli_ref")

    df_ri_invalid = df_valid.join(
        cli_df, df_valid["id_cli"] == cli_df["id_cli_ref"], "left_anti"
    )
    send_to_error_table(df_ri_invalid, table, "ID_CLI_NO_EXISTE_EN_DIMENSION", batch_id)

    # 4. Tipos de datos
    df_valid = (
        df_ri_valid
        .withColumn("vr_aprobado",    F.col("vr_aprobado").cast(DoubleType()))
        .withColumn("vr_desembolsado",F.col("vr_desembolsado").cast(DoubleType()))
        .withColumn("sdo_capital",    F.col("sdo_capital").cast(DoubleType()))
        .withColumn("vr_cuota",       F.col("vr_cuota").cast(DoubleType()))
        .withColumn("dias_mora_act",  F.col("dias_mora_act").cast(IntegerType()))
        .withColumn("fec_desembolso", F.to_date("fec_desembolso", "yyyy-MM-dd"))
        .withColumn("fec_venc",       F.to_date("fec_venc", "yyyy-MM-dd"))
        # Anomalía: dias_mora negativos → corregir a 0
        .withColumn("dias_mora_act",
                    F.when(F.col("dias_mora_act") < 0, F.lit(0))
                     .otherwise(F.col("dias_mora_act")))
        .fillna({"num_cuotas_pend": 0, "calif_riesgo": "A"})
    )

    null_pct   = compute_null_pct(df_valid)
    n_output   = write_silver(df_valid, table, "id_oblig", batch_id)
    n_rejected = n_input - df_valid.count()
    duration   = (datetime.utcnow() - t_start).total_seconds()
    log_quality_report(table, batch_id, n_input, n_output, n_rejected,
                       null_pct, duration)
    return {"table": table, "status": "SUCCESS", "records": n_output}


def process_movimientos(batch_id: str, clientes_ids: list,
                        productos_ids: list) -> dict:
    t_start = datetime.utcnow()
    table   = "TB_MOV_FINANCIEROS"
    log.info(f"Procesando Silver: {table}")

    df = read_bronze(table)
    n_input = df.count()

    # 1. Duplicados exactos (id_mov + fec_mov + vr_mov)
    df = df.dropDuplicates(["id_mov", "fec_mov", "vr_mov"])

    # 2. Campos obligatorios nulos
    df_valid = df.filter(
        F.col("id_mov").isNotNull() &
        F.col("id_cli").isNotNull() &
        F.col("vr_mov").isNotNull() &
        F.col("fec_mov").isNotNull()
    )
    df_invalid = df.subtract(df_valid)
    send_to_error_table(df_invalid, table, "CAMPO_OBLIGATORIO_NULO", batch_id)

    # 3. Integridad referencial
    cli_df = spark.createDataFrame([(c,) for c in clientes_ids], ["id_cli_ref"])
    df_ri_valid   = df_valid.join(cli_df, df_valid["id_cli"] == cli_df["id_cli_ref"], "inner").drop("id_cli_ref")
    df_ri_invalid = df_valid.join(cli_df, df_valid["id_cli"] == cli_df["id_cli_ref"], "left_anti")
    send_to_error_table(df_ri_invalid, table, "ID_CLI_NO_EXISTE_EN_DIMENSION", batch_id)

    # 4. Tipos y validación de fechas
    df_valid = (
        df_ri_valid
        .withColumn("fec_mov",  F.to_date("fec_mov", "yyyy-MM-dd"))
        .withColumn("vr_mov",   F.col("vr_mov").cast(DoubleType()))
        .withColumn("cod_canal",F.upper(F.trim("cod_canal")))
        .withColumn("tip_mov",  F.upper(F.trim("tip_mov")))
        .fillna({"cod_ciudad": "DESCONOCIDO", "id_dispositivo": "DESCONOCIDO"})
    )

    # Filtrar fechas fuera de rango (anomalía documentada)
    df_oor = df_valid.filter(
        (F.col("fec_mov") < F.lit(str(DATE_MIN))) |
        (F.col("fec_mov") > F.lit(str(DATE_MAX)))
    )
    send_to_error_table(df_oor, table, "FECHA_FUERA_DE_RANGO", batch_id)
    df_valid = df_valid.filter(
        F.col("fec_mov").between(F.lit(str(DATE_MIN)), F.lit(str(DATE_MAX)))
    )

    # 5. Calcular ind_sospechoso — REGLA DE NEGOCIO CRÍTICA
    # Una transacción es sospechosa cuando su vr_mov supera en más de
    # 3 desviaciones estándar el promedio de los últimos 30 días del cliente.
    window_30d = (
        Window.partitionBy("id_cli")
        .orderBy(F.col("fec_mov").cast("long"))
        .rangeBetween(-30 * 86400, -1)  # 30 días en segundos, excluyendo el día actual
    )

    df_valid = (
        df_valid
        .withColumn("_avg_30d",  F.avg("vr_mov").over(window_30d))
        .withColumn("_std_30d",  F.stddev("vr_mov").over(window_30d))
        .withColumn("ind_sospechoso",
                    F.when(
                        F.col("_avg_30d").isNotNull() &
                        F.col("_std_30d").isNotNull() &
                        (F.col("vr_mov") > F.col("_avg_30d") + 3 * F.col("_std_30d")),
                        F.lit(1)
                    ).otherwise(F.lit(0))
        )
        .drop("_avg_30d", "_std_30d")
    )

    # 6. Flag de horario hábil (lunes-viernes 6:00-20:00)
    df_valid = df_valid.withColumn(
        "ind_horario_habil",
        F.when(
            (F.dayofweek("fec_mov").isin([2, 3, 4, 5, 6])) &  # lun-vie (Spark: 1=Dom)
            (F.hour(F.to_timestamp("hra_mov", "HH:mm:ss")).between(6, 19)),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    null_pct   = compute_null_pct(df_valid)
    n_output   = write_silver(df_valid, table, "id_mov", batch_id)
    n_rejected = n_input - df_valid.count()
    duration   = (datetime.utcnow() - t_start).total_seconds()
    log_quality_report(table, batch_id, n_input, n_output, n_rejected,
                       null_pct, duration)
    return {"table": table, "status": "SUCCESS", "records": n_output}


def process_comisiones(batch_id: str, clientes_ids: list,
                       productos_ids: list) -> dict:
    t_start = datetime.utcnow()
    table   = "TB_COMISIONES_LOG"
    log.info(f"Procesando Silver: {table}")

    df = read_bronze(table)
    n_input = df.count()

    df = df.dropDuplicates(["id_comision"])
    df_valid = df.filter(
        F.col("id_comision").isNotNull() &
        F.col("id_cli").isNotNull() &
        F.col("vr_comision").isNotNull()
    )
    df_invalid = df.subtract(df_valid)
    send_to_error_table(df_invalid, table, "CAMPO_OBLIGATORIO_NULO", batch_id)

    cli_df = spark.createDataFrame([(c,) for c in clientes_ids], ["id_cli_ref"])
    df_ri_valid   = df_valid.join(cli_df, df_valid["id_cli"] == cli_df["id_cli_ref"], "inner").drop("id_cli_ref")
    df_ri_invalid = df_valid.join(cli_df, df_valid["id_cli"] == cli_df["id_cli_ref"], "left_anti")
    send_to_error_table(df_ri_invalid, table, "ID_CLI_NO_EXISTE_EN_DIMENSION", batch_id)

    df_valid = (
        df_ri_valid
        .withColumn("fec_cobro",    F.to_date("fec_cobro", "yyyy-MM-dd"))
        .withColumn("vr_comision",  F.col("vr_comision").cast(DoubleType()))
        .withColumn("tip_comision", F.upper(F.trim(F.coalesce("tip_comision", F.lit("NO_INFORMADO")))))
        .withColumn("estado_cobro", F.upper(F.trim(F.coalesce("estado_cobro", F.lit("PENDIENTE")))))
    )

    null_pct   = compute_null_pct(df_valid)
    n_output   = write_silver(df_valid, table, "id_comision", batch_id)
    n_rejected = n_input - df_valid.count()
    duration   = (datetime.utcnow() - t_start).total_seconds()
    log_quality_report(table, batch_id, n_input, n_output, n_rejected,
                       null_pct, duration)
    return {"table": table, "status": "SUCCESS", "records": n_output}


# ─────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────

# COMMAND ----------

# Leer batch_id desde el notebook de Bronze (pasado por el orquestador)
try:
    bronze_output = json.loads(dbutils.widgets.get("bronze_output"))
    batch_id = bronze_output.get("batch_id", datetime.utcnow().strftime("BATCH_%Y%m%d_%H%M%S"))
except Exception:
    batch_id = datetime.utcnow().strftime("BATCH_%Y%m%d_%H%M%S")

log.info("=" * 60)
log.info(f"FinBank S.A. — Procesamiento Silver | Batch: {batch_id}")
log.info("=" * 60)

# Cargar IDs de dimensiones para validación de integridad referencial
clientes_ids = [
    r["id_cli"] for r in
    spark.read.format("delta").load(f"{BRONZE_ROOT}/TB_CLIENTES_CORE")
    .select("id_cli").distinct().collect()
]
productos_ids = [
    r["cod_prod"] for r in
    spark.read.format("delta").load(f"{BRONZE_ROOT}/TB_PRODUCTOS_CAT")
    .select("cod_prod").distinct().collect()
]

log.info(f"Dimensiones cargadas: {len(clientes_ids):,} clientes | {len(productos_ids)} productos")

# COMMAND ----------

results = []
failed  = []

for fn, kwargs in [
    (process_clientes,    {"batch_id": batch_id}),
    (process_productos,   {"batch_id": batch_id}),
    (process_sucursales,  {"batch_id": batch_id}),
    (process_obligaciones,{"batch_id": batch_id, "clientes_ids": clientes_ids, "productos_ids": productos_ids}),
    (process_movimientos, {"batch_id": batch_id, "clientes_ids": clientes_ids, "productos_ids": productos_ids}),
    (process_comisiones,  {"batch_id": batch_id, "clientes_ids": clientes_ids, "productos_ids": productos_ids}),
]:
    try:
        r = fn(**kwargs)
        results.append(r)
    except Exception as e:
        log.error(f"ERROR en {fn.__name__}: {e}")
        failed.append(fn.__name__)
        results.append({"table": fn.__name__, "status": "FAILED", "error": str(e)})

# COMMAND ----------

log.info("\n" + "=" * 60)
log.info("RESUMEN DE PROCESAMIENTO SILVER")
log.info("=" * 60)
total = 0
for r in results:
    log.info(f"  {r.get('table','?'):<25}: {r.get('status','?'):<10} | {r.get('records',0):>10,} registros")
    total += r.get("records", 0)
log.info(f"\n  Total registros conformes en Silver: {total:,}")

if failed:
    raise Exception(f"Silver completado con errores en: {failed}")

log.info("Procesamiento Silver completado exitosamente.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "batch_id": batch_id,
    "total_records_silver": total,
    "tables": results,
    "status": "SUCCESS" if not failed else "PARTIAL_FAILURE",
}))
