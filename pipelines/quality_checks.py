# Databricks notebook source
# FinBank S.A. — Pruebas de Calidad de Datos
# ===============================================================
# Implementa mínimo 5 verificaciones automatizadas sobre las capas
# Silver y Gold. Se ejecuta como tarea final del pipeline.

# COMMAND ----------
# %pip install great-expectations==0.18.12

# COMMAND ----------

import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("quality_checks")

spark = SparkSession.builder.getOrCreate()

def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope="finbank-kv", key=key)

STORAGE_ACCOUNT = get_secret("storage-account-name")
SILVER_ROOT = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_ROOT   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ERRORS_ROOT = f"abfss://errors@{STORAGE_ACCOUNT}.dfs.core.windows.net"

QUALITY_RESULTS_PATH = f"{ERRORS_ROOT}/quality_check_results"

# ─────────────────────────────────────────────────────
# Framework de validaciones personalizado
# (compatible con Databricks sin necesidad de GE context)
# ─────────────────────────────────────────────────────

class Expectation:
    def __init__(self, name: str, table: str, layer: str):
        self.name  = name
        self.table = table
        self.layer = layer
        self.passed = False
        self.message = ""
        self.details = {}

    def mark_pass(self, details: dict = None):
        self.passed  = True
        self.message = "PASSED"
        self.details = details or {}

    def mark_fail(self, message: str, details: dict = None):
        self.passed  = False
        self.message = message
        self.details = details or {}


def run_check(expectation: Expectation) -> Expectation:
    status = "✅ PASS" if expectation.passed else "❌ FAIL"
    log.info(f"  [{expectation.layer}/{expectation.table}] {expectation.name}: {status}")
    if not expectation.passed:
        log.warning(f"    → {expectation.message}")
        if expectation.details:
            log.warning(f"    → Detalle: {expectation.details}")
    return expectation


# ─────────────────────────────────────────────────────
# VALIDACIONES (mínimo 5 requeridas)
# ─────────────────────────────────────────────────────

all_results = []

# ──────────────────────────────────────
# CHECK 1: No nulos en PKs críticas de Silver
# ──────────────────────────────────────
exp1 = Expectation("expect_pk_not_null", "TB_CLIENTES_CORE", "Silver")
try:
    df = spark.read.format("delta").load(f"{SILVER_ROOT}/TB_CLIENTES_CORE")
    null_count = df.filter(F.col("id_cli").isNull()).count()
    if null_count == 0:
        exp1.mark_pass({"null_pk_count": 0})
    else:
        exp1.mark_fail(f"{null_count} registros con id_cli nulo en Silver", {"null_count": null_count})
except Exception as e:
    exp1.mark_fail(str(e))
all_results.append(run_check(exp1))

# ──────────────────────────────────────
# CHECK 2: No duplicados en dim_clientes (Gold)
# ──────────────────────────────────────
exp2 = Expectation("expect_no_duplicate_pk", "dim_clientes", "Gold")
try:
    df = spark.read.format("delta").load(f"{GOLD_ROOT}/dim_clientes")
    total     = df.count()
    distinct  = df.select("id_cli").distinct().count()
    dup_count = total - distinct
    if dup_count == 0:
        exp2.mark_pass({"total": total, "distinct": distinct})
    else:
        exp2.mark_fail(f"{dup_count} id_cli duplicados en dim_clientes",
                       {"total": total, "distinct": distinct, "duplicates": dup_count})
except Exception as e:
    exp2.mark_fail(str(e))
all_results.append(run_check(exp2))

# ──────────────────────────────────────
# CHECK 3: bucket_mora solo contiene valores válidos (Gold)
# ──────────────────────────────────────
exp3 = Expectation("expect_bucket_mora_valid_values", "fact_cartera", "Gold")
try:
    df = spark.read.format("delta").load(f"{GOLD_ROOT}/fact_cartera")
    valid_buckets = ["AL_DIA", "RANGO_1", "RANGO_2", "RANGO_3", "DETERIORADO"]
    invalid_count = df.filter(~F.col("bucket_mora").isin(valid_buckets)).count()
    if invalid_count == 0:
        exp3.mark_pass({"valid_values": valid_buckets})
    else:
        invalid_sample = (
            df.filter(~F.col("bucket_mora").isin(valid_buckets))
            .select("bucket_mora").distinct().limit(5).collect()
        )
        exp3.mark_fail(
            f"{invalid_count} registros con bucket_mora inválido",
            {"invalid_count": invalid_count, "sample": str(invalid_sample)}
        )
except Exception as e:
    exp3.mark_fail(str(e))
all_results.append(run_check(exp3))

# ──────────────────────────────────────
# CHECK 4: provision_estimada >= 0 y coherente con sdo_capital
# ──────────────────────────────────────
exp4 = Expectation("expect_provision_non_negative_and_consistent", "fact_cartera", "Gold")
try:
    df = spark.read.format("delta").load(f"{GOLD_ROOT}/fact_cartera")
    negatives = df.filter(F.col("provision_estimada") < 0).count()
    # Provisión nunca puede superar el saldo de capital
    exceed_sdo = df.filter(F.col("provision_estimada") > F.col("sdo_capital") * 1.001).count()
    if negatives == 0 and exceed_sdo == 0:
        exp4.mark_pass({"negatives": 0, "exceed_sdo": 0})
    else:
        exp4.mark_fail(
            f"provision_estimada inconsistente: {negatives} negativos, {exceed_sdo} exceden sdo_capital",
            {"negatives": negatives, "exceed_sdo": exceed_sdo}
        )
except Exception as e:
    exp4.mark_fail(str(e))
all_results.append(run_check(exp4))

# ──────────────────────────────────────
# CHECK 5: ind_sospechoso solo toma valores 0 o 1 (Silver)
# ──────────────────────────────────────
exp5 = Expectation("expect_ind_sospechoso_binary", "TB_MOV_FINANCIEROS", "Silver")
try:
    df = spark.read.format("delta").load(f"{SILVER_ROOT}/TB_MOV_FINANCIEROS")
    invalid = df.filter(~F.col("ind_sospechoso").isin([0, 1])).count()
    total_suspect = df.filter(F.col("ind_sospechoso") == 1).count()
    total = df.count()
    pct_suspect = round(total_suspect / total * 100, 4) if total > 0 else 0
    if invalid == 0:
        exp5.mark_pass({
            "invalid_values": 0,
            "transacciones_sospechosas": total_suspect,
            "pct_sospechoso": pct_suspect
        })
    else:
        exp5.mark_fail(f"{invalid} registros con ind_sospechoso inválido")
except Exception as e:
    exp5.mark_fail(str(e))
all_results.append(run_check(exp5))

# ──────────────────────────────────────
# CHECK 6: Volumen mínimo en cada capa Gold (anti-pipeline vacío)
# ──────────────────────────────────────
exp6 = Expectation("expect_minimum_row_count_gold", "ALL_GOLD_TABLES", "Gold")
try:
    gold_tables = [
        "dim_clientes", "dim_productos", "dim_geografia", "dim_canal",
        "fact_transacciones", "fact_cartera", "fact_rentabilidad_cliente",
        "kpis_diarios_cartera",
    ]
    counts = {}
    failed_tables = []
    for t in gold_tables:
        n = spark.read.format("delta").load(f"{GOLD_ROOT}/{t}").count()
        counts[t] = n
        if n == 0:
            failed_tables.append(t)
    if not failed_tables:
        exp6.mark_pass(counts)
    else:
        exp6.mark_fail(f"Tablas Gold vacías: {failed_tables}", counts)
except Exception as e:
    exp6.mark_fail(str(e))
all_results.append(run_check(exp6))

# ──────────────────────────────────────
# CHECK 7: Integridad referencial fact_cartera → dim_clientes
# ──────────────────────────────────────
exp7 = Expectation("expect_fact_cartera_cli_referential_integrity", "fact_cartera", "Gold")
try:
    df_fact = spark.read.format("delta").load(f"{GOLD_ROOT}/fact_cartera").select("id_cli")
    df_dim  = spark.read.format("delta").load(f"{GOLD_ROOT}/dim_clientes").select("id_cli")
    orphans = df_fact.join(df_dim, "id_cli", "left_anti").count()
    if orphans == 0:
        exp7.mark_pass({"orphan_records": 0})
    else:
        exp7.mark_fail(
            f"{orphans} registros en fact_cartera sin id_cli en dim_clientes",
            {"orphan_count": orphans}
        )
except Exception as e:
    exp7.mark_fail(str(e))
all_results.append(run_check(exp7))

# ──────────────────────────────────────
# CHECK 8: tasa_mora_pct entre 0% y 100% en kpis_diarios_cartera
# ──────────────────────────────────────
exp8 = Expectation("expect_tasa_mora_pct_in_range", "kpis_diarios_cartera", "Gold")
try:
    df = spark.read.format("delta").load(f"{GOLD_ROOT}/kpis_diarios_cartera")
    out_of_range = df.filter(
        (F.col("tasa_mora_pct") < 0) | (F.col("tasa_mora_pct") > 100)
    ).count()
    if out_of_range == 0:
        exp8.mark_pass({"out_of_range_count": 0})
    else:
        exp8.mark_fail(f"{out_of_range} registros con tasa_mora_pct fuera de [0,100]")
except Exception as e:
    exp8.mark_fail(str(e))
all_results.append(run_check(exp8))

# ─────────────────────────────────────────────────────
# Resumen y persistencia de resultados
# ─────────────────────────────────────────────────────

# COMMAND ----------

passed_count = sum(1 for r in all_results if r.passed)
failed_count = len(all_results) - passed_count

log.info("\n" + "=" * 60)
log.info("RESUMEN DE CALIDAD DE DATOS")
log.info("=" * 60)
log.info(f"  Total checks  : {len(all_results)}")
log.info(f"  Pasaron       : {passed_count}")
log.info(f"  Fallaron      : {failed_count}")

# Persistir resultados como Delta para auditoría
schema = (
    "check_name STRING, table_name STRING, layer STRING, "
    "passed BOOLEAN, message STRING, details STRING, "
    "executed_at TIMESTAMP"
)
rows = [
    (r.name, r.table, r.layer, r.passed, r.message,
     json.dumps(r.details), datetime.utcnow())
    for r in all_results
]
df_results = spark.createDataFrame(rows, schema=schema)
df_results.write.format("delta").mode("append").save(QUALITY_RESULTS_PATH)

# COMMAND ----------

# Alerta: si algún check falla, el orquestador recibe la señal
if failed_count > 0:
    failed_names = [r.name for r in all_results if not r.passed]
    log.warning(f"Checks fallidos: {failed_names}")
    # No lanzar excepción aquí — el orquestador decide si bloquear o solo alertar

dbutils.notebook.exit(json.dumps({
    "total_checks":  len(all_results),
    "passed":        passed_count,
    "failed":        failed_count,
    "failed_checks": [r.name for r in all_results if not r.passed],
    "status":        "ALL_PASSED" if failed_count == 0 else "CHECKS_FAILED",
}))
