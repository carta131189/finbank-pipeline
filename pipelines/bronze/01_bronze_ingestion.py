# Databricks notebook source
# FinBank S.A. — Capa Bronze: Ingesta de datos crudos
# =====================================================
# Lee las 6 tablas desde Azure SQL Database y las escribe
# en ADLS Gen2 en formato Delta Lake con particionamiento
# Soporta modo completo (primera ejecución) e incremental.

# COMMAND ----------
# %pip install great-expectations==0.18.12
# Reiniciar kernel tras instalación en Databricks

# COMMAND ----------

import json
import logging
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from delta.tables import DeltaTable

# ─────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────

# En Databricks estos valores se leen desde dbutils.secrets
# que apunta al Azure Key Vault vinculado al workspace.
# Se usa un scope llamado "finbank-kv".

def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope="finbank-kv", key=key)

SQL_SERVER   = get_secret("sql-server-fqdn")
SQL_DATABASE = get_secret("sql-database-name")
SQL_USER     = get_secret("sql-admin-user")
SQL_PASSWORD = get_secret("sql-admin-password")

STORAGE_ACCOUNT = get_secret("storage-account-name")
BRONZE_ROOT     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ERRORS_ROOT     = f"abfss://errors@{STORAGE_ACCOUNT}.dfs.core.windows.net"

SOURCE_SYSTEM = "AzureSQL_FinBankCore"

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER}:1433;"
    f"database={SQL_DATABASE};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"loginTimeout=30;"
)

JDBC_PROPS = {
    "user":     SQL_USER,
    "password": SQL_PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# Tablas a ingestar con su clave primaria (para modo incremental)
# y columna de marca de tiempo para detectar cambios.
TABLES_CONFIG = {
    "TB_CLIENTES_CORE":   {"pk": "id_cli",      "watermark_col": "fec_alta"},
    "TB_PRODUCTOS_CAT":   {"pk": "cod_prod",     "watermark_col": None},
    "TB_SUCURSALES_RED":  {"pk": "cod_suc",      "watermark_col": None},
    "TB_OBLIGACIONES":    {"pk": "id_oblig",     "watermark_col": "fec_desembolso"},
    "TB_MOV_FINANCIEROS": {"pk": "id_mov",       "watermark_col": "fec_mov"},
    "TB_COMISIONES_LOG":  {"pk": "id_comision",  "watermark_col": "fec_cobro"},
}

# Tabla de control de watermarks (estado del último batch)
WATERMARK_TABLE_PATH = f"{BRONZE_ROOT}/_control/watermarks"
LOG_TABLE_PATH       = f"{BRONZE_ROOT}/_control/ingestion_log"

# ─────────────────────────────────────────────────────
# Spark Session — configurar acceso a ADLS Gen2 via
# Managed Identity del cluster Databricks
# ─────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()

spark.conf.set(
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
)
# MSI sin client_id usa la Managed Identity del cluster

# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bronze")


def get_batch_id() -> str:
    return datetime.utcnow().strftime("BATCH_%Y%m%d_%H%M%S")


def add_audit_columns(df: DataFrame, batch_id: str) -> DataFrame:
    """Agrega las tres columnas de auditoría obligatorias en Bronze."""
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system",       F.lit(SOURCE_SYSTEM))
        .withColumn("_batch_id",            F.lit(batch_id))
    )


def get_partition_columns() -> list:
    """Columnas de partición: año/mes/día de ingesta."""
    return ["_year", "_month", "_day"]


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Deriva columnas de particionamiento desde el timestamp de ingesta."""
    return (
        df
        .withColumn("_year",  F.year(F.col("_ingestion_timestamp")).cast(StringType()))
        .withColumn("_month", F.lpad(F.month(F.col("_ingestion_timestamp")).cast(StringType()), 2, "0"))
        .withColumn("_day",   F.lpad(F.dayofmonth(F.col("_ingestion_timestamp")).cast(StringType()), 2, "0"))
    )


def read_watermark(table_name: str) -> str | None:
    """Lee el último watermark registrado para una tabla."""
    try:
        df = spark.read.format("delta").load(WATERMARK_TABLE_PATH)
        row = df.filter(F.col("table_name") == table_name).orderBy(
            F.col("updated_at").desc()
        ).limit(1).collect()
        return row[0]["last_watermark"] if row else None
    except Exception:
        return None


def write_watermark(table_name: str, watermark_value: str, batch_id: str):
    """Persiste el watermark del último batch exitoso."""
    schema = "table_name STRING, last_watermark STRING, batch_id STRING, updated_at TIMESTAMP"
    row_df = spark.createDataFrame(
        [(table_name, watermark_value, batch_id, datetime.utcnow())],
        schema=schema
    )
    if DeltaTable.isDeltaTable(spark, WATERMARK_TABLE_PATH):
        (
            DeltaTable.forPath(spark, WATERMARK_TABLE_PATH)
            .alias("wm")
            .merge(row_df.alias("new"), "wm.table_name = new.table_name")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        row_df.write.format("delta").mode("overwrite").save(WATERMARK_TABLE_PATH)


def log_ingestion(table_name: str, batch_id: str, n_records: int,
                  file_size_mb: float, duration_sec: float, status: str,
                  error_msg: str = ""):
    """Registra el log de cada ejecución de ingesta."""
    schema = (
        "table_name STRING, batch_id STRING, n_records LONG, "
        "file_size_mb DOUBLE, duration_sec DOUBLE, status STRING, "
        "error_msg STRING, logged_at TIMESTAMP"
    )
    row_df = spark.createDataFrame(
        [(table_name, batch_id, n_records, file_size_mb,
          duration_sec, status, error_msg, datetime.utcnow())],
        schema=schema
    )
    row_df.write.format("delta").mode("append").save(LOG_TABLE_PATH)


# ─────────────────────────────────────────────────────
# Lectura desde Azure SQL
# ─────────────────────────────────────────────────────

def read_from_sql(table_name: str, watermark_col: str | None,
                  last_watermark: str | None) -> DataFrame:
    """
    Lee una tabla desde Azure SQL.
    Si hay watermark y columna de marca de tiempo, aplica filtro incremental.
    """
    if watermark_col and last_watermark:
        query = (
            f"(SELECT * FROM dbo.{table_name} "
            f"WHERE {watermark_col} > '{last_watermark}') AS t"
        )
        log.info(f"  Modo incremental: {table_name} WHERE {watermark_col} > {last_watermark}")
    else:
        query = f"(SELECT * FROM dbo.{table_name}) AS t"
        log.info(f"  Modo completo: {table_name}")

    return (
        spark.read
        .format("jdbc")
        .option("url",        JDBC_URL)
        .option("dbtable",    query)
        .option("user",       SQL_USER)
        .option("password",   SQL_PASSWORD)
        .option("driver",     JDBC_PROPS["driver"])
        .option("numPartitions", 8)           # paralelismo de lectura
        .option("fetchsize",  10000)
        .load()
    )


# ─────────────────────────────────────────────────────
# Escritura en Bronze (Delta Lake)
# ─────────────────────────────────────────────────────

def write_to_bronze(df: DataFrame, table_name: str, pk: str,
                    incremental: bool) -> int:
    """
    Escribe en Bronze usando merge para garantizar idempotencia.
    Particionado por _year/_month/_day.
    """
    target_path = f"{BRONZE_ROOT}/{table_name}"

    if not incremental or not DeltaTable.isDeltaTable(spark, target_path):
        # Primera carga: overwrite completo
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(*get_partition_columns())
            .option("overwriteSchema", "true")
            .save(target_path)
        )
    else:
        # Cargas incrementales: MERGE para idempotencia
        delta_table = DeltaTable.forPath(spark, target_path)
        (
            delta_table.alias("tgt")
            .merge(
                df.alias("src"),
                f"tgt.{pk} = src.{pk}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    count = spark.read.format("delta").load(target_path).count()
    return count


# ─────────────────────────────────────────────────────
# Pipeline principal por tabla
# ─────────────────────────────────────────────────────

def ingest_table(table_name: str, config: dict, batch_id: str) -> dict:
    """Ejecuta el pipeline completo de ingesta para una tabla."""
    t_start = datetime.utcnow()
    pk             = config["pk"]
    watermark_col  = config["watermark_col"]
    last_watermark = read_watermark(table_name) if watermark_col else None
    incremental    = last_watermark is not None

    log.info(f"Iniciando ingesta: {table_name} | batch={batch_id} | incremental={incremental}")

    try:
        # 1. Leer desde SQL
        df_raw = read_from_sql(table_name, watermark_col, last_watermark)

        n_raw = df_raw.count()
        if n_raw == 0 and incremental:
            log.info(f"  {table_name}: sin registros nuevos desde último batch. Omitiendo.")
            log_ingestion(table_name, batch_id, 0, 0.0,
                          (datetime.utcnow() - t_start).total_seconds(),
                          "SKIPPED_NO_NEW_RECORDS")
            return {"table": table_name, "status": "SKIPPED", "records": 0}

        # 2. Agregar columnas de auditoría y partición
        df_audit = add_audit_columns(df_raw, batch_id)
        df_final = add_partition_columns(df_audit)

        # 3. Escribir en Bronze
        total_count = write_to_bronze(df_final, table_name, pk, incremental)

        # 4. Actualizar watermark
        if watermark_col:
            max_wm = (
                df_raw
                .agg(F.max(F.col(watermark_col)).cast(StringType()).alias("max_wm"))
                .collect()[0]["max_wm"]
            )
            if max_wm:
                write_watermark(table_name, max_wm, batch_id)

        duration = (datetime.utcnow() - t_start).total_seconds()

        # Estimar tamaño (aproximación: n_records * avg_row_bytes)
        file_size_mb = n_raw * 0.0005  # ~500 bytes por fila estimado

        log_ingestion(table_name, batch_id, n_raw, file_size_mb, duration, "SUCCESS")

        log.info(
            f"  {table_name}: {n_raw:,} registros nuevos | "
            f"{total_count:,} total en Bronze | {duration:.1f}s"
        )
        return {
            "table":    table_name,
            "status":   "SUCCESS",
            "records":  n_raw,
            "duration": duration,
        }

    except Exception as e:
        duration = (datetime.utcnow() - t_start).total_seconds()
        log.error(f"  ERROR en {table_name}: {e}")
        log_ingestion(table_name, batch_id, 0, 0.0, duration, "FAILED", str(e))

        # Registrar en tabla de errores del pipeline
        error_schema = "table_name STRING, batch_id STRING, error_msg STRING, failed_at TIMESTAMP"
        err_df = spark.createDataFrame(
            [(table_name, batch_id, str(e), datetime.utcnow())],
            schema=error_schema
        )
        err_df.write.format("delta").mode("append").save(
            f"{ERRORS_ROOT}/pipeline_errors"
        )

        return {"table": table_name, "status": "FAILED", "error": str(e)}


# ─────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────

# COMMAND ----------

batch_id = get_batch_id()
log.info("=" * 60)
log.info(f"FinBank S.A. — Ingesta Bronze | Batch: {batch_id}")
log.info("=" * 60)

results = []
for table_name, config in TABLES_CONFIG.items():
    result = ingest_table(table_name, config, batch_id)
    results.append(result)

# COMMAND ----------

# Resumen de ejecución
log.info("\n" + "=" * 60)
log.info("RESUMEN DE INGESTA BRONZE")
log.info("=" * 60)
total_records = 0
failed_tables = []
for r in results:
    status  = r["status"]
    records = r.get("records", 0)
    total_records += records
    log.info(f"  {r['table']:<25}: {status:<10} | {records:>10,} registros")
    if status == "FAILED":
        failed_tables.append(r["table"])

log.info(f"\n  Total registros procesados: {total_records:,}")

if failed_tables:
    raise Exception(
        f"Ingesta Bronze completada con errores en: {failed_tables}. "
        f"Revisar tabla de errores en {ERRORS_ROOT}/pipeline_errors"
    )

log.info("Ingesta Bronze completada exitosamente.")

# COMMAND ----------
# Retornar métricas para el orquestador (Databricks Workflows)
dbutils.notebook.exit(json.dumps({
    "batch_id":      batch_id,
    "total_records": total_records,
    "tables":        results,
    "status":        "SUCCESS" if not failed_tables else "PARTIAL_FAILURE",
}))
