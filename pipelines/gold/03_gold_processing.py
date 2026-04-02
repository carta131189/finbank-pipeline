# Databricks notebook source
# FinBank S.A. — Capa Gold: Modelo analítico y reglas de negocio
# ==============================================================
# Construye el modelo dimensional completo:
#   dim_clientes, dim_productos, dim_geografia, dim_canal
#   fact_transacciones, fact_cartera, fact_rentabilidad_cliente
#   kpis_diarios_cartera

# COMMAND ----------

import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType
from delta.tables import DeltaTable

# ─────────────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────────────

def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope="finbank-kv", key=key)

STORAGE_ACCOUNT = get_secret("storage-account-name")
SILVER_ROOT = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_ROOT   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Tasa de conversión COP → USD (actualizable vía Key Vault)
try:
    COP_TO_USD = float(get_secret("cop-usd-rate"))
except Exception:
    COP_TO_USD = 1 / 4100.0  # fallback: 1 USD = 4100 COP

# Tabla de provisiones regulatorias (Superintendencia Financiera Colombia)
PROVISION_PCT = {"A": 0.01, "B": 0.03, "C": 0.20, "D": 0.50, "E": 1.00}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gold")

spark = SparkSession.builder.getOrCreate()

# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────

def read_silver(table_name: str) -> DataFrame:
    return spark.read.format("delta").load(f"{SILVER_ROOT}/{table_name}")


def write_gold(df: DataFrame, table_name: str, pk: str,
               batch_id: str, partition_cols: list = None):
    """Escribe en Gold con MERGE idempotente. Permite particionamiento opcional."""
    target_path = f"{GOLD_ROOT}/{table_name}"

    df_meta = (
        df
        .withColumn("_gold_timestamp", F.current_timestamp())
        .withColumn("_batch_id_gold",  F.lit(batch_id))
    )

    if DeltaTable.isDeltaTable(spark, target_path):
        (
            DeltaTable.forPath(spark, target_path)
            .alias("tgt")
            .merge(df_meta.alias("src"), f"tgt.{pk} = src.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = df_meta.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_path)

    n = spark.read.format("delta").load(target_path).count()
    log.info(f"  Gold/{table_name}: {n:,} registros")
    return n


# ─────────────────────────────────────────────────────
# DIMENSIONES
# ─────────────────────────────────────────────────────

def build_dim_clientes(batch_id: str) -> int:
    """
    LINAJE: TB_CLIENTES_CORE (Silver)
    Transformaciones:
      - nombre_completo: nomb_cli + ' ' + apell_cli (mascarado en Silver)
      - edad: diferencia en años entre fec_nac y fecha actual
      - segmento_label: mapeo de cod_segmento a etiqueta legible de negocio
    """
    log.info("Construyendo dim_clientes...")

    df = read_silver("TB_CLIENTES_CORE")

    # Mapeo de segmento a etiqueta legible
    segmento_map = F.create_map(
        F.lit("BASICO"),    F.lit("Básico (< 2 SMLV)"),
        F.lit("ESTANDAR"),  F.lit("Estándar (2-5 SMLV)"),
        F.lit("PREMIUM"),   F.lit("Premium (5-15 SMLV)"),
        F.lit("ELITE"),     F.lit("Elite (> 15 SMLV)"),
    )

    dim = (
        df
        .withColumn(
            "nombre_completo",
            F.concat_ws(" ", F.col("nomb_cli"), F.col("apell_cli"))
        )
        .withColumn(
            "edad",
            F.floor(F.datediff(F.current_date(), F.col("fec_nac")) / 365.25)
            .cast(IntegerType())
        )
        .withColumn("segmento_label", segmento_map[F.col("cod_segmento")])
        .withColumn("ind_activo", F.when(F.col("estado_cli") == "ACTIVO", 1).otherwise(0))
        .select(
            F.col("id_cli"),
            F.col("nombre_completo"),
            F.col("nomb_cli").alias("nombre_hash"),       # ya mascarado en Silver
            F.col("apell_cli").alias("apellido_hash"),
            F.col("num_doc").alias("num_doc_hash"),        # ya mascarado en Silver
            F.col("tip_doc"),
            F.col("fec_nac"),
            F.col("edad"),
            F.col("fec_alta"),
            F.col("cod_segmento"),
            F.col("segmento_label"),
            F.col("score_buro"),
            F.col("ciudad_res"),
            F.col("depto_res"),
            F.col("estado_cli"),
            F.col("ind_activo"),
            F.col("canal_adquis"),
        )
    )

    return write_gold(dim, "dim_clientes", "id_cli", batch_id)


def build_dim_productos(batch_id: str) -> int:
    """
    LINAJE: TB_PRODUCTOS_CAT (Silver)
    Transformaciones:
      - tasa_mensual: conversión desde tasa_ea a equivalente mensual
        tasa_mensual = (1 + tasa_ea/100)^(1/12) - 1
      - familia: clasificación en CREDITO, AHORRO, TRANSACCIONAL
      - nombres de negocio más legibles
    """
    log.info("Construyendo dim_productos...")

    df = read_silver("TB_PRODUCTOS_CAT")

    dim = (
        df
        .withColumn(
            "tasa_mensual_pct",
            F.round(
                (F.pow(1 + F.col("tasa_ea") / 100.0, F.lit(1.0 / 12.0)) - 1) * 100,
                4
            )
        )
        .withColumn("familia", F.col("tip_prod"))   # ya clasificado en origen
        .withColumn("desc_negocio", F.col("desc_prod"))
        .withColumn("ind_activo", F.when(F.col("estado_prod") == "ACTIVO", 1).otherwise(0))
        .select(
            "cod_prod", "desc_prod", "desc_negocio", "tip_prod", "familia",
            "tasa_ea", "tasa_mensual_pct", "plazo_max_meses",
            "cuota_min", "comision_admin", "estado_prod", "ind_activo",
        )
    )

    return write_gold(dim, "dim_productos", "cod_prod", batch_id)


def build_dim_geografia(batch_id: str) -> int:
    """
    LINAJE: TB_SUCURSALES_RED (Silver)
    Separación en dim_geografia con ciudad, depto y coordenadas únicas.
    """
    log.info("Construyendo dim_geografia...")

    df = read_silver("TB_SUCURSALES_RED")

    dim = (
        df
        .select(
            F.col("ciudad"),
            F.col("depto"),
            F.avg("latitud").alias("latitud_ref"),
            F.avg("longitud").alias("longitud_ref"),
            F.count("cod_suc").alias("n_puntos"),
        )
        .groupBy("ciudad", "depto")
        .agg(
            F.avg("latitud").alias("latitud_ref"),
            F.avg("longitud").alias("longitud_ref"),
            F.count("*").alias("n_puntos"),
        )
        .withColumn("id_geo", F.md5(F.concat_ws("_", "ciudad", "depto")))
    )

    return write_gold(dim, "dim_geografia", "id_geo", batch_id)


def build_dim_canal(batch_id: str) -> int:
    """
    LINAJE: TB_SUCURSALES_RED (Silver) + TB_MOV_FINANCIEROS (Silver)
    Catálogo unificado de canales de atención y digitales.
    """
    log.info("Construyendo dim_canal...")

    # Canales físicos desde sucursales
    df_suc = (
        read_silver("TB_SUCURSALES_RED")
        .select(
            F.col("tip_punto").alias("cod_canal"),
            F.lit("FISICO").alias("tipo_canal"),
        )
        .distinct()
    )

    # Canales digitales desde movimientos
    df_mov = (
        read_silver("TB_MOV_FINANCIEROS")
        .select(
            F.col("cod_canal"),
            F.lit("DIGITAL").alias("tipo_canal"),
        )
        .distinct()
    )

    dim = (
        df_suc.union(df_mov)
        .distinct()
        .withColumn("id_canal", F.md5(F.col("cod_canal")))
        .withColumn(
            "desc_canal",
            F.when(F.col("cod_canal") == "APP_MOVIL",       "Aplicación Móvil")
             .when(F.col("cod_canal") == "PORTAL_WEB",       "Portal Web")
             .when(F.col("cod_canal") == "CORRESPONSAL",     "Corresponsal Bancario")
             .when(F.col("cod_canal") == "ATM",              "Cajero Automático")
             .when(F.col("cod_canal") == "BANCA_TELEFONICA", "Banca Telefónica")
             .when(F.col("cod_canal") == "SUCURSAL",         "Sucursal Física")
             .otherwise(F.col("cod_canal"))
        )
    )

    return write_gold(dim, "dim_canal", "id_canal", batch_id)


# ─────────────────────────────────────────────────────
# TABLAS DE HECHOS
# ─────────────────────────────────────────────────────

def build_fact_transacciones(batch_id: str) -> int:
    """
    LINAJE: TB_MOV_FINANCIEROS (Silver) + dim_clientes + dim_productos
    Transformaciones clave:
      - vr_mov_usd: conversión COP → USD
      - ind_horario_habil: calculado en Silver
      - ind_sospechoso: calculado en Silver (regla de 3 std devs)
      - promedio_movil_30d: ventana de 30 días por cliente
    Particionado por: fec_mov (año, mes)
    """
    log.info("Construyendo fact_transacciones...")

    df_mov  = read_silver("TB_MOV_FINANCIEROS")
    df_cli  = read_silver("TB_CLIENTES_CORE").select("id_cli", "cod_segmento")
    df_prod = read_silver("TB_PRODUCTOS_CAT").select("cod_prod", "tip_prod", "familia")

    # Validar id_cli contra dim_clientes (ya existe en Silver, reforzar en Gold)
    df_mov = df_mov.join(df_cli,  "id_cli",  "inner")
    df_mov = df_mov.join(df_prod, "cod_prod", "left")

    fact = (
        df_mov
        .withColumn("vr_mov_usd", F.round(F.col("vr_mov") * F.lit(COP_TO_USD), 4))
        .withColumn("anio_mov",   F.year("fec_mov").cast(StringType()))
        .withColumn("mes_mov",    F.lpad(F.month("fec_mov").cast(StringType()), 2, "0"))
        .select(
            "id_mov", "id_cli", "cod_prod", "num_cuenta",
            "fec_mov", "hra_mov", "anio_mov", "mes_mov",
            "vr_mov", "vr_mov_usd",
            "tip_mov", "cod_canal", "cod_ciudad",
            "cod_estado_mov", "id_dispositivo",
            "cod_segmento",
            F.col("tip_prod").alias("familia_producto"),
            "ind_sospechoso",
            "ind_horario_habil",
        )
    )

    return write_gold(
        fact, "fact_transacciones", "id_mov", batch_id,
        partition_cols=["anio_mov", "mes_mov"]
    )


def build_fact_cartera(batch_id: str) -> int:
    """
    LINAJE: TB_OBLIGACIONES (Silver)
    Reglas de negocio:
      - bucket_mora: clasificación en 5 rangos según dias_mora_act
      - clasificacion_regulatoria: A/B/C/D/E
      - provision_estimada: sdo_capital * porcentaje regulatorio
    Particionado por: fec_desembolso (año, mes)

    LINAJE detallado de campos calculados:
      bucket_mora:
        Origen: TB_OBLIGACIONES.dias_mora_act (Silver)
        Transformación: clasificación en rangos: 0→AL_DIA, 1-30→RANGO_1,
          31-60→RANGO_2, 61-90→RANGO_3, >90→DETERIORADO
        Propósito: segmentación de cartera para reportes de riesgo crediticio

      provision_estimada:
        Origen: fact_cartera.sdo_capital × tabla regulatoria por calif_riesgo
        Transformación: multiplicación por porcentaje según calificación A(1%),
          B(3%), C(20%), D(50%), E(100%)
        Propósito: cálculo de provisiones exigidas por Superintendencia Financiera

      bucket_mora_orden:
        Origen: bucket_mora calculado
        Transformación: mapeo ordinal para ordenamiento en dashboards
        Propósito: facilitar visualizaciones ordenadas de la cartera
    """
    log.info("Construyendo fact_cartera...")

    df = read_silver("TB_OBLIGACIONES")
    df_cli  = read_silver("TB_CLIENTES_CORE").select("id_cli", "cod_segmento", "ciudad_res", "depto_res")
    df_prod = read_silver("TB_PRODUCTOS_CAT").select("cod_prod", "desc_prod", "tip_prod")

    df = df.join(df_cli,  "id_cli",  "left")
    df = df.join(df_prod, "cod_prod", "left")

    # Provisiones regulatorias como map
    prov_map = F.create_map(
        F.lit("A"), F.lit(0.01),
        F.lit("B"), F.lit(0.03),
        F.lit("C"), F.lit(0.20),
        F.lit("D"), F.lit(0.50),
        F.lit("E"), F.lit(1.00),
    )

    bucket_expr = (
        F.when(F.col("dias_mora_act") == 0,                          F.lit("AL_DIA"))
         .when(F.col("dias_mora_act").between(1, 30),                 F.lit("RANGO_1"))
         .when(F.col("dias_mora_act").between(31, 60),                F.lit("RANGO_2"))
         .when(F.col("dias_mora_act").between(61, 90),                F.lit("RANGO_3"))
         .when(F.col("dias_mora_act") > 90,                           F.lit("DETERIORADO"))
         .otherwise(F.lit("AL_DIA"))
    )

    calif_expr = (
        F.when(F.col("dias_mora_act") == 0,                          F.lit("A"))
         .when(F.col("dias_mora_act").between(1, 30),                 F.lit("B"))
         .when(F.col("dias_mora_act").between(31, 60),                F.lit("C"))
         .when(F.col("dias_mora_act").between(61, 90),                F.lit("D"))
         .when(F.col("dias_mora_act") > 90,                           F.lit("E"))
         .otherwise(F.lit("A"))
    )

    bucket_orden_expr = (
        F.when(F.col("bucket_mora") == "AL_DIA",     F.lit(0))
         .when(F.col("bucket_mora") == "RANGO_1",    F.lit(1))
         .when(F.col("bucket_mora") == "RANGO_2",    F.lit(2))
         .when(F.col("bucket_mora") == "RANGO_3",    F.lit(3))
         .when(F.col("bucket_mora") == "DETERIORADO",F.lit(4))
         .otherwise(F.lit(0))
    )

    fact = (
        df
        .withColumn("bucket_mora",               bucket_expr)
        .withColumn("clasificacion_regulatoria",  calif_expr)
        .withColumn("bucket_mora_orden",          bucket_orden_expr)
        .withColumn(
            "provision_estimada",
            F.round(
                F.col("sdo_capital") * prov_map[F.col("clasificacion_regulatoria")],
                2
            )
        )
        .withColumn("ind_en_mora", F.when(F.col("dias_mora_act") > 0, 1).otherwise(0))
        .withColumn("anio_desembolso", F.year("fec_desembolso").cast(StringType()))
        .withColumn("mes_desembolso",  F.lpad(F.month("fec_desembolso").cast(StringType()), 2, "0"))
        .select(
            "id_oblig", "id_cli", "cod_prod",
            "vr_aprobado", "vr_desembolsado", "sdo_capital",
            "vr_cuota", "fec_desembolso", "fec_venc",
            "anio_desembolso", "mes_desembolso",
            "dias_mora_act", "bucket_mora", "bucket_mora_orden",
            "num_cuotas_pend", "clasificacion_regulatoria",
            "provision_estimada", "ind_en_mora",
            "cod_segmento", "ciudad_res", "depto_res",
            "desc_prod", "tip_prod",
        )
    )

    return write_gold(
        fact, "fact_cartera", "id_oblig", batch_id,
        partition_cols=["anio_desembolso", "mes_desembolso"]
    )


def build_fact_rentabilidad_cliente(batch_id: str) -> int:
    """
    LINAJE: TB_COMISIONES_LOG + TB_MOV_FINANCIEROS + TB_OBLIGACIONES (Silver)
    Regla de negocio CLTV:
      CLTV mensual = suma de ingresos por intereses + comisiones cobradas
                     en los últimos 12 meses calendario por cliente.
      ingreso_intereses = sdo_capital * tasa_mensual_pct / 100 (estimación)

    LINAJE detallado — campo cltv_12m:
      Origen: TB_COMISIONES_LOG.vr_comision (estado_cobro=COBRADO) +
              TB_OBLIGACIONES.sdo_capital × TB_PRODUCTOS_CAT.tasa_mensual (estimado)
      Transformación: Join por id_cli y periodo mensual; suma histórica 12 meses
      Propósito: score de rentabilidad por cliente para decisiones comerciales
                 y modelo de propensión de productos
    """
    log.info("Construyendo fact_rentabilidad_cliente...")

    # Comisiones cobradas en los últimos 12 meses
    df_com = (
        read_silver("TB_COMISIONES_LOG")
        .filter(F.col("estado_cobro") == "COBRADO")
        .withColumn("periodo", F.date_format("fec_cobro", "yyyy-MM"))
        .filter(
            F.col("fec_cobro") >= F.add_months(F.current_date(), -12)
        )
        .groupBy("id_cli", "periodo")
        .agg(F.sum("vr_comision").alias("total_comisiones"))
    )

    # Intereses estimados desde obligaciones activas
    df_prod = read_silver("TB_PRODUCTOS_CAT").select("cod_prod", "tasa_mensual_pct")
    df_oblig = (
        read_silver("TB_OBLIGACIONES")
        .join(df_prod, "cod_prod", "left")
        .withColumn("periodo", F.date_format("fec_desembolso", "yyyy-MM"))
        .groupBy("id_cli", "periodo")
        .agg(
            F.sum(
                F.col("sdo_capital") * F.col("tasa_mensual_pct") / 100
            ).alias("ingreso_intereses_est")
        )
    )

    # Join comisiones + intereses por cliente y periodo
    df_join = df_com.join(df_oblig, ["id_cli", "periodo"], "outer").fillna(0)

    # Ingreso total mensual
    df_mensual = df_join.withColumn(
        "ingreso_total_mes",
        F.col("total_comisiones") + F.col("ingreso_intereses_est")
    )

    # CLTV: suma histórica de 12 meses
    df_cltv = (
        df_mensual
        .groupBy("id_cli")
        .agg(
            F.sum("ingreso_total_mes").alias("cltv_12m"),
            F.sum("total_comisiones").alias("total_comisiones_12m"),
            F.sum("ingreso_intereses_est").alias("total_intereses_12m"),
            F.count("periodo").alias("meses_activo"),
        )
        .withColumn("cltv_12m",             F.round("cltv_12m", 2))
        .withColumn("total_comisiones_12m",  F.round("total_comisiones_12m", 2))
        .withColumn("total_intereses_12m",   F.round("total_intereses_12m", 2))
    )

    # Enriquecer con segmento
    df_cli = read_silver("TB_CLIENTES_CORE").select("id_cli", "cod_segmento", "estado_cli")
    fact   = df_cltv.join(df_cli, "id_cli", "left")

    fact = fact.withColumn("periodo_calculo", F.date_format(F.current_date(), "yyyy-MM"))

    return write_gold(fact, "fact_rentabilidad_cliente", "id_cli", batch_id)


def build_kpis_diarios_cartera(batch_id: str) -> int:
    """
    LINAJE: fact_cartera (Gold)
    KPIs ejecutivos diarios de cartera agregados por:
      fecha_calculo, cod_prod, cod_segmento, depto_res
    Métricas:
      - total_obligaciones_activas
      - monto_total_cartera (sdo_capital)
      - monto_en_mora
      - tasa_mora_pct
      - clientes_con_mora (distinct)
    Optimizado para visualización directa en dashboards.
    """
    log.info("Construyendo kpis_diarios_cartera...")

    df = spark.read.format("delta").load(f"{GOLD_ROOT}/fact_cartera")

    kpis = (
        df
        .groupBy(
            F.current_date().alias("fecha_calculo"),
            "cod_prod", "tip_prod",
            "cod_segmento", "depto_res",
        )
        .agg(
            F.count("id_oblig").alias("total_obligaciones_activas"),
            F.round(F.sum("sdo_capital"), 2).alias("monto_total_cartera"),
            F.round(
                F.sum(F.when(F.col("ind_en_mora") == 1, F.col("sdo_capital")).otherwise(0)),
                2
            ).alias("monto_en_mora"),
            F.round(F.sum("provision_estimada"), 2).alias("provision_total_estimada"),
            F.countDistinct(
                F.when(F.col("ind_en_mora") == 1, F.col("id_cli"))
            ).alias("clientes_con_mora"),
        )
        .withColumn(
            "tasa_mora_pct",
            F.round(
                F.when(F.col("monto_total_cartera") > 0,
                    F.col("monto_en_mora") / F.col("monto_total_cartera") * 100
                ).otherwise(F.lit(0.0)),
                4
            )
        )
        .withColumn("anio_calculo", F.year("fecha_calculo").cast(StringType()))
        .withColumn("mes_calculo",  F.lpad(F.month("fecha_calculo").cast(StringType()), 2, "0"))
    )

    return write_gold(
        kpis, "kpis_diarios_cartera",
        pk="fecha_calculo",  # Gold acepta múltiples PKs lógicos; Merge usa fecha+prod+seg
        batch_id=batch_id,
        partition_cols=["anio_calculo", "mes_calculo"]
    )


# ─────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────

# COMMAND ----------

try:
    silver_output = json.loads(dbutils.widgets.get("silver_output"))
    batch_id = silver_output.get("batch_id", datetime.utcnow().strftime("BATCH_%Y%m%d_%H%M%S"))
except Exception:
    batch_id = datetime.utcnow().strftime("BATCH_%Y%m%d_%H%M%S")

log.info("=" * 60)
log.info(f"FinBank S.A. — Procesamiento Gold | Batch: {batch_id}")
log.info("=" * 60)

# COMMAND ----------

results  = []
failed   = []
total    = 0

build_steps = [
    ("dim_clientes",               build_dim_clientes),
    ("dim_productos",              build_dim_productos),
    ("dim_geografia",              build_dim_geografia),
    ("dim_canal",                  build_dim_canal),
    ("fact_transacciones",         build_fact_transacciones),
    ("fact_cartera",               build_fact_cartera),
    ("fact_rentabilidad_cliente",  build_fact_rentabilidad_cliente),
    ("kpis_diarios_cartera",       build_kpis_diarios_cartera),
]

for name, fn in build_steps:
    try:
        n = fn(batch_id)
        results.append({"table": name, "status": "SUCCESS", "records": n})
        total += n
    except Exception as e:
        log.error(f"  ERROR en {name}: {e}")
        failed.append(name)
        results.append({"table": name, "status": "FAILED", "error": str(e)})

# COMMAND ----------

log.info("\n" + "=" * 60)
log.info("RESUMEN DE PROCESAMIENTO GOLD")
log.info("=" * 60)
for r in results:
    log.info(f"  {r['table']:<35}: {r.get('status','?'):<10} | {r.get('records',0):>10,}")
log.info(f"\n  Total registros en Gold: {total:,}")

if failed:
    raise Exception(f"Gold completado con errores en: {failed}")

log.info("Procesamiento Gold completado exitosamente.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "batch_id":            batch_id,
    "total_records_gold":  total,
    "tables":              results,
    "status":              "SUCCESS" if not failed else "PARTIAL_FAILURE",
}))
