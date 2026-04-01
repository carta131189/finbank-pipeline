"""
FinBank S.A. - Generador de Datos Sintéticos
=============================================
Genera datos sintéticos para las 6 tablas del sistema transaccional de FinBank.
Reproducible mediante semilla aleatoria fija definida en config.yaml.

Uso:
    pip install -r requirements.txt
    python generate_data.py
    python generate_data.py --config config.yaml --output ./output
"""

import os
import yaml
import random
import hashlib
import argparse
import logging
from datetime import datetime, timedelta, date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Carga de configuración
# ──────────────────────────────────────────────

def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ──────────────────────────────────────────────
# Catálogos internos
# ──────────────────────────────────────────────

DEPARTAMENTOS = {
    "Cundinamarca": ["Bogotá", "Soacha", "Facatativá", "Zipaquirá"],
    "Antioquia":    ["Medellín", "Bello", "Itagüí", "Envigado"],
    "Valle":        ["Cali", "Buenaventura", "Palmira", "Tuluá"],
    "Atlántico":    ["Barranquilla", "Soledad", "Malambo"],
    "Santander":    ["Bucaramanga", "Floridablanca", "Girón"],
    "Bolívar":      ["Cartagena", "Magangué"],
    "Nariño":       ["Pasto", "Ipiales", "Tumaco"],
    "Córdoba":      ["Montería", "Lorica"],
    "Huila":        ["Neiva", "Pitalito"],
    "Meta":         ["Villavicencio", "Acacías"],
}

CANALES = ["APP_MOVIL", "PORTAL_WEB", "CORRESPONSAL", "ATM", "BANCA_TELEFONICA"]
CANALES_PESOS = [0.45, 0.25, 0.15, 0.10, 0.05]

TIPOS_MOVIMIENTO = ["PAGO", "TRANSFERENCIA", "RECARGA", "AVANCE", "COMPRA", "RETIRO"]
TIPOS_MOV_PESOS  = [0.30, 0.25, 0.15, 0.10, 0.15, 0.05]

ESTADOS_MOV  = ["APROBADO", "RECHAZADO", "PENDIENTE", "REVERSADO"]
ESTADOS_PESOS = [0.88, 0.07, 0.03, 0.02]

TIPOS_DOC  = ["CC", "CE", "NIT", "PP"]
TIPOS_PESOS = [0.82, 0.10, 0.05, 0.03]

SEGMENTOS  = ["BASICO", "ESTANDAR", "PREMIUM", "ELITE"]
SEG_PESOS  = [0.40, 0.35, 0.18, 0.07]

CANALES_ADQUIS = ["APP_MOVIL", "REFERIDO", "CORRESPONSAL", "PORTAL_WEB", "FUERZA_VENTAS"]
CA_PESOS       = [0.40, 0.25, 0.15, 0.12, 0.08]

TIPOS_PROD  = ["CREDITO", "AHORRO", "TRANSACCIONAL"]
CALIF_RIESGO = ["A", "B", "C", "D", "E"]

TIPOS_COMISION = ["ADMINISTRACION", "DESEMBOLSO", "CONSULTA_BURO", "TRANSACCION", "INACTIVIDAD"]
TIPOS_COMISION_PESOS = [0.40, 0.20, 0.15, 0.15, 0.10]

TIPOS_PUNTO = ["SUCURSAL", "CORRESPONSAL", "ATM", "PUNTO_PAGO"]


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def random_date(start: date, end: date, rng: np.random.Generator) -> date:
    delta = (end - start).days
    return start + timedelta(days=int(rng.integers(0, delta + 1)))


def random_datetime_in_day(d: date, rng: np.random.Generator,
                           peak_hours: bool = True) -> datetime:
    """Genera un datetime con distribución por horarios pico (9-12h y 15-18h)."""
    if peak_hours and rng.random() < 0.65:
        # Horario hábil con concentración en picos
        if rng.random() < 0.5:
            hour = int(rng.integers(9, 12))
        else:
            hour = int(rng.integers(15, 18))
    else:
        hour = int(rng.integers(0, 24))
    minute = int(rng.integers(0, 60))
    second = int(rng.integers(0, 60))
    return datetime(d.year, d.month, d.day, hour, minute, second)


def apply_nulls(df: pd.DataFrame, non_critical_cols: list,
                null_rate: float, rng: np.random.Generator) -> pd.DataFrame:
    """Aplica null_rate% de nulos en columnas no críticas."""
    df = df.copy()
    n = len(df)
    for col in non_critical_cols:
        if col not in df.columns:
            continue
        mask = rng.random(n) < null_rate
        df.loc[mask, col] = None
    return df


def inject_duplicates(df: pd.DataFrame, n: int,
                      rng: np.random.Generator) -> pd.DataFrame:
    """Duplica n filas aleatorias para simular anomalía de duplicados."""
    idx = rng.choice(len(df), size=n, replace=False)
    duplicates = df.iloc[idx].copy()
    log.info(f"  [ANOMALÍA] Inyectados {n} registros duplicados")
    return pd.concat([df, duplicates], ignore_index=True)


# ──────────────────────────────────────────────
# Generadores por tabla
# ──────────────────────────────────────────────

def gen_clientes(n: int, start: date, end: date,
                 rng: np.random.Generator) -> pd.DataFrame:
    log.info(f"Generando TB_CLIENTES_CORE ({n:,} registros)...")

    deptos = list(DEPARTAMENTOS.keys())
    deptos_pesos = [0.35, 0.20, 0.12, 0.08, 0.07, 0.05, 0.04, 0.03, 0.03, 0.03]

    nombres = [
        "Carlos", "María", "Juan", "Luisa", "Andrés", "Paola", "Diego", "Ana",
        "Jorge", "Camila", "Felipe", "Valentina", "Sergio", "Natalia", "Alejandro",
        "Laura", "Ricardo", "Daniela", "Mauricio", "Juliana", "Cristian", "Melissa",
        "Héctor", "Sandra", "Miguel", "Patricia", "Óscar", "Claudia", "Iván", "Yolanda"
    ]
    apellidos = [
        "García", "Rodríguez", "Martínez", "López", "González", "Pérez", "Sánchez",
        "Ramírez", "Torres", "Flores", "Rivera", "Morales", "Jiménez", "Herrera",
        "Medina", "Vargas", "Castillo", "Ruiz", "Romero", "Díaz", "Vásquez", "Ortiz",
        "Gómez", "Restrepo", "Ospina", "Cardona", "Muñoz", "Salcedo", "Arango", "Nieto"
    ]

    records = []
    for i in range(n):
        depto = rng.choice(deptos, p=deptos_pesos)
        ciudad = rng.choice(DEPARTAMENTOS[depto])
        fec_nac_date = random_date(date(1960, 1, 1), date(2002, 12, 31), rng)
        fec_alta_date = random_date(start, end, rng)
        segmento = rng.choice(SEGMENTOS, p=SEG_PESOS)

        # Score buró correlacionado con segmento
        base_score = {"BASICO": 400, "ESTANDAR": 550, "PREMIUM": 700, "ELITE": 800}[segmento]
        score = int(np.clip(rng.normal(base_score, 80), 300, 950))

        records.append({
            "id_cli":        f"CLI{i+1:07d}",
            "nomb_cli":      rng.choice(nombres),
            "apell_cli":     f"{rng.choice(apellidos)} {rng.choice(apellidos)}",
            "tip_doc":       rng.choice(TIPOS_DOC, p=TIPOS_PESOS),
            "num_doc":       str(int(rng.integers(10_000_000, 1_200_000_000))),
            "fec_nac":       fec_nac_date.isoformat(),
            "fec_alta":      fec_alta_date.isoformat(),
            "cod_segmento":  segmento,
            "score_buro":    score,
            "ciudad_res":    ciudad,
            "depto_res":     depto,
            "estado_cli":    rng.choice(["ACTIVO", "INACTIVO", "BLOQUEADO"],
                                        p=[0.88, 0.09, 0.03]),
            "canal_adquis":  rng.choice(CANALES_ADQUIS, p=CA_PESOS),
        })

    df = pd.DataFrame(records)
    df = apply_nulls(df, ["score_buro", "ciudad_res", "canal_adquis"], 0.05, rng)
    log.info(f"  TB_CLIENTES_CORE: {len(df):,} filas OK")
    return df


def gen_productos(n: int, rng: np.random.Generator) -> pd.DataFrame:
    log.info(f"Generando TB_PRODUCTOS_CAT ({n} registros)...")

    nombres_credito = [
        "Crédito Libre Inversión Básico", "Crédito Libre Inversión Plus",
        "Crédito Rotativo Digital", "Tarjeta Digital Gold", "Tarjeta Digital Platinum",
        "Microcrédito Emprendedor", "Crédito Nómina", "Crédito Educativo",
        "Crédito Vehículo", "Crédito Vivienda Digital",
        "Crédito Pyme Digital", "Línea de Crédito Revolving",
        "Crédito de Temporada", "Crédito Garantía Solidaria",
        "Crédito Universitario",
    ]
    nombres_ahorro = [
        "Cuenta de Ahorros Digital Básica", "Cuenta de Ahorros Premium",
        "Cuenta de Ahorros Elite", "Cuenta Infantil Digital",
        "Depósito a Término Fijo 30d", "Depósito a Término Fijo 90d",
        "Depósito a Término Fijo 180d", "Cuenta Nómina Digital",
        "Cuenta Pensionados", "Cuenta Emprendedores",
        "Cuenta Ahorro Meta", "CDT Digital 360d",
        "Cuenta Joven Digital", "Ahorro Programado",
        "Cuenta Migrante Digital",
    ]
    nombres_trans = [
        "Pago PSE Empresas", "Transferencia ACH Nacional",
        "Corresponsalía Bancaria", "Recarga Celular",
        "Pago Servicios Públicos", "Giro Nacional",
        "Pago de Impuestos DIAN", "Recaudo Empresarial",
        "Billetera Digital", "Pago QR Comercios",
        "Pago Seguridad Social", "Transferencia Internacional",
        "Pago Factura Crédito", "Desembolso Inmediato",
        "Pago Peajes Electrónicos", "Recaudo Cartera Digital",
        "Débito Automático", "Domiciliación Nómina",
        "Pago Seguros Digitales", "Transferencia Interbancaria",
    ]

    tipos_lista = (
        [("CREDITO", n) for n in nombres_credito] +
        [("AHORRO",  n) for n in nombres_ahorro]  +
        [("TRANSACCIONAL", n) for n in nombres_trans]
    )
    # Tomar exactamente n productos
    tipos_lista = tipos_lista[:n]

    records = []
    for i, (tipo, desc) in enumerate(tipos_lista):
        if tipo == "CREDITO":
            tasa_ea   = round(float(rng.uniform(18.0, 36.0)), 4)
            plazo_max = int(rng.choice([12, 24, 36, 48, 60]))
            cuota_min = round(float(rng.uniform(50_000, 500_000)), 0)
            comision  = round(float(rng.uniform(5_000, 30_000)), 0)
        elif tipo == "AHORRO":
            tasa_ea   = round(float(rng.uniform(3.0, 8.5)), 4)
            plazo_max = 0
            cuota_min = round(float(rng.uniform(0, 10_000)), 0)
            comision  = round(float(rng.uniform(0, 5_000)), 0)
        else:
            tasa_ea   = 0.0
            plazo_max = 0
            cuota_min = 0.0
            comision  = round(float(rng.uniform(500, 5_000)), 0)

        records.append({
            "cod_prod":        f"PROD{i+1:03d}",
            "desc_prod":       desc,
            "tip_prod":        tipo,
            "tasa_ea":         tasa_ea,
            "plazo_max_meses": plazo_max,
            "cuota_min":       cuota_min,
            "comision_admin":  comision,
            "estado_prod":     rng.choice(["ACTIVO", "DESCONTINUADO"], p=[0.90, 0.10]),
        })

    df = pd.DataFrame(records)
    log.info(f"  TB_PRODUCTOS_CAT: {len(df)} filas OK")
    return df


def gen_sucursales(n: int, rng: np.random.Generator) -> pd.DataFrame:
    log.info(f"Generando TB_SUCURSALES_RED ({n} registros)...")

    deptos = list(DEPARTAMENTOS.keys())
    records = []
    for i in range(n):
        depto = rng.choice(deptos)
        ciudad = rng.choice(DEPARTAMENTOS[depto])
        lat_base = {"Cundinamarca": 4.60, "Antioquia": 6.25, "Valle": 3.43,
                    "Atlántico": 10.96, "Santander": 7.11, "Bolívar": 10.39,
                    "Nariño": 1.21, "Córdoba": 8.75, "Huila": 2.93, "Meta": 4.15}
        lon_base = {"Cundinamarca": -74.08, "Antioquia": -75.56, "Valle": -76.52,
                    "Atlántico": -74.79, "Santander": -73.11, "Bolívar": -75.50,
                    "Nariño": -77.28, "Córdoba": -75.88, "Huila": -75.28, "Meta": -73.64}

        records.append({
            "cod_suc":   f"SUC{i+1:04d}",
            "nom_suc":   f"Punto {rng.choice(TIPOS_PUNTO)} {ciudad} {i+1}",
            "tip_punto": rng.choice(TIPOS_PUNTO, p=[0.30, 0.40, 0.20, 0.10]),
            "ciudad":    ciudad,
            "depto":     depto,
            "latitud":   round(lat_base.get(depto, 4.60) + float(rng.uniform(-0.5, 0.5)), 6),
            "longitud":  round(lon_base.get(depto, -74.08) + float(rng.uniform(-0.5, 0.5)), 6),
            "activo":    int(rng.choice([1, 0], p=[0.92, 0.08])),
        })

    df = pd.DataFrame(records)
    log.info(f"  TB_SUCURSALES_RED: {len(df)} filas OK")
    return df


def gen_obligaciones(n: int, clientes_ids: list, productos_df: pd.DataFrame,
                     start: date, end: date, rng: np.random.Generator,
                     anomaly_n: int = 100) -> pd.DataFrame:
    log.info(f"Generando TB_OBLIGACIONES ({n:,} registros)...")

    prod_credito = productos_df[productos_df["tip_prod"] == "CREDITO"]["cod_prod"].tolist()
    sample_clis  = rng.choice(clientes_ids, size=n, replace=True)

    records = []
    for i in range(n):
        id_cli  = sample_clis[i]
        cod_prod = rng.choice(prod_credito)
        prod_row = productos_df[productos_df["cod_prod"] == cod_prod].iloc[0]

        vr_aprobado    = round(float(rng.uniform(500_000, 50_000_000)), 0)
        vr_desembolsado = round(vr_aprobado * float(rng.uniform(0.85, 1.0)), 0)
        sdo_capital    = round(vr_desembolsado * float(rng.uniform(0.0, 1.0)), 0)
        plazo          = int(prod_row["plazo_max_meses"]) if prod_row["plazo_max_meses"] > 0 else 12
        vr_cuota       = round(vr_desembolsado / plazo, 0)
        fec_desembolso = random_date(start, end, rng)
        fec_venc       = fec_desembolso + timedelta(days=plazo * 30)

        # Distribución de mora: mayoría al día, cola larga
        mora_probs = [0.60, 0.18, 0.10, 0.07, 0.05]
        mora_rango = rng.choice(["al_dia", "r1", "r2", "r3", "deteriorado"], p=mora_probs)
        dias_mora  = {"al_dia": 0, "r1": int(rng.integers(1, 30)),
                      "r2": int(rng.integers(31, 60)),
                      "r3": int(rng.integers(61, 90)),
                      "deteriorado": int(rng.integers(91, 360))}[mora_rango]

        calif = ("A" if dias_mora == 0 else
                 "B" if dias_mora <= 30 else
                 "C" if dias_mora <= 60 else
                 "D" if dias_mora <= 90 else "E")

        num_cuotas_pend = max(0, int(plazo - (sdo_capital / vr_cuota)))

        records.append({
            "id_oblig":         f"OBL{i+1:07d}",
            "id_cli":           id_cli,
            "cod_prod":         cod_prod,
            "vr_aprobado":      vr_aprobado,
            "vr_desembolsado":  vr_desembolsado,
            "sdo_capital":      sdo_capital,
            "vr_cuota":         vr_cuota,
            "fec_desembolso":   fec_desembolso.isoformat(),
            "fec_venc":         fec_venc.isoformat(),
            "dias_mora_act":    dias_mora,
            "num_cuotas_pend":  num_cuotas_pend,
            "calif_riesgo":     calif,
        })

    df = pd.DataFrame(records)
    df = apply_nulls(df, ["num_cuotas_pend", "calif_riesgo"], 0.05, rng)

    # ANOMALÍA 3: dias_mora_act negativos (inconsistencia)
    idx_anom = rng.choice(len(df), size=anomaly_n, replace=False)
    df.loc[idx_anom, "dias_mora_act"] = -rng.integers(1, 10, size=anomaly_n)
    log.info(f"  [ANOMALÍA] Inyectados {anomaly_n} registros con dias_mora_act negativo")
    log.info(f"  TB_OBLIGACIONES: {len(df):,} filas OK")
    return df


def gen_movimientos(n: int, clientes_ids: list, productos_df: pd.DataFrame,
                    sucursales_df: pd.DataFrame,
                    start: date, end: date,
                    rng: np.random.Generator,
                    dup_n: int = 150,
                    oor_n: int = 80) -> pd.DataFrame:
    log.info(f"Generando TB_MOV_FINANCIEROS ({n:,} registros)...")

    all_prods = productos_df["cod_prod"].tolist()
    ciudades  = sucursales_df["ciudad"].tolist()

    # Distribución de montos: log-normal para reflejar comportamiento real
    montos = np.exp(rng.normal(12.5, 1.8, size=n))  # en COP
    montos = np.clip(montos, 1_000, 50_000_000)

    sample_clis = rng.choice(clientes_ids, size=n, replace=True)

    all_dates = [
        random_date(start, end, rng) for _ in range(n)
    ]

    records = []
    for i in range(n):
        d = all_dates[i]
        dt = random_datetime_in_day(d, rng)
        records.append({
            "id_mov":          f"MOV{i+1:09d}",
            "id_cli":          sample_clis[i],
            "cod_prod":        rng.choice(all_prods),
            "num_cuenta":      f"CTA{int(rng.integers(1_000_000, 9_999_999)):07d}",
            "fec_mov":         d.isoformat(),
            "hra_mov":         dt.strftime("%H:%M:%S"),
            "vr_mov":          round(float(montos[i]), 2),
            "tip_mov":         rng.choice(TIPOS_MOVIMIENTO, p=TIPOS_MOV_PESOS),
            "cod_canal":       rng.choice(CANALES, p=CANALES_PESOS),
            "cod_ciudad":      rng.choice(ciudades),
            "cod_estado_mov":  rng.choice(ESTADOS_MOV, p=ESTADOS_PESOS),
            "id_dispositivo":  f"DEV{int(rng.integers(100_000, 999_999)):06d}",
        })

    df = pd.DataFrame(records)
    df = apply_nulls(df, ["cod_ciudad", "id_dispositivo", "num_cuenta"], 0.05, rng)

    # ANOMALÍA 1: duplicados
    df = inject_duplicates(df, dup_n, rng)

    # ANOMALÍA 2: fechas fuera de rango (año 2099 o 2010)
    idx_oor = rng.choice(len(df), size=oor_n, replace=False)
    bad_dates = [
        rng.choice([date(2099, 1, 1), date(2010, 6, 15)])
        for _ in range(oor_n)
    ]
    df.loc[idx_oor, "fec_mov"] = [d.isoformat() for d in bad_dates]
    log.info(f"  [ANOMALÍA] Inyectadas {oor_n} fechas fuera de rango")
    log.info(f"  TB_MOV_FINANCIEROS: {len(df):,} filas OK")
    return df


def gen_comisiones(n: int, clientes_ids: list, productos_df: pd.DataFrame,
                   start: date, end: date, rng: np.random.Generator) -> pd.DataFrame:
    log.info(f"Generando TB_COMISIONES_LOG ({n:,} registros)...")

    all_prods   = productos_df["cod_prod"].tolist()
    sample_clis = rng.choice(clientes_ids, size=n, replace=True)

    records = []
    for i in range(n):
        fec_cobro = random_date(start, end, rng)
        tip_com   = rng.choice(TIPOS_COMISION, p=TIPOS_COMISION_PESOS)
        vr_com    = round(float(rng.uniform(500, 50_000)), 2)
        records.append({
            "id_comision":   f"COM{i+1:08d}",
            "id_cli":        sample_clis[i],
            "cod_prod":      rng.choice(all_prods),
            "fec_cobro":     fec_cobro.isoformat(),
            "vr_comision":   vr_com,
            "tip_comision":  tip_com,
            "estado_cobro":  rng.choice(["COBRADO", "PENDIENTE", "REVERSO"],
                                         p=[0.85, 0.10, 0.05]),
        })

    df = pd.DataFrame(records)
    df = apply_nulls(df, ["tip_comision", "estado_cobro"], 0.05, rng)
    log.info(f"  TB_COMISIONES_LOG: {len(df):,} filas OK")
    return df


# ──────────────────────────────────────────────
# Persistencia: CSV y Parquet
# ──────────────────────────────────────────────

def save_table(df: pd.DataFrame, name: str, out_dir: Path, formats: list):
    out_dir.mkdir(parents=True, exist_ok=True)
    if "csv" in formats:
        path_csv = out_dir / f"{name}.csv"
        df.to_csv(path_csv, index=False)
        log.info(f"  Guardado: {path_csv} ({path_csv.stat().st_size / 1024:.1f} KB)")
    if "parquet" in formats:
        path_pq = out_dir / f"{name}.parquet"
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, path_pq, compression="snappy")
        log.info(f"  Guardado: {path_pq} ({path_pq.stat().st_size / 1024:.1f} KB)")


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FinBank - Generador de datos sintéticos")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)

    seed     = cfg["generation"]["seed"]
    rng      = np.random.default_rng(seed)
    random.seed(seed)

    start_d  = date.fromisoformat(cfg["generation"]["date_start"])
    end_d    = date.fromisoformat(cfg["generation"]["date_end"])
    out_dir  = Path(args.output or cfg["output"]["directory"])
    formats  = cfg["output"]["formats"]
    vols     = cfg["volumes"]
    anom     = cfg["anomalies"]

    log.info("=" * 60)
    log.info("FinBank S.A. - Generación de datos sintéticos")
    log.info(f"Semilla: {seed} | Rango: {start_d} → {end_d}")
    log.info("=" * 60)

    # Orden de generación respetando integridad referencial
    df_clientes   = gen_clientes(vols["TB_CLIENTES_CORE"], start_d, end_d, rng)
    df_productos  = gen_productos(vols["TB_PRODUCTOS_CAT"], rng)
    df_sucursales = gen_sucursales(vols["TB_SUCURSALES_RED"], rng)

    clientes_ids  = df_clientes["id_cli"].tolist()

    df_obligaciones = gen_obligaciones(
        vols["TB_OBLIGACIONES"], clientes_ids, df_productos,
        start_d, end_d, rng, anom["inconsistent_mora_records"]
    )
    df_movimientos = gen_movimientos(
        vols["TB_MOV_FINANCIEROS"], clientes_ids, df_productos,
        df_sucursales, start_d, end_d, rng,
        anom["duplicate_transactions"], anom["out_of_range_dates"]
    )
    df_comisiones = gen_comisiones(
        vols["TB_COMISIONES_LOG"], clientes_ids, df_productos,
        start_d, end_d, rng
    )

    # Guardado
    log.info("\nGuardando archivos...")
    tables = {
        "TB_CLIENTES_CORE":   df_clientes,
        "TB_PRODUCTOS_CAT":   df_productos,
        "TB_SUCURSALES_RED":  df_sucursales,
        "TB_OBLIGACIONES":    df_obligaciones,
        "TB_MOV_FINANCIEROS": df_movimientos,
        "TB_COMISIONES_LOG":  df_comisiones,
    }
    for name, df in tables.items():
        save_table(df, name, out_dir, formats)

    # Resumen final
    log.info("\n" + "=" * 60)
    log.info("RESUMEN DE GENERACIÓN")
    log.info("=" * 60)
    for name, df in tables.items():
        log.info(f"  {name:<25}: {len(df):>8,} filas")
    log.info(f"\nAnomálías documentadas:")
    log.info(f"  - {anom['duplicate_transactions']} transacciones duplicadas en TB_MOV_FINANCIEROS")
    log.info(f"  - {anom['out_of_range_dates']} fechas fuera de rango en TB_MOV_FINANCIEROS")
    log.info(f"  - {anom['inconsistent_mora_records']} registros con dias_mora negativo en TB_OBLIGACIONES")
    log.info("Generación completada exitosamente.")


if __name__ == "__main__":
    main()
