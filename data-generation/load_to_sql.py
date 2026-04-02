"""
FinBank S.A. - Cargador a Azure SQL Database
=============================================
Lee los CSV generados y los carga en Azure SQL Database.
Se conectará directamente usando los datos fijos del servidor y base de datos.

Uso:
    python load_to_sql.py --input ./output
"""


# ------------------- Configuración de conexión -------------------
# Cambia aquí tus datos de Azure SQL
server = 'sql-finbank-src-dev-vse2sf.database.windows.net'
database = 'finbank-source-dev'
username = 'finbank_admin'
password = 'TuContraseñaSegura123!'

# load_to_sql_ordered.py
import argparse
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# Configuración logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configura tu conexión a Azure SQL aquí
SERVER = "sql-finbank-src-dev-vse2sf.database.windows.net"
DATABASE = "finbank-source-dev"
USERNAME = "finbank_admin"
PASSWORD = "TuContraseñaSegura123!"
DRIVER = "ODBC Driver 18 for SQL Server"

engine = create_engine(
    f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}"
)

# Orden de carga de datos
TABLAS_ORDEN = [
    "TB_CLIENTES_CORE",
    "TB_PRODUCTOS_CAT",
    "TB_SUCURSALES_RED",
    "TB_OBLIGACIONES",
    "TB_MOV_FINANCIEROS",
    "TB_COMISIONES_LOG",
]

# Orden para limpiar primero las tablas dependientes
TABLAS_DEPENDIENTES = [
    "TB_COMISIONES_LOG",
    "TB_MOV_FINANCIEROS",
    "TB_OBLIGACIONES",
    "TB_SUCURSALES_RED",
    "TB_PRODUCTOS_CAT",
    "TB_CLIENTES_CORE",
]

def limpiar_tablas():
    with engine.connect() as conn:
        for tabla in TABLAS_DEPENDIENTES:
            logging.info(f"Eliminando datos de {tabla}...")
            conn.execute(text(f"DELETE FROM {tabla}"))

def cargar_csv(input_path: Path):
    for tabla in TABLAS_ORDEN:
        archivo = input_path / f"{tabla}.csv"
        if archivo.exists():
            logging.info(f"Cargando {archivo} en {tabla}...")
            df = pd.read_csv(archivo)
            df.to_sql(tabla, engine, if_exists="append", index=False)
            logging.info(f"{tabla} cargada correctamente con {len(df)} filas.")
        else:
            logging.warning(f"No se encontró el archivo para {tabla}: {archivo}")

def main(input_path_str):
    input_path = Path(input_path_str).expanduser()
    if not input_path.exists():
        logging.error(f"La ruta {input_path} no existe.")
        return

    logging.info("Limpiando tablas en orden correcto...")
    limpiar_tablas()
    logging.info("Iniciando carga de CSVs en orden correcto...")
    cargar_csv(input_path)
    logging.info("Carga completada.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cargar CSVs a Azure SQL Database en orden")
    parser.add_argument("--input", required=True, help="Ruta de la carpeta con los CSVs")
    args = parser.parse_args()
    main(args.input)