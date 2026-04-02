"""
FinBank S.A. - Cargador a Azure SQL Database
=============================================
Lee los CSV generados y los carga en Azure SQL Database.
Se conectará directamente usando los datos fijos del servidor y base de datos.

Uso:
    python load_to_sql.py --input ./output
"""

# load_to_azure_sql.py

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine

# --- Configuración de conexión ---
server = 'sql-finbank-src-dev-vse2sf.database.windows.net'
database = 'finbank-source-dev'
username = 'finbank_admin'
password = 'TuContraseñaSegura123!'
driver = 'ODBC Driver 18 for SQL Server'

connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'

def load_csv_to_sql(file_path, engine, table_name):
    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✔ Archivo {file_path} cargado en la tabla {table_name}")
    except Exception as e:
        print(f"❌ Error al cargar {file_path}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Ruta de la carpeta con los archivos CSV')
    args = parser.parse_args()

    # Expandir ~ a la ruta completa
    input_folder = os.path.expanduser(args.input)
    engine = create_engine(connection_string)

    # Iterar sobre todos los archivos CSV de la carpeta
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            table_name = os.path.splitext(filename)[0]  # tabla = nombre del archivo sin extensión
            load_csv_to_sql(file_path, engine, table_name)