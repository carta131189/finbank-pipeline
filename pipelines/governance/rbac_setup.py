# Databricks notebook source
# FinBank S.A. — Gobierno y Seguridad: Roles RBAC
# ================================================
# Define e implementa los tres roles diferenciados del proyecto.
# Se ejecuta una sola vez durante el onboarding de la plataforma
# o cuando se incorporan nuevos miembros al equipo.
#
# Roles definidos:
#   1. Ingeniero de Datos  — lectura y escritura en todas las capas
#   2. Analista            — solo lectura en Gold (sin acceso a PII)
#   3. Administrador       — control total sobre recursos del proyecto
#
# Principio de mínimo privilegio: cada componente del pipeline
# ejecuta bajo su propia Service Principal con permisos mínimos.

# COMMAND ----------

import json
import logging
import requests
from dataclasses import dataclass, field

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rbac")

def get_secret(key: str) -> str:
    return dbutils.secrets.get(scope="finbank-kv", key=key)

DATABRICKS_HOST  = get_secret("databricks-host")
DATABRICKS_TOKEN = get_secret("databricks-admin-token")
STORAGE_ACCOUNT  = get_secret("storage-account-name")

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json",
}

# ─────────────────────────────────────────────────────
# Definición de roles y sus permisos
# ─────────────────────────────────────────────────────

@dataclass
class RoleDefinition:
    name:        str
    description: str
    # Permisos en Unity Catalog por capa
    bronze_permissions: list = field(default_factory=list)
    silver_permissions: list = field(default_factory=list)
    gold_permissions:   list = field(default_factory=list)
    # Permisos en Databricks Workspace
    workspace_permission: str = "USER"
    # Permisos en clusters
    cluster_permission: str = "CAN_ATTACH_TO"


ROLES = [
    RoleDefinition(
        name        = "finbank_data_engineer",
        description = "Ingeniero de Datos: lectura y escritura en todas las capas Medallón.",
        bronze_permissions = ["SELECT", "MODIFY", "CREATE TABLE", "CREATE VIEW"],
        silver_permissions = ["SELECT", "MODIFY", "CREATE TABLE", "CREATE VIEW"],
        gold_permissions   = ["SELECT", "MODIFY", "CREATE TABLE", "CREATE VIEW"],
        workspace_permission = "CAN_USE",
        cluster_permission   = "CAN_RESTART",
    ),
    RoleDefinition(
        name        = "finbank_analyst",
        description = (
            "Analista: solo lectura en capa Gold. "
            "Sin acceso a Bronze ni Silver. "
            "Las columnas PII (mascaradas en Silver) no son accesibles en sus vistas originales."
        ),
        bronze_permissions = [],          # sin acceso
        silver_permissions = [],          # sin acceso
        gold_permissions   = ["SELECT"],  # solo lectura Gold
        workspace_permission = "CAN_USE",
        cluster_permission   = "CAN_ATTACH_TO",
    ),
    RoleDefinition(
        name        = "finbank_admin",
        description = "Administrador: control total sobre todos los recursos del proyecto.",
        bronze_permissions = ["ALL PRIVILEGES"],
        silver_permissions = ["ALL PRIVILEGES"],
        gold_permissions   = ["ALL PRIVILEGES"],
        workspace_permission = "IS_ADMIN",
        cluster_permission   = "CAN_MANAGE",
    ),
]

# ─────────────────────────────────────────────────────
# Implementación de permisos en Unity Catalog
# (requiere Unity Catalog habilitado en el workspace)
# ─────────────────────────────────────────────────────

def grant_catalog_permissions(role: RoleDefinition):
    """
    Aplica permisos en Unity Catalog por esquema (Bronze/Silver/Gold).
    Ejecutado como SQL desde el notebook.
    """
    layer_map = {
        "bronze": role.bronze_permissions,
        "silver": role.silver_permissions,
        "gold":   role.gold_permissions,
    }

    for schema, permissions in layer_map.items():
        if not permissions:
            log.info(f"  {role.name} — sin acceso a esquema '{schema}' (diseño intencional)")
            continue
        for perm in permissions:
            sql = f"GRANT {perm} ON SCHEMA finbank.{schema} TO `{role.name}`"
            log.info(f"  Ejecutando: {sql}")
            spark.sql(sql)

    # Garantizar acceso al catálogo (sin permisos implica no ver las tablas)
    if role.gold_permissions:
        spark.sql(f"GRANT USE CATALOG ON CATALOG finbank TO `{role.name}`")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA finbank.gold TO `{role.name}`")
    if role.silver_permissions:
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA finbank.silver TO `{role.name}`")
    if role.bronze_permissions:
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA finbank.bronze TO `{role.name}`")


def set_cluster_permissions(cluster_id: str, group_name: str, permission_level: str):
    """Aplica permisos de cluster via API de Databricks."""
    url  = f"{DATABRICKS_HOST}/api/2.0/permissions/clusters/{cluster_id}"
    body = {
        "access_control_list": [
            {"group_name": group_name, "permission_level": permission_level}
        ]
    }
    resp = requests.patch(url, headers=HEADERS, json=body)
    if resp.status_code == 200:
        log.info(f"  Cluster {cluster_id}: {group_name} → {permission_level}")
    else:
        log.warning(f"  Error cluster permissions: {resp.text}")


def create_groups_and_apply_roles():
    """
    Crea los grupos en el workspace de Databricks y aplica permisos.
    En producción los grupos se sincronizan desde Azure AD via SCIM.
    """
    for role in ROLES:
        log.info(f"\nAplicando rol: {role.name}")
        log.info(f"  Descripción: {role.description}")

        # Crear grupo en Databricks (idempotente)
        url  = f"{DATABRICKS_HOST}/api/2.0/groups/create"
        body = {"group_name": role.name}
        resp = requests.post(url, headers=HEADERS, json=body)
        if resp.status_code in [200, 409]:   # 409 = ya existe
            log.info(f"  Grupo '{role.name}': OK")
        else:
            log.warning(f"  Error creando grupo: {resp.text}")

        # Aplicar permisos en Unity Catalog
        try:
            grant_catalog_permissions(role)
        except Exception as e:
            log.warning(f"  Unity Catalog no disponible o error: {e}")
            log.info("  → Aplicar permisos manualmente desde el SQL Editor de Databricks")


# ─────────────────────────────────────────────────────
# Identidades de servicio por componente del pipeline
# ─────────────────────────────────────────────────────

SERVICE_PRINCIPALS = {
    "sp-finbank-pipeline": {
        "description": "Ejecuta los notebooks Bronze/Silver/Gold. Permisos mínimos sobre ADLS y SQL.",
        "adls_roles": [
            "Storage Blob Data Contributor",   # ADLS Gen2: leer y escribir
        ],
        "sql_permissions":  ["db_datawriter", "db_datareader"],
        "keyvault_roles":   ["Key Vault Secrets User"],
    },
    "sp-finbank-terraform": {
        "description": "Despliega infraestructura via Terraform CI/CD. Solo durante apply.",
        "adls_roles":       ["Storage Account Contributor"],
        "sql_permissions":  ["dbmanager"],
        "keyvault_roles":   ["Key Vault Administrator"],
    },
    "sp-finbank-readonly": {
        "description": "Consumo de datos Gold por herramientas de BI externas (Power BI, etc.).",
        "adls_roles":       ["Storage Blob Data Reader"],
        "sql_permissions":  [],
        "keyvault_roles":   [],
    },
}

# ─────────────────────────────────────────────────────
# Generación de SQL de enmascaramiento dinámico para Analista
# ─────────────────────────────────────────────────────

DYNAMIC_MASKING_SQL = """
-- FinBank S.A. — Enmascaramiento dinámico para rol Analista
-- Los datos originales (PII) NO son accesibles para finbank_analyst.
-- Las columnas ya vienen hasheadas desde Silver; estas vistas agregan
-- una capa adicional de ofuscación visual para el analista.

-- Vista Gold dim_clientes sin PII (para finbank_analyst)
CREATE OR REPLACE VIEW finbank.gold.vw_dim_clientes_analista AS
SELECT
    id_cli,
    CONCAT(LEFT(nombre_hash, 4), '****')   AS nombre_ofuscado,
    CONCAT('***', RIGHT(num_doc_hash, 4))  AS doc_ofuscado,
    tip_doc,
    edad,
    fec_alta,
    cod_segmento,
    segmento_label,
    -- score_buro: rango en lugar de valor exacto
    CASE
        WHEN score_buro < 400 THEN 'BAJO'
        WHEN score_buro < 600 THEN 'MEDIO'
        WHEN score_buro < 800 THEN 'ALTO'
        ELSE 'MUY_ALTO'
    END AS rango_score_buro,
    depto_res,         -- ciudad no se expone al analista
    estado_cli,
    ind_activo,
    canal_adquis
FROM finbank.gold.dim_clientes;

-- Otorgar acceso a la VISTA (no a la tabla base) para el analista
GRANT SELECT ON VIEW finbank.gold.vw_dim_clientes_analista TO `finbank_analyst`;

-- Revocar acceso del analista a la tabla base (PII protegida)
REVOKE SELECT ON TABLE finbank.gold.dim_clientes FROM `finbank_analyst`;
"""

# ─────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────

# COMMAND ----------

log.info("=" * 60)
log.info("FinBank S.A. — Configuración de Roles y Accesos RBAC")
log.info("=" * 60)

# 1. Crear catálogo y esquemas en Unity Catalog
for schema in ["bronze", "silver", "gold", "errors"]:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS finbank.{schema}")
        log.info(f"Esquema finbank.{schema}: OK")
    except Exception as e:
        log.warning(f"Esquema finbank.{schema}: {e}")

# 2. Crear grupos y aplicar permisos por rol
create_groups_and_apply_roles()

# 3. Crear vistas con enmascaramiento dinámico
log.info("\nCreando vistas con enmascaramiento para rol Analista...")
for stmt in DYNAMIC_MASKING_SQL.strip().split(";"):
    stmt = stmt.strip()
    if stmt and not stmt.startswith("--"):
        try:
            spark.sql(stmt)
            log.info(f"  SQL ejecutado: {stmt[:80]}...")
        except Exception as e:
            log.warning(f"  Error en SQL: {e}")

# 4. Resumen de service principals documentados
log.info("\n" + "=" * 60)
log.info("SERVICE PRINCIPALS — Principio de mínimo privilegio")
log.info("=" * 60)
for sp, config in SERVICE_PRINCIPALS.items():
    log.info(f"\n  {sp}")
    log.info(f"    {config['description']}")
    log.info(f"    ADLS roles:    {config['adls_roles']}")
    log.info(f"    SQL permisos:  {config['sql_permissions']}")
    log.info(f"    KV roles:      {config['keyvault_roles']}")

log.info("\nConfiguración RBAC completada.")
