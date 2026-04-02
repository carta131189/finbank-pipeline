# FinBank S.A. — Pipeline de Datos Medallón en Azure

**Sector:** ESCENARIO A — BANCA Y SERVICIOS FINANCIEROS 
**Plataforma:** Microsoft Azure  
**Orquestador:** Databricks Workflows  
**IaC:** Terraform 1.6+  
**Formato de datos:** Delta Lake (Bronze / Silver / Gold)

---

##  Overview
Proyecto de ingeniería de datos basado en arquitectura Medallion (Bronze, Silver, Gold) usando Azure Data Factory.

---

## Justificación de decisiones técnicas

**¿Por qué Microsoft Azure?** Azure ofrece integración nativa entre ADLS Gen2, Databricks y Key Vault mediante Managed Identities, lo que elimina la necesidad de gestionar credenciales manualmente. Azure SQL Database es el motor relacional con menor fricción para el sistema origen de FinBank dado que el banco ya opera en el ecosistema Microsoft.

**¿Por qué Databricks Workflows sobre Apache Airflow?** Airflow requiere infraestructura dedicada (Kubernetes o máquina virtual) y mantenimiento operacional continuo. Databricks Workflows es nativo al workspace, incluye monitoreo, reintentos y alertas sin configuración adicional, y los notebooks de transformación se despliegan directamente desde el repositorio Git.

**¿Por qué Delta Lake sobre Parquet plano?** Delta aporta transacciones ACID, soporte nativo de MERGE para idempotencia, versionado de datos y viajes en el tiempo. El overhead de escribir el transaction log es mínimo frente a los beneficios operacionales en un pipeline que se ejecuta diariamente sobre datos financieros.

**¿Por qué Terraform sobre Bicep?** Terraform es agnóstico a la nube, lo que facilita una eventual migración parcial o extensión multi-cloud. El ecosistema de módulos de la comunidad es más maduro y el equipo ya tiene experiencia con HCL.

---

## Estructura del repositorio

```
finbank-pipeline/
├── data-generation/
│   ├── generate_data.py         # Generador de datos sintéticos
│   ├── load_to_sql.py           # Cargador a Azure SQL Database
│   ├── config.yaml              # Parámetros de generación centralizados
│   └── requirements.txt
├── infra/
│   ├── main.tf                  # Configuración principal de Terraform
│   ├── variables.tf             # Definición de variables
│   ├── outputs.tf               # Exports de recursos creados
│   ├── bootstrap_backend.sh     # Script de inicialización (ejecutar 1 vez)
│   ├── .gitignore               # Excluye terraform.tfstate del repo
│   ├── modules/
│   │   ├── adls/                # ADLS Gen2 + contenedores Medallón
│   │   ├── databricks/          # Workspace Databricks
│   │   ├── keyvault/            # Azure Key Vault + secretos placeholder
│   │   └── monitoring/          # Log Analytics + Action Group + Alertas
│   └── environments/
│       ├── dev/terraform.tfvars
│       └── prod/terraform.tfvars
├── pipelines/
│   ├── bronze/
│   │   └── 01_bronze_ingestion.py
│   ├── silver/
│   │   └── 02_silver_processing.py
│   ├── gold/
│   │   └── 03_gold_processing.py
│   ├── governance/
│   │   └── rbac_setup.py
│   └── quality_checks.py
├── orchestration/
│   ├── finbank_workflow.json           # Definición del DAG Databricks
│   ├── pipeline_summary_notification.py
│   └── deploy_workflow.py             # Script de despliegue via API
├── docs/
│   ├── data_catalog.md                # Catálogo completo de tablas
│   └── architecture.md               # Diagrama y descripción de arquitectura
├── .github/
│   └── workflows/
│       └── cicd.yml                   # Pipeline CI/CD GitHub Actions
├── README.md
└── CHANGELOG.md
```

---

##  Estado actual
✔ Desarrollo completo del proyecto

---

## Guía de despliegue paso a paso

### Paso 1 — Clonar el repositorio
### Paso 2 — Autenticarse en Azure
### Paso 3 — Inicializar el backend remoto de Terraform
### Paso 4 — Inicializar y aplicar Terraform
### Paso 5 — Cargar secretos en Azure Key Vault
### Paso 6 — Generar los datos sintéticos
### Paso 7 — Cargar datos en Azure SQL Database
### Paso 8 — Configurar Databricks
#### 8.1 Crear el scope de secretos vinculado a Key Vault
#### 8.2 Configurar acceso del cluster al ADLS Gen2 (Managed Identity)
#### 8.3 Subir los notebooks al workspace
### Paso 9 — Desplegar el workflow de Databricks
### Paso 10 — Configurar roles RBAC
### Paso 11 — Verificar el pipeline
### Paso 12 — Configurar CI/CD

## Preguntas frecuentes

**¿Qué pasa si ejecuto el pipeline dos veces sobre los mismos datos?**  
El MERGE sobre la clave primaria en cada capa garantiza idempotencia. No se generan duplicados.

**¿Cómo agrego un nuevo miembro al equipo con rol Analista?**  
En Azure AD, agrega al usuario al grupo `finbank_analyst`. La sincronización SCIM propagará el acceso al workspace de Databricks automáticamente. El usuario solo verá las tablas Gold y la vista `vw_dim_clientes_analista` (sin PII).

**¿Cómo cambio el email de alertas?**  
Actualiza el secreto `alert-email-to` en Key Vault: `az keyvault secret set --vault-name $KV_NAME --name "alert-email-to" --value "nuevo@email.com"`. No requiere redespliegue de infraestructura.

**¿Dónde veo los registros rechazados por integridad referencial?**  
En la tabla Delta `errors/referential_integrity_errors` en el contenedor errors del ADLS. Cada registro incluye la tabla de origen, el motivo del rechazo y el batch_id.