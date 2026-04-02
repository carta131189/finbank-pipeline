# FinBank S.A. — Pipeline de Datos Medallón en Azure

**Sector:** Banca Digital  
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
├── .github/
│   └── workflows/
│       └── cicd.yml                   # Pipeline CI/CD GitHub Actions
├── README.md
└── CHANGELOG.md
```

---


##  Objetivo

Construir un pipeline end-to-end que permita:
- Análisis de riesgo crediticio
- Detección de fraude
- Cálculo de CLTV
- KPIs regulatorios


##  Arquitectura

- Bronze: datos crudos
- Silver: datos limpios y validados
- Gold: modelo analítico


##  Estado actual
✔ Estructura del proyecto creada  
✔ Generación de datos sintéticos en desarrollo  
✔ Creacion de arquitectura en medallon 
✔ Creacion y ejecucion de pipeline y GitHub Actions


##  Próximos pasos
- Completar documentacion
- Ajustes necesario