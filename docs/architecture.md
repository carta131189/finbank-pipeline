```mermaid
flowchart TD
    subgraph SOURCE["Fuente Origen — Azure SQL Database"]
        SQL[(Azure SQL Database\nfinbank-source-dev)]
        T1[TB_CLIENTES_CORE\n10,000 reg]
        T2[TB_PRODUCTOS_CAT\n50 reg]
        T3[TB_MOV_FINANCIEROS\n500,000 reg]
        T4[TB_OBLIGACIONES\n30,000 reg]
        T5[TB_SUCURSALES_RED\n200 reg]
        T6[TB_COMISIONES_LOG\n80,000 reg]
        SQL --- T1 & T2 & T3 & T4 & T5 & T6
    end

    subgraph IaC["Infraestructura — Terraform"]
        TF[Terraform Modules\nRG + ADLS + Databricks\nKey Vault + Monitoring]
        BACKEND[(tfstate remoto\nAzure Storage)]
        TF -. estado .-> BACKEND
    end

    subgraph SECURITY["Seguridad"]
        KV[Azure Key Vault\nCredenciales + Secretos]
        MI[Managed Identity\nSP por componente]
        RBAC[RBAC\nData Engineer\nAnalista\nAdmin]
    end

    subgraph ADLS["ADLS Gen2 — Arquitectura Medallón"]
        direction LR
        BRONZE["Bronze\nDelta Lake\nDato crudo + auditoría\n+ watermarks"]
        SILVER["Silver\nDelta Lake\nLimpio + tipado\nPII hasheado\nind_sospechoso"]
        GOLD["Gold\nDelta Lake\nModelo dimensional\nKPIs ejecutivos"]
        BRONZE -->|limpieza\ndedup\nRI| SILVER
        SILVER -->|reglas negocio\nagregaciones| GOLD
    end

    subgraph DATABRICKS["Azure Databricks"]
        direction TB
        WF[Databricks Workflows\nDAG principal\n02:00 COT diario]
        NB1[01_bronze_ingestion\nJDBC incremental\npartición año/mes/día]
        NB2[02_silver_processing\nSHA-256 PII\n3σ anomalías]
        NB3[03_gold_processing\ndims + facts\nCLTV + KPIs]
        NB4[quality_checks\n8 validaciones Delta]
        NB5[summary_notification\nemail HTML]
        WF --> NB1 -->|SUCCESS| NB2 -->|SUCCESS| NB3 -->|SUCCESS| NB4 -->|SUCCESS| NB5
    end

    subgraph MONITORING["Monitoreo"]
        LAW[Log Analytics Workspace\nDiagnostic Settings]
        AG[Action Group\nEmail Alerts]
        NOTIF[Reporte diario\nAlerta anomalía volumen\nAlerta fallo de tarea]
        LAW --> AG --> NOTIF
    end

    subgraph CONSUMPTION["Consumo"]
        BI[Power BI / Dashboards\nfin_analyst vía Gold]
        RISK[Equipo Riesgo\nkpis_diarios_cartera\nfact_cartera]
        FRAUD[Motor de Fraude\nind_sospechoso\nfact_transacciones]
        COMM[Equipo Comercial\nfact_rentabilidad_cliente\ndim_clientes]
    end

    SOURCE -->|JDBC incremental| NB1
    KV -->|secretos| DATABRICKS
    MI -->|autenticación| DATABRICKS
    IaC -->|provisiona| ADLS
    IaC -->|provisiona| DATABRICKS
    IaC -->|provisiona| MONITORING
    DATABRICKS -->|escribe| ADLS
    DATABRICKS -->|logs| LAW
    GOLD --> CONSUMPTION
    RBAC -->|controla acceso| ADLS
    RBAC -->|controla acceso| DATABRICKS
```

## Descripción de la arquitectura

### Flujo de datos

1. **Fuente:** Azure SQL Database actúa como sistema origen. Los datos sintéticos se generan con Python y se cargan mediante `load_to_sql.py`.

2. **Bronze:** El notebook `01_bronze_ingestion.py` lee desde SQL via JDBC con paralelismo de 8 particiones. Agrega columnas de auditoría (_ingestion_timestamp, _source_system, _batch_id), particiona por año/mes/día y escribe en Delta Lake. El modo incremental usa watermarks persistidos en `bronze/_control/watermarks`.

3. **Silver:** El notebook `02_silver_processing.py` aplica seis transformaciones en orden: deduplicación, validación de nulos obligatorios, integridad referencial (rechazados a tabla de errores), estandarización de tipos, enmascaramiento SHA-256 de PII y cálculo de ind_sospechoso con ventana estadística de 30 días.

4. **Gold:** El notebook `03_gold_processing.py` construye 4 dimensiones y 4 tablas de hechos/KPIs con reglas de negocio específicas de FinBank (bucket_mora, provisiones regulatorias, CLTV 12 meses).

5. **Orquestación:** Databricks Workflows ejecuta el DAG  con dependencias estrictas SUCCESS-only, 3 reintentos con backoff y notificación automática al finalizar.

### Decisiones de arquitectura

- **Delta Lake sobre Parquet plano:** aporta transacciones ACID, soporte de MERGE para idempotencia, versionado y viajes en el tiempo sin overhead operacional significativo.
- **Managed Identity + Key Vault:** ninguna credencial aparece en código fuente ni en variables de entorno sin cifrar. Todos los secretos se resuelven en tiempo de ejecución.
- **Databricks Workflows sobre Apache Airflow:** integración nativa con el workspace, sin infraestructura adicional que mantener, monitoreo incluido en la UI de Databricks.
- **Terraform con módulos separados:** permite reutilizar la infraestructura en múltiples entornos (dev/prod) con un solo comando y sin duplicar código.
