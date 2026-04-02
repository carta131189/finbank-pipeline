# CHANGELOG — FinBank S.A. Pipeline de Datos

## [1.0.0] — 2026-03-31

**Autor:** Johan Cartagena 
**Descripción:** Versión inicial del pipeline de datos Medallón para FinBank S.A.

### Añadido

- Generación de datos sintéticos con semilla fija (seed=42) para las 6 tablas del sistema transaccional: TB_CLIENTES_CORE (10,000), TB_PRODUCTOS_CAT (50), TB_SUCURSALES_RED (200), TB_OBLIGACIONES (30,000), TB_MOV_FINANCIEROS (500,000) y TB_COMISIONES_LOG (80,000)
- 3 anomalías documentadas inyectadas: duplicados en movimientos, fechas fuera de rango y dias_mora negativos
- Salida en CSV y Parquet (Snappy) con config.yaml centralizado
- Script de carga a Azure SQL Database con DDL completo y orden de integridad referencial
- Infraestructura como Código con Terraform: Resource Group, ADLS Gen2 con contenedores bronze/silver/gold/errors, Databricks Workspace Premium, Azure Key Vault con purge protection, Log Analytics Workspace y Action Group con alertas
- Módulos Terraform separados por recurso con soporte para entornos dev y prod
- Backend remoto del estado Terraform en Azure Storage (terraform.tfstate excluido del repo)
- Pipeline Bronze: ingesta incremental con watermarks, 3 columnas de auditoría, particionamiento por año/mes/día, modo MERGE idempotente, log de ejecución  persistido en Delta
- Pipeline Silver: deduplicación, validación de campos obligatorios, integridad referencial con tabla de errores documentada, enmascaramiento SHA-256 de PII (nomb_cli, apell_cli, num_doc), cálculo de ind_sospechoso con ventana estadística de 30 días (3σ), flag ind_horario_habil, reporte de calidad por batch
- Pipeline Gold: 4 dimensiones (dim_clientes, dim_productos, dim_geografia, dim_canal) y 4 tablas de hechos/KPIs (fact_transacciones, fact_cartera, fact_rentabilidad_cliente, kpis_diarios_cartera) con reglas de negocio completas
- Regla bucket_mora en 5 rangos: AL_DIA, RANGO_1, RANGO_2, RANGO_3, DETERIORADO
- Clasificación regulatoria A/B/C/D/E y provisiones según tabla Superintendencia Financiera Colombia
- CLTV mensual de 12 meses por cliente integrando comisiones cobradas e intereses estimados
- KPIs diarios de cartera particionados por año/mes
- 8 quality checks automatizados con resultados persistidos en Delta
- Orquestación con Databricks Workflows: DAG de 5 tareas con dependencias estrictas SUCCESS-only, schedule 02:00 COT, reintentos con backoff (3 intentos), timeouts por tarea coherentes con el volumen
- Notificación de resumen diario por email con HTML estructurado
- Alerta de anomalía de volumen: disparo diferenciado si el batch difiere >30% del promedio de 7 ejecuciones
- Roles RBAC: finbank_data_engineer, finbank_analyst, finbank_admin con principio de mínimo privilegio
- 3 Service Principals con permisos mínimos por función: pipeline, terraform, readonly
- Vistas de enmascaramiento dinámico en Gold para rol Analista (sin acceso a PII)
- Catálogo de datos completo con linaje documentado de campos calculados
- CI/CD con GitHub Actions: lint, validación Terraform, escaneo de secretos, despliegue automático
- README con avances
- CHANGELOG.md con avances

### Infraestructura

- **Plataforma:** Microsoft Azure (eastus2)
- **Orquestador:** Databricks Workflows
- **Formato de datos:** Delta Lake (Medallón)
- **IaC:** Terraform 1.6+ con backend remoto en Azure Storage
- **Autenticación:** Managed Identity + Azure Key Vault (sin credenciales en código)

---

## [Próximas versiones — Backlog]

### Planificado para [1.1.0]

- Documentacion

### Planificado para [1.2.0]

- Pasos de ejecucion del proyecto