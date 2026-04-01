# CHANGELOG — FinBank S.A. Pipeline de Datos

## [1.0.0] — 2026-03-31

**Autor:** Johan Cartagena 
**Descripción:** Versión inicial del pipeline de datos Medallón para FinBank S.A.

### Añadido

- Generación de datos sintéticos con semilla fija (seed=42) para las 6 tablas del sistema transaccional: TB_CLIENTES_CORE (10,000), TB_PRODUCTOS_CAT (50), TB_SUCURSALES_RED (200), TB_OBLIGACIONES (30,000), TB_MOV_FINANCIEROS (500,000) y TB_COMISIONES_LOG (80,000)
- 3 anomalías documentadas inyectadas: duplicados en movimientos, fechas fuera de rango y dias_mora negativos
- Salida en CSV y Parquet (Snappy) con config.yaml centralizado
- README inicial
- CHANGELOG.md inicial

### Infraestructura

- **Plataforma:** Microsoft Azure (eastus2)
- **Orquestador:** Databricks Workflows
- **Formato de datos:** Delta Lake (Medallón)
- **IaC:** Terraform 1.6+ con backend remoto en Azure Storage
- **Autenticación:** Managed Identity + Azure Key Vault (sin credenciales en código)

---

## [Próximas versiones — Backlog]

### Planificado para [1.1.0]

- Modelo de detección de fraude en tiempo real sobre fact_transacciones
- Particionamiento Z-Order en fact_cartera por cod_segmento y depto_res
- Migración de transformaciones Silver/Gold a dbt para linaje automático
- Dashboard de monitoreo del pipeline en Databricks SQL

### Planificado para [1.2.0]

- Implementación de Slowly Changing Dimensions Tipo 2 en dim_clientes
- Integración con Azure Purview para linaje automático centralizado
- Alertas en Microsoft Teams además de email
