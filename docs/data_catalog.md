# Catálogo de Datos — FinBank S.A.

**Proyecto:** Pipeline de Datos Medallón — Microsoft Azure / Databricks   
**Última actualización:** 2026-04-02  
**Catálogo Unity:** `finbank`

---

## Convenciones

| Símbolo | Significado |
|---------|-------------|
| 🔴 PII  | Campo con información de identificación personal |
| 🟡 HASH | Campo PII enmascarado con SHA-256 desde capa Silver |
| 🟢      | Campo seguro para consumo analítico |
| ⚙️      | Campo calculado (no existe en la fuente origen) |

---

## Capa Silver — `finbank.silver`

### TB_CLIENTES_CORE

**Descripción:** Clientes activos e inactivos del banco digital. Los campos PII se enmascaran con SHA-256 antes de persistir en Silver.  
**Origen:** `dbo.TB_CLIENTES_CORE` (Azure SQL Database)  
**Formato:** Delta Lake | Partición: ninguna | PK: `id_cli`

| Campo | Tipo | Sensible | Descripción |
|-------|------|----------|-------------|
| id_cli | STRING | 🟢 | Identificador único del cliente |
| nomb_cli | STRING | 🟡 HASH | Nombre del cliente (SHA-256) |
| apell_cli | STRING | 🟡 HASH | Apellidos del cliente (SHA-256) |
| tip_doc | STRING | 🟢 | Tipo de documento (CC, CE, NIT, PP) |
| num_doc | STRING | 🟡 HASH | Número de documento (SHA-256) |
| fec_nac | DATE | 🟢 | Fecha de nacimiento |
| fec_alta | DATE | 🟢 | Fecha de vinculación al banco |
| cod_segmento | STRING | 🟢 | Segmento interno: BASICO, ESTANDAR, PREMIUM, ELITE |
| score_buro | INTEGER | 🟢 | Score de buró crediticio (0-950); 0 si no disponible |
| ciudad_res | STRING | 🟢 | Ciudad de residencia; DESCONOCIDO si nulo |
| depto_res | STRING | 🟢 | Departamento de residencia |
| estado_cli | STRING | 🟢 | Estado del cliente: ACTIVO, INACTIVO, BLOQUEADO |
| canal_adquis | STRING | 🟢 | Canal de adquisición; NO_INFORMADO si nulo |
| _ingestion_timestamp | TIMESTAMP | 🟢 | Marca de tiempo de ingesta en Bronze |
| _source_system | STRING | 🟢 | Sistema fuente: AzureSQL_FinBankCore |
| _batch_id | STRING | 🟢 | Identificador del lote de procesamiento |
| _silver_timestamp | TIMESTAMP | 🟢 | Marca de tiempo de procesamiento en Silver |

**Estrategia de nulos:** `score_buro` → 0; `ciudad_res` → 'DESCONOCIDO'; `canal_adquis` → 'NO_INFORMADO'

---

### TB_PRODUCTOS_CAT

**Descripción:** Catálogo de productos financieros del banco. Sin campos PII.  
**Origen:** `dbo.TB_PRODUCTOS_CAT` | **PK:** `cod_prod`

| Campo | Tipo | Sensible | Descripción |
|-------|------|----------|-------------|
| cod_prod | STRING | 🟢 | Código único del producto |
| desc_prod | STRING | 🟢 | Descripción larga del producto |
| tip_prod | STRING | 🟢 | Tipo: CREDITO, AHORRO, TRANSACCIONAL |
| tasa_ea | DOUBLE | 🟢 | Tasa efectiva anual (%) |
| plazo_max_meses | INTEGER | 🟢 | Plazo máximo en meses (0 para productos sin plazo) |
| cuota_min | DOUBLE | 🟢 | Cuota mínima en COP |
| comision_admin | DOUBLE | 🟢 | Comisión de administración mensual en COP |
| estado_prod | STRING | 🟢 | ACTIVO o DESCONTINUADO |

---

### TB_MOV_FINANCIEROS

**Descripción:** Transacciones financieras. Incluye flag de sospecha calculado por ventana estadística de 30 días.  
**Origen:** `dbo.TB_MOV_FINANCIEROS` | **PK:** `id_mov` | **Partición:** `_year / _month / _day`

| Campo | Tipo | Sensible | Descripción |
|-------|------|----------|-------------|
| id_mov | STRING | 🟢 | Identificador único del movimiento |
| id_cli | STRING | 🟢 | FK → TB_CLIENTES_CORE.id_cli |
| cod_prod | STRING | 🟢 | FK → TB_PRODUCTOS_CAT.cod_prod |
| num_cuenta | STRING | 🟢 | Número de cuenta; DESCONOCIDO si nulo |
| fec_mov | DATE | 🟢 | Fecha del movimiento |
| hra_mov | STRING | 🟢 | Hora del movimiento (HH:MM:SS) |
| vr_mov | DOUBLE | 🟢 | Valor del movimiento en COP |
| tip_mov | STRING | 🟢 | Tipo: PAGO, TRANSFERENCIA, RECARGA, AVANCE, COMPRA, RETIRO |
| cod_canal | STRING | 🟢 | Canal de la transacción |
| cod_ciudad | STRING | 🟢 | Ciudad donde ocurrió; DESCONOCIDO si nulo |
| cod_estado_mov | STRING | 🟢 | Estado: APROBADO, RECHAZADO, PENDIENTE, REVERSADO |
| id_dispositivo | STRING | 🟢 | ID del dispositivo; DESCONOCIDO si nulo |
| ind_sospechoso | INTEGER | 🟢 ⚙️ | 1 si vr_mov > avg_30d + 3σ del cliente; 0 si no |
| ind_horario_habil | INTEGER | 🟢 ⚙️ | 1 si ocurre lunes-viernes entre 06:00-19:59; 0 si no |

**Anomalías documentadas gestionadas:**
- Duplicados exactos: eliminados por (id_mov, fec_mov, vr_mov)
- Fechas fuera de rango (< 2000-01-01 o > 2030-12-31): enviadas a tabla de errores

---

### TB_OBLIGACIONES

**Descripción:** Créditos desembolsados y su estado de mora. Los dias_mora negativos (anomalía documentada) se corrigen a 0.  
**Origen:** `dbo.TB_OBLIGACIONES` | **PK:** `id_oblig`

| Campo | Tipo | Sensible | Descripción |
|-------|------|----------|-------------|
| id_oblig | STRING | 🟢 | Identificador único de la obligación |
| id_cli | STRING | 🟢 | FK → TB_CLIENTES_CORE.id_cli |
| cod_prod | STRING | 🟢 | FK → TB_PRODUCTOS_CAT.cod_prod |
| vr_aprobado | DOUBLE | 🟢 | Valor aprobado en COP |
| vr_desembolsado | DOUBLE | 🟢 | Valor efectivamente desembolsado en COP |
| sdo_capital | DOUBLE | 🟢 | Saldo de capital vigente en COP |
| vr_cuota | DOUBLE | 🟢 | Valor de la cuota periódica en COP |
| fec_desembolso | DATE | 🟢 | Fecha de desembolso |
| fec_venc | DATE | 🟢 | Fecha de vencimiento del crédito |
| dias_mora_act | INTEGER | 🟢 | Días en mora actuales (corregido: mínimo 0) |
| num_cuotas_pend | INTEGER | 🟢 | Cuotas pendientes de pago; 0 si nulo |
| calif_riesgo | STRING | 🟢 | Calificación: A, B, C, D, E; 'A' si nulo |

---

### TB_COMISIONES_LOG y TB_SUCURSALES_RED

Estructura análoga a las tablas anteriores. Sin campos PII. Ver fuente origen para detalle de campos.

---

## Capa Gold — `finbank.gold`

### dim_clientes

**Descripción:** Dimensión maestra de clientes con campos calculados de negocio.  
**PK:** `id_cli` | **Acceso Analista:** vía `vw_dim_clientes_analista` (PII adicional ofuscado)

| Campo | Tipo | ⚙️ | Descripción |
|-------|------|----|-------------|
| id_cli | STRING | | Identificador único del cliente |
| nombre_completo | STRING | ⚙️ | Concatenación nombre_hash + apellido_hash (texto hasheado) |
| nombre_hash | STRING | 🟡 | nomb_cli hasheado en Silver |
| apellido_hash | STRING | 🟡 | apell_cli hasheado en Silver |
| num_doc_hash | STRING | 🟡 | num_doc hasheado en Silver |
| tip_doc | STRING | | Tipo de documento |
| fec_nac | DATE | | Fecha de nacimiento |
| edad | INTEGER | ⚙️ | **Linaje:** FLOOR(DATEDIFF(current_date, fec_nac) / 365.25). Propósito: segmentación etaria para campañas y análisis de riesgo |
| fec_alta | DATE | | Fecha de vinculación |
| cod_segmento | STRING | | Código interno de segmento |
| segmento_label | STRING | ⚙️ | **Linaje:** mapeo de cod_segmento a etiqueta legible ('Básico (< 2 SMLV)', etc.). Propósito: reportes ejecutivos legibles |
| score_buro | INTEGER | | Score de buró (0-950) |
| ciudad_res | STRING | | Ciudad de residencia |
| depto_res | STRING | | Departamento de residencia |
| estado_cli | STRING | | Estado del cliente |
| ind_activo | INTEGER | ⚙️ | 1 si estado_cli = 'ACTIVO', 0 en caso contrario |
| canal_adquis | STRING | | Canal de adquisición |

---

### dim_productos

**Descripción:** Dimensión de productos con tasa mensual equivalente calculada.  
**PK:** `cod_prod`

| Campo | Tipo | ⚙️ | Descripción |
|-------|------|----|-------------|
| cod_prod | STRING | | Código del producto |
| desc_prod | STRING | | Descripción original |
| tip_prod | STRING | | CREDITO, AHORRO, TRANSACCIONAL |
| familia | STRING | | Igual a tip_prod (preparado para extensión) |
| tasa_ea | DOUBLE | | Tasa efectiva anual (%) |
| tasa_mensual_pct | DOUBLE | ⚙️ | **Linaje:** `((1 + tasa_ea/100)^(1/12) - 1) × 100`. Origen: TB_PRODUCTOS_CAT.tasa_ea. Propósito: cálculo de intereses mensuales en fact_rentabilidad_cliente |
| plazo_max_meses | INTEGER | | Plazo máximo |
| cuota_min | DOUBLE | | Cuota mínima |
| comision_admin | DOUBLE | | Comisión mensual |
| estado_prod | STRING | | ACTIVO / DESCONTINUADO |
| ind_activo | INTEGER | ⚙️ | 1 si estado_prod = 'ACTIVO' |

---

### dim_geografia

**Descripción:** Dimensión geográfica con ciudad, departamento y coordenadas de referencia.  
**PK:** `id_geo` (MD5 de ciudad + depto)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| id_geo | STRING | Hash MD5 de ciudad_depto |
| ciudad | STRING | Ciudad |
| depto | STRING | Departamento |
| latitud_ref | DOUBLE | Latitud promedio de los puntos en esa ciudad |
| longitud_ref | DOUBLE | Longitud promedio |
| n_puntos | LONG | Número de puntos de atención en la ciudad |

---

### dim_canal

**Descripción:** Dimensión de canales físicos y digitales unificados.  
**PK:** `id_canal` (MD5 de cod_canal)

| Campo | Tipo | Descripción |
|-------|------|-------------|
| id_canal | STRING | Hash MD5 del código de canal |
| cod_canal | STRING | Código del canal |
| tipo_canal | STRING | FISICO o DIGITAL |
| desc_canal | STRING | Descripción legible del canal |

---

### fact_transacciones

**Descripción:** Tabla de hechos de transacciones financieras con enriquecimiento analítico.  
**PK:** `id_mov` | **Partición:** `anio_mov / mes_mov`

| Campo | Tipo | ⚙️ | Descripción |
|-------|------|----|-------------|
| id_mov | STRING | | PK del movimiento |
| id_cli | STRING | | FK → dim_clientes |
| cod_prod | STRING | | FK → dim_productos |
| fec_mov | DATE | | Fecha del movimiento |
| anio_mov | STRING | ⚙️ | Año de la transacción (partición) |
| mes_mov | STRING | ⚙️ | Mes de la transacción 2 dígitos (partición) |
| vr_mov | DOUBLE | | Valor en COP |
| vr_mov_usd | DOUBLE | ⚙️ | **Linaje:** vr_mov × tasa_cop_usd (Key Vault). Propósito: comparación internacional de volúmenes |
| tip_mov | STRING | | Tipo de movimiento |
| cod_canal | STRING | | Canal |
| cod_ciudad | STRING | | Ciudad |
| cod_estado_mov | STRING | | Estado |
| cod_segmento | STRING | | Segmento del cliente |
| familia_producto | STRING | | Familia del producto |
| ind_sospechoso | INTEGER | | Heredado de Silver (regla 3σ) |
| ind_horario_habil | INTEGER | | Heredado de Silver |

---

### fact_cartera

**Descripción:** Tabla de hechos de obligaciones crediticias con clasificación regulatoria y provisiones.  
**PK:** `id_oblig` | **Partición:** `anio_desembolso / mes_desembolso`

| Campo | Tipo | ⚙️ | Descripción |
|-------|------|----|-------------|
| id_oblig | STRING | | PK de la obligación |
| id_cli | STRING | | FK → dim_clientes |
| cod_prod | STRING | | FK → dim_productos |
| sdo_capital | DOUBLE | | Saldo vigente en COP |
| dias_mora_act | INTEGER | | Días en mora |
| bucket_mora | STRING | ⚙️ | **Linaje:** clasificación de dias_mora_act en AL_DIA(0), RANGO_1(1-30), RANGO_2(31-60), RANGO_3(61-90), DETERIORADO(>90). Origen: TB_OBLIGACIONES.dias_mora_act. Propósito: segmentación de cartera para reportes de riesgo y Superintendencia Financiera |
| clasificacion_regulatoria | STRING | ⚙️ | **Linaje:** mapeo de dias_mora a calificación A/B/C/D/E según normativa SFC. Propósito: cumplimiento regulatorio |
| provision_estimada | DOUBLE | ⚙️ | **Linaje:** sdo_capital × porcentaje_provision[clasificacion_regulatoria]. Tabla: A=1%, B=3%, C=20%, D=50%, E=100%. Propósito: estimación de provisiones exigidas por Superintendencia Financiera Colombia |
| ind_en_mora | INTEGER | ⚙️ | 1 si dias_mora_act > 0 |
| bucket_mora_orden | INTEGER | ⚙️ | Ordinal numérico de bucket_mora para ordenamiento en dashboards |
| cod_segmento | STRING | | Segmento del cliente |
| ciudad_res | STRING | | Ciudad de residencia |
| depto_res | STRING | | Departamento |

---

### fact_rentabilidad_cliente

**Descripción:** CLTV y métricas de rentabilidad por cliente. Calculado mensualmente.  
**PK:** `id_cli`

| Campo | Tipo | ⚙️ | Descripción |
|-------|------|----|-------------|
| id_cli | STRING | | PK del cliente |
| cltv_12m | DOUBLE | ⚙️ | **Linaje:** SUM(comisiones_cobradas_12m + intereses_estimados_12m) agrupado por id_cli. Fuentes: TB_COMISIONES_LOG (estado_cobro=COBRADO) y TB_OBLIGACIONES × TB_PRODUCTOS_CAT.tasa_mensual. Propósito: score de rentabilidad para decisiones de oferta comercial y propensión de productos |
| total_comisiones_12m | DOUBLE | ⚙️ | Comisiones efectivamente cobradas en 12 meses |
| total_intereses_12m | DOUBLE | ⚙️ | Intereses estimados = sdo_capital × tasa_mensual |
| meses_activo | LONG | ⚙️ | Meses con al menos una transacción en los últimos 12 |
| cod_segmento | STRING | | Segmento del cliente |
| estado_cli | STRING | | Estado del cliente |
| periodo_calculo | STRING | ⚙️ | Mes de cálculo (yyyy-MM) |

---

### kpis_diarios_cartera

**Descripción:** KPIs ejecutivos de cartera agregados por fecha, producto, segmento y departamento. Optimizado para consumo directo en dashboards sin transformaciones adicionales.  
**PK lógico:** `fecha_calculo + cod_prod + cod_segmento + depto_res` | **Partición:** `anio_calculo / mes_calculo`

| Campo | Tipo | Descripción |
|-------|------|-------------|
| fecha_calculo | DATE | Fecha del cálculo |
| cod_prod | STRING | Producto financiero |
| tip_prod | STRING | Familia del producto |
| cod_segmento | STRING | Segmento de clientes |
| depto_res | STRING | Departamento de residencia |
| total_obligaciones_activas | LONG | Total de obligaciones en el corte |
| monto_total_cartera | DOUBLE | Suma de sdo_capital en COP |
| monto_en_mora | DOUBLE | Saldo de capital con dias_mora > 0 |
| provision_total_estimada | DOUBLE | Suma de provisiones estimadas |
| clientes_con_mora | LONG | Clientes distintos con al menos una obligación en mora |
| tasa_mora_pct | DOUBLE | monto_en_mora / monto_total_cartera × 100 |

---

## Tablas de control y errores

| Tabla / Path | Descripción |
|---|---|
| `bronze/_control/watermarks` | Último valor de watermark por tabla para ingesta incremental |
| `bronze/_control/ingestion_log` | Log de cada ejecución de ingesta: registros, tamaño, duración |
| `silver/_control/quality_log` | Reporte de calidad por tabla y batch: % conformes, nulos, rechazados |
| `errors/referential_integrity_errors` | Registros rechazados por integridad referencial con motivo documentado |
| `errors/pipeline_errors` | Excepciones capturadas durante la ejecución del pipeline |
| `errors/quality_check_results` | Resultados históricos de los 8 quality checks automatizados |
