# Reporte de Calidad de Datos — Pipeline Medallon

**Proyecto:** Prediccion de Demanda de Transporte Publico — ODS 11  
**Dataset:** UrbanBus (registros AFC, Octubre 2017)  
**Muestra procesada:** 2,000,000 registros de `BUS_DATA_OCT_2017.csv` (~11 GB)  
**Fecha de generacion:** Septiembre 2026

---

## 1. Arquitectura del Pipeline

El procesamiento de datos sigue la arquitectura de **medallon** (Bronze → Silver → Gold),
donde cada capa tiene una responsabilidad unica y persiste su salida en Apache Parquet
para trazabilidad y reproducibilidad.

```
CSV crudo (~11 GB)
    │  scan_csv lazy + head(2M)
    ▼
[BRONZE] bus_data_oct2017_raw.parquet ── 2,000,000 filas × 13 cols
    │  read_parquet                      (foto fiel del dato crudo)
    ▼
[SILVER] bus_data_oct2017_clean.parquet ── 1,952,152 filas × 16 cols
    │  read_parquet                        (dato limpio + muestras de descartados)
    ▼
[GOLD] dataset_clean_15min.parquet ── 506,545 filas × 8 cols
       dataset_clean_15min.csv         (agregado por estacion × ventana 15 min)
```

| Capa | Notebook | Filas | Columnas | Tamano Parquet |
|------|----------|------:|--------:|---------------:|
| Bronze | `00_bronze_ingesta.ipynb` | 2,000,000 | 13 | 30.50 MB |
| Silver | `01_silver_limpieza.ipynb` | 1,952,152 | 16 | 60.95 MB |
| Gold | `02_gold_agregacion.ipynb` | 506,545 | 8 | 1.52 MB |

---

## 2. Perfilado del Dato Crudo (Capa Bronze)

### 2.1 Esquema del dataset

| Columna | Tipo | Valores unicos | Ejemplo |
|---------|------|---------------:|---------|
| `Card_Number` | Int64 | 2,000,000 | 0, 1, 2, ... |
| `Card_Type` | String | 4 | A, SC, ... |
| `Travel_Mode` | String | 1 | Bus |
| `Bus_Service_Number` | String | 62 | SER_1253, SER_dccb |
| `Direction` | String | 2 | Start, Return |
| `Bus_Trip_Num` | String | 55 | TRIP_7b1b |
| `Bus_Reg_Num` | String | 2,535 | REG_4caf |
| `Boarding_stop_stn` | String | 761 | EJ_2, MH_2, IU_1 |
| `Alighting_stop_stn` | String | 758 | AGV_1, I_4 |
| `Ride_start_date` | String | 31 | 2017-10-01 |
| `Ride_start_time` | String | 70,295 | 21:32:41 |
| `Ride_end_date` | String | 32 | 2017-10-31 |
| `Ride_end_time` | String | 70,279 | 21:35:21 |

### 2.2 Metricas de calidad del dato crudo

| Metrica | Valor |
|---------|-------|
| Total de registros | 2,000,000 |
| Columnas con valores nulos | **0 de 13** |
| Completitud global | **100.00%** |
| Duplicados exactos (todas las columnas) | **0** (0.0000%) |
| Rango de fechas | 2017-10-01 a 2017-10-31 |
| Dias cubiertos | 31 |
| Estaciones de abordaje unicas | 761 |
| Lineas de bus unicas | 62 |

> **Observacion:** El dataset presenta una completitud del 100% y cero duplicados
> exactos en la muestra de 2M registros, lo que indica un sistema AFC de alta
> calidad en la captura de datos. No obstante, las reglas de limpieza se aplican
> de forma preventiva para garantizar robustez ante otros meses que pudieran
> presentar problemas de calidad.

---

## 3. Proceso de Limpieza (Capa Silver)

### 3.1 Reglas aplicadas y justificacion

| Regla | Descripcion | Justificacion academica |
|-------|-------------|------------------------|
| **R1** | Conversion de `Ride_start_date` + `Ride_start_time` a campo `datetime` unificado | Prerequisito tecnico para operaciones espacio-temporales (extraccion de hora, agrupacion en ventanas) |
| **R2** | Eliminacion de registros con nulos en `Boarding_stop_stn` o `ride_start_datetime` | Sin coordenada espacial o temporal, un registro es inservible para prediccion de demanda (Liu et al., 2020; Zou et al., 2022) |
| **R3** | Filtrado de registros fuera del horario comercial (05:00–23:00) | Los outliers temporales distorsionan los patrones regulares de demanda aprendidos por el modelo (Hao et al., 2019 — MGC-RNN; DST-TransitNet, 2023) |
| **R4** | Eliminacion de transacciones duplicadas (clave: `Card_Number`, `Boarding_stop_stn`, `ride_start_datetime`, `Bus_Service_Number`) | Los duplicados AFC inflan artificialmente el conteo de pasajeros por errores de doble validacion o retransmision |

### 3.2 Tabla acumulativa de limpieza (Cleaning Log)

| Regla | Descripcion | Antes | Despues | Eliminados | % Eliminado |
|-------|-------------|------:|--------:|-----------:|------------:|
| R1 | Conversion datetime | 2,000,000 | 2,000,000 | 0 | 0.0000% |
| R2 | Nulos en columnas criticas | 2,000,000 | 2,000,000 | 0 | 0.0000% |
| R3 | Filtro horario (05:00–23:00) | 2,000,000 | 1,952,152 | **47,848** | **2.3924%** |
| R4 | Transacciones duplicadas | 1,952,152 | 1,952,152 | 0 | 0.0000% |
| **TOTAL** | | **2,000,000** | **1,952,152** | **47,848** | **2.3924%** |

**Tasa de retencion: 97.61%**

### 3.3 Desglose de registros descartados por R3 (horario)

La regla R3 es la unica que elimina registros en esta muestra. El desglose por hora
de los 47,848 registros descartados es:

| Hora | Registros descartados | Interpretacion |
|------|----------------------:|----------------|
| 00:00 | 8,209 | Ultimo servicio del dia anterior o errores de fecha |
| 01:00 | 220 | Servicio nocturno residual |
| 02:00 | 2 | Registro anomalo |
| 03:00 | 1 | Registro anomalo |
| 04:00 | 4 | Registro anomalo |
| 23:00 | 39,412 | Ultimo tramo del servicio nocturno |

> **Nota:** El 82.4% de los descartados (39,412) corresponden a la hora 23:00,
> que esta en el limite del horario comercial. Los registros de madrugada
> (00:00–04:00) suman 8,436 y son consistentes con el cierre gradual del
> servicio nocturno.

### 3.4 Muestras de registros descartados

Se persisten muestras (hasta 20 filas) de los registros eliminados por cada regla
en `data/silver/discarded_samples/` para auditoria posterior:

| Archivo | Regla | Filas |
|---------|-------|------:|
| `discarded_r3_horario.parquet` | R3 (horario) | 20 |

> Las reglas R2 y R4 no generaron archivos de descartados porque no eliminaron
> registros en esta muestra.

### 3.5 Evidencia visual

Los notebooks Silver generan dos graficos de evidencia:

1. **Grafico de cascada (waterfall):** Muestra la reduccion progresiva del volumen
   de datos a medida que se aplica cada regla. Las barras rojas indican reglas con
   eliminaciones, las grises reglas sin efecto, y la barra verde el resultado final.

2. **Grafico doble de barras:** Presenta el impacto de cada regla en valores absolutos
   (panel izquierdo) y porcentuales (panel derecho), permitiendo comparar la
   magnitud relativa de cada regla.

---

## 4. Agregacion y Feature Engineering (Capa Gold)

### 4.1 Granularidad temporal: ventanas de 15 minutos

La agregacion se realiza truncando el timestamp al intervalo de 15 minutos mas
cercano hacia abajo (`dt.truncate('15m')`). Esta granularidad es el estandar optimo
en la literatura de prediccion de demanda de transporte publico:

- **Zou et al. (2022):** Predecir segundo a segundo genera matrices espacio-temporales
  de dimensionalidad intratable.
- **Hao et al. (2019) — MGC-RNN:** Ventanas de 15 minutos se alinean con los
  intervalos de despacho operativo.
- **Liu et al. (2020):** Ventanas menores (1–5 min) producen ruido estocastico;
  ventanas mayores (30–60 min) suavizan picos relevantes.
- **DST-TransitNet (2023):** Valida experimentalmente la superioridad de 15 min
  frente a otras granularidades.

### 4.2 Esquema del dataset Gold

| Columna | Tipo | Descripcion |
|---------|------|-------------|
| `Boarding_stop_stn` | String | Identificador de la estacion de abordaje |
| `time_bin_15min` | Datetime | Inicio de la ventana temporal de 15 minutos |
| `passenger_count` | UInt32 | **Conteo de pasajeros** (variable objetivo para ML) |
| `unique_services` | UInt32 | Numero de lineas de bus unicas en la ventana |
| `dominant_card_type` | String | Tipo de tarjeta mas frecuente en la ventana |
| `hour` | Int8 | Hora del dia (5–22) |
| `day_of_week` | Int8 | Dia de la semana (1=Lunes, 7=Domingo) |
| `date` | Date | Fecha del registro |

### 4.3 Validacion de reproducibilidad

| Metrica | Esperado (pipeline original) | Obtenido (medallon) | Estado |
|---------|-----------------------------:|--------------------:|--------|
| Filas | 506,545 | 506,545 | OK |
| Columnas | 8 | 8 | OK |
| Nombres de columnas | `[Boarding_stop_stn, ...]` | Coinciden | OK |
| Top estacion por demanda | EJ_2 | EJ_2 | OK |
| Tasa de retencion Silver | 97.61% | 97.61% | OK |

---

## 5. Referencias Bibliograficas

1. Zou, X. et al. (2022). *Passenger Flow Prediction Using Smart Card Data from Connected Bus Systems.* Wireless Communications and Mobile Computing.
2. Hao, S. et al. (2019). *Multi-Graph Convolutional-Recurrent Neural Network (MGC-RNN) for Short-Term Forecasting of Transit Passenger Flow.*
3. Liu, Y. et al. (2020). *Short-term origin-destination demand prediction in urban rail transit.*
4. DST-TransitNet (2023). *A Dynamic Spatio-Temporal Model for Robust Station-Level Transit Ridership Prediction.*
5. AI-based Neural Network Models for Bus Passenger Demand Forecasting.
