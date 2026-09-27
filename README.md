# Prediccion de Demanda de Transporte Publico con Machine Learning

> **PC1 — Procesamiento y Comunicacion de Datos**  
> ODS 11 · Ciudades y Comunidades Sostenibles

Sistema de prediccion de flujo de pasajeros en redes de transporte publico urbano, utilizando datos transaccionales de tarjetas inteligentes AFC (*Automated Fare Collection*).

---

## Descripcion del Proyecto

Este proyecto aborda la prediccion espacio-temporal de demanda de pasajeros en estaciones de bus, procesando datos masivos del sistema UrbanBus (archivos individuales de 11-19 GB). El pipeline utiliza una **arquitectura de medallon** (Bronze → Silver → Gold) para garantizar trazabilidad y evidencia cuantitativa en cada etapa:

1. **Bronze** — Ingesta cruda con perfilado exhaustivo (schema, nulos, duplicados, rangos)
2. **Silver** — Limpieza con evidencia por regla (tabla acumulativa, muestras de descartados, grafico de cascada)
3. **Gold** — Agregacion en ventanas de 15 minutos + feature engineering temporal

---

## Estructura del Proyecto

```
PC1/
├── README.md
├── .gitignore
├── notebooks/
│   ├── 00_bronze_ingesta.ipynb         # Capa Bronze: ingesta y perfilado
│   ├── 01_silver_limpieza.ipynb        # Capa Silver: limpieza con evidencia
│   ├── 02_gold_agregacion.ipynb        # Capa Gold: agregacion y exportacion
│   └── 01_exploracion_y_limpieza.ipynb # Pipeline original (referencia/legacy)
├── data/
│   ├── bronze/                         # Parquet crudo (no versionado)
│   ├── silver/                         # Parquet limpio (no versionado)
│   │   └── discarded_samples/          # Muestras de registros descartados
│   ├── gold/                           # Dataset final agregado
│   │   ├── dataset_clean_15min.parquet
│   │   └── dataset_clean_15min.csv
│   └── processed/                      # Copia de compatibilidad
├── docs/
│   └── data_quality_report.md          # Reporte de calidad de datos
└── PAPERS/
    └── (10 papers de referencia + resumen)
```

---

## Pipeline de Datos (Arquitectura Medallon)

```
CSV crudo (~11 GB)
    │  Polars scan_csv lazy + head(2M)
    ▼
[BRONZE] 2,000,000 filas × 13 cols ── Perfilado: 100% completitud, 0 duplicados
    │
    ▼
[SILVER] 1,952,152 filas × 16 cols ── Limpieza: -47,848 registros (retencion 97.61%)
    │
    ▼
[GOLD]   506,545 filas × 8 cols   ── Agregacion por estacion × ventana 15 min
```

### Resumen de Limpieza (Cleaning Log)

| Regla | Descripcion | Eliminados | % |
|-------|-------------|----------:|---:|
| R1 | Conversion datetime | 0 | 0.00% |
| R2 | Nulos en columnas criticas | 0 | 0.00% |
| R3 | Filtro horario (05:00–23:00) | 47,848 | 2.39% |
| R4 | Transacciones duplicadas | 0 | 0.00% |
| **Total** | | **47,848** | **2.39%** |

> Ver detalles completos en [`docs/data_quality_report.md`](docs/data_quality_report.md)

---

## Dataset

### Fuente Original

| Propiedad | Detalle |
|-----------|---------|
| **Nombre** | UrbanBus |
| **Tipo** | Registros transaccionales AFC (tarjetas inteligentes) |
| **Periodo** | Octubre 2017 — Marzo 2018 (6 meses) |
| **Tamano total** | ~80 GB (6 archivos CSV de 11-19 GB cada uno) |

> **Dataset completo (Google Drive):**  
> [UrbanBus — Descargar aqui](https://drive.google.com/drive/folders/1M-zHSNxdp9NfYwu7H3F1uJtsAnyou6rP)

### Dataset Procesado (`data/gold/`)

Resultado del pipeline medallon, agregado en **ventanas de 15 minutos** por estacion:

| Columna | Tipo | Descripcion |
|---------|------|-------------|
| `Boarding_stop_stn` | String | Estacion de abordaje |
| `time_bin_15min` | Datetime | Inicio de la ventana temporal |
| `passenger_count` | UInt32 | **Conteo de pasajeros** (variable objetivo) |
| `unique_services` | UInt32 | Lineas de bus unicas en la ventana |
| `dominant_card_type` | String | Tipo de tarjeta mas frecuente |
| `hour` | Int8 | Hora del dia (5-22) |
| `day_of_week` | Int8 | Dia de la semana (1=Lun, 7=Dom) |
| `date` | Date | Fecha del registro |

---

## Tecnologias

| Herramienta | Uso |
|-------------|-----|
| **Python 3.11** | Lenguaje principal |
| **Polars** | Procesamiento de datos a gran escala (lazy evaluation) |
| **Pandas** | Compatibilidad y reportes tabulares |
| **Matplotlib / Seaborn** | Visualizaciones y evidencia grafica |
| **Apache Parquet** | Formato de persistencia entre capas |

---

## Como Ejecutar

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/JJva-06/PC1-PCD.git
   cd PC1-PCD
   ```

2. **Descargar el dataset crudo** (solo si se desea re-ejecutar desde Bronze):
   - Descargar desde [Google Drive](https://drive.google.com/drive/folders/1M-zHSNxdp9NfYwu7H3F1uJtsAnyou6rP)
   - Colocar los archivos `.csv` en la carpeta `data/`

3. **Instalar dependencias:**
   ```bash
   pip install polars pandas numpy matplotlib seaborn pyarrow
   ```

4. **Ejecutar el pipeline en orden:**
   ```bash
   # Capa Bronze: ingesta y perfilado
   jupyter notebook notebooks/00_bronze_ingesta.ipynb

   # Capa Silver: limpieza con evidencia
   jupyter notebook notebooks/01_silver_limpieza.ipynb

   # Capa Gold: agregacion y exportacion
   jupyter notebook notebooks/02_gold_agregacion.ipynb
   ```

---

## Referencias

- Zou, X. et al. (2022). *Passenger Flow Prediction Using Smart Card Data from Connected Bus Systems*
- Hao, S. et al. (2019). *Multi-Graph Convolutional-Recurrent Neural Network (MGC-RNN) for Short-Term Forecasting of Transit Passenger Flow*
- Liu, Y. et al. (2020). *Short-term origin-destination demand prediction in urban rail transit*
- DST-TransitNet (2023). *A Dynamic Spatio-Temporal Model for Robust Station-Level Transit Ridership Prediction*
- AI-based Neural Network Models for Bus Passenger Demand Forecasting