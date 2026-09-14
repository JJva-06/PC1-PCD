# Predicción de Demanda de Transporte Público con Machine Learning

> **PC1 — Procesamiento y Comunicación de Datos**  
> ODS 11 · Ciudades y Comunidades Sostenibles

Sistema de predicción de flujo de pasajeros en redes de transporte público urbano, utilizando datos transaccionales de tarjetas inteligentes AFC (*Automated Fare Collection*).

---

## Descripción del Proyecto

Este proyecto aborda la predicción espacio-temporal de demanda de pasajeros en estaciones de bus, procesando datos masivos del sistema UrbanBus (archivos individuales de 11-19 GB). El pipeline incluye:

1. **Ingesta eficiente** con Polars (evaluación lazy, bajo consumo de memoria)
2. **Análisis Exploratorio (EDA)** con visualizaciones de distribución espacial y temporal
3. **Limpieza** basada en reglas justificadas con literatura académica
4. **Feature Engineering** con agregación en ventanas de 15 minutos

---

## Estructura del Proyecto

```
PC1/
├── README.md
├── .gitignore
├── notebooks/
│   └── 01_exploracion_y_limpieza.ipynb   # Pipeline completo (Fases 1-4)
├── data/
│   └── processed/
│       ├── dataset_clean_15min.parquet    # Dataset limpio (formato optimizado)
│       └── dataset_clean_15min.csv        # Dataset limpio (formato compatible)
└── PAPERS/
    └── (10 papers de referencia + resumen)
```

---

## Dataset

### Fuente Original

| Propiedad | Detalle |
|-----------|---------|
| **Nombre** | UrbanBus |
| **Tipo** | Registros transaccionales AFC (tarjetas inteligentes) |
| **Período** | Octubre 2017 — Marzo 2018 (6 meses) |
| **Tamaño total** | ~80 GB (6 archivos CSV de 11-19 GB cada uno) |
| **Registros** | Decenas de millones de transacciones |

> **Dataset completo (Google Drive):**  
> [UrbanBus — Descargar aquí](https://drive.google.com/drive/folders/1M-zHSNxdp9NfYwu7H3F1uJtsAnyou6rP)

### Archivos Crudos (no incluidos en el repo por su tamaño)

| Archivo | Mes | Tamaño Aprox. |
|---------|-----|---------------|
| `BUS_DATA_OCT_2017.csv` | Octubre 2017 | ~11 GB |
| `BUS_DATA_NOV_2017.csv` | Noviembre 2017 | ~14 GB |
| `BUS_DATA_DEC_2017.csv` | Diciembre 2017 | ~13 GB |
| `BUS_DATA_JAN_2018.csv` | Enero 2018 | ~15 GB |
| `BUS_DATA_FEB_2018.csv` | Febrero 2018 | ~13 GB |
| `BUS_DATA_MAR_2018.csv` | Marzo 2018 | ~19 GB |

### Variables del Dataset Crudo

| Variable | Tipo | Descripción |
|----------|------|-------------|
| `Card_Number` | Int64 | ID anónimo de la tarjeta inteligente |
| `Card_Type` | String | Categoría (A = adulto, SC = senior, etc.) |
| `Travel_Mode` | String | Modo de transporte (Bus) |
| `Bus_Service_Number` | String | Línea de bus (hash anonimizado) |
| `Direction` | String | Dirección (Start = ida, Return = vuelta) |
| `Bus_Trip_Num` | String | ID del viaje |
| `Bus_Reg_Num` | String | Registro del vehículo |
| `Boarding_stop_stn` | String | **Estación de abordaje** |
| `Alighting_stop_stn` | String | Estación de descenso |
| `Ride_start_date` | String | Fecha de inicio (YYYY-MM-DD) |
| `Ride_start_time` | String | Hora de inicio (HH:MM:SS) |
| `Ride_end_date` | String | Fecha de fin (YYYY-MM-DD) |
| `Ride_end_time` | String | Hora de fin (HH:MM:SS) |

### Dataset Procesado (`data/processed/`)

Resultado del pipeline de limpieza, agregado en **ventanas de 15 minutos** por estación:

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `Boarding_stop_stn` | String | Estación de abordaje |
| `time_bin_15min` | Datetime | Inicio de la ventana temporal |
| `passenger_count` | UInt32 | **Conteo de pasajeros** (variable objetivo) |
| `unique_services` | UInt32 | Líneas de bus únicas en la ventana |
| `dominant_card_type` | String | Tipo de tarjeta más frecuente |
| `hour` | Int8 | Hora del día (5-22) |
| `day_of_week` | Int8 | Día de la semana (1=Lun, 7=Dom) |
| `date` | Date | Fecha del registro |

**Métricas del procesamiento:**

| Métrica | Valor |
|---------|-------|
| Muestra procesada | 2,000,000 registros |
| Tasa de retención | ~97.61% |
| Combinaciones estación × ventana | ~506,545 |
| Horario de operación filtrado | 05:00 – 23:00 |
| Tamaño Parquet | ~1.5 MB |
| Tamaño CSV | ~26 MB |

---

## Tecnologías

| Herramienta | Uso |
|-------------|-----|
| **Python 3.11** | Lenguaje principal |
| **Polars** | Procesamiento de datos a gran escala (lazy evaluation) |
| **Pandas** | Compatibilidad y visualización |
| **Matplotlib / Seaborn** | Visualizaciones del EDA |
| **Apache Parquet** | Formato de exportación optimizado |

---

## Cómo Ejecutar

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/JJva-06/PC1-PCD.git
   cd PC1-PCD
   ```

2. **Descargar el dataset crudo** (opcional, solo si deseas re-ejecutar el pipeline):
   - Descargar desde [Google Drive](https://drive.google.com/drive/folders/1M-zHSNxdp9NfYwu7H3F1uJtsAnyou6rP)
   - Colocar los archivos `.csv` en la carpeta `data/`

3. **Instalar dependencias:**
   ```bash
   pip install polars pandas numpy matplotlib seaborn
   ```

4. **Ejecutar el notebook:**
   ```bash
   jupyter notebook notebooks/01_exploracion_y_limpieza.ipynb
   ```

---

## Referencias

- Zou, X. et al. (2022). *Passenger Flow Prediction Using Smart Card Data from Connected Bus System Based on Interpretable XGBoost*
- He, Y. et al. (2022). *Multi-Graph Convolutional-Recurrent Neural Network (MGC-RNN) for Short-Term Forecasting of Transit Passenger Flow*
- Zhang, J. et al. (2021). *Short-term origin-destination demand prediction in urban rail transit systems: A channel-wise attentive split-convolutional neural network method*
- Wang, J. & Shalaby, A. (2025). *DST-TransitNet: A Dynamic Spatio-Temporal Model for Robust Station-Level Transit Ridership Prediction*
- Xiu, C. et al. (2024). *Correlation-based feature selection and parallel spatiotemporal networks for efficient passenger flow forecasting in metro systems*
- Zhai, X. & Shen, Y. (2023). *Short-Term Bus Passenger Flow Prediction Based on Graph Diffusion Convolutional Recurrent Neural Network*
- Wang, X. et al. (2024).*Large-Scale Origin–Destination Prediction for Urban Rail Transit Network Based on Graph Convolutional Neural Network*
- Talusan, J. et al. (2022). *On Designing Day Ahead and Same Day Ridership Level Prediction Models for City-Scale Transit Networks Using Noisy APC Data*
- Liyanage, S. et al. (2022). *AI-based neural network models for bus passenger demand forecasting using smart card data*
