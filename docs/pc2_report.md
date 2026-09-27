# Informe PC2 — Procesamiento Concurrente de Datos (UrbanBus)

**Curso:** Programación Concurrente y Distribuida (CC65)  
**Proyecto:** Predicción de Demanda de Transporte Público (ODS 11)  

---

## 1. Contexto y Enlace con PC1
En la Práctica Calificada 1 (PC1) se implementó una arquitectura Medallón (Bronze → Silver → Gold). En esta PC2, aplicaremos procesamiento concurrente a la transformación más pesada de ese pipeline: **Silver a Gold**. 

El objetivo del algoritmo es leer `1,952,152` registros transaccionales validados (CSV Silver, ~290 MB), parsear las fechas, calcular ventanas temporales de 15 minutos (binning), y agregar métricas por estación de abordaje (conteo de pasajeros, servicios únicos y tipo de tarjeta dominante), para exportar el dataset Gold final.

---

## 2. Diseño del Algoritmo Concurrente (Go)

Dada la naturaleza del problema, se descartó el uso de un cerrojo global (`sync.Mutex`) por cada línea del CSV debido a la alta contención que generaría. En su lugar, se implementó un **Worker Pool con Reducción Local (Patrón Map-Reduce)**.

### Fases del Pipeline:
1. **Productor (Lector I/O):** Un único *Goroutine* lee el CSV secuencialmente. Para amortizar el costo de sincronización en los canales, empaqueta los registros en *Chunks* de 5,000 líneas y los envía a un canal de tareas (`jobs`). Al llegar al EOF, cierra el canal.
2. **Workers (Mapeadores):** Un *pool* estático de `W` *Goroutines* consume del canal `jobs`. Cada worker procesa sus lotes, convirtiendo los timestamps y agregando la demanda en un **mapa de memoria local**. 
   - *Trade-off:* Al usar memoria local, duplicamos el uso de RAM, pero logramos **0 contención (Zero Race Conditions)**, acelerando dramáticamente el paso por CPU.
3. **Reductor (Escritor I/O):** Al finalizar (coordinado vía `sync.WaitGroup`), los workers envían su mapa local por un canal `results`. El hilo principal unifica los diccionarios, ordena los resultados para garantizar determinismo, y escribe el CSV final secuencialmente.

---

## 3. Modelo de Sincronización en Promela
Para demostrar matemáticamente la ausencia de bloqueos (*Deadlocks*) y condiciones de carrera, se abstrajo la lógica de Go a `map_reduce_sync.pml`.

**Invariantes verificados en SPIN:**
- **No Race Conditions:** Los workers únicamente mutan una variable `local_count`. Nunca tocan la variable de memoria compartida global.
- **Drenado de Canales (No Deadlocks):** El productor inyecta *tokens* de EOF (`0`) en el canal de tamaño acotado de manera proporcional al número de workers, permitiendo un apagado limpio.
- **Correctitud Funcional:** Se introdujo una pre-condición y post-condición `assert(final_count == TOTAL_CHUNKS)`. Al estar el *Merge* centralizado en el *Reducer* de forma secuencial tras recibir los parciales por canales seguros, el flujo demuestra 100% de fiabilidad en todo entrelazado de hilos.

---

## 4. Metodología de Benchmarking

Las pruebas se automatizaron en un arnés (`harness.go`) diseñado para rigor estadístico:
- **Ground Truth:** Se verificó que el *hash SHA-256* de las salidas secuencial y concurrente fueran idénticos byte a byte.
- **Múltiples corridas:** Cada configuración se ejecutó **15 veces**.
- **Media Recortada (Trimmed Mean):** Para aislar latencias impredecibles del Sistema Operativo, se descartó el 10% superior e inferior de los tiempos de ejecución, promediando el resto.
- **Uso de Recursos:** Se inyectó `runtime.ReadMemStats(&m)` al final del binario para capturar la memoria alojada por el SO.

### Tabla de Resultados (CPU Cores: 12)

| Workers | Tiempo Recortado (ms) | Memoria Máx (MB) | Speedup | Eficiencia |
|---------|-----------------------|------------------|---------|------------|
| 1 (Seq) | 6,206.92 | 846.31 | 1.00x | - |
| 1 (Conc)| 5,446.08 | 1,095.00 | 1.14x | 113.97% |
| **2**   | **4,645.46** | 1,275.38 | **1.34x** | **66.81%** |
| 4       | 4,929.15 | 1,525.00 | 1.26x | 31.48% |
| 8       | 5,239.23 | 1,721.23 | 1.18x | 14.81% |
| 12      | 5,520.00 | 1,817.85 | 1.12x | 9.37% |
| 16      | 5,462.85 | 1,863.00 | 1.14x | 7.10% |

> *(Nota: La versión concurrente de 1 worker supera a la versión 100% secuencial debido al pipeline I/O-CPU: mientras el worker procesa en memoria, el hilo principal puede seguir leyendo el disco duro en paralelo).*

---

## 5. Análisis de Escalabilidad y Ley de Amdahl

Los resultados revelan dinámicas de concurrencia profundas, limitadas intrínsecamente por hardware y arquitectura:

1. **El "Sweet Spot":** El punto óptimo de la arquitectura se ubica en **W=2**. A partir de este umbral escalar el pool degrada el rendimiento.
2. **Cuello de Botella de I/O (Amdahl):** Según la Ley de Amdahl, el speedup teórico está asintóticamente limitado por la fracción estrictamente secuencial del algoritmo. En nuestro caso, leer ~290 MB del disco (Productor) y escribir la salida ordenada (Reductor) acapara gran parte del tiempo total de reloj. A partir de W=2, el procesamiento de cadenas satura la capacidad del lector de proveer *chunks*.
3. **Presión de Memoria y GC (Overhead):** El trade-off de crear diccionarios locales para evitar *Mutexes* es el enorme costo en RAM. La versión concurrente con 16 workers reserva casi **1.9 GB** de memoria del sistema, comparado con los 846 MB del código secuencial. La recolección de esta basura generada por las rutinas cortas (el *Garbage Collector* de Go) bloquea la ejecución de rutinas útiles, explicando por qué la curva se invierte y el tiempo sube a >5500 ms al agregar más hilos.
