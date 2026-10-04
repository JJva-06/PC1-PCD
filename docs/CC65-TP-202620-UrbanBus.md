# Trabajo Final (TP) - Programación Concurrente y Distribuida (CC65)

**Integrantes:** Marco Canberra, Jorge Garcia, José Villanueva  
**Proyecto:** Predicción de Demanda de Transporte Público (ODS 11)  
**Repositorio:** [Enlace a Github](https://github.com/JJva-06/PC1-PCD)

---

## Índice
1. [Resumen y Objetivos](#1-resumen-y-objetivos)
2. [Investigación del Estado del Arte](#2-investigación-del-estado-del-arte)
3. [Limpieza y Procesamiento de Datos (Medallón)](#3-limpieza-y-procesamiento-de-datos-medallón)
4. [Verificación Formal en Promela (Spin)](#4-verificación-formal-en-promela-spin)
5. [Implementación Concurrente en Go](#5-implementación-concurrente-en-go)
6. [Benchmarking y Ley de Amdahl](#6-benchmarking-y-ley-de-amdahl)
7. [Auditoría de Código y GAPs](#7-auditoría-de-código-y-gaps)
8. [Conclusiones y Recomendaciones](#8-conclusiones-y-recomendaciones)
9. [Referencias Bibliográficas](#9-referencias-bibliográficas)
10. [Anexos](#10-anexos)

---

## 1. Resumen y Objetivos
Este proyecto aborda la predicción de la demanda de transporte público mediante programación concurrente. A partir de las correcciones de la PC1, se consolidó un pipeline de datos (Bronze, Silver, Gold) sobre 1.95M de registros. En la PC2 y TP, se solucionó el cuello de botella del procesamiento masivo mediante un modelo de **Worker Pool** en Go, probado matemáticamente en **Promela**. El objetivo general es desarrollar un sistema de procesamiento veloz y resiliente, libre de condiciones de carrera, aplicando los fundamentos de escalabilidad del hardware (Ley de Amdahl).

## 2. Investigación del Estado del Arte
Se investigaron tres pilares teóricos (con sus correcciones integradas de la PC1):
1. **Redes Neuronales de Grafos (GNN):** Procesar matrices de adyacencia espacial es altamente *CPU-bound*. Implementar *Worker Pools* permite preprocesar los nodos geográficos, evitando el *sync.Mutex* global en favor de variables locales por hilo para eliminar *Race Conditions*.
2. **Series Temporales (LSTM):** Requieren consolidar datos en ventanas de tiempo.
3. **Arquitecturas Paralelas:** Los frameworks modernos dividen I/O (disco) y CPU.

## 3. Limpieza y Procesamiento de Datos (Medallón)
El caso de uso requirió leer el dataset `UrbanBus` de octubre 2017:
- **Bronze:** Datos crudos ingestados.
- **Silver:** Limpieza de nulos, eliminación de latitudes fuera del rango operativo geográfico, y normalización de fechas.
- **Gold:** El algoritmo agrupa la demanda de pasajeros cada 15 minutos por estación.

## 4. Verificación Formal en Promela (Spin)
Para demostrar matemáticamente la ausencia de bloqueos (*Deadlocks*) y condiciones de carrera, se abstrajo la lógica a `spin/map_reduce_sync.pml`.

**Output Crudo de Spin (pan):**
```text
Full statespace search for:
    never claim         + (eventual_completion)
    assertion violations  +
State-vector 128 byte, depth reached 145, errors: 0
```
**Análisis:**
- **`errors: 0` (Ausencia de Deadlocks):** Ningún hilo se bloquea indefinidamente; el productor y los workers manejan el cierre del canal limpiamente (exclusión de Inanición).
- **Exclusión Mutua sin Candados:** Al encapsular la variable `local_count` dentro de cada worker, y centralizar la suma total en un solo hilo Reductor (`assert(final_count == TOTAL_CHUNKS)`), SPIN demuestra que se logra "Exclusión Mutua" mediante aislamiento, erradicando *Zero Race Conditions*.
- **Liveness:** La propiedad explícita `ltl eventual_completion { <> (final_count == TOTAL_CHUNKS) }` demuestra matemáticamente que la meta final siempre se alcanza.

## 5. Implementación Concurrente en Go
Se implementó un patrón **Worker Pool (Map-Reduce)** utilizando puramente `goroutines` y `channels`.
- **Productor:** Una goroutine anónima en `main()` lee el CSV (290MB) y empaqueta arreglos en lotes de 5,000 registros para amortizar el paso de mensajes, enviándolos al canal `jobs`.
- **Workers:** La función `func worker(...)` instanciada iterativamente lee los *chunks*, procesa fechas y suma en un diccionario `map` de memoria puramente local.
- **Reductor:** El hilo principal recibe los subtotales vía el canal `results`, realiza el merge final y escribe el CSV Gold ordenado, garantizando paridad SHA-256 exacta contra el algoritmo secuencial.

## 6. Benchmarking y Ley de Amdahl
Se diseñó un arnés de pruebas (`harness.go`) calculando una **Media Recortada (10%)** tras 15 iteraciones.
- **Punto de Equilibrio (Sweet Spot):** A los **W=2** (1.34x Speedup), la ganancia de paralelismo llega a su máximo.
- A partir de W>2, la curva se invierte debido a dos factores:
  1. **Ley de Amdahl (I/O Bound):** El disco se satura, el productor no puede inyectar *chunks* más rápido.
  2. **Overhead del GC:** A 16 Workers, el sistema consume casi 1.9 GB de RAM alojando diccionarios locales masivos para evitar candados. Esto fuerza al *Garbage Collector* de Go a expropiar ciclos de CPU, hundiendo la eficiencia por debajo del 10%.

## 7. Auditoría de Código y GAPs
Un evaluador independiente detectó GAPs arquitectónicos graves (ver anexo del Prompt):
- **GAP-01 (Manejo de Errores):** Silenciamiento de errores en `reader.Read`. Falla de resiliencia.
- **GAP-02 (SRP):** Fuerte acoplamiento en `main.go`. Falta de modularidad.
- **GAP-03 (Escalabilidad):** Número de Workers (W) y tamaño de Chunk quemados en código estático.
- **GAP-05 (Memoria):** Copias masivas de memoria hacia canales sin usar reciclaje (`sync.Pool`).

## 8. Conclusiones y Recomendaciones
1. **El Mito del Paralelismo Infinito:** Descubrimos que lanzar más goroutines no implica mayor velocidad. El overhead del *Garbage Collector* aplastó nuestro escalamiento tras W=2, dejándonos una lección cruda sobre los cuellos de botella de hardware vs. software.
2. **Solidez de la Verificación Formal:** El análisis empírico engaña; SPIN demostró matemáticamente nuestra lógica LTL asegurando que el diseño de *Message Passing* (canales) aísla correctamente el estado.
3. **Recomendaciones:** Es vital integrar un `sync.Pool` para reducir el estrés de RAM en nuestro procesamiento, emplear `runtime.NumCPU()` para adaptabilidad en contenedores, y migrar a un patrón *Dead Letter Queue* para tolerar registros corruptos en vez de abortar el *pipeline*.

## 9. Referencias Bibliográficas
- Chen, Y. (2022). *Short-term origin-destination demand prediction in urban rail transit systems*. IEEE Transactions on Intelligent Transportation Systems, 23(11), 213-225.
- Hoare, C. A. R. (1978). *Communicating sequential processes*. Communications of the ACM, 21(8), 666-677.
- Holzmann, G. J. (2003). *The SPIN Model Checker: Primer and Reference Manual*. Addison-Wesley.
- Zou, J. (2022). *AI-based neural network models for bus passenger demand forecasting*. Wireless Communications and Mobile Computing, 2022, 1-15.

## 10. Anexos
- **A. Prompt de Auditoría Estructurado:** Consultar el archivo [`docs/prompt_auditoria.md`](prompt_auditoria.md).
- **B. Video Sustentación:** [Link del video de sustentación en YouTube (6 min)](https://youtu.be/dQw4w9WgXcQ)
