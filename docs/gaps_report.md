# Reporte de Auditoría Externa (Análisis de GAPs)

*Reporte generado por el Juez Auditor Externo a partir del Prompt Estructurado.*

## 1. Resumen de Hallazgos

| ID | Categoría | Severidad | Descripción Corta | Riesgo Principal |
|----|-----------|-----------|-------------------|------------------|
| **GAP-01** | Resiliencia / Errores | Alta | Silenciamiento de errores durante la lectura I/O (`reader.Read`). | Pérdida silenciosa de datos corruptos sin logs ni recuperación. |
| **GAP-02** | Calidad / SOLID | Media | Fuerte acoplamiento en `main.go` (Violación de Single Responsibility). | Difícil de testear unitariamente (lógica I/O mezclada con agregación). |
| **GAP-03** | Concurrencia | Alta | Hardcoding del número de Workers y tamaño de Chunks estático. | Subutilización de hardware en máquinas grandes; overhead en máquinas chicas. |
| **GAP-04** | Seguridad / Recursos | Media | Gestión de cierres de archivo con `defer` sin manejo de errores. | Fuga de descriptores de archivo en caso de un `panic` inesperado. |
| **GAP-05** | Concurrencia (Overhead) | Media | Clonación profunda de estructuras (Slices) hacia los canales. | Presión extrema sobre el *Garbage Collector* de Go por asignaciones en el Heap. |

## 2. Análisis Detallado y Recomendaciones

### GAP-01: Silenciamiento y Fragilidad en el Manejo de Errores (Error Handling)
- **Problema:** En el Goroutine del Productor, el código verifica `err == io.EOF` para cerrar el ciclo, pero si ocurre un `csv.ParseError` u otro error de I/O, el código no lo gestiona correctamente (suele hacer panic o descartar la línea).
- **Consecuencia:** En un dataset del mundo real, una sola fila malformada en el CSV de la capa Silver derrumbará todo el pipeline concurrente o generará pérdida de datos indetectable.
- **Recomendación:** Implementar el patrón **Dead Letter Queue (DLQ)**. Los errores de parseo deben ser capturados y enviados a un canal secundario `chan error_record` para guardarse en un archivo de logs, permitiendo que el pipeline siga procesando las líneas sanas.

### GAP-02: Violación del Principio de Responsabilidad Única (SRP)
- **Problema:** Toda la lógica del programa (apertura de archivos, parseo CSV, agregación de métricas de transporte, y concurrencia) está aglomerada en un único archivo `main.go` e incrustada en funciones monolíticas.
- **Consecuencia:** La lógica de agregación (el verdadero negocio) no es testeable de manera aislada sin depender de un archivo físico en disco.
- **Recomendación:** Refactorizar usando arquitectura limpia. Separar en paquetes: `io/` (para lectura/escritura CSV), `domain/` (para las reglas de agregación de UrbanBus), y `concurrency/` (para orquestar el Worker Pool).

### GAP-03: Hardcoding de Recursos Concurrentes
- **Problema:** El código asume un número fijo arbitrario de workers (`W=...`) y un tamaño de chunk (`5000`). 
- **Consecuencia:** Falta de elasticidad. Si este ejecutable se despliega en un contenedor Docker con 2 vCPUs o en un servidor Bare-Metal con 32 vCPUs, el rendimiento no escalará porque los valores están en el código fuente.
- **Recomendación:** Usar `runtime.NumCPU()` para calcular estocásticamente el tamaño inicial del pool, e inyectar el tamaño del chunk mediante variables de entorno (e.g., `os.Getenv("CHUNK_SIZE")`).

### GAP-04: Riesgo de Fugas (Resource Leaks)
- **Problema:** El uso de `defer file.Close()` asegura que el archivo intente cerrarse al salir de `main`, pero en Go los `defers` no gestionan los errores del cierre en sí mismo.
- **Consecuencia:** En un sistema de alta disponibilidad que abre miles de archivos diarios, un fallo silencioso al cerrar descriptores puede agotar los *File Descriptors* del OS.
- **Recomendación:** Capturar explícitamente el error del `defer`: `defer func() { if err := file.Close(); err != nil { log.Printf(...) } }()`.

### GAP-05: Presión por Asignación en el Heap (GC Overhead)
- **Problema:** El productor empaqueta las cadenas (`string`) del CSV y las envía a través del canal `jobs`. En Go, las strings son inmutables y generar millones de copias de arrays presiona fuertemente la memoria Heap.
- **Consecuencia:** Como se demostró en el benchmark de PC2, el recolector de basura (*GC*) se vuelve el cuello de botella.
- **Recomendación:** Adoptar el patrón **`sync.Pool`**. Reutilizar objetos de arreglos vacíos o buffers de bytes en lugar de alojar nueva memoria para cada chunk, mitigando el estrés sobre el *Garbage Collector*.
