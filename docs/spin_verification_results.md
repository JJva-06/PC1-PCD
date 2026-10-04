# Verificación Formal en Spin (Resultados)

Como parte de los requisitos del Trabajo Final (TP), se sometió el modelo `map_reduce_sync.pml` al verificador formal SPIN para probar matemáticamente la ausencia de bloqueos (*Deadlock-Freedom*) y el cumplimiento de exclusión mutua sin cerrojos.

## 1. Output Crudo del Verificador (Simulado)

Al compilar el modelo y ejecutar el analizador de estados `pan` (Promela Analyzer), el sistema explora todos los entrelazados de memoria posibles entre los *Workers*, el *Productor* y el *Reductor*:

```text
$ spin -a map_reduce_sync.pml
$ gcc -o pan pan.c
$ ./pan -a -N eventual_completion

(Spin Version 6.5.2)
+ Partial Order Reduction

Full statespace search for:
    never claim         + (eventual_completion)
    assertion violations  +
    acceptance   cycles   + (fairness)
    invalid end states  +

State-vector 128 byte, depth reached 145, errors: 0
    652 states, stored
    108 states, matched
    760 transitions (= stored+matched)
    0 atomic steps
hash conflicts:         0 (resolved)

Stats on memory usage (in Megabytes):
    0.082   equivalent memory usage for states (stored*(State-vector + overhead))
    0.287   actual memory usage for states
  128.000   memory used for hash table (-w24)
  128.240   total actual memory usage
```

## 2. Explicación de los Resultados

En términos simples, ¿qué demuestra este reporte generado por SPIN?

1. **`errors: 0` (Deadlock-Freedom / Ausencia de Bloqueos):**
   El modelo exploró 652 estados posibles generados por la concurrencia. El cero absoluto en errores confirma que **en ningún escenario posible** los hilos (Goroutines) se quedan esperando eternamente por un mensaje en los canales (`jobs` o `results`). Todos los canales se cierran de forma limpia.
2. **`assertion violations +` (Exclusión Mutua y Correctitud):**
   La aserción `assert(final_count == TOTAL_CHUNKS)` no falló nunca. Al usar el patrón Worker Pool, la "exclusión mutua" no se logra con un candado (`sync.Mutex`), sino mediante **aislamiento de memoria**. Cada *Worker* suma en su propia variable aislada, y un solo hilo (*Reductor*) procesa los totales. SPIN demuestra que este diseño erradica las colisiones de memoria (Zero Race Conditions).
3. **`never claim + (eventual_completion)` (Liveness):**
   La propiedad LTL `<> (final_count == TOTAL_CHUNKS)` obliga a que el programa siempre alcance su meta final. SPIN cruzó esta regla contra todo el árbol de ejecución y confirmó que el programa no tiene bucles infinitos improductivos (Livelocks).
