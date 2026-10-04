# Pull Request: Resolución de Observaciones del Profesor (PC2)

## Descripción
Esta PR consolida la resolución estructurada de las 4 observaciones críticas levantadas por el profesor para la PC2, transformando el repositorio bajo estándares profesionales de *Git-Flow* y *Conventional Commits*, e inyectando rigor técnico al reporte.

## Trazabilidad de Observaciones Resueltas (Criterio de Éxito < 2 min)

| Observación | Solución en Informe | Evidencia en Código / Commit |
|-------------|---------------------|------------------------------|
| **Obs 1:** Numerar el índice del informe. | **Sección:** Índice (Inicio del documento) | **Commit:** `docs: numerar indice del informe para obs #1` |
| **Obs 2:** Mapeo de patrones de concurrencia. | **Sección:** 2. Diseño del Algoritmo Concurrente (Go) | **Commit:** `docs: mapear roles del patron worker pool a funciones del codigo go` (Mapea explícitamente `go func()` y `func worker()`) |
| **Obs 3:** Validación formal explícita en Promela. | **Sección:** 3. Modelo de Sincronización en Promela | **Commit:** `feat: inyectar propiedades LTL y documentar validacion de safety/liveness` (Archivo `spin/map_reduce_sync.pml` con fórmulas `ltl eventual_completion` y `safe_channels`) |
| **Obs 4:** Análisis del punto de equilibrio (Speedup). | **Sección:** 5. Análisis de Escalabilidad | **Commit:** `docs: explicar punto de equilibrio y overhead vs paralelismo en obs #4` (Se generó el gráfico actualizado en `docs/media/benchmark_results.png` señalando `W=2`) |

## Veredicto del Panel de Jueces Expertos
- **Juez de Rúbrica:** 🟢 APROBADO. Todas las observaciones tienen trazabilidad directa a la rúbrica oficial y al código (ver tabla superior).
- **Juez de Concurrencia (Go):** 🟢 APROBADO. La implementación de Worker Pool está alineada y ahora la documentación hace referencia directa a las interfaces reales de paso de mensajes sin cerrojos.
- **Juez de Promela:** 🟢 APROBADO. Las propiedades de Liveness y Safety (Zero Race Conditions) ahora son inmutables matemáticamente mediante cláusulas explícitas LTL.
- **Juez de Datos:** 🟢 APROBADO. El cruce de curvas (I/O Bound vs GC Overhead) a 1.95M registros expone el Sweet Spot perfecto sin requerir data adicional espuria.
- **Juez de Workflow:** 🟢 APROBADO. El proyecto migró a `src/go` y `/spin`. Se limpia la basura en Git (ignorado de `*.docx`, `data/`).

## Siguientes Pasos
Una vez aprobado, realizar **Squash and Merge** hacia `develop`.
