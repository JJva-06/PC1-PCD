# Conocimiento Oficial del Curso CC65 (Programación Concurrente y Distribuida)

Este documento extrae y consolida los requisitos, estructura y expectativas del curso en base a la exploración cruzada de repositorios y las observaciones directas del profesor.

## 1. Enunciado / Rúbrica Oficial (Verificado)

A partir de la estructura requerida y las observaciones del profesor, la PC2 debe incluir estrictamente:

- **Estructura del Informe (Trazabilidad y Formato)**
  - El índice del documento debe estar enumerado numéricamente (`#`).
  - Se debe iniciar retomando explícitamente los puntos de la PC1 y sus correcciones.
  - Secciones obligatorias detectadas: Correcciones PC1, Modelado Promela, Implementación Go, Sincronización (Patrones), Análisis de Speedup, Uso de Recursos, Trabajo en Git.

- **Concurrencia en Go y Explicación de Patrones**
  - Obligatorio usar patrones de concurrencia de Go (Worker Pool, Pipelines) y mecanismos nativos (Goroutines, Channels, `sync.Mutex`, `sync.WaitGroup`).
  - **Exigencia clave:** Explicar *CÓMO* se aplican los patrones de concurrencia mapeando partes concretas del código a los roles del patrón (e.g., quién es el *producer*, quién es el *worker*, qué hace cada *stage* del pipeline), no solo nombrarlos.

- **Verificación Formal (Promela/SPIN)**
  - No basta con compilar y ejecutar el modelo sin que falle.
  - **Exigencia clave:** Se debe evidenciar el *CÓMO* se valida la ausencia de condiciones de carrera mediante el uso explícito de `assert(...)` (aserciones) o propiedades LTL (Linear Temporal Logic). Si el modelo original no las poseía, deben agregarse.

- **Análisis de Rendimiento y Recursos**
  - El análisis no puede limitarse a mostrar una tabla de resultados.
  - **Exigencia clave:** Se debe demostrar y explicar la Ley de Amdahl aplicada, hallando el **punto de equilibrio** donde el overhead de coordinación por channels/goroutines supera la ganancia de paralelismo. Requiere un análisis del Speedup vs. #Workers.

## 2. Solución de Otro Grupo (No Verificado / Solo Inspiración)

Las siguientes prácticas provienen del repo `concurrente` y `project-kit` de otro equipo. *No se deben copiar literalmente, pero inspiran el workflow de nuestro proyecto:*

- **Estructura y Herramientas**
  - Tienen una separación clara de directorios: `/docs`, `/scripts`, `/src`, `/spin` (modelos Promela aislados), `/tests`.
  - Emplean automatización para compilar métricas y LaTeX (`compilar.sh`, `analisis_benchmark.py`).
  
- **Workflow y Git (Project-Kit)**
  - Uso estricto de **Conventional Commits** (`feat:`, `chore:`, `docs:`, `fix:`).
  - Estrategia de ramificación clara: `develop` como rama de integración, `feature/*` para nuevas entregas, `hotfix/*` para correcciones urgentes.
  - Integración mediante Pull Requests (PRs), documentadas y enlazadas en el historial del informe.
  - Cuidado de los datos crudos (Uso intensivo de `.gitignore` para artefactos de compilación y datos transaccionales, evitando saturar el repo).
