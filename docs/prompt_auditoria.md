# Anexo: Prompt Estructurado para Auditoría de Código

El siguiente *prompt* fue diseñado utilizando técnicas de ingeniería de prompts (asignación de rol, restricciones de salida, contexto específico) para ejecutar la evaluación automatizada del código del repositorio, tal como exige la rúbrica del Trabajo Final.

```markdown
# CONTEXTO
Eres un Ingeniero de Software Principal y Auditor de Seguridad con experiencia en Go (Golang) y arquitecturas distribuidas. Se te ha entregado el código fuente de un proyecto universitario (procesamiento concurrente de 2M de registros CSV usando el patrón Worker Pool).

# TAREA
Realiza una auditoría exhaustiva y estricta del código Go (`src/go/concurrent/main.go`). No evalúes si "funciona" (ya sabemos que compila y da el resultado correcto), evalúa **CÓMO** está construido. Debes encontrar GAPs (brechas) reales y críticos. Un reporte sin hallazgos negativos será rechazado.

# ÁREAS DE EVALUACIÓN (Obligatorio)
1. **Calidad de Código y Deuda Técnica:** modularidad, principios SOLID, inyección de dependencias, hardcoding.
2. **Seguridad y Resiliencia:** manejo de errores, fugas de recursos (memory/file leaks), validación de inputs.
3. **Patrones de Concurrencia:** cuellos de botella no evidentes, overhead, riesgos de escalabilidad, sincronización.

# FORMATO DE SALIDA
Genera un documento Markdown con una tabla resumen de los hallazgos categorizados por severidad (Alta, Media, Baja), seguido de una explicación técnica detallada de cada GAP y la recomendación de refactorización. Usa un tono objetivo, crudo y directamente técnico.
```
