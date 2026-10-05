# Guion de Video - Sustentación TP (Máx. 6 minutos)

**Integrantes:** Jorge Garcia, José Villanueva  
**Pauta:** Hablar pausado, apoyarse en gráficos (Speedup, Spin, Diagrama de Pipeline).

---

### [0:00 - 3:00] Jorge Garcia: Contexto, Arquitectura de Datos y Concurrencia
*(Slide: Arquitectura Medallón y Diseño de Worker Pool)*
- **Saludo y Contexto:** "Hola, somos el equipo encargado de la predicción de demanda para transporte público, enfocado en el ODS 11 de Ciudades Sostenibles. Nuestro objetivo es procesar masivamente transacciones de tarjetas (dataset UrbanBus)."
- **El Pipeline (PC1):** "Implementamos un pipeline Medallón: Bronze para ingesta, Silver para limpieza estricta (filtrando nulos y coordenadas inválidas), y Gold para agrupar la demanda en ventanas de 15 minutos."
- **El Problema:** "El procesamiento secuencial de casi 2 millones de registros (290 MB) era un cuello de botella ineficiente. Necesitábamos concurrencia pura."
- **Arquitectura en Go:** "Para resolverlo, descartamos usar un `sync.Mutex` global porque generaba altísima contención. Implementamos un patrón *Worker Pool* con *Local Reduction*: un productor envía los registros por canales, y múltiples workers suman los datos en diccionarios aislados en memoria, erradicando por completo las condiciones de carrera."

### [3:00 - 6:00] José Villanueva: Verificación Formal, Optimización y Conclusiones
*(Slide: Output de SPIN, Gráfico de Speedup y Sharding)*
- **Verificación Formal (SPIN):** "Para probar matemáticamente nuestro diseño concurrente, modelamos el código en Promela y usamos el verificador SPIN. Inyectamos fórmulas LTL para validar *Liveness* y *Safety*. El output demostró cero errores, confirmando que nuestros canales acotados son libres de *Deadlocks* y logran exclusión mutua perfecta por diseño."
- **Benchmarking y Amdahl:** "Inicialmente, nuestra aceleración se estancaba rápido. Al aplicar un *profiling* avanzado, descubrimos un cuello de botella de CPU: la reducción secuencial masiva al final. Para solucionarlo, implementamos **Sharding Estático** con $N$ reducers paralelos (hasheando por estación y tiempo)."
- **Auditoría y Mejoras:** "Además, mitigamos la presión de memoria implementando un `sync.Pool` para reciclar estructuras y evadir al Garbage Collector, logrando nuestro mejor tiempo con **4 Workers y 8 Reducers (1.56x de Speedup)**."
- **Conclusiones:** "En conclusión, logramos transformar nuestro proyecto en un pipeline concurrente validado matemáticamente, tolerante a fallos (`io.EOF`), y altamente optimizado. Muchas gracias."

---
> 💡 **RECORDATORIO PARA EL EQUIPO:** Graben el video en Zoom/Teams, súbanlo a YouTube (Oculto) o Google Drive (Público), y **peguen el enlace en la portada de su informe final** antes de enviarlo al profesor.
