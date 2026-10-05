# Guion de Video - Sustentación TP (Máx. 6 minutos)

**Integrantes:** Marco Canberra, Jorge Garcia, José Villanueva  
**Pauta:** Hablar pausado, apoyarse en gráficos (Speedup, Spin, Diagrama de Pipeline).

---

### [0:00 - 2:00] Marco Canberra: Contexto y Arquitectura de Datos
*(Slide: Arquitectura Medallón y Limpieza)*
- **Saludo:** "Hola, somos el equipo encargado de la predicción de demanda para transporte público, enfocado en el ODS 11 de Ciudades Sostenibles. Nuestro objetivo es procesar masivamente millones de transacciones de tarjetas (dataset UrbanBus) para entrenar modelos predictivos."
- **PC1 - Datos:** "En la primera entrega implementamos un pipeline Medallón. En la capa Bronze ingestamos la data cruda. En la capa Silver aplicamos limpieza estricta: eliminamos nulos, filtramos coordenadas fuera de ruta y unificamos fechas. Finalmente, la capa Gold agrupa la demanda en ventanas de 15 minutos."
- **El Problema:** "Sin embargo, el procesamiento secuencial de casi 2 millones de registros (290 MB) era un cuello de botella ineficiente en Go. Necesitábamos concurrencia pura."

### [2:00 - 4:00] Jorge Garcia: Concurrencia y Verificación Formal (Spin)
*(Slide: Patrón Worker Pool y Output de SPIN)*
- **Arquitectura en Go:** "Para resolverlo, descartamos usar un `sync.Mutex` global porque generaba alta contención. Implementamos un patrón *Worker Pool* con *Local Reduction*: un productor envía los registros por canales, y múltiples workers suman los datos en diccionarios aislados en memoria."
- **Cero Condiciones de Carrera:** "Esto erradica las *Race Conditions*. Para probarlo matemáticamente, modelamos nuestro código en Promela y usamos el verificador SPIN."
- **Fórmulas LTL:** "Inyectamos fórmulas LTL para validar *Liveness* y *Safety*. El output de SPIN arrojó cero errores, demostrando que nuestros canales acotados son libres de *Deadlocks* y logran exclusión mutua perfecta por diseño."

### [4:00 - 6:00] José Villanueva: Benchmarking, Refactorización y Conclusiones
*(Slide: Gráfico de Speedup y Sharding)*
- **Análisis de Rendimiento y Amdahl:** "Inicialmente, nuestro límite de aceleración (Speedup) se estancaba pronto. Al aplicar un *profiling* avanzado, descubrimos un cuello de botella de CPU: la reducción secuencial de los diccionarios. Para solucionarlo, implementamos **Sharding Estático** con $N$ reducers paralelos (hasheando por estación y tiempo)."
- **Auditoría y Correcciones:** "Adicionalmente, corregimos brechas técnicas: implementamos un `sync.Pool` para reciclar memoria y evitar que el Garbage Collector nos quite ciclos de CPU, logrando nuestro mejor tiempo con **4 Workers y 8 Reducers (1.56x de Speedup)**."
- **Conclusiones:** "En conclusión, logramos transformar nuestro proyecto en un pipeline concurrente validado matemáticamente, tolerante a fallos mediante control estricto de errores (`io.EOF`), y con una estructura limpia orientada a microservicios. Muchas gracias."

---
> 💡 **RECORDATORIO PARA EL EQUIPO:** Graben el video en Zoom/Teams, súbanlo a YouTube (Oculto) o Google Drive (Público), y **peguen el enlace en la portada de su informe final** antes de enviarlo al profesor.
