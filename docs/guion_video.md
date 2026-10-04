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
- **Arquitectura en Go:** "Para la PC2, descartamos usar un `sync.Mutex` global porque generaba alta contención. Implementamos un patrón *Worker Pool* con *Local Reduction*: un productor envía los registros por canales, y múltiples workers suman los datos en sus diccionarios locales de memoria."
- **Cero Condiciones de Carrera:** "Esto erradica las *Race Conditions*. Para probarlo matemáticamente, modelamos nuestro código en Promela y usamos el verificador SPIN."
- **Fórmulas LTL:** "Inyectamos fórmulas LTL para validar *Liveness* y *Safety*. El output de SPIN arrojó cero errores y ninguna aserción fallida, demostrando que nuestros canales acotados son libres de *Deadlocks* y logran exclusión mutua perfecta por diseño, sin candados."

### [4:00 - 6:00] José Villanueva: Benchmarking, GAPs y Conclusiones
*(Slide: Gráfico de Speedup y Ley de Amdahl)*
- **Análisis de Rendimiento:** "Al correr el benchmark con una media recortada, hallamos que el *Sweet Spot* es con apenas 2 Workers (1.34x de Speedup). A partir de ahí, la ganancia cae drásticamente. Esto ilustra la Ley de Amdahl: nuestro cuello de botella es la lectura/escritura del disco duro (I/O Bound)."
- **Auditoría (GAPs):** "Además, corrimos una auditoría externa automatizada que arrojó 5 GAPs. El más crítico es la presión de memoria: a 16 workers, consumimos 1.9 GB de RAM, forzando al Garbage Collector de Go a expropiar ciclos de CPU y hundiendo la eficiencia."
- **Recomendaciones:** "Como conclusión, demostramos teórica y matemáticamente nuestro diseño concurrente, pero para llevarlo a un entorno Cloud productivo, recomendamos implementar un `sync.Pool` para reciclar memoria, usar variables de entorno para los workers, y un patrón *Dead Letter Queue* para ser tolerantes a errores de parseo. Muchas gracias."

---
> ⚠️ **RECORDATORIO PARA EL EQUIPO:** Graben el video en Zoom/Teams, súbanlo a YouTube (Oculto) o Google Drive (Público), y **peguen el enlace en el Anexo B del documento `tp_reporte_final.md` antes de enviar el PDF al profesor.**
