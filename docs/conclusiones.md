# Conclusiones y Recomendaciones

## Conclusiones del Proyecto

1. **Paralelismo vs. Overhead (Ley de Amdahl en la práctica):** 
   A lo largo del proyecto, descubrimos que lanzar más hilos (Goroutines) no equivale automáticamente a más velocidad. El hito más revelador de la PC2 fue graficar nuestro rendimiento y descubrir que el "Sweet Spot" estaba en apenas 2 workers. El intento de evitar candados globales (`sync.Mutex`) mediante la asignación de memoria local por worker fue brillante matemáticamente (0 colisiones probadas en SPIN), pero físicamente castigó al *Garbage Collector* de Go. Aprendimos que el diseño concurrente es un juego constante de *trade-offs* entre procesamiento, I/O y memoria RAM.

2. **La Verificación Formal es Indispensable:** 
   Escribir el código en Go y ver que "funcionaba bien" y arrojaba el hash SHA-256 correcto nos daba una falsa sensación de seguridad. Abstraer nuestro algoritmo a Promela (SPIN) e inyectar fórmulas LTL nos enseñó que la ausencia de *Deadlocks* y *Race Conditions* no debe suponerse mediante pruebas empíricas, sino demostrarse con rigor matemático sobre todos los estados de ejecución posibles.

3. **Inmadurez del Código frente a Entornos de Producción:**
   Si bien cumplimos el objetivo algorítmico, el reporte del Auditor Externo (mediante IA) nos abrió los ojos sobre la deuda técnica de nuestra solución. Aunque es concurrente, nuestro código es frágil. Silenciar errores de I/O en la capa *Silver* o hardcodear los recursos (como el número de workers = `W`) son prácticas que harían colapsar el pipeline en un clúster en la nube.

## Recomendaciones para Trabajo Futuro

1. **Implementación de `sync.Pool` para Aliviar el GC:**
   Dado que el cuello de botella actual es la presión sobre la memoria, recomendamos refactorizar el envío de "Chunks". En lugar de asignar memoria nueva constantemente para cada lote de 5,000 registros, deberíamos utilizar un `sync.Pool` de Golang para reciclar los arreglos y aliviar el trabajo del recolector de basura.

2. **Dinamismo en la Escalabilidad:**
   Se recomienda eliminar el *hardcoding* de la cantidad de hilos y reemplazarlo por la consulta `runtime.NumCPU()`. Adicionalmente, el tamaño de los lotes (*chunks*) debería poder parametrizarse vía variables de entorno, permitiendo que la aplicación se ajuste automáticamente al tamaño del contenedor Docker donde se despliegue.

3. **Manejo Resiliente de Errores (Dead Letter Queue):**
   Debe refactorizarse el *Productor* para que, frente a registros CSV malformados, no aborte el procesamiento ni descarte datos en silencio. Se recomienda introducir el patrón *Dead Letter Queue*, enviando los registros anómalos a un canal dedicado para su posterior auditoría, salvaguardando así la integridad del pipeline general.
