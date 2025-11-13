Conclusiones
El stack (Airflow > Spark > Hive) fue el correcto. Este proyecto demostró por qué Spark es esencial. Hive por sí solo no habría podido leer el CSV de origen debido a los nombres de columna anidados (ej. owner.id, vehicle.make). Spark fue la herramienta perfecta para leer este esquema complejo, aplanarlo y mapearlo a la tabla relacional de Hive.

La arquitectura Padre-Hijo fue un éxito. Separar la lógica de "Ingesta" (Padre) de la de "Procesamiento" (Hijo) hizo que el pipeline fuera mucho más limpio, fácil de depurar y reutilizable.

Los scripts de "Verificación" son vitales. La tarea verifica_tabla_hive (con SELECT COUNT(*)) fue clave. Nos permitió saber que el pipeline había cargado 5,851 filas exitosamente.

Recomendaciones
Optimizar el Almacenamiento (Parquet): El script de Spark guarda los datos en el formato por defecto de Hive. Para un rendimiento de consultas mucho más rápido, el script de Spark debería guardar la tabla en formato Parquet.

Cambio: .write.mode("overwrite").format("parquet").saveAsTable(...)

Manejo de Variables de Entorno: Hardcodear (escribir directamente) la IP del NameNode (hdfs://172.17.0.2:9000) en el script de Spark no es ideal. La mejor práctica sería guardar esa IP como una Variable de Airflow y pasarla al script de Spark.

Usar el Segundo Archivo: Ingestamos el archivo us_states_georef.csv pero nunca lo usamos. Un siguiente paso lógico sería crear una nueva tarea de Spark que haga un JOIN entre car_rental_analytics y us_states_georef.csv (usando la columna state) para enriquecer los datos con información geoespacial.

1. ☁️ Proponer una Arquitectura Alternativa (Cloud)
La arquitectura que usamos (Hadoop, Spark, Hive) es un stack On-Premise clásico. Una arquitectura alternativa moderna usando Google Cloud Platform (GCP) se vería así:

Ingesta (Reemplazo de car_rental_ingest.sh):

Un Cloud Scheduler (un "cron" en la nube) se ejecuta en un horario y llama a una Cloud Function.

Esta Cloud Function (una pequeña función sin servidor) ejecuta el wget para descargar los CSVs y, en lugar de HDFS, los guarda en un "Data Lake" en Google Cloud Storage (GCS) (un bucket de almacenamiento).

Procesamiento (Reemplazo de Spark):

Se usaría un job de Dataproc Serverless. Este servicio ejecuta tu script de PySpark (car_rental_transformation.py) sin necesidad de configurar o administrar un clúster.

El job lee el CarRentalData.csv desde GCS, aplica la misma lógica de transformación (mapeo de `owner.id`, etc.) y guarda los datos limpios (en formato Parquet) en otra carpeta de GCS.

Data Warehouse (Reemplazo de Hive):

Google BigQuery. Es el Data Warehouse sin servidor de Google.

Se crea una "Tabla Externa" en BigQuery que apunta directamente a los archivos Parquet limpios en GCS.

Todas las consultas SQL que escribimos funcionarían de manera idéntica (o más rápida) en BigQuery.

Orquestación (Reemplazo de Airflow):

Cloud Composer. ¡Este servicio es, de hecho, Apache Airflow administrado por Google!

Subiríamos nuestros mismos DAGs Padre/Hijo a Cloud Composer y el pipeline funcionaría igual, pero sin tener que administrar el servidor de Airflow.
