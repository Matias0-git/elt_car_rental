# Proyecto: Pipeline ELT de Alquiler de Autos (Car Rental)

Este proyecto implementa un pipeline de datos completo para ingestar, procesar y analizar datos de alquileres de vehículos en EE. UU.

El pipeline utiliza un stack de herramientas de Big Data y sigue una arquitectura "Padre-Hijo":

1.  **Orquestación (Apache Airflow):** Un DAG "Padre" (`car_rental_parent_ingest`) inicia el proceso, y un DAG "Hijo" (`car_rental_child_process`) realiza el procesamiento.
2.  **Ingesta (`car_rental_ingest.sh`):** Un script de Bash descarga los archivos CSV de una fuente pública y los transfiere al Data Lake en HDFS.
3.  **Procesamiento (Apache Spark):** Un script de PySpark (`car_rental_transformation.py`) lee los CSVs "crudos" de HDFS, los transforma (limpiando y mapeando columnas) y los guarda en una tabla de Hive.
4.  **Almacenamiento (Apache Hive):** Los datos limpios residen en un Data Warehouse de Hive (`car_rental_db`) listos para el análisis SQL.
