# 1. Proceso de Ingesta (Bash)

Esta etapa, ejecutada por el DAG Padre, descarga los 2 archivos CSV fuente y los coloca en HDFS.

### Script: `car_rental_ingest.sh`

Este script se encuentra en `/home/hadoop/landing/` y debe tener permisos de ejecución (`chmod +x`).
