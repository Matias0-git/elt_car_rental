#!/bin/bash

# --- SALIR INMEDIATAMENTE SI UN COMANDO FALLA ---
# Esto evita "fallas silenciosas" y le reporta el error a Airflow
set -e

# Directorio temporal de destino
LANDING_DIR="/home/hadoop/landing"

# Directorio de HDFS de destino
HDFS_INGEST_DIR="/ingest"

# --- RUTA ABSOLUTA AL COMANDO HDFS ---
# Corrección para el error "command not found" en Airflow
HDFS_CMD="/home/hadoop/hadoop/bin/hdfs"

# Array con las URLs de los archivos a descargar
URLS=(
  "[https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv](https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv)"
  "[https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv](https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv)"
)

# Array con los nombres de archivo de destino (debe coincidir en orden con URLS)
FILENAMES=(
  "CarRentalData.csv"
  "us_states_georef.csv"
)

# 1. Crear el directorio temporal si no existe
echo "Creando el directorio temporal $LANDING_DIR si no existe..."
mkdir -p "$LANDING_DIR"

# 2. Iterar sobre cada URL, descargar, subir a HDFS y borrar
for i in "${!URLS[@]}"; do

  FILE_URL="${URLS[$i]}"
  FILE_NAME="${FILENAMES[$i]}"
  LOCAL_FILE_PATH="$LANDING_DIR/$FILE_NAME" # Ruta local completa

  echo "--- Procesando: $FILE_NAME ---"

  # 2a. Descargar el archivo
  echo "Descargando $FILE_NAME a $LOCAL_FILE_PATH..."
  wget -O "$LOCAL_FILE_PATH" "$FILE_URL"

  # 2b. Enviar el archivo a HDFS usando la ruta absoluta
  echo "Enviando $LOCAL_FILE_PATH a HDFS en $HDFS_INGEST_DIR..."
  $HDFS_CMD dfs -put -f "$LOCAL_FILE_PATH" "$HDFS_INGEST_DIR"

  # 2c. Borrar el archivo del directorio temporal
  echo "Borrando el archivo local $LOCAL_FILE_PATH..."
  rm "$LOCAL_FILE_PATH"

  echo "--- $FILE_NAME procesado exitosamente ---"

done

echo "Script terminado. Todos los archivos han sido procesados."
