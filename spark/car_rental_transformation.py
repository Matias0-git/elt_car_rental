import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Iniciar Spark Session
spark = SparkSession.builder \
    .appName("CarRentalETL-v2") \
    .enableHiveSupport() \
    .getOrCreate()

# Ruta HDFS corregida (apuntando al NameNode)
HDFS_FILE_PATH = "hdfs://172.17.0.2:9000/ingest/CarRentalData.csv"

# --- 1. Lectura de Datos ---
# Leer el CSV real
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(HDFS_FILE_PATH)

print("Esquema leído del CSV:")
df.printSchema()

# --- 2. Transformaciones ---
# Mapear las columnas del CSV (izquierda) a las columnas de la tabla Hive (derecha)
# Usamos .alias() para renombrar cada columna a lo que espera la tabla de Hive.
df_clean = df.select(
    col("owner.id").cast("string").alias("Id"), # Castear a String
    col("vehicle.make").alias("manufacturer"),
    col("vehicle.model").alias("model"),
    col("vehicle.year").alias("year"),
    col("vehicle.type").alias("category"),
    col("rate.daily").cast("double").alias("price"), # Castear a Double
    col("fuelType").alias("fuel"),

    # --- Columnas Faltantes ---
    # Estas columnas estaban en la tabla Hive pero no en el CSV.
    # Las cargamos como NULL.
    lit(None).cast("string").alias("transmission"),
    lit(None).cast("double").alias("mileage"),
    lit(None).cast("double").alias("engine_v"),
    lit(None).cast("string").alias("color"),

    col("rating").cast("double").alias("rating"), # Aseguramos que rating sea double
    col("location.city").alias("city"),
    col("location.state").alias("state")
)

print("Esquema transformado para Hive:")
df_clean.printSchema()

# --- 3. Carga de Datos ---
print("Guardando datos limpios en car_rental_db.car_rental_analytics...")
df_clean.write \
    .mode("overwrite") \
    .saveAsTable("car_rental_db.car_rental_analytics")

print("Proceso ETL de Car Rental completado exitosamente.")

# Detener la sesión de Spark
spark.stop()
