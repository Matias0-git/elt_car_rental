# 4. Data Warehouse y Análisis (Apache Hive)

Hive se utiliza como el Data Warehouse principal. Todas las consultas se ejecutan después de que el pipeline de Airflow/Spark haya cargado los datos limpios.

### Definición de la Base de Datos y Tabla (DDL)

Estos comandos se ejecutan una sola vez en Hive para crear la estructura.

```sql
-- 1. Crear la base de datos
CREATE DATABASE IF NOT EXISTS car_rental_db;

-- 2. Seleccionar la base de datos para usarla
USE car_rental_db;

-- 3. Crear la tabla (Esquema completo con 14 columnas)
CREATE TABLE IF NOT EXISTS car_rental_analytics (
    Id STRING,
    manufacturer STRING,
    model STRING,
    year INT,
    category STRING,
    price DOUBLE,
    fuel STRING,
    transmission STRING,
    mileage DOUBLE,
    engine_v DOUBLE,
    color STRING,
    rating DOUBLE,
    city STRING,
    state STRING
)
COMMENT 'Tabla para el análisis de alquiler de autos';
