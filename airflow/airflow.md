# 2. Orquestación (Apache Airflow)

Se utiliza una arquitectura Padre-Hijo para separar la Ingesta del Procesamiento.

---

### DAG Padre: `car_rental_parent_dag.py`

**Ruta:** `/home/hadoop/airflow/dags/car_rental_parent_dag.py`

Este DAG se encarga de la ingesta y de disparar el DAG hijo.

### DAG Hijo: `car_rental_child_dag.py`
**Ruta:** `/home/hadoop/airflow/dags/car_rental_child_dag.py`

Este DAG es disparado por el padre y ejecuta el pipeline de Spark y la verificación de Hive.
