#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'hadoop',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='car_rental_child_process', # Coincide con el 'trigger_dag_id' del padre
    default_args=default_args,
    description='DAG Hijo: Procesa Car Rental con Spark y carga en Hive',
    schedule_interval=None,         # Se ejecuta solo cuando es llamado
    start_date=days_ago(1),
    catchup=False,
    tags=['car_rental', 'etl', 'hijo', 'spark'],
) as dag:

    inicio_procesamiento = DummyOperator(task_id='inicio_procesamiento')

    # Tarea 1: Procesa los datos con Spark
    procesa_spark = BashOperator(
        task_id='procesa_spark',
        # Usa 'local[*]' para evitar problemas de YARN
        bash_command='spark-submit --master "local[*]" /home/hadoop/scripts/car_rental_transformation.py',
    )

    # Tarea 2: Verifica la tabla final en Hive
    verifica_tabla_hive = BashOperator(
        task_id='verifica_tabla_hive',
        bash_command='beeline -u jdbc:hive2://localhost:10000 -e "USE car_rental_db; SELECT COUNT(*) AS total FROM car_rental_analytics;"',
    )

    fin_procesamiento = DummyOperator(task_id='fin_procesamiento')

    # --- Definiendo el Flujo ---
    inicio_procesamiento >> procesa_spark >> verifica_tabla_hive >> fin_procesamiento
