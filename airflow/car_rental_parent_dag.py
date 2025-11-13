#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id='car_rental_parent_ingest',
    default_args=default_args,
    description='DAG Padre: Ingesta datos de Car Rental y llama al DAG hijo',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['car_rental', 'etl', 'padre'],
) as dag:

    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Ejecuta el script de ingesta (con el espacio al final)
    ingesta_datos = BashOperator(
        task_id='ingesta_datos',
        bash_command='bash /home/hadoop/landing/car_rental_ingest.sh ',
    )

    # Tarea 2: Dispara el DAG hijo
    trigger_procesamiento = TriggerDagRunOperator(
        task_id='trigger_procesamiento_hijo',
        trigger_dag_id='car_rental_child_process',

        # --- CORRECCIÓN DE DEADLOCK ---
        # Se cambia a False para que el padre libere el slot
        # y el hijo (que está en 'queued') pueda empezar.
        wait_for_completion=False,
    )

    fin = DummyOperator(task_id='fin_proceso')

    # --- Definiendo el Flujo ---
    inicio >> ingesta_datos >> trigger_procesamiento >> fin
