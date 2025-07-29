from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from extract import extract
from transform import transform
from load_to_bigquery import load

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl"]
) as dag:

    t1 = PythonOperator(
        task_id="extraer_datos",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="transformar_datos",
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id="cargar_datos",
        python_callable=load
    )

    t1 >> t2 >> t3
