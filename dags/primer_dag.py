from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Función simple
def saludar():
    print("Hola Santiago, tu DAG ya ejecuta tareas.")

# Definición del DAG
with DAG(
    dag_id="primer_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["demo"]
) as dag:

    # Tarea del DAG
    saludo = PythonOperator(
        task_id="saludo",
        python_callable=saludar
    )
