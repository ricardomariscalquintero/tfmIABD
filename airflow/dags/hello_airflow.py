from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define el DAG
with DAG(
    dag_id='hello_airflow',
    description='Un DAG sencillo de prueba',
    schedule_interval='@daily',  # se ejecuta una vez al día
    start_date=datetime(2023, 1, 1),
    catchup=False,  # no ejecuta tareas anteriores a la fecha de hoy
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['pruebas']
) as dag:

    tarea_saludo = BashOperator(
        task_id='decir_hola',
        bash_command='echo "¡Hola desde Airflow!"'
    )
