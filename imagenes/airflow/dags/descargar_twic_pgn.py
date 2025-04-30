from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from twic_utils import descargar_ultimo_pgn

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id            = 'descargar_y_subir_twic_pgn_zip',
    default_args      = default_args,
    schedule_interval = '@weekly',
    catchup           = False,
    description       = 'Descarga el último ZIP PGN de TWIC y lo sube a HDFS',
    tags              = ['twic', 'pgn', 'hdfs', 'ajedrez'],
    
) as dag:

    descargar_y_subir = PythonOperator(
        task_id = 'descargar_y_subir_zip_a_hdfs',
        python_callable = descargar_ultimo_pgn,
    )

    descargar_y_subir

