from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from twic_utils import descargar_pgns_local, convertir_pgns_a_csv, subir_a_hdfs

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id            = 'desc_inic_pgns_warehouse',
    default_args      = default_args,
    description       = 'Descarga, convierte, sube a HDFS y genera el Data Warehouse',
    schedule_interval = None,
    catchup           = False,
    tags              = ['twic', 'ajedrez', 'pgn', 'hdfs'],
) as dag:

    tarea_descargar = PythonOperator(
        task_id         = 'descargar_pgns',
        python_callable = descargar_pgns_local,
        op_kwargs       = {'destLocal': '/tmp', 'maxArchivos': 5},
    )

    tarea_convertir = PythonOperator(
        task_id         = 'convertir_a_csv',
        python_callable = convertir_pgns_a_csv,
        op_kwargs       = {'dirPGNs': '/tmp', 'csvPath': '/tmp/partidas.csv', 'borrarDespues': True},
    )

    tarea_subir = PythonOperator(
        task_id         = 'subir_a_hdfs',
        python_callable = subir_a_hdfs,
        op_kwargs       = {
            'dirLocal': '/tmp',
            'csvName': 'partidas.csv',
            'hdfsURL': 'http://namenode:9870',
            'hdfsPathPGN': '/user/ajedrez/raw',
            'hdfsPathCSV': '/user/ajedrez/procesado'
        },
    )

    tarea_generar_warehouse = BashOperator(
        task_id      = 'generar_datawarehouse',
        bash_command = (
            "docker exec spark-client spark-submit --master yarn --deploy-mode client "
            "/opt/spark/scripts/warehouse.py "
            "--input hdfs://namenode:9000/user/ajedrez/procesado/partidas.csv"
        ),
    )
    
    tarea_descargar >> tarea_convertir >> tarea_subir >> tarea_generar_warehouse

