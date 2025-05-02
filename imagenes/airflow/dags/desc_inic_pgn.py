from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from twic_utils import descargar_pgns_local, convertir_pgns_a_csv, subir_a_hdfs

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='desc_inic_pgns',
    default_args=default_args,
    description='Descarga, convierte y sube partidas PGN de TWIC a HDFS',
    schedule_interval=None,    # no se ejecuta automáticamente
    catchup=False,             # no hace ejecuciones pasadas
    tags=['twic', 'ajedrez', 'pgn', 'hdfs'],
) as dag:

    tarea_descargar = PythonOperator(
        task_id='descargar_pgns',
        python_callable=descargar_pgns_local,
        op_kwargs={'destLocal': '/tmp', 'maxArchivos': 25},
    )

    tarea_convertir = PythonOperator(
        task_id='convertir_a_csv',
        python_callable=convertir_pgns_a_csv,
        op_kwargs={'dirPGNs': '/tmp', 'csvPath': '/tmp/partidas.csv', 'incluir_jugadas': False},
    )

    tarea_subir = PythonOperator(
        task_id='subir_a_hdfs',
        python_callable=subir_a_hdfs,
        op_kwargs={
            'dirLocal': '/tmp',
            'csvName': 'partidas.csv',
            'hdfsURL': 'http://namenode:9870',
            'hdfsPathPGN': '/user/ajedrez/raw',
            'hdfsPathCSV': '/user/ajedrez/procesado'
        },
    )

    tarea_descargar >> tarea_convertir >> tarea_subir

