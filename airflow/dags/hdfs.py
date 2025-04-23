from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from hdfs import InsecureClient

# Configura HDFS
HDFS_URL = 'http://namenode:9870'  # Ajusta el nombre si tu contenedor es distinto
HDFS_DIR = '/user/ajedrez'
LOCAL_FILE = '/tmp/ajedrez.tar'
REMOTE_FILE = f'{HDFS_DIR}/ajedrez.tar'
DOWNLOAD_URL = 'https://l--l.top/AJ-OTB-CB17-001'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 23),
    'retries': 1,
}

dag = DAG(
    'descargar_y_subir_a_hdfs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Descarga un archivo de ajedrez y lo sube a HDFS'
)

def descargar_archivo():
    response = requests.get(DOWNLOAD_URL)
    response.raise_for_status()
    with open(LOCAL_FILE, 'wb') as f:
        f.write(response.content)

def subir_a_hdfs():
    client = InsecureClient(HDFS_URL, user='root')
    client.makedirs(HDFS_DIR)
    client.upload(REMOTE_FILE, LOCAL_FILE, overwrite=True)

t1 = PythonOperator(
    task_id='descargar_archivo',
    python_callable=descargar_archivo,
    dag=dag
)

t2 = PythonOperator(
    task_id='subir_a_hdfs',
    python_callable=subir_a_hdfs,
    dag=dag
)

t1 >> t2
