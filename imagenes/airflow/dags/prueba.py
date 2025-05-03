from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ejecutar_spark_prueba',
    default_args=default_args,
    description='Lanza spark-submit desde DockerOperator apuntando a /tmp/prueba.py',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'docker'],
) as dag:

    tarea_spark = DockerOperator(
        task_id='ejecutar_script_spark',
        image='hadoop-cluster_spark-client',
        api_version='auto',
        auto_remove=True,
        command='spark-submit /tmp/prueba.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='hadoop-net'
    )

    tarea_spark


