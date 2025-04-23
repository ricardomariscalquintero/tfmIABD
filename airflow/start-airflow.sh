#!/bin/bash
airflow db upgrade
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow webserver & 
exec airflow scheduler
