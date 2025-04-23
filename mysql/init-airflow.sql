CREATE DATABASE IF NOT EXISTS airflow;
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED WITH mysql_native_password BY 'airflow';
GRANT ALL PRIVILEGES ON airflow.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
