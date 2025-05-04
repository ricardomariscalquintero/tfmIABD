FROM apache/airflow:2.8.1-python3.10

# Instala pymysql dentro de la imagen
RUN pip install pymysql
RUN pip install hdfs
RUN pip install requests beautifulsoup4
RUN pip install python-chess
RUN pip install geopy
RUN pip install pycountry
#RUN pip install apache-airflow-providers-apache-spark==4.0.0
RUN pip install apache-airflow-providers-docker==3.12.0

# Configuramos los permisos para que pueda acceder desde el contenedor a socket.
USER root

RUN groupadd -g 985 docker && usermod -aG docker airflow

USER airflow

CMD ["/bin/bash"]
