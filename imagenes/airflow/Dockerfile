#Usamos como base la imagen oficial de Airflow, ya preconfigurada con Python 3.10.
FROM apache/airflow:2.8.1-python3.10

#Instalamos las librerías necesarias para utilizarlas desde los DAGs.
RUN pip install pymysql
RUN pip install hdfs
RUN pip install requests beautifulsoup4
RUN pip install python-chess
RUN pip install geopy
RUN pip install pycountry
#RUN pip install apache-airflow-providers-apache-spark==4.0.0
RUN pip install apache-airflow-providers-docker==3.12.0

#Realizamos un cambio a usuario root para añadir un usuario a un grupo.
USER root

#Creamos el grupo docker con gid 985 y añadimos al usuario airflow.
#De esta manera, si necesitamos utilizar el operador de Docker desde Airflow,
#podríamos hacerlo.
RUN groupadd -g 985 docker && usermod -aG docker airflow

#Revertimos el cambio de usuario a airflow.
USER airflow

CMD ["/bin/bash"]
