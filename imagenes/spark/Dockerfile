#Partimos de la imagen de la openjdk.
FROM openjdk:8-jdk

### --- Hadoop ---
ENV HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME  
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV YARN_HOME=$HADOOP_HOME
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin

ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

#Descargamos Hadoop y lo instalamos.
RUN apt-get update && apt-get install -y wget net-tools && \
    wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xvzf hadoop-$HADOOP_VERSION.tar.gz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

#Copiamos las variables de entorno en el bashrc.
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop" >> /root/.bashrc && \
    echo "export HADOOP_COMMON_HOME=${HADOOP_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_HDFS_HOME=${HADOOP_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_MAPRED_HOME=${HADOOP_HOME}" >> /root/.bashrc && \
    echo "export YARN_HOME=${HADOOP_HOME}" >> /root/.bashrc && \
    echo "export PATH=${PATH}" >> /root/.bashrc

#Copiamos las configuraciones.
COPY configuraciones/hadoop/* $HADOOP_HOME/etc/hadoop/

### --- Hive ---
ENV HIVE_VERSION=3.1.3
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

#Descargamos e instalamos Hive.
RUN wget https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    tar -xzvf apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt && \
    mv /opt/apache-hive-${HIVE_VERSION}-bin $HIVE_HOME && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

#Copiamos las variables de entorno asociadas a Hive.
RUN echo "export HIVE_HOME=${HIVE_HOME}" >> /root/.bashrc && \
    echo "export PATH=${PATH}" >> /root/.bashrc

#Copiamos las configuraciones de Hive.
COPY configuraciones/hive/* $HIVE_HOME/conf/
COPY imagenes/hive/lib-hive/* $HIVE_HOME/lib/

#Instalamos dockerize para esperar a MySQL.
#Esto es debido a que necesitamos que MySQL esté corriendo para inicializar el esquema.
RUN wget https://github.com/jwilder/dockerize/releases/download/v0.6.1/dockerize-linux-amd64-v0.6.1.tar.gz && \
    tar -C /usr/local/bin -xzvf dockerize-linux-amd64-v0.6.1.tar.gz && \
    rm dockerize-linux-amd64-v0.6.1.tar.gz

### --- Spark ---

ENV SPARK_VERSION=3.5.5
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

#Instalamos Python y las librerías útiles para PySpark.
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip install python-chess

#Descargamos Spark y lo instalamos.
#RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

#Copiamos las variables de entorno al bashrc.
RUN echo "export SPARK_HOME=${SPARK_HOME}" >> /root/.bashrc && \
    echo "export PATH=${PATH}" >> /root/.bashrc

#Revisamos los permisos de los archivos de logs de Spark.
RUN mkdir -p /tmp/spark-events && chmod 1777 /tmp/spark-events

#Copiamos las configuraciones de Spark.
COPY configuraciones/spark/* $SPARK_HOME/conf/

### --- Python y Jupyter ---
#Para facilitar la interacción con PySPARK, instalamos jupyterlab.
RUN pip3 install --no-cache-dir \
    numpy \
    pandas \
    pyarrow \
    findspark \
    jupyterlab \
    notebook \
    matplotlib \
    seaborn \
    pyspark \
    tensorflow==2.13.0 \    
    tensorflowonspark \ 	
    scikit-learn \ 			
    h5py					

#Creamos el directorio de los notebooks.
RUN mkdir -p /opt/notebooks

EXPOSE 8888

CMD ["/bin/bash"]

