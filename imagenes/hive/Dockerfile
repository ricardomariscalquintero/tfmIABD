#Partimos de la imagen de la openjdk.
FROM openjdk:8-jdk

### --- Hadoop ---
#Definimos las variables de entorno.
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

CMD ["/bin/bash"]


