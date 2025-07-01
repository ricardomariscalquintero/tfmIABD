#Partimos de la imagen de la openjdk.
FROM openjdk:8-jdk

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

#Descargamos hadoop y lo instalamos.
RUN apt-get update && apt-get install -y wget net-tools && \
    wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xvzf hadoop-$HADOOP_VERSION.tar.gz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

#Escribimos en el bashrc las variables de entorno.
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop" >> /root/.bashrc && \
    echo "export HADOOP_COMMON_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export HADOOP_HDFS_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export HADOOP_MAPRED_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export YARN_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export PATH=${PATH}" >> /root/.bashrc

#Copiamos las configuraciones.
COPY configuraciones/hadoop/* $HADOOP_HOME/etc/hadoop/

CMD ["/bin/bash"]

#Instalamos Python 3.9 y pip.
RUN apt-get update && \
    apt-get install -y python3.9 python3.9-distutils curl && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.9

#Creamos enlaces simbólicos.
RUN ln -s /usr/bin/python3.9 /usr/bin/python && \
    ln -s /usr/local/bin/pip /usr/bin/pip

#Instalamos python-chess.
RUN pip install --no-cache-dir python-chess

#Instalamos numpy, pandas y pyarrow para usar en UDFs de Spark y procesamiento de datos.
RUN pip install --no-cache-dir numpy pandas pyarrow

