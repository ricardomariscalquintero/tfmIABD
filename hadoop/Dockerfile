FROM openjdk:11-jdk

ENV HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME  
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV YARN_HOME=$HADOOP_HOME
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin

ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# Instalar dependencias mínimas y Hadoop
RUN apt-get update && apt-get install -y wget net-tools && \
    wget https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xvzf hadoop-$HADOOP_VERSION.tar.gz -C /opt && \
    mv /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# Opcional: exportar variables para shells interactivos
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> /root/.bashrc && \
    echo "export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop" >> /root/.bashrc && \
    echo "export HADOOP_COMMON_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export HADOOP_HDFS_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export HADOOP_MAPRED_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export YARN_HOME=$HADOOP_HOME" >> /root/.bashrc && \
    echo "export PATH=${PATH}" >> /root/.bashrc

# Copiar configuraciones personalizadas
COPY config/* $HADOOP_HOME/etc/hadoop/

CMD ["/bin/bash"]
