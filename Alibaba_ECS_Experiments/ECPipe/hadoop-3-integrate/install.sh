#!/bin/bash

echo "start to install ECPipe in Hadoop-3"

echo "copy the source to hadoop-src"

HADOOP_SRC_DIR=/home/zh/桌面/file/hadoop-3.1.1-src

cp DFSConfigKeys.java ${HADOOP_SRC_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs

cp -R erasurecode ${HADOOP_SRC_DIR}/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode

echo "update the pom.xml in hadoop-src"

cp pom.xml ${HADOOP_SRC_DIR}/hadoop-hdfs-project/hadoop-hdfs

echo "rebuild hadoop"

cd ${HADOOP_SRC_DIR}; mvn package -DskipTests -Dtar -Dmaven.javadoc.skip=true -Drequire.isal -Pdist,native -DskipShade -e 

echo "Jedis"

cd -
cp jedis-3.0.0-SNAPSHOT.jar  ~/.m2/repository/redis/clients/jedis/2.9.0/jedis-2.9.0.jar
