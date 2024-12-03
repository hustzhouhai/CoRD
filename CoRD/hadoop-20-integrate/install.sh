#!/bin/bash

# 0. user edit this two path
ECPipe_Home=/home/username/ECPipe
Hadoop_Home=/home/username/hadoop-20


# 1. copy files to hadoop
Integrate_Home=${ECPipe_Home}/hadoop-20-integrate

echo "Integrate build.xml"
cp ${Integrate_Home}/build.xml ${Hadoop_Home}
echo "Integrate stop-raidnode.sh"
cp ${Integrate_Home}/bin/stop-raidnode.sh ${Hadoop_Home}/bin
echo "Integrate third-party libraries"
cp ${Integrate_Home}/lib/* ${Hadoop_Home}/lib
echo "Integrate rackAware.sh"
cp ${Integrate_Home}/rackAware.sh ${Hadoop_Home}

echo "Integrate BlockPlacementPolicyECPipe.java"
cp ${Integrate_Home}/src/BlockPlacementPolicyECPipe.java ${Hadoop_Home}/src/contrib/raid/src/java/org/apache/hadoop/hdfs/server/namenode/BlockPlacementPolicyECPipe.java
echo "Integrate BlockReconstructor.java"
cp ${Integrate_Home}/src/BlockReconstructor.java ${Hadoop_Home}/src/contrib/raid/src/java/org/apache/hadoop/raid/BlockReconstructor.java
echo "Integrate Decoder.java"
cp ${Integrate_Home}/src/Decoder.java ${Hadoop_Home}/src/contrib/raid/src/java/org/apache/hadoop/raid/Decoder.java
echo "Integrate DistributedRaidFileSystem.java"
cp ${Integrate_Home}/src/DistributedRaidFileSystem.java ${Hadoop_Home}/src/contrib/raid/src/java/org/apache/hadoop/hdfs/DistributedRaidFileSystem.java
echo "Integrate ECPipeInputStream.java"
cp ${Integrate_Home}/src/ECPipeInputStream.java ${Hadoop_Home}/src/contrib/raid/src/java/org/apache/hadoop/raid/ECPipeInputStream.java
echo "Integrate NetworkTopology.java"
cp ${Integrate_Home}/src/NetworkTopology.java ${Hadoop_Home}/src/core/org/apache/hadoop/net/NetworkTopology.java
echo "Integrate PurgeMonitor.java"
cp ${Integrate_Home}/src/PurgeMonitor.java ${Hadoop_Home}/src/contrib/raid/src/java/org/apache/hadoop/raid/PurgeMonitor.java
echo "Integrate RaidNode.java"
cp ${Integrate_Home}/src/RaidNode.java ${Hadoop_Home}/src/contrib/raid/src/java/org/apache/hadoop/raid/RaidNode.java

# 2. compile
echo "Compile hadoop-20"
cd ${Hadoop_Home}
ant -Dversion=0.20 -Dcompile.native=true clean jar bin-package
