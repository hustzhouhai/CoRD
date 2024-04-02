#!/bin/bash

ECPipe_Home=/home/username/ECPipe-Hadoop3
QFS_Home=/home/username/qfs

# 1. copy files to qfs
Integrate_Home=${ECPipe_Home}/qfs-integrate

# jhli add
echo "Integrate CMakeLists.txt"
cp ${Integrate_Home}/Chunk-CMakeLists.txt ${QFS_Home}/src/cc/chunk/CMakeLists.txt
cp ${Integrate_Home}/CMakeLists.txt ${QFS_Home}/CMakeLists.txt
# jhli end 

echo "create ecpipe directory"
mkdir -p ${QFS_Home}/src/cc/ecpipe
mkdir -p ${QFS_Home}/src/cc/ecpipe/src
mkdir -p ${QFS_Home}/src/cc/ecpipe/src/Util

echo "Integrate Config.hh"
cp ${Integrate_Home}/src/Config.hh ${QFS_Home}/src/cc/ecpipe/src/Config.hh
echo "Integrate Config.cc"
cp ${Integrate_Home}/src/Config.cc ${QFS_Home}/src/cc/ecpipe/src/Config.cc
echo "Integrate ECPipeInputStream.hh"
cp ${Integrate_Home}/src/ECPipeInputStream.hh ${QFS_Home}/src/cc/ecpipe/src/ECPipeInputStream.hh
echo "Integrate ECPipeInputStream.cc"
cp ${Integrate_Home}/src/ECPipeInputStream.cc ${QFS_Home}/src/cc/ecpipe/src/ECPipeInputStream.cc
echo "Integrate tinyxml2.h"
cp ${Integrate_Home}/src/tinyxml2.h ${QFS_Home}/src/cc/ecpipe/src/Util/tinyxml2.h
echo "Integrate tinyxml2.cpp"
cp ${Integrate_Home}/src/tinyxml2.cpp ${QFS_Home}/src/cc/ecpipe/src/Util/tinyxml2.cpp
echo "Integrate chunkserver_main.cc"
cp ${Integrate_Home}/src/chunkserver_main.cc ${QFS_Home}/src/cc/chunk/chunkserver_main.cc
echo "Integrate cptoqfs_main.cc"
cp ${Integrate_Home}/src/cptoqfs_main.cc ${QFS_Home}/src/cc/tools/cptoqfs_main.cc
echo "Integrate CMakeLists.txt"
cp ${Integrate_Home}/src/CMakeLists.txt ${QFS_Home}/src/cc/chunk/CMakeLists.txt
echo "Integrate Replicator.cc"
cp ${Integrate_Home}/src/Replicator.cc ${QFS_Home}/src/cc/chunk/Replicator.cc
echo "Integrate KfsClient.cc"
cp ${Integrate_Home}/src/KfsClient.cc ${QFS_Home}/src/cc/libclient/KfsClient.cc
echo "Integrate KfsClient.h"
cp ${Integrate_Home}/src/KfsClient.h ${QFS_Home}/src/cc/libclient/KfsClient.h
echo "Integrate KfsClientInt.h"
cp ${Integrate_Home}/src/KfsClientInt.h ${QFS_Home}/src/cc/libclient/KfsClientInt.h
# echo "Integrate CMakeLists.txt"
# cp ${Integrate_Home}/CMakeLists.txt ${QFS_Home}/src/cc/chunk/CMakeLists.txt

# 2. compile
echo "Compile Quantcat File System"
cd $QFS_Home
make
