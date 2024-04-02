#!/bin/bash

WK_DIR=ins_pac

apt install sudo

sudo apt-get update

sudo apt-get install -y g++ make psmisc git

cd $WK_DIR

# install redis 3.2.8
tar -zxvf redis-3.2.8.tar.gz
cd redis-3.2.8/
make
sudo make install

cd utils/
sudo ./install_server.sh

sudo service redis_6379 stop
sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/6379.conf
sudo sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/6379.conf
sudo service redis_6379 start

cd ..
cd ..

# install hiredis
tar -zxvf hiredis.tar.gz
cd hiredis/
make 
sudo make install

cd ..

# install wondershaper
tar -zxvf wondershaper.tar.gz
cd wondershaper/
sudo make install

cd ..

# install gf-complete
tar -zxvf gf-complete.tar.gz
cd gf-complete/
chmod +x ./autogen.sh
./autogen.sh
./configure
make
sudo make install

sudo cp /usr/local/lib/libgf_complete.so.1 /usr/lib/ 
sudo ldconfig

cd ..
cd ..






<<zh
RES_DIR=res

# install dependencies
sudo apt-get update

# Ubuntu 14
# sudo apt-get install -y g++ make libssl-dev autoconf libtool yasm libz-dev gcc-multilib g++-multilib libxrender1 libxtst6 libXi6 build-essential libboost-system-dev libboost-thread-dev libboost-program-options-dev libboost-test-dev libeigen3-dev zlib1g-dev libbz2-dev liblzma-dev

# Ubuntu 18
sudo apt-get install -y g++ make libssl-dev autoconf libtool yasm libz-dev gcc-multilib g++-multilib libxrender1 libxtst6 build-essential libboost-system-dev libboost-thread-dev libboost-program-options-dev libboost-test-dev libeigen3-dev zlib1g-dev libbz2-dev liblzma-dev


# edit ~/.bashrc in advance
sudo echo "export LD_LIBRARY_PATH=/usr/local/ssl/include/openssl:/usr/lib:/usr/local/lib:/usr/lib/pkgconfig:/usr/local/include/wx-2.8/wx:\$LD_LIBRARY_PATH
export PKG_CONFIG_PATH=/usr/lib/pkgconfig
export OPENSSL_ROOT_DIR=/usr/local/ssl
export OPENSSL_LIBRARIES=/usr/local/ssl/lib/

PATH=/usr/local/ssl/bin:\$PATH

export PATH=\$PATH:/usr/local/cmake/bin

# apache maven
export M2_HOME=/home/openec/apache-maven-3.8.5
export PATH=\$PATH:\$M2_HOME/bin

# java
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_333
export JRE_HOME=\${JAVA_HOME}/jre
export CLASSPATH=.:\${JAVA_HOME}/lib:\${JRE_HOME}/lib
export PATH=\${JAVA_HOME}/bin:\$PATH

# hadoop
export HADOOP_SRC_DIR=/home/openec/hadoop-3.0.0-src
export HADOOP_HOME=\$HADOOP_SRC_DIR/hadoop-dist/target/hadoop-3.0.0
export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
export HADOOP_CLASSPATH=\$JAVA_HOME/lib/tools.jar:\$HADOOP_CLASSPATH
export CLASSPATH=\`hadoop classpath --glob\`
export LD_LIBRARY_PATH=\$HADOOP_HOME/lib/native:\$JAVA_HOME/jre/lib/amd64/server/:/usr/local/lib:\$LD_LIBRARY_PATH" >> ~/.bashrc

source ~/.bashrc

cd $RES_DIR

# install cmake 3.22.0
tar -zxvf cmake-3.22.4.tar.gz
cd cmake-3.22.4/
./bootstrap
make
sudo make install

sudo ln -s /usr/local/bin/cmake /usr/bin/

cd ..

# install redis 3.2.8
tar -zxvf redis-3.2.8.tar.gz
cd redis-3.2.8/
make
sudo make install

cd utils/
sudo ./install_server.sh

sudo service redis_6379 stop
sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.0/' /etc/redis/6379.conf
sudo sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/6379.conf
sudo service redis_6379 start

cd ..
cd ..

# install hiredis
tar -zxvf hiredis-1.0.2.tar.gz
cd hiredis-1.0.2/
make 
sudo make install

cd ..

# install gf-complete
tar -zxvf gf-complete.tar.gz
cd gf-complete/
chmod +x ./autogen.sh
./autogen.sh
./configure
make
sudo make install

cd ..

# install ISA-L
tar -zxvf isa-l-2.30.0.tar.gz
cd isa-l-2.30.0/
./autogen.sh
./configure
make
sudo make install

cd ..

# install maven
tar -zxvf apache-maven-3.8.5-bin.tar.gz
mv apache-maven-3.8.5 ~/

# install java8
sudo mkdir /usr/lib/jvm
tar -zxvf jdk-8u333-linux-x64.tar.gz
sudo mv jdk1.8.0_333/ /usr/lib/jvm/

# install hadoop3
tar -zxvf hadoop-3.0.0-src.tar.gz
mv hadoop-3.0.0-src ~/

source ~/.bashrc

# install protobuf 2.5.0
tar -zxvf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0/
./autogen.sh
./configure
make
sudo make install

cd ..

# install openec-hdfs3-integration
sudo cp /usr/include/x86_64-linux-gnu/zconf.h /usr/include/

# sudo apt-get install -y zip
# unzip openec.zip
# cd openec/
# cd hdfs3-integration/
# source ~/.bashrc
# ./install.sh

# cd ..

zh
