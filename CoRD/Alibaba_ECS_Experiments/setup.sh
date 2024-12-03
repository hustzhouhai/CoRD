#!/bin/bash

WK_DIR=ins_pac

apt install sudo

sudo apt-get update

cd $WK_DIR

# install redis 3.2.8
tar -zxvf redis-3.2.8.tar.gz
cd redis-3.2.8/
make
sudo make install

cd utils/
sudo ./install_server.sh

sudo service redis_6379 stop
sudo sed -i 's/bind 127.0.0.1/bind 0.0.0.1/' /etc/redis/6379.conf
sudo sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/6379.conf
sudo service redis_6379 start

cd ..
cd ..

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


