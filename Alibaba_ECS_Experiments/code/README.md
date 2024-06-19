# ECPipe

ECPipe is a system reducing time complexity of
degraded read operations to O(1).

## System Design

ECPipe is **NOT** designed to be a part of some specific file system.  Instead,
ECPipe is running as a separated system attached to file system.  

### Inter-host Communication
Using [Redis](http://redis.io/).

```

```

Commands format:

  * **Issue dr request** (Client to Coordinator)

| requestor IP | lost block name length | lost block name |
|--------------|------------------------|-----------------|
|   4 Bytes    |         4 Bytes        |     ? Bytes     |

  * **Distribute dr request** (Coordinator to Helper)
  * **Send back data** (Helper to Client)

**A little explanation**: I spent quit a lot time hesitating what to use for
the inter-host communication.  I intended to use raw socket programing or
ZeroMQ to do the work.  

However...  I realized that it would be a total mass trying to manage the
serialization of degraded read requests...  Think about two requests going
through same hop and the two nodes get commands in different orders.  This is
also the same reason that drives me to use a dedicated degraded read
coordinator. 

Well, we can use ZeroMQ to handle data flow.  But not sure whether we can use
it in the first realize.

### Intra-host Communication
Using **Conditional Variable** to coordinate different threads.

```

  recv from     ______________    send to
   Network      |            |    network
--------------->|   Memory   |--------------->
                |            |
                --------------
                 read ^
                 from |
                 disk |
```

## Source code structure
  * src/
  * src/java 
    Source codes implementing the input stream of ECPipe

sudo cp /usr/local/lib/libgf_complete.so.1 /usr/lib/ && sudo ldconfig

cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

sudo /etc/init.d/redis_6379 stop
sudo /etc/init.d/redis_6379 start

rm /var/run/redis_6379.pid

/* This is the reply object returned by redisCommand() */
typedef struct redisReply {
    /*命令执行结果的返回类型*/
    int type; /* REDIS_REPLY_* */
    /*存储执行结果返回为整数*/
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    /*字符串值的长度*/
    size_t len; /* Length of string */
    /*存储命令执行结果返回是字符串*/
    char *str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    /*返回结果是数组的大小*/
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    /*存储执行结果返回是数组*/
    struct redisReply **element; /* elements vector for REDIS_REPLY_ARRAY */
} redisReply;

export HADOOP_SRC_DIR=/home/zh/桌面/file/hadoop-3.1.1-src
export HADOOP_HOME=$HADOOP_SRC_DIR/hadoop-dist/target/hadoop-3.1.1
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar:$HADOOP_CLASSPATH
export CLASSPATH=$JAVA_HOME/lib:$CLASSPATH
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_HOME/jre/lib/amd64/server/:/usr/local/lib:$LD_LIBRARY_PATH

hadoop3配置
1、修改hadoop-3-integrate的HADOOP_SRC_DIR为你安装包的目录，且注意后面的要加 /
2、在~/.bashrc 目录下添加pdf中环境配置和maven环境变量，记得source，同时maven环境变量命名为M2_HOME

安装 protobuf-2.5.0.tar.gz
apt install protobuf-compiler


修改coor的更新方案（raid  delta  crd）即可完成，node不需要修改
记得config.xml中的是输入ecK和ecN，后面是ecN，不是ecM
对于新机器，一定记得哪怕是setup.sh中已经开启了redis，也要手动重新关闭redis后再开启redis!!!!!


47.109.16.211    172.29.6.35      coor  

47.109.108.169   172.25.193.16    n0
47.108.249.103   172.29.6.34      n1
47.108.226.190   172.29.6.33      n2
47.109.23.105    172.29.6.32      n3
47.109.62.40     172.29.6.37      n4
47.109.63.97     172.29.6.42      n5
47.109.68.195    172.29.6.36      n6
47.109.60.67     172.29.6.40      n7
47.109.70.162    172.29.6.39      n8
47.109.72.37     172.29.6.41      n9
47.109.69.194    172.29.6.38      n10
47.109.61.226    172.29.6.43      n11
47.109.66.177    172.29.6.44      n12
47.109.72.190    172.29.6.45      n13