# Installation
We have tested CoRD on Ubuntu 16.04 LTS.

# Software Requirement
- Python 3.7.0
- g++ v5.4.0
- redis 3.2.8
- hiredis
- gf-complete
- wondershaper
- git
- make v4.1
- psmisc
- Hadoop 3.1.1

We have put some packages in the directory 'CoRD/Alibaba_ECS_Experiments/ins_pac'. You can also automatically install all software by executing the script file 'setup.sh'.

Expected Reproduction Time (in Minutes): The installation time for all software is at least 60 minutes.

# Hadoop-3.1.1 Setting
In addition, the HDFS configuration process is as follows:

- ISA-L (needed in Hadoop 3.1.1)
  $ git clone https://github.com/01org/isa-l.git
  $ cd isa-l/
  $ ./autogen.sh
  $ ./configure
  $ make
  $ sudo make install

- Hadoop 3.1.1
  $ wget http://apache.01link.hk/hadoop/common/hadoop-3.1.1/hadoop-3.1.1-src.tar.gz
  $ tar -zxvf hadoop-3.1.1-src.tar.gz 
  $ cd hadoop-3.1.1-src/
  $ mvn package -DskipTests -Dtar -Dmaven.javadoc.skip=true -Drequire.isal -Pdist,native -DskipShade -e     # use maven to compile the HDFS for supporting erasure coding

Set the environment variables for HDFS and JAVA in ~/.bashrc. The following is an sample used in our testbed.
  $ export JAVA_HOME=/root/java
  $ export HADOOP_HOME=/root/hadoop-3.1.1 
  $ export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
  
Make the environment variables work 
  $ source ~/.bashrc

Configure the configuration files under the folder hadoop-3.1.1/etc/hadoop/, including core-site.xml, hadoop-env.sh, hdfs-site.xml, and workers. 

At the hadoop client side, create a file named "datafile", whose size should be multiple times of the data size of a stripe (i.e., the size of data blocks in a stripe). As we use RS(k = 10, m = 4) as an example in the following descriptions, we can create a file with the size of 6.1GB.  
  $ dd if=/dev/urandom of=datafile bs=64k count=10000    # create a data file (6.1GB)

At the client side, select and enable an erasure coding scheme and write data to the HDFS. Here we use RS(k = 10, m = 4) as an instance. If you use other erasure coding schemes, please ensure that the size of datafile should be multiple times of the data size of a stripe. 
  $ hadoop fs -mkdir /ec_file                                           
  $ hdfs ec -enablePolicy -policy RS-10-4-64k      
  $ hdfs ec -setPolicy -path /ec_file -policy RS-10-4-64k    
  $ hdfs ec -getPolicy -path /ec_file              
  $ hadoop fs -put datafile /ec_file             

# Trace
We provide a small part of the load for simple testing. Specifically, the load directory of large-scale simulation is 'CoRD/trace', and the load directory of Alibaba cloud experiment is 'CoRD/Alibaba_ECS_Experiments/Trace'.

To obtain the complete load (2TB storage space may be required), you can download it at the following link:
Alibaba Cloud, https://github.com/alibaba/block-traces.
Tencent Cloud, http://iotta.snia.org/traces/27917.

# Compile CoRD
  $ cd ECPipe
  $ make
  
Expected Reproduction Time (in Minutes): The compile time of CoRD is about 1 minutes.

# Large-Scale Simulation
You can enter the directory 'CoRD/Large_Scale_Simulation' to conduct simulation. We provide three simulation methods, corresponding to different folders. After entering different folders, there will be a 'Simulation experiments.py' file, in which the user configurable information of the main function is as follows:

  1. k : the parameter of RS code, the number of data blocks
  2. m : the parameter of RS code, the number of parity blocks
  3. n : the number of cluster nodes
  4. block_size : the block size
  5. trace_type : the trace type, including AliCloud and TenCloud
  
Hardware: (1) CPU: Intel(R)11th Gen Intel(R) Core(TM) i7-11700 @ 2.50GHz(2496 MHz); (2) In-memory: 16.00 GB (2133 MHz); (3) Disk: 2000 GB (WDC WD20EZAZ-00L9GB0);

You can modify these configuration information to obtain the update network traffic of the three update schemes under different cluster configurations (Exp. 1-3). 

- Sec. VII-A: Experiment 1 (Update traffic reduction), Figure 10
  Modify Simulation experiments.py in folder /Update_traffic_reduction
  1. k = 10, m = 4, n = 100, block_size = 64, trace_type = Ali or Ten // Line 316-323
  2. python3 Simulation experiments.py
  
- Sec. VII-A: Experiment 2 (Impact of erasure coding), Figure 11
  Modify Simulation experiments.py in folder /Impact_of_erasure_coding
  1. k = 6, m = 3; k = 10, m = 4; k = 12, m = 4;  // Line 316-317
  2. n = 100, block_size = 64, trace_type = Ali or Ten  // Line 318-323
  3. python3 Simulation experiments.py
  
- Sec. VII-A: Experiment 3 (Impact of block size), Figure 12
  Modify Simulation experiments.py in folder /Impact_of_block_size
  1. block_size = 64; 128; 256;  // Line 319
  2. k = 10, m = 4, n = 100, trace_type = Ali or Ten  // Line 316-318, 323
  3. python3 Simulation experiments.py
  
Expected Reproduction Time (in Minutes): Considering that steps 2-3 in the experimental alone require a minimum of 1 month to execute for all traces, we provide a small trace for the simulation in folder CoRD/Alibaba_ECS_Experiments/Trace. We expect to run more traces to get a comprehensive evaluation.

Expected Results: The experimental results will generate a CSV file under the local folder. The first column of the file represents the results of each volume, and the second to fourth columns of the file represent the update network traffic values of the three schemes of Raid-based, Delta-based and CoRD. The update network traffic of CoRD should be less than Raid and Delta in most volumes.

# Alibaba ECS Experiments
To demonstrate how CoRD works, we consider an example in which there is a cluster of 15 storage nodes. One node is the coordinator, and the other 14 nodes are updater. We configure the settings of CoRD via the configuration file the python file 'CoRD/Alibaba_ECS_Experiments/ECPipe/scripts/start.py' and 'CoRD/Alibaba_ECS_Experiments/ECPipe/conf/config.xml' in XML format.

Hardware: (1) CPU: 2vCPUs; (2) In-memory: 8GB RAM; (3) Network: 3Gb/s;

For the configuration file 'start.py', you can limite the network bandwidths of all updater by modifying the parameter 'upload_bw' and 'download_bw'. You need to get the interface identification of network equipment of each updater and set parameter 'net_adapter' (Exp. 4). For the configuration file 'config.xml', you can set the 'log.size(MB)' to configure different log sizes, and set the 'update.opt' to use the flipping idea (Exp. 5-7). Clearly, you can also modify other configurations to complete the results other than the paper experiment.

- Fixed configuration
  config.xml in folder /ECPipe/src
  1. <name>erasure.code.k</name><value>10</value>  // Line 2
  2. <name>erasure.code.n</name><value>14</value>  // Line 3
  3. <name>update.policy</name><value>raid</value>  // Line 8, adopting "raid", "delta", and "crd" to change different update scheme
  4. <name>block.size(KB)</name><value>64</value>  // Line 12
  5. <name>trace.type</name><value>Ali</value>  // adopting "Ali", and "Ten"

- Sec. VII-B: Experiment 4 (Impact of network bandwidth), Figure 13
  Modify start.py in folder /ECPipe/scripts
  1. upload_bw = 0.5 * 1024 * 1024; 1 * 1024 * 1024; 3 * 1024 * 1024
  2. download_bw = 0.5 * 1024 * 1024; 1 * 1024 * 1024; 3 * 1024 * 1024
  Modify config.xml in folder /ECPipe/src
  3. <name>update.opt</name><value>false</value>  // Line 9
  4. <name>log.size(MB)</name><value>4</value>  // Line 10
  Running program
  5. $ hdfs fsck / -files -blocks -locations
  6. $ cd CoRD/Alibaba_ECS_Experiments/ECPipe/
  7. $ python3 scripts/start.py
  
- Sec. VII-B: Experiment 5 (Impact of log size), Figure 14
  Modify start.py in folder /ECPipe/scripts
  1. upload_bw = 3 * 1024 * 1024
  2. download_bw = 3 * 1024 * 1024
  Modify config.xml in folder /ECPipe/src
  3. <name>log.size(MB)</name><value>4</value>  // Line 10, varying 1MB, 4MB, and 16MB
  Running program
  4. $ cd CoRD/Alibaba_ECS_Experiments/ECPipe/
  5. $ python3 scripts/start.py

- Sec. VII-B: Experiment 6 (Impact of  flipping enhancement), Figure 15
  Modify config.xml in folder /ECPipe/src
  1. <name>update.opt</name><value>true</value>  // Line 9
  Running program
  2. $ cd CoRD/Alibaba_ECS_Experiments/ECPipe/
  3. $ python3 scripts/start.py
  
Expected Reproduction Time (in Minutes): Executing all traces require about 1.5 months, we provide a small trace for the simulation in folder CoRD/Alibaba_ECS_Experiments/Trace. We expect to run more traces to get a comprehensive evaluation.
  
Expected Results: The experimental results will generate a CSV file under the local folder. The first column of the file represents the results of each volume, and the second to fourth columns of the file represent the update throughput of the three schemes of Raid-based, Delta-based and CoRD. The update throughput of CoRD should be higher than Raid and Delta in most volumes.

- Sec. VII-B: Experiment 7 (Computation time and memory analysis), Table 2
  Modify config.xml in folder /ECPipe/src
  1. <name>update.request.way</name><value>random</value>  // Line 11
  Running program
  2. $ cd CoRD/Alibaba_ECS_Experiments/ECPipe/
  3. $ python3 scripts/start.py

Expected Reproduction Time (in Minutes): The expected computational time of this artifact is less 1 min.

Expected Results: CoRD has extremely low computation time and memory overhead.
