Beaver is the project to deploy Hive on Spark and Pig automatically onto Hadoop cluster.

## Goal
1. Redeploy cluster with new patch for supported services(Pig, Spark, Hive and Hadoop if needed). 
2. Switch Spark version(update ENVs and needed services)

## Prerequisite
packager is http://10.239.47.156/software/Python-2.7.13.tgz, the step you can refer http://tecadmin.net/install-python-2-7-on-centos-rhel/, after installation, append following in ~/.bashrc
alias python='python2.7'

## Overview
1. Customize configuration in conf/*.custom via key=value pair.
2. Specify you slave in conf/hadoop/slaves.custom.

```
hostname ip username password role
```

3. Customize the environment key=value for cluster nodes in conf/*/env.

4. run following command to create the cluster(for hadoop).

```
bin/cluster hadoop
```

## Environmental preparation
```
source bin/setup-env.sh
```

## Customized configurations
The following settings need to configure by yourself, the configuration value appears below is just for reference.
### 1. hadoop
#### 1) conf/hadoop/env
```
HADOOP_HOME=/opt/Beaver/hadoop
JAVA_HOME=/opt/Beaver/jdk
JDK_VERSION=8u112
HADOOP_VERSION=2.7.3
```
#### 2) conf/hadoop/core-site.xml.custom
```
fs.defaultFS=hdfs://<master_hostname>:9000
```
#### 3) conf/hadoop/hdfs-site.xml.custom
```
dfs.datanode.data.dir=/home/tmp/hadoop/data
dfs.namenode.name.dir=/home/tmp/hadoop/name
```
#### 4) conf/hadoop/mapred-site.xml.custom
```
mapreduce.jobtracker.http.address=<master_hostname>:50030
mapreduce.jobhistory.address=<master_hostname>:10020
mapreduce.jobhistory.webapp.address=<master_hostname>:19888
mapred.job.tracker=<master_hostname>:9001
```
#### 5) conf/hadoop/yarn-site.xml.custom
```
yarn-nodemanager.resource.cpu-vcores=
yarn.resourcemanager.hostname=<master_hostname>
yarn.resourcemanager.address=<master_hostname>:8032
yarn.resourcemanager.scheduler.address=<master_hostname>:8030
yarn.resourcemanager.resource-tracker.address=1<master_hostname>:8031
yarn.resourcemanager.admin.address=<master_hostname>:8033
yarn.resourcemanager.webapp.address=<master_hostname>:8088
yarn.web-proxy.address=<master_hostname>:12345
yarn.nodemanager.aux-services=mapreduce_shuffle,spark_shuffle
yarn.nodemanager.aux-services.spark_shuffle.class=org.apache.spark.network.yarn.YarnShuffleService
spark.shuffle.service.port=7337
```
#### 6) conf/hadoop/slaves.custom
```
<hostname> <ip> <username> <password> master
<hostname> <ip> <username> <password> slave
```
#### 7) conf/hadoop/capacity-scheduler.custom
```
yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator
```
### 2. spark
#### 1) conf/spark/env
```
SPARK_HOME=/opt/Beaver/spark
SPARK_VERSION=2.0.0
```
#### 2) conf/spark/spark-defaults.conf.custom
```
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs://<master_hostname>:9000/spark-history-server
spark.history.fs.logDirectory=hdfs://<master_hostname>:9000/spark-history-server
```
### 3. hive
#### 1) conf/hive/env
```
HIVE_HOME=/opt/Beaver/hive
```
#### 2) conf/hive/hive-site.xml.custom
```
javax.jdo.option.ConnectionURL=jdbc:mysql://localhost:3306/metastore?createDatabaseIfNotExist=true
javax.jdo.option.ConnectionDriverName=com.mysql.jdbc.Driver
javax.jdo.option.ConnectionUserName=<username>
javax.jdo.option.ConnectionPassword=<password>
hive.metastore.uris=thrift://<master_hostname>:9083
spark.eventLog.dir=hdfs://<master_hostname>:8020/spark-history-server
```
#### 3) conf/hive/hive-log4j2.properties.custom
```
property.hive.log.dir =/opt/Beaver/hive/logs
```
## Deploy
```
cd <Beaver_HOME>
```
<Beaver_HOME> means the directory of Beaver project.
```
bin/cluster.py <component>
```

## About mysql version
We will install mysql-5.6 for you by default, if you want to install mysql5.7, please modify the /etc/yum.repos.d/mysql-community
,then modify the "MYSQL 5.6"  enabled value=0 and "MYSQL 5.7" enabled value=1. (For 5.7, we can't set mysql password by this script, you
need to reset it by yourself)

##Running the TPC-DS on hive on spark
0. download DS project in https://github.com/kellyzly/hive-testbench 
1. build TPC-DS
     #./tpcds-build.sh
2. generate  100 GB of TPC-DS data:
     #./tpcds-setup.sh 100

3. run queries and all infomation in log.ds
     #setsid  ./runSuite.pl tpcds 100   (setsid will put the command in background)
     #tail  -f log.ds
          start time:2017/01/03 23:36:57
filename,status,time,rows
query12.sql,success,63,100
query13.sql,success,146,1
query15.sql,success,83,100

There are 2 problems in origin benchmark and i have fixed the problems in my git project
1. all the application started by hive on spark is "Hive on Spark", this is inconvenient for us to locate problem - Fixed
2. print all info in console not file- Fixed
