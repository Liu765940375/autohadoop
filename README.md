Beaver is the project to deploy SQL on Hadoop automatically onto Hadoop cluster.

## Goal
1. Redeploy cluster with new patch for supported services(Pig, Spark, Hive and Hadoop if needed). 
2. Switch Spark version(update ENVs and needed services)


## Environmental preparation

1 . run following command to set up the enviroment of python.

```
bin/Python_install.sh
```
You can also install python by yourself.
packager is http://10.239.47.156/software/Python-2.7.13.tgz, the step you can refer http://tecadmin.net/install-python-2-7-on-centos-rhel/, after installation, append following in ~/.bashrc
alias python='python2.7'

2 . Specify you slave in conf/slaves.custom.

```
hostname ip username password role(master or slave)
```

3 . run following command to set up the enviroment of Beaver(some related packege).

```
source bin/setup-env.sh
```

##How to use Beaver
There are some kinds of conditions running tests now.
Please ensure that python has been installed in your system first, you can use bin/Python_install.sh to install Python in your system.

1 . switch workload
```
eg.first run BBonHoS, then run BBonSparkSQL
bin/setup-env.sh
bin/runBBonHoS.py deploy_run [confdir](like "/workspace/conf1/conf")
bin/runBBonHoS.py undeploy [confdir]
bin/runBBonSparkSQL.py deploy_run [confdir]
bin/runBBonSparkSQL.py undeploy [confdir]
```

2 . nochange in workload, just replace conf
```
eg.first run BBonHoS conf1, then run BBonHoS conf2
bin/setup-env.sh
bin/runBBonHoS.py deploy_run [confdir](like "/workspace/conf1/conf")
bin/runBBonHoS.py replace_conf_run [confdir]
bin/runBBonHoS.py undeploy [confdir]
```

3 . use different packages on same workload
```
eg.run BBonHoS, first use hive-a.tar.gz, then use hive-b.tar.gz
bin/setup-env.sh
#now please change HIVE_VERSION in [confdir]/env(HIVE_VERSION=a when use hive-a.tar.gz)
#then put hive-a.tar.gz in Beaver/package
bin/runBBonHoS.py deploy_run [confdir](like "/workspace/conf1/conf")
bin/runBBonHoS.py undeploy [confdir]
#then change HIVE_VERSION in [confdir]/env(HIVE_VERSION=b when use hive-b.tar.gz)
#then put hive-b.tar.gz in Beaver/package
bin/runBBonHoS.py deploy_run [confdir]
bin/runBBonHoS.py undeploy [confdir]
```

4 . switch benchmark
```
eg.first run BBonHoS, then run TPC-DSonHoS
bin/setup-env.sh
bin/runBBonHoS.py deploy_run [confdir](like "/workspace/conf1/conf")
\#The following features have not been implemented
\#bin/beaver.py bb undeploy [confdir](like "/workspace/conf1/conf")
\#bin/beaver.py tpc_ds deploy [confdir](like "/workspace/conf1/conf")

```



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

## About mysql version
We will install mysql-5.6 for you by default, if you want to install mysql5.7, please modify the /etc/yum.repos.d/mysql-community
,then modify the "MYSQL 5.6"  enabled value=0 and "MYSQL 5.7" enabled value=1. (For 5.7, we can't set mysql password by this script, you
need to reset it by yourself)

## NTP configuration
There two steps to configure the NTP. If you already have an NTP Server,You can skip the step 1.If not,you should build the NTP Server in a machine by the step 1.
Suppose you have two machines. Node1 is the NTP Server, Node2 is the NTP Client.Node2 is the machine that you want to update it time by NTP.

1 . Install NTP Server
   Install NTP Server by this command:
```
yum install -y ntp
```
    After NTP has been installed. Edit the file of /etc/ntp.conf Red font is the part that required to modify.
```
# Hosts on local network are less restricted.
restrict 192.168.1.0 mask 255.255.255.0 nomodify notrap # Finally, we allow all the LAN client to connect to the server synchronization time, but refused to allow them to modify the time on the server
# Use public servers from the pool.ntp.org project.
# Please consider joining the pool (http://www.pool.ntp.org/join.html).
#server 0.centos.pool.ntp.org iburst  #comment the original time server
#server 1.centos.pool.ntp.org iburst
#server 2.centos.pool.ntp.org iburst
#server 3.centos.pool.ntp.org iburst
server 127.127.1.0        #Set this machine to a time server
fudge 127.127.1.0 stratum 10  # This is the time server hierarchy. Set to 0 for the top, if you want to update the time to another NTP server, do not set it to 0
```
    Then,you can start NTP Server by command:
```
/etc/init.d/ntpd start
```
Now the NTP Server has been started successfully.
You can see the information about NTP by :
```
ntpq -p
```
2 . NTP Client configuration
First you should install some server related to NTP by command:
```
yum install -y ntpdate*
```
Then edit the file /etc/ntp.conf
```
# Hosts on local network are less restricted.
 restrict 192.168.1.0 mask 255.255.255.0 nomodify notrap
# Use public servers from the pool.ntp.org project.
# Please consider joining the pool (http://www.pool.ntp.org/join.html).
#server 0.centos.pool.ntp.org iburst
#server 1.centos.pool.ntp.org iburst
#server 2.centos.pool.ntp.org iburst
#server 3.centos.pool.ntp.org iburst
server Node1  #Set Node1 to be a time server
```
You should alse start NTP Server by command:
```
Centos6:
/etc/init.d/ntpd start
Centos7:
systemctl  enable ntpd  // Ntp service should be set to boot
systemctl start ntpd
```
Finally,you can update system time by command:
```
ntpdate -d Node1
```
-d means use the debug mode.
(If –d don’t work.You can try : ntpdate -u Node1)

## Docker configuration for Beaver
In order to test the function of the project. We need a pure system environment of CentOS7. But it's cumbersome to reinstall the system everytime.
So we use the Docker to build a image about centOS7. It on need a few seconds to run a container for centOS7 system from an image.There are several steps to build a docker environment for our project.

1 . Install Docker
```
yum install -y docker
```
2 .If your network need a proxy for connecting to Internet,you may need to set the proxy for docker
```
edit the file /usr/lib/systemd/system/docker.service
Add belows text to it.
Environment=http_proxy=$proxy_url:$proxy_port
Environment=https_proxy=$proxy_url:$proxy_port
Environment=ftp_proxy=$proxy_url:$proxy_port
```
after set the proxy,you should reload daemon
```
systemctl daemon-reload
```

3 . Now you can start docker
```
systemctl start docker
```

4 . Pull an image
```
docker pull centos:centos7.3.1611
```
5 . Build an image by our Dockerfile
```
dockerfile_path=$Beaver_home/itest/docker_integration
docker build --rm -t centos7:beaver $dockerfile_path
```
centos7 is the name of the image.
beaver is the TAG of the centos7

6 .Now you can test Beaver

Before you test Beaver by the file of run_beaver_docker.sh . 
You should confirm that you have installed the expect. 
You can install it by command:yum install expect -y
```
sh $Beaver_home/itest/docker_integration/run_beaver_docker.sh Beaver centos7:beaver $beaver_path
```
The file of run_beaver_docker.sh will start two container from our image of centos7. And run Beaver on it to build a cluster.
Beaver is the project name.
centos7:beaver is the image name.
$beaver_path is the path of Beaver(It's not the Beaver_HOME)

##Jenkins configuration for Docker
After you have setup jenkins and docker in you system. You can build our project by jenkins automatically.

1 .Login you jenkins page and create a new item(Freestyle project).

2 .You need to do some configuration to your item. You should only modify the configuration item bellows.

###General
![image](https://github.com/intel-hadoop/Beaver/blob/master/itest/docker_integration/images/General.PNG)

###source Code Management 
![image](https://github.com/intel-hadoop/Beaver/blob/master/itest/docker_integration/images/SourceCode.PNG)

###Build Triggers
![image](https://github.com/intel-hadoop/Beaver/blob/master/itest/docker_integration/images/BuildTrigger.PNG)

###Build Environment
![image](https://github.com/intel-hadoop/Beaver/blob/master/itest/docker_integration/images/BuildEnvironment.PNG)

###Build
![image](https://github.com/intel-hadoop/Beaver/blob/master/itest/docker_integration/images/Build.PNG)
