This is the project to deploy Hive Spark and Pig automatically onto Hadoop cluster.

## Goal
1. Redeploy cluster with new patch for supported services(Pig, Spark, Hive and Hadoop if needed). 
2. Switch Spark version(update ENVs and needed services)

## Requirements
1. Keep old configurations when redeploying
2. Replace Yarn related configurations if updating configurations like External.shuffle.services

## Createrepo
#####The following command of createrepo defaults to run in CentOS7
### 1.how to create
#### 1) install creterepo
```
yum -y install createrepo
```
#### 2) createrepo
```
createrepo /srv/my/repo/  //the path depend on your location of repo
```
### 2.how to use
   Make a new file in /etc/yum.repos.d/.File can be named anything but the extension has to be .repo. 
   Like this 
```
vim /etc/yum.repos.d/bdperepo.repo
```
   In the file you just need to include the following:
```
[bdperepo]
name = This is my repo
baseurl = file:///srv/my/repo/
```
   That all you need in your file.bdperepo is the naem of your repo.And the name must be unique. "name" is the detail of the repo."baseurl" is the path of your repo.  
   But if your client is another machine,you should install a HttpServer in your server machine.And change the baseurl to "baseurl=http://(your machine ip)".  
   Now we have build a httpServer and createrepo in our machine bdpe833n2(10.239.47.53).So the baseurl is "baseurl = http://10.239.47.53/".  

### 3.repo include
   Now we have created an internal repo at node bdpe833n2 (10.239.47.53).You can get software by command wget http://10.239.47.53/ (path of the software).  
   Now we have these software. 
* /hive/apache-hive-2.2.0-SNAPSHOT-bin.tar.gz
- /spark/spark-2.0.0-bin-hadoop2-without-hive.tar.gz
- /benchmark/BB_HoS.tar.gz
- /hadoop/hadoop-2.7.3.tar.gz
- /software/pssh-2.3.1.tar.gz

## Cluster deploy
#### The following instructions are using to setup a cluster environment
    Python package management software check, since python version on CentOS is 2.7.5, a bit too old.
    pip is package management of python, CentOS 7 can NOT install python-pip software directly. Use following
    instruction to install pip and ssh library "paramiko".

```
yum -y install gcc python-devel.x86_64 libffi-devel.x86_64 openssl-devel.x86_64
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
python get-pip.py
pip install paramiko
```

###  Customize your cluster.
1. Customize configuration in conf/*.custom via key=value pair.
2. Specify you slave in conf/slaves.custom.

```
hostname ip username password role
```

3. Customize the environment key=value for cluster nodes in conf/env.

4. run following command to create the cluster

```
python cluster.py --version=2.7.3 --component=hadoop
```
