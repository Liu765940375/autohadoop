#!/usr/bin/python
from cluster import *

def fail_on_msg():
    pass

def deploy_hive_on_spark():
    pass

if __name__ == '__main__':

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = os.path.dirname(current_path)
    script_path = os.path.join(project_path, "scripts")
    config_path = os.path.join(project_path, "conf")
    package_path = os.path.join(project_path, "packages")

    slaves = get_slaves(os.path.join(config_path, "hadoop/slaves.custom"))
    deploy_hadoop(project_path)
    print "Running commands on master"
    master = get_master_node(slaves)
    for node in slaves:
        ssh_execute(node, "systemctl stop firewalld")
    ssh_execute(master, "yes | $HADOOP_HOME/bin/hdfs namenode -format")
    ssh_execute(master, "$HADOOP_HOME/sbin/stop-all.sh")
    ssh_execute(master, "$HADOOP_HOME/sbin/start-all.sh")
    ssh_execute(master, "$HADOOP_HOME/sbin/yarn-daemon.sh start proxyserver")
    ssh_execute(master, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver")
    ssh_execute(master, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver")

    deploy_spark(project_path)
    start_spark_history(slaves, os.path.join((config_path), "spark/spark-defaults.conf"))

    deploy_hive(project_path)

    # Step 1: check hadoop install
    #check_hadoop(host)
    #subprocess.Popen("cluster", shell=True)
    #print ("Hadoop not ready, please use cluster.py to setup")
    # Step 2: check spark install
    #check_spark(host)
    #print ("Spark not ready, please use cluster.py to setup")
    # Step 3: check hive install
    #check_hive(host)
    #print ("Hive not ready, please use cluster.py to setup")
    # Step 4: deploy service
    #deploy_hive_on_spark()

    #pass