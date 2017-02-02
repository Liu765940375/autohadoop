#!/usr/bin/python
from distribute.deploy import *
from distribute.utils import *
from distribute.node import *

# Start Spark history server
def start_spark_history(slaves, spark_conf):
    spark_eventLog = ""
    spark_history = ""
    with open(spark_conf) as f:
        for line in f:
            if not line.startswith('#') and line.split():
                line = line.split()
                if line[0] == "spark.eventLog.dir":
                    spark_eventLog = line[1]
                if line[0] == "spark.history.fs.logDirectory":
                    spark_history = line[1]
    master = get_master_node(slaves)
    if spark_eventLog is not None:
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir -p " + spark_eventLog)
    if spark_history is not None:
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir -p " + spark_history)
    ssh_execute(master, "$SPARK_HOME/sbin/stop-history-server.sh")
    ssh_execute(master, "$SPARK_HOME/sbin/start-history-server.sh")



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: bin/cluster.py <component>")
        sys.exit(1)

    component = sys.argv[1]
    if component != "hadoop" and component != "spark" \
            and component != "hive" and component != "BB":
        print("Beaver now only support deploy hadoop, spark, hive, BB")
        sys.exit(1)

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = os.path.dirname(current_path)
    script_path = os.path.join(project_path, "scripts")
    config_path = os.path.join(project_path, "conf")
    package_path = os.path.join(project_path, "packages")

    slaves = get_slaves(os.path.join(config_path, "hadoop/slaves.custom"))
    if component == "hadoop":
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
    if component == "spark":
        deploy_spark(project_path)
        start_spark_history(slaves, os.path.join((config_path), "spark/spark-defaults.conf"))
    if component == "hive":
        deploy_hive(project_path)
    if component == "BB":
        deploy_BB(project_path)