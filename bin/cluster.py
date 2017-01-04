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

def manage_cluster(component, service):
    if component == "hadoop" and service == "deploy":
        deploy_hadoop(project_path)
        print "Hadoop is formatting the filesystem and starting services"
        master = get_master_node(slaves)
        for node in slaves:
            ssh_execute(node, "systemctl stop firewalld")
        ssh_execute(master, "yes | $HADOOP_HOME/bin/hdfs namenode -format")
        stop_hadoop_service()
        start_hadoop_service()
    if component == "hadoop" and service == "restart":
        config_file_names = get_config_files("hadoop", config_path)
        copy_configurations(slaves, config_file_names, os.path.join(config_path, "hadoop"), "hadoop", "restart")
        stop_hadoop_service()
        start_hadoop_service()

    if component == "spark" and service == "deploy":
        deploy_spark(project_path)
        start_spark_history(slaves, os.path.join((config_path), "spark/spark-defaults.conf"))
    if component == "hive" and service == "deploy":
        deploy_hive(project_path)
    if component == "BB" and service == "deploy":
        deploy_BB(project_path)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: bin/cluster.py <component> <service>")
        sys.exit(1)

    component = sys.argv[1]
    if component != "hadoop" and component != "spark" \
            and component != "hive" and component != "BB":
        print("Beaver now only support deploy hadoop, spark, hive, BB")
        sys.exit(1)

    service = sys.argv[2]
    if service != "deploy" and service != "restart":
        print("Beaver now only support deploy and restart service")
        sys.exit(1)

    manage_cluster(component, service)
