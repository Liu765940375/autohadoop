#!/usr/bin/python

import os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from utils.util import *

config_file_names = get_config_files("spark")
spark_conf_path = os.path.join(config_path, "spark")

def deploy_spark(version):
    setup_env_dist([master], spark_env, "spark")
    set_path("spark", [master])

    copy_packages([master], "spark", version)
    copy_spark_shuffle([master], version, hadoop_env.get("HADOOP_HOME"))

    if version[0:3] == "1.6":
        print colors.LIGHT_BLUE + "Copy spark jars to hive lib" + colors.ENDC
        os.system("cp -f " + spark_home + "/lib/*" + " " + hive_home + "/lib")
    elif version[0:3] == "2.0":
        print colors.LIGHT_BLUE + "Create HDFS directory /spark-2.0.0-bin-hadoop" + colors.ENDC
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir /spark-2.0.0-bin-hadoop")
        print colors.LIGHT_BLUE + "Copy Spark jars from SPARK_HOME to HDFS directory /spark-2.0.0-bin-hadoop, it may take a while......" + colors.ENDC
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -copyFromLocal $SPARK_HOME/jars/* /spark-2.0.0-bin-hadoop")
        ssh_execute(master,
                    "echo \"spark.yarn.jars=hdfs://" + master.hostname + ":9000/spark-2.0.0-bin-hadoop/*\" >> $SPARK_HOME/conf/spark-defaults.conf")

    copy_configurations([master], config_file_names, spark_conf_path, "spark", "deploy")
    print colors.LIGHT_BLUE + "Restart yarn service" + colors.ENDC
    ssh_execute(master, "$HADOOP_HOME/sbin/stop-yarn.sh")
    ssh_execute(master, "$HADOOP_HOME/sbin/start-yarn.sh")
    ssh_execute(master, "$HADOOP_HOME/sbin/yarn-daemon.sh start proxyserver")
    start_spark_history()

def undeploy_spark(version):
    stop_history_server()
    os.system("rm -rf /opt/Beaver/spark-" + version + ";" + "rm -rf /opt/Beaver/spark;rm -rf /opt/Beaver/sparkrc")

# Start Spark history server
def start_spark_history():
    print colors.LIGHT_BLUE + "Start spark history server" + colors.ENDC
    spark_default_path = os.path.join(spark_conf_path, "spark-defaults.conf")
    spark_eventLog = ""
    spark_history = ""
    with open(spark_default_path) as f:
        for line in f:
            if not line.startswith('#') and line.split():
                line = line.split()
                if line[0] == "spark.eventLog.dir":
                    spark_eventLog = line[1]
                if line[0] == "spark.history.fs.logDirectory":
                    spark_history = line[1]
    if spark_eventLog is not None:
        print colors.LIGHT_BLUE+ "\nCreate spark eventlog HDFS directory" + colors.ENDC
        ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_eventLog)
    if spark_history is not None:
        ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_history)
    stop_history_server()
    ssh_execute(master, spark_home + "/sbin/start-history-server.sh")

def stop_history_server():
    # os.system("$SPARK_HOME/sbin/stop-history-server.sh")
    print colors.LIGHT_BLUE + "Stop Spark history-server service..." + colors.ENDC
    stdout = ssh_execute_withReturn(master, "ps -ef | grep HistoryServer")
    for line in stdout:
        if "org.apache.spark.deploy.history.HistoryServer" in line:
            process_id = line.split()[1]
            os.system("kill -9 " + process_id)

def restart_history_server():
    stop_history_server()
    copy_configurations([master], config_file_names, spark_conf_path, "spark", "restart")
    start_spark_history()

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-v', '--version',
                      dest='version',
                      default="2.7.3")
    parser.add_option('--conf',
                      dest='conf_dir',
                      default="")
    parser.add_option('--script',
                      dest='script',
                      default="")
    parser.add_option('--action',
                      dest='action')

    options, remainder = parser.parse_args()

    version = options.version
    conf_dir = options.conf_dir
    script = options.script
    action = options.action

    version = spark_env.get("SPARK_VERSION")

    if action == "update_conf":
        update_conf(conf_dir)
    elif action == "deploy":
        deploy_spark(version)
    if action == "undeploy":
        undeploy_spark(version)
    if action == "stop":
        stop_history_server()
    if action == "start":
        stop_history_server()
        start_spark_history()
    if action == "restart":
        restart_history_server()

    if action != "deploy" and action != "undeploy" and action != "start" and action != "stop" and action != "restart":
        print colors.LIGHT_BLUE + "We currently only support following services: install, uninstall, start, stop and restart." \
                                  " Please enter correct parameters!" + colors.ENDC
