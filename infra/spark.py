#!/usr/bin/python

import os
from utils.util import *

SPARK_COMPONENT = "spark"

def deploy_spark_internal(default_conf, custom_conf, master, beaver_env):
    spark_verion = beaver_env.get("SPARK_VERSION")
    setup_env_dist([master], beaver_env, SPARK_COMPONENT)
    set_path(SPARK_COMPONENT, [master], beaver_env.get("SPARK_HOME"))
    clean_spark(master)
    copy_packages([master], SPARK_COMPONENT, spark_verion)
    update_copy_spark_conf(master, default_conf, custom_conf, beaver_env)
    copy_spark_shuffle([master], spark_verion, beaver_env.get("HADOOP_HOME"))

def clean_spark(master):
    ssh_execute(master, "rm -rf /opt/Beaver/spark*")

def create_related_hdfs_dir(spark_output_conf, master, hadoop_home):
    spark_default_path = os.path.join(spark_output_conf, "spark-defaults.conf")
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
        print (colors.LIGHT_BLUE+ "\nCreate spark eventlog HDFS directory" + colors.ENDC)
        ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_eventLog)
    if spark_history is not None:
        ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_history)

# Start Spark history server
def start_spark_history_server(master, spark_home):
    stop_spark_history_server(master)
    print (colors.LIGHT_BLUE + "Start spark history server" + colors.ENDC)
    ssh_execute(master, spark_home + "/sbin/start-history-server.sh")

def stop_spark_history_server(master):
    print (colors.LIGHT_BLUE + "Stop Spark history-server service..." + colors.ENDC)
    ssh_execute(master, "ps aux | grep 'HistoryServer' | grep 'org.apache.spark.deploy.history.HistoryServer' | awk '{print $2}' | xargs -r kill -9")

def restart_hadoop_yarn(master, hadoop_home):
    print (colors.LIGHT_BLUE + "Restart yarn service" + colors.ENDC)
    ssh_execute(master, hadoop_home + "/sbin/stop-yarn.sh")
    ssh_execute(master, hadoop_home + "/sbin/start-yarn.sh")
    ssh_execute(master, hadoop_home + "/sbin/yarn-daemon.sh start proxyserver")

def deploy_spark(default_conf, custom_conf, master, beaver_env):
    stop_spark_service(master, beaver_env)
    deploy_spark_internal(default_conf, custom_conf, master, beaver_env)
    restart_hadoop_yarn(master, beaver_env.get("HADOOP_HOME"))

def deploy_start_spark(default_conf, custom_conf, master, beaver_env):
    stop_spark_service(master, beaver_env)
    deploy_spark_internal(default_conf, custom_conf, master, beaver_env)
    restart_hadoop_yarn(master, beaver_env.get("HADOOP_HOME"))
    start_spark_history_server(master, beaver_env.get("SPARK_HOME"))

def undeploy_spark(master):
    stop_spark_history_server(master)
    clean_spark(master)

def start_spark_service(master, beaver_env):
    restart_hadoop_yarn(master, beaver_env.get("HADOOP_HOME"))
    start_spark_history_server(master, beaver_env.get("SPARK_HOME"))

def stop_spark_service(master, beaver_env):
    stop_spark_history_server(master, beaver_env.get("SPARK_HOME"))

def update_copy_spark_conf(master, default_conf, custom_conf, beaver_env):
    spark_output_conf = update_conf(SPARK_COMPONENT, default_conf, custom_conf)
    copy_configurations([master], spark_output_conf, SPARK_COMPONENT, beaver_env.get("SPARK_HOME"))
    create_related_hdfs_dir(spark_output_conf, master, beaver_env.get("HADOOP_HOME"))

'''
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
    elif action == "undeploy":
        undeploy_spark(version)
    elif action == "stop":
        stop_history_server()
    elif action == "start":
        stop_history_server()
        start_spark_history()
    else:
        print("not support")
'''
