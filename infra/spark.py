#!/usr/bin/python

from infra.hadoop import *
from utils.util import *

SPARK_COMPONENT = "spark"


def deploy_spark_internal(default_conf, custom_conf, master, slaves, beaver_env):
    clean_spark(master)
    spark_verion = beaver_env.get("SPARK_VERSION")
    setup_env_dist([master], beaver_env, SPARK_COMPONENT)
    copy_packages([master], SPARK_COMPONENT, spark_verion)
    update_copy_spark_conf(master, slaves, default_conf, custom_conf, beaver_env)
    copy_spark_shuffle(slaves, spark_verion, beaver_env.get("HADOOP_HOME"))


def clean_spark(master):
    ssh_execute(master, "rm -rf /opt/Beaver/spark*")


# Calculate statistics of hardware information
def calculate_hardware(master):
    list = []
    cmd = "cat /proc/cpuinfo | grep \"processor\" | wc -l"
    stdout = ssh_execute_withReturn(master, cmd)
    for line in stdout:
        vcore_num = int(line)
        list.append(vcore_num)
    cmd = "cat /proc/meminfo | grep \"MemTotal\""
    stdout = ssh_execute_withReturn(master, cmd)
    for line in stdout:
        memory = int(int(line.split()[1]) / 1024 * 0.85)
        list.append(memory)
    return list


def create_related_hdfs_dir(spark_output_conf, master, slaves, beaver_env):
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
    if spark_eventLog is not None or spark_history is not None:
        start_hadoop_service(master, slaves, beaver_env)
        hadoop_home = beaver_env.get("HADOOP_HOME")
        if spark_eventLog is not None:
            print (colors.LIGHT_BLUE+ "\nCreate spark eventlog HDFS directory" + colors.ENDC)
            ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_eventLog)
        if spark_history is not None:
            ssh_execute(master, hadoop_home + "/bin/hadoop fs -mkdir -p " + spark_history)
        stop_hadoop_service(master, slaves)

# Start Spark history server
def start_spark_history_server(master, beaver_env):
    stop_spark_history_server(master)
    print (colors.LIGHT_BLUE + "Start spark history server" + colors.ENDC)
    ssh_execute(master, beaver_env.get("SPARK_HOME") + "/sbin/start-history-server.sh")


def stop_spark_history_server(master):
    print (colors.LIGHT_BLUE + "Stop Spark history-server service..." + colors.ENDC)
    ssh_execute(master, "ps aux | grep 'HistoryServer' | grep 'org.apache.spark.deploy.history.HistoryServer' | awk '{print $2}' | xargs -r kill -9")


def deploy_spark(default_conf, custom_conf, master, slaves, beaver_env):
    stop_spark_service(master)
    deploy_spark_internal(default_conf, custom_conf, master, slaves, beaver_env)


def undeploy_spark(master):
    stop_spark_history_server(master)
    clean_spark(master)


def stop_spark_service(master):
    stop_spark_history_server(master)


def start_spark_service(master, beaver_env):
    start_spark_history_server(master, beaver_env)


def update_copy_spark_conf(master, slaves, default_conf, custom_conf, beaver_env):
    spark_output_conf = update_conf(SPARK_COMPONENT, default_conf, custom_conf)
    for conf_file in [file for file in os.listdir(spark_output_conf) if file.endswith(('.conf', '.xml'))]:
        output_conf_file = os.path.join(spark_output_conf, conf_file)
        dict = get_spark_replace_dict(master, slaves)
        replace_conf_value(output_conf_file, dict)
    copy_configurations([master], spark_output_conf, SPARK_COMPONENT, beaver_env.get("SPARK_HOME"))
    create_related_hdfs_dir(spark_output_conf, master, slaves, beaver_env)


def get_spark_replace_dict(master, slaves):
    print("Calculate vcore and memory configurations into spark-defaults.conf")
    hardware_config_list = calculate_hardware(master)
    node_num = len(slaves)
    total_cores = int(hardware_config_list[0]) * node_num
    total_memory = hardware_config_list[1] * node_num
    executor_cores = 4
    instances = int(total_cores / executor_cores)
    executor_memory = str(int(total_memory / instances / 1024 * 0.8))
    executor_memoryOverhead = str(int(total_memory / instances * 0.2))
    dict = {'master_hostname':master.hostname,
            '{%spark.executor.cores%}':str(executor_cores), '{%spark.executor.instances%}':str(instances),
            '{%spark.executor.memory%}':executor_memory, '{%spark.yarn.executor.memoryOverhead%}':executor_memoryOverhead}
    return dict


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
