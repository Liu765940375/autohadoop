#!/usr/bin/python

import os
import sys

# deprecated
current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from utils.util import *

config_file_names = get_config_files("BB")
bb_config_path = os.path.join(project_path, "conf/BB")

def deploy_BB():
        setup_env_dist([master], bb_env, "BB")
        copy_packages([master], "BB", "1.0")
        copy_configurations([master], config_file_names, bb_config_path, "BB")
        auto_spark_config()

def undeploy_BB():
    os.system("rm -rf /opt/Beaver/BB-1.0;" + "rm -rf /opt/Beaver/BB;rm -rf /opt/Beaver/BBrc")

def restart_BB():
    copy_configurations([master], config_file_names, bb_config_path, "BB")
    auto_spark_config()

def run_BB():
    os.system(bb_home + "/bin/bigBench runBenchmark")

def auto_spark_config():
    list = calculate_hardware()
    vcore_num = int(list[0])
    memory = list[1]
    executor_cores = 4
    instances = int(vcore_num / executor_cores)
    executor_memory = str(int(memory / instances / 1024 * 0.8))
    executor_memoryOverhead = str(int(memory / instances * 0.2))
    with open(os.path.join(bb_config_path, "engineSettings.sql.custom"), 'w') as wf:
        wf.write("set spark.eventLog.enabled=true" + "\n")
        wf.write("set spark.eventLog.dir=hdfs://" + master.hostname + ":9000/spark-history-server" + "\n")
        wf.write("set spark.executor.cores=" + str(executor_cores) + "\n")
        wf.write("set spark.executor.instances=" + str(instances) + "\n")
        wf.write("set spark.executor.memory=" + executor_memory + "g" + "\n")
        wf.write("set spark.yarn.executor.memoryOverhead=" + executor_memoryOverhead + "\n")
    generate_configuration_kv(master, os.path.join(bb_config_path, "engineSettings.sql"), os.path.join(bb_config_path, "engineSettings.sql.custom"), os.path.join(bb_config_path, "engineSettings.sql.final"))
    ssh_copy(master, os.path.join(bb_config_path, "engineSettings.sql.final"),
             "/opt/Beaver/BB/engines/hive/conf/" + "engineSettings.sql")

if __name__ == '__main__':
    args = sys.argv
    action = args[1]

    if action == "deploy":
        deploy_BB()
    if action == "undeploy":
        undeploy_BB()
    if action == "restart":
        restart_BB()
    if action == "run":
        run_BB()

    if action != "deploy" and action != "undeploy" and action != "start" and action != "stop" and action != "restart":
        print colors.LIGHT_BLUE + "We currently only support following services: install, uninstall, start, stop and restart." \
                                  " Please enter correct parameters!" + colors.ENDC