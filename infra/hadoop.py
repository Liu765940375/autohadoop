#!/usr/bin/python

import os
import sys
import optparse
from utils.util import *
from utils.ssh import *

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
runtime_path = os.path.join(project_path, "runtime")
hadoop_env = get_env_list(os.path.join(runtime_path, "env"))
config_file_names = get_config_files("hadoop")
default_conf_dir = os.path.join(config_path, "hadoop")

# Deploy Hadoop component
def deploy_hadoop(version, conf_dir, slaves):
    generate_slaves(slaves, target_conf_dir)
    setup_nopass(slaves)
    update_etc_hosts(slaves)
    setup_env_dist(slaves, hadoop_env, "hadoop")
    set_path("hadoop", slaves)
    copy_packages(slaves, "hadoop", version)
    copy_spark_shuffle(slaves, spark_env.get("SPARK_VERSION"), hadoop_env.get("HADOOP_HOME"))
    copy_jdk(hadoop_env.get("JDK_VERSION"), slaves)

    copy_configurations(slaves, target_conf_dir, "hadoop")
    print colors.LIGHT_BLUE + "Stop firewall service" + colors.ENDC
    for node in slaves:
        ssh_execute(node, "systemctl stop firewalld")
    stop_hadoop_service()
    print colors.LIGHT_BLUE + "Format filesystem" + colors.ENDC
    ssh_execute(master, "yes | " + hadoop_home + "/bin/hdfs namenode -format")
    start_hadoop_service(master)
    #display_hadoop_perf()

def auto_hardware_config():
    print "Get configs from hardware detect"

def display_hadoop_perf():
    shell_path = os.path.join(project_path, "shell")
    print colors.LIGHT_BLUE + "Test cluster performance, this will take a while..." + colors.ENDC
    print colors.LIGHT_GREEN + "Test hdfs io..." + colors.ENDC
    cmd = ". " + os.path.join(shell_path, "dfstest.sh") + " " + hadoop_home
    os.system(cmd)
    print colors.LIGHT_GREEN + "Test disk io..." + colors.ENDC
    data_dir = "/opt/Beaver/hadoop/data"
    cmd = ". " + os.path.join(shell_path, "diskiotest.sh") + " " + data_dir
    os.system(cmd)

def undeploy_hadoop(version, slaves):
    stop_hadoop_service()
    jdk_version = hadoop_env.get("JDK_VERSION")
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/hadoop-" + version + ";" + "rm -rf /opt/Beaver/hadoop;rm -rf /opt/Beaver/hadooprc;")
        ssh_execute(node, "rm -rf /opt/Beaver/jdk-" + jdk_version + ";" + "rm -rf /opt/Beaver/jdk;")
        ssh_execute(node, "source ~/.bashrc")

# Generate Hadoop slaves config file
def generate_slaves(slaves, conf_dir):
    with open(os.path.join(conf_dir, "slaves"), "w") as f:
        for node in slaves:
            #if node.role == "slave":
            f.write(node.ip + "\n")

# TODO: It should only setup one time.
def copy_jdk(version, slaves):
    package = "jdk-" + version + "-linux-x64.tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC
    copy_package_dist(slaves, os.path.join(package_path, package), "jdk", version)

# Stop hadoop related services
def stop_hadoop_service(slaves):
    print colors.LIGHT_BLUE + "Stop hadoop related services, it may take a while..." + colors.ENDC
    process_list = ["NameNode", "DataNode", "SecondaryNameNode", "NodeManager", "ResourceManager", "WebAppProxyServer", "JobHistoryServer"]
    for node in slaves:
        ssh_execute(node, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver")
        ssh_execute(node, "$HADOOP_HOME/sbin/stop-all.sh")

        stdout = ssh_execute_withReturn(node, "jps")
        process_dict = {}
        for line in stdout:
            k, v = line.partition(" ")[::2]
            process_dict[v.strip()] = k.strip()
        for process in process_list:
            if process_dict.has_key(process):
                ssh_execute(node, "kill -9 " + process_dict.get(process))
        del process_dict

# Start Hadoop related services
def start_hadoop_service(master):
    print colors.LIGHT_BLUE + "Start hadoop related services,  it may take a while..." + colors.ENDC
    ssh_execute(master, hadoop_home + "/sbin/start-all.sh")
    ssh_execute(master, hadoop_home + "/sbin/yarn-daemon.sh start proxyserver")
    ssh_execute(master, hadoop_home + "/sbin/mr-jobhistory-daemon.sh start historyserver")

def restart_hadoop_service(slaves):
    master = get_master_node(slaves)
    auto_hardware_config()
    start_hadoop_service(master)

def update_conf(custom_conf_dir):
    default_conf_dir = os.path.join(project_path, "/conf/hadoop")
    runtime_conf_dir = os.path.join(project_path, "/runtime/hadoop/conf")
    os.system("mkdir -p " + runtime_conf)
    merge_configuration(config_file_names,
            default_conf_dir, custom_conf_dir, runtime_conf_dir)

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

    cluster_config_file = os.path.join(conf_dir, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)

    if action == "deploy":
        deploy_hadoop(version, conf_dir, slaves)
    if action == "update_conf":
        update_conf(conf_dir)
    elif action == "undeploy":
        undeploy_hadoop(version, slaves)
    elif action == "start":
        stop_hadoop_service(slaves)
        start_hadoop_service(slaves)
    elif action == "stop":
        stop_hadoop_service(slaves)
    elif action == "restart":
        restart_hadoop_service()
    elif action == "format":
        print "format hdfs"
        ssh_execute(master, "yes | " + hadoop_home + "/bin/hdfs namenode -format")
    else:
        print "Not support"
        sys.exit(-1)
