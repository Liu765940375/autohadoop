#!/usr/bin/python

import  os
import sys

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from utils.util import *

config_file_names = get_config_files("hadoop")
hadoop_conf_path = os.path.join(config_path, "hadoop")

# Deploy Hadoop component
def deploy_hadoop(version):
    generate_slaves(hadoop_conf_path)
    setup_nopass()
    set_hosts()

    setup_env_dist(slaves, hadoop_env, "hadoop")
    set_path("hadoop", slaves)

    copy_packages(slaves, "hadoop", version)
    copy_spark_shuffle(slaves, spark_env.get("SPARK_VERSION"), hadoop_env.get("HADOOP_HOME"))
    copy_jdk(hadoop_env.get("JDK_VERSION"), slaves)

    copy_configurations(slaves, config_file_names, hadoop_conf_path, "hadoop", "deploy")
    auto_hardware_config()
    print colors.LIGHT_BLUE + "Stop firewall service" + colors.ENDC
    for node in slaves:
        ssh_execute(node, "systemctl stop firewalld")
    stop_hadoop_service()
    print colors.LIGHT_BLUE + "Format filesystem" + colors.ENDC
    ssh_execute(master, "yes | " + hadoop_home + "/bin/hdfs namenode -format")
    start_hadoop_service()
    display_hadoop_perf()

def auto_hardware_config():
    hardware_config = calculate_hardware()
    with open(os.path.join(hadoop_conf_path, "yarn-site.xml.custom"), 'w') as wf:
        wf.write("yarn.nodemanager.resource.cpu-vcores=" + str(hardware_config[0]) + "\n")
        wf.write("yarn.nodemanager.resource.memory-mb=" + str(hardware_config[1]) + "\n")
        wf.write("yarn.scheduler.maximum-allocation-mb=" + str(hardware_config[1]))
    generate_configuration_xml(master, os.path.join(hadoop_conf_path, "yarn-site.xml"), os.path.join(hadoop_conf_path, "yarn-site.xml.custom"),
                               os.path.join(hadoop_conf_path, "yarn-site.xml.final"))
    for node in slaves:
        ssh_copy(node, os.path.join(hadoop_conf_path, "yarn-site.xml.final"), hadoop_home + "/etc/hadoop/" + "yarn-site.xml")

def display_hadoop_perf():
    shell_path = os.path.join(project_path, "shell")
    print colors.LIGHT_BLUE + "Test cluster performance, this will take a while..." + colors.ENDC
    print colors.LIGHT_GREEN + "Test hdfs io..." + colors.ENDC
    cmd = ". " + os.path.join(shell_path, "dfstest.sh") + " " + hadoop_home
    os.system(cmd)
    print colors.LIGHT_GREEN + "Test disk io..." + colors.ENDC
    data_dir = "/opt/Beaver/hadoop/data"
    # name_dir = "/opt/Beaver/hadoop/data"
    cmd = ". " + os.path.join(shell_path, "diskiotest.sh") + " " + data_dir
    os.system(cmd)
    # cmd = "sh /home/workspace/Beaver/shell/iperfinstall.sh"
    # os.system(cmd)
    # cmd = "sh /home/workspace/Beaver/shell/netiotest.sh " + "tt20" + " " + "bdpe18"
    # os.system(cmd)

def undeploy_hadoop(version):
    stop_hadoop_service()
    jdk_version = hadoop_env.get("JDK_VERSION")
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/hadoop-" + version + ";" + "rm -rf /opt/Beaver/hadoop;rm -rf /opt/Beaver/hadooprc;")
        ssh_execute(node, "rm -rf /opt/Beaver/jdk-" + jdk_version + ";" + "rm -rf /opt/Beaver/jdk;")
        ssh_execute(node, "source ~/.bashrc")

# Generate Hadoop slaves config file
def generate_slaves(hadoop_conf_path):
    with open(os.path.join(hadoop_conf_path, "slaves"), "w") as f:
        for node in slaves:
            f.write(node.ip + "\n")

# Set no-password login from master to slave nodes
def setup_nopass():
    print colors.LIGHT_BLUE + "Set password-free login for clusters:" + colors.ENDC
    home = os.path.expanduser("~")
    privkey = home + "/.ssh/id_rsa"
    pubkey = privkey + ".pub"
    if not os.path.isfile(pubkey):
        os.system("ssh-keygen -t rsa -P '' -f " + privkey)

    os.system("rm ~/.ssh/known_hosts")
    os.system("ssh-keyscan -H `hostname -f` > ~/.ssh/known_hosts")
    os.system("ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts")
    os.system("ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts")
    os.system("ssh-keyscan -H localhost >> ~/.ssh/known_hosts")
    for node in slaves:
        os.system("ssh-keyscan -H " + node.hostname  + " >> ~/.ssh/known_hosts")
        os.system("ssh-keyscan -H " + node.ip + " >> ~/.ssh/known_hosts")
        ssh_copy(node, pubkey, "/tmp/id_rsa.pub")
        ssh_execute(node, "mkdir -p ~/.ssh")
        ssh_execute(node, "cat /tmp/id_rsa.pub >> ~/.ssh/authorized_keys")
        ssh_execute(node, "chmod 0600 ~/.ssh/authorized_keys")

# Copy JDK
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
def stop_hadoop_service():
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
def start_hadoop_service():
    print colors.LIGHT_BLUE + "Start hadoop related services,  it may take a while..." + colors.ENDC
    ssh_execute(master, hadoop_home + "/sbin/start-all.sh")
    ssh_execute(master, hadoop_home + "/sbin/yarn-daemon.sh start proxyserver")
    ssh_execute(master, hadoop_home + "/sbin/mr-jobhistory-daemon.sh start historyserver")

def restart_hadoop_service():
    # stop_hadoop_service()
    # copy_configurations(slaves, config_file_names, hadoop_conf_path, "hadoop", "restart")
    auto_hardware_config()
    start_hadoop_service()

if __name__ == '__main__':
    args = sys.argv
    action = args[1]

    if len(args) == 3:
        version = args[2]
    else:
        version = hadoop_version

    if action == "deploy":
        deploy_hadoop(version)
    if action == "undeploy":
        undeploy_hadoop(version)
    if action == "start":
        stop_hadoop_service()
        start_hadoop_service()
    if action == "stop":
        stop_hadoop_service()
    if action == "restart":
        restart_hadoop_service()

    if action != "deploy" and action != "undeploy" and action != "start" and action != "stop" and action != "restart":
        print colors.LIGHT_BLUE + "We currently only support following services: install, uninstall, start, stop and restart." \
                                  " Please enter correct parameters!" + colors.ENDC