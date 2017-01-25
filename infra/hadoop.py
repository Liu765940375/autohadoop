#!/usr/bin/python

import os
import sys
import optparse
import fnmatch
from utils.util import *
from utils.ssh import *
from utils.node import *

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
default_conf_path = os.path.join(project_path, "conf")
config_file_names = get_config_files("hadoop")
default_conf_dir = os.path.join(config_path, "hadoop")

# Deploy Hadoop component
def deploy_hadoop(beaver_custom_conf, slaves):
    setup_nopass(slaves)
    update_etc_hosts(slaves)
    default_hadoop_conf = os.path.join(default_conf_path, "hadoop")
    beaver_env = get_env_list(os.path.join(default_conf_path, "env"))
    clean_hadoop(slaves)
    setup_env_dist(slaves, beaver_env, "hadoop")
    set_path("hadoop", slaves)
    copy_packages(slaves, "hadoop", beaver_env.get("HADOOP_VERSION"))
    copy_spark_shuffle(slaves, spark_env.get("SPARK_VERSION"), beaver_env.get("HADOOP_HOME"))
    output_hadoop_conf = update_hadoop_conf(default_hadoop_conf, beaver_custom_conf, master, slaves)
    copy_configurations(slaves, output_hadoop_conf, "hadoop")
    print (colors.LIGHT_BLUE + "Stop firewall service" + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "systemctl stop firewalld")
    #stop_hadoop_service(master, slaves)
    #print (colors.LIGHT_BLUE + "Format filesystem" + colors.ENDC)
    #ssh_execute(master, "yes | " + hadoop_home + "/bin/hdfs namenode -format")
    #start_hadoop_service(master)
    #display_hadoop_perf()

def clean_hadoop(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/hadoop*")
        ssh_execute(node, "source ~/.bashrc")

def deploy_jdk(slaves):
    beaver_env = get_env_list(os.path.join(default_conf_path, "env"))
    copy_jdk(beaver_env.get("JDK_VERSION"), slaves)

def clean_jdk(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/jdk*")

def auto_hardware_config():
    print ("Get configs from hardware detect")

def display_hadoop_perf():
    shell_path = os.path.join(project_path, "shell")
    print (colors.LIGHT_BLUE + "Test cluster performance, this will take a while..." + colors.ENDC)
    print (colors.LIGHT_GREEN + "Test hdfs io..." + colors.ENDC)
    cmd = ". " + os.path.join(shell_path, "dfstest.sh") + " " + hadoop_home
    os.system(cmd)
    print (colors.LIGHT_GREEN + "Test disk io..." + colors.ENDC)
    data_dir = "/opt/Beaver/hadoop/data"
    cmd = ". " + os.path.join(shell_path, "diskiotest.sh") + " " + data_dir
    os.system(cmd)

# Generate Hadoop slaves config file
def generate_slaves(slaves, conf_dir):
    with open(os.path.join(conf_dir, "slaves"), "w") as f:
        for node in slaves:
            #if node.role == "slave":
            f.write(node.ip + "\n")

# merge configuration file
def update_hadoop_conf(default_hadoop_conf, custom_conf, master, slaves):
    custom_hadoop_conf = os.path.join(custom_conf, "hadoop")
    output_hadoop_conf = os.path.join(custom_conf, "output/hadoop")
    # create output dir for merged configuration file
    os.system("rm -rf " + output_hadoop_conf)
    os.system("mkdir -p " + output_hadoop_conf)
    processed_file = {}
    # loop in default_hadoop_conf, merge with custom conf file and copy to output_hadoop_conf
    for conf_file in [file for file in os.listdir(default_hadoop_conf) if fnmatch.fnmatch(file, '*.xml')]:
        custom_conf_file = os.path.join(custom_hadoop_conf, conf_file)
        default_conf_file = os.path.join(default_hadoop_conf, conf_file)
        output_conf_file = os.path.join(output_hadoop_conf, conf_file)
        if os.path.isfile(custom_conf_file):
            merge_conf_file(default_conf_file, custom_conf_file, output_conf_file)
        else:
            os.system("cp " + default_conf_file + " " + output_conf_file)
        processed_file[conf_file] = ""
    # copy unprocessed file in custom_hadoop_conf to output_hadoop_conf
    for conf_file in [file for file in os.listdir(custom_hadoop_conf) if fnmatch.fnmatch(file, '*.xml')]:
        if conf_file not in processed_file:
            custom_conf_file = os.path.join(custom_hadoop_conf, conf_file)
            output_conf_file = os.path.join(output_hadoop_conf, conf_file)
            os.system("cp " + custom_conf_file + " " + output_conf_file)
    # generate slaves file
    generate_slaves(slaves, output_hadoop_conf)
    # for all conf files, replace the related value, eg, replace master_hostname with real hostname
    for conf_file in [file for file in os.listdir(output_hadoop_conf) if fnmatch.fnmatch(file, '*.xml')]:
        output_conf_file = os.path.join(output_hadoop_conf, conf_file)
        replace_conf_value(output_conf_file, "master_hostname", master.hostname)
        format_xml_file(output_conf_file)
    return output_hadoop_conf

def copy_hadoop_conf(default_hadoop_conf, beaver_custom_conf):
    os.system("cp -r " + default_hadoop_conf + "/hadoop " + beaver_custom_conf)

# TODO: It should only setup one time.
def copy_jdk(version, slaves):
    package = "jdk-" + version + "-linux-x64.tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print (colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist(slaves, os.path.join(package_path, package), "jdk", version)

# Stop hadoop related services
def stop_hadoop_service(master, slaves):
    print (colors.LIGHT_BLUE + "Stop hadoop related services, it may take a while..." + colors.ENDC)
    process_list = ["NameNode", "DataNode", "SecondaryNameNode", "NodeManager", "ResourceManager", "WebAppProxyServer", "JobHistoryServer"]
    ssh_execute(master, "$HADOOP_HOME/sbin/stop-all.sh")
    for node in slaves:
        ssh_execute(node, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver")

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
    print (colors.LIGHT_BLUE + "Start hadoop related services,  it may take a while..." + colors.ENDC)
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
    parser.add_option('--conf',
                      dest='conf_dir',
                      default="")
    parser.add_option('--script',
                      dest='script',
                      default="")
    parser.add_option('--action',
                      dest='action')

    options, remainder = parser.parse_args()

    conf_dir = options.conf_dir
    script = options.script
    action = options.action

    cluster_config_file = os.path.join(conf_dir, "slaves.custom")
    slaves = get_slaves(cluster_config_file)
    master = get_master_node(slaves)

    if action == "deploy":
        deploy_hadoop(conf_dir, slaves)
    elif action == "update_conf":
        update_conf(conf_dir)
    elif action == "undeploy":
        clean_hadoop(slaves)
    elif action == "start":
        stop_hadoop_service(slaves)
        start_hadoop_service(slaves)
    elif action == "stop":
        stop_hadoop_service(slaves)
    elif action == "restart":
        restart_hadoop_service()
    elif action == "format":
        print ("format hdfs")
        ssh_execute(master, "yes | " + hadoop_home + "/bin/hdfs namenode -format")
    else:
        print ("Not support")
        sys.exit(-1)
