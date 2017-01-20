import glob
import re
import shutil
import time
import tempfile

from config_utils import *
from node import *
from ssh import *
from colors import *

download_server = "10.239.47.156"
current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
package_path = os.path.join(project_path, "package")
config_path = os.path.join(project_path, "conf")

slaves = get_slaves(os.path.join(project_path, "conf/slaves.custom"))
master = get_master_node(slaves)

# Execute command on slave nodes
def execute_command_dist(slaves, command):
    print "Execute commands over slaves"
    for node in slaves:
        ssh_execute(node, command)

# Get all the defined envs
def get_env_list(filename):
    envs = {}
    if not os.path.isfile(filename):
        return envs
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            envs[key.strip()] = value.strip()

    return envs

hadoop_env = get_env_list(os.path.join(config_path, "hadoop/env"))
hadoop_version = hadoop_env.get("HADOOP_VERSION")
hadoop_home = hadoop_env.get("HADOOP_HOME")
java_home = hadoop_env.get("JAVA_HOME")
hive_env = get_env_list(os.path.join(config_path, "hive/env"))
hive_version = hive_env.get("HIVE_VERSION")
hive_home = hive_env.get("HIVE_HOME")
spark_env = get_env_list(os.path.join(config_path, "spark/env"))
spark_version = spark_env.get("SPARK_VERSION")
spark_home = spark_env.get("SPARK_HOME")
bb_env = get_env_list(os.path.join(config_path, "BB/env"))
bb_version = bb_env.get("BB_VERSION")
bb_home = bb_env.get("BB_HOME")

# Set environment variables
def setup_env_dist(slaves, envs, component):
        print colors.LIGHT_BLUE+ "Set environment variables over all nodes" + colors.ENDC
        # list = ["HADOOP_HOME", "JAVA_HOME", "HIVE_HOME", "BB_HOME"]
        rcfile = "/opt/Beaver/" + component + "rc"
        for node in slaves:
            cmd = ""
            cmd += "rm -f " + rcfile + "; "
            # with open(rcfile, 'w') as f:
            #     for key, value in envs.iteritems():
            #         if key in list:
            #             line = key + "=" + value
            #             f.write(line + "\n")
            for key, value in envs.iteritems():
                cmd += "echo \"export " + key + "=" + value + "\">> /opt/Beaver/" + component + "rc;"
            if detect_rcfile(node, component):
                cmd += "echo \"" + "if [ -f " + rcfile + " ]; then\n" \
                        + "   . " + rcfile + "\n" \
                        + "fi\"" + " >> ~/.bashrc;"
            ssh_execute_withReturn(node, cmd)

# Detect whether the "~/.bashrc" file of this node has been written in this statement: "./opt/Beaver/<component>rc"
def detect_rcfile(node, component):
    if not os.path.exists(package_path):
        os.makedirs(package_path)
    remote_path = "/" + node.username + "/.bashrc"
    ssh_download(node, remote_path, os.path.join(package_path, "bashrc"))
    bashfile = os.path.join(package_path, "bashrc")
    strLine = ". /opt/Beaver/" + component + "rc"
    with open(bashfile) as f:
        flag = True
        for line in f:
            str = re.findall(strLine, line)
            if len(str) > 0:
                flag = False
    os.remove(bashfile)
    return flag

# # Wirte IP address of all nodes to "/etc/hosts" file
# def set_hosts():
#     print colors.LIGHT_BLUE + "Update \"/etc/hosts\" file" + colors.ENDC
#     str_hosts = "127.0.0.1 localhost\n"
#     for node in slaves:
#         str_hosts += node.ip + "  " + node.hostname + "\n"
#
#     for node in slaves:
#         ssh_execute(node, "echo \"" + str_hosts + "\">/etc/hosts;")

# Wirte IP address of all nodes to "/etc/hosts" file
def set_hosts():
    old_file = "/etc/hosts"
    temp_file = tempfile.mktemp()
    with open(old_file) as rf, open(temp_file, "w") as wf:
        flag = True
        for node in slaves:
            for line in rf:
                str = re.findall(node.ip, line)
                if len(str) > 0:
                    wf.writelines(node.ip + " " + node.hostname)
            else:
                wf.writelines(line)
                flag = False
            wf.writelines(line)
        if flag:
            wf.writelines(node.ip + " " + node.hostname + "\n")

    cmd = "cp /etc/hosts " + "/etc/hosts." + time.strftime('%Y%m%d%H%M%S') + ";"
    os.system(cmd)
    os.remove(old_file)
    shutil.copy(temp_file, old_file)
    os.remove(temp_file)

# Add binary files of different components into PATH
def set_path(component, slaves):
    print colors.LIGHT_BLUE+ "Add binary files of " + component + " into PATH env" + colors.ENDC
    for node in slaves:
        if component == "hadoop":
            cmd = "echo \"export PATH=" + java_home + "/bin:" + hadoop_home + "/bin:" + hadoop_home + "/sbin:$PATH\" >> /opt/Beaver/" + component + "rc"
        if component == "spark":
            cmd = "echo \"export PATH=" + spark_home + "/bin:" + spark_home + "/sbin:$PATH\" >> /opt/Beaver/" + component + "rc"
        if component == "hive":
            cmd = "echo \"export PATH=" + hive_home + "/bin:$PATH\" >> /opt/Beaver/" + component + "rc"
        ssh_execute(node, cmd)
        ssh_execute(node, "source ~/.bashrc")

# Get the list of configuration files for different components
def get_config_files(component):
    if component == "hadoop":
        config_file_names = ["hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml", "capacity-scheduler.xml"]
    if component == "spark":
        config_file_names = ["spark-defaults.conf"]
    if component == "hive":
        config_file_names = ["hive-site.xml", "hive-log4j2.properties"]
    if component == "BB":
        config_file_names = ["engineSettings.sql", "bigBench.properties", "userSettings.conf"]
    return config_file_names

# Copy and unpack a package to slave nodes
def copy_package_dist(slaves, file, component, version):
    for node in slaves:
        print colors.LIGHT_BLUE + "\tCopy " + component + " to " + node.hostname + "..." + colors.ENDC
        ssh_execute(node, "mkdir -p /opt/Beaver/")
        ssh_copy(node, file, "/opt/Beaver/" + os.path.basename(file))
        print colors.LIGHT_BLUE + "\tUnzip " + component + " on " + node.hostname + "..." + colors.ENDC
        softlink = "/opt/Beaver/" + component
        package = "/opt/Beaver/" + os.path.basename(file)
        component_home = "/opt/Beaver/" + component + "-" + version
        cmd = "rm -rf " + softlink + ";"
        cmd += "rm -rf " + component_home + ";"
        cmd += "mkdir -p " + component_home + ";"\
            + "tar zxf " + package + " -C " + component_home + " --strip-components=1 > /dev/null"
        ssh_execute(node, cmd)
        cmd = "ln -s " + component_home + " " + softlink + ";"\
            + "rm -rf " + package
        ssh_execute(node, cmd)

# Copy component package to slave nodes
def copy_packages(slaves, component, version):
    print colors.LIGHT_BLUE + "Distrubte " + "tar.gz file" + " for " + component + colors.ENDC
    download_url = "http://" + download_server + "/" + component
    package = component + "-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        print colors.LIGHT_BLUE + "\tDownloading " + package + " from our repo..." + colors.ENDC
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC
    if component == "hive" or component =="spark" or component =="BB":
        slaves = [master]
    copy_package_dist(slaves, os.path.join(package_path, package), component, version)

# Copy "spark-<version>-yarn-shuffle.jar" to all of Hadoop nodes
def copy_spark_shuffle(slaves, spark_version, hadoop_home):
    package = "spark-" + spark_version + "-yarn-shuffle.jar"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print colors.LIGHT_BLUE + "Downloading " + package + " from our repo..." + colors.ENDC
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC
    des_path = os.path.join(hadoop_home, "share/hadoop/yarn/lib")
    cmd = "rm -rf " + des_path + "/spark-*-yarn-shuffle.jar"
    des = os.path.join(des_path, package)
    for node in slaves:
        print colors.LIGHT_BLUE + "\tCopy spark-shuffle jar to " + node.hostname + "..." + colors.ENDC
        ssh_execute(node, cmd)
        ssh_copy(node, os.path.join(package_path, package), des)

# Generate final configuration file and copy this files to destination node
def copy_configurations(slaves, config_file_names, config_path, component, action):
    print colors.LIGHT_BLUE + "Distribute configuration files for " + component + ":" + colors.ENDC
    print colors.LIGHT_BLUE + "\tGenerate final configuration files of " + component + colors.ENDC
    for config_file in config_file_names:
        template_config = os.path.join(config_path, config_file) + ".template"
        custom_config = os.path.join(config_path, config_file) + ".custom"
        if config_file == "hdfs-site.xml" and action == "deploy":
            custom_configs = {}
            get_configs_from_kv(custom_config, custom_configs)
            data_dir = "/opt/Beaver/hadoop/name"
            name_dir = "/opt/Beaver/hadoop/data"
            if custom_configs.has_key("dfs.namenode.name.dir"):
                data_dir = custom_configs.get("dfs.namenode.name.dir")
            if custom_configs.has_key("dfs.namenode.data.dir"):
                name_dir = custom_configs.get("dfs.namenode.data.dir")
            print colors.LIGHT_BLUE + "\tDelete existed namenode name.dir and data.dir" + colors.ENDC
            for node in slaves:
                ssh_execute(node, "rm -rf " + data_dir)
                ssh_execute(node, "rm -rf " + name_dir)

        target_config = os.path.join(config_path, config_file)
        if component == "hadoop" or component == "hive" and config_file != "hive-log4j2.properties":
            generate_configuration_xml(master, template_config, custom_config, target_config)
        if component == "spark" or component == "BB":
            generate_configuration_kv(master, template_config, custom_config, target_config)
        if component == "hive" and config_file == "hive-log4j2.properties":
            generate_configuration_kv(master, template_config, custom_config, target_config)
    path = config_path + "/*"
    final_config_files = glob.glob(path)
    copy_final_configs(slaves, final_config_files, component)

#Copy final configuration files to destination
def copy_final_configs(slaves, config_files, component):
    print colors.LIGHT_BLUE + "\tCopy configuration files of " + component + " to all nodes" + colors.ENDC
    if component == "hadoop":
        conf_link = os.path.join(hadoop_home, "etc/hadoop")
        conf_path = os.path.join(hadoop_home, "etc/") + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    if component == "hive":
        conf_link = os.path.join(hive_home, "conf")
        conf_path = hive_home + "/config/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    if component == "spark":
        conf_link = os.path.join(spark_home, "conf")
        conf_path = spark_home + "/config/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    for node in slaves:
        if component == "BB":
            break
        ssh_execute(node, "mkdir -p " + conf_path)
        ssh_execute(node, "cp -r " + conf_link + "/*" + " " + conf_path)
        for file in config_files:
            ssh_copy(node, file, conf_path + os.path.basename(file))
        ssh_execute(node, "rm -rf " + conf_link)
        ssh_execute(node, "ln -s " + conf_path + " " + conf_link)
    if component == "BB":
        conf_link = os.path.join(bb_home, "conf")
        conf_path = os.path.join(bb_home, "config/") + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
        for node in slaves:
            ssh_execute(node, "mkdir -p " + conf_path)
            ssh_execute(node, "cp -r " + conf_link + "/*" + " " + conf_path)
            for file in config_files:
                if os.path.basename(file) == "engineSettings.sql":
                    conf_link_tmp = os.path.join(bb_home, "engines/hive/conf")
                    # conf_path_tmp = os.path.join(bb_home, "engines/hive/config/") + str(
                    #     time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
                    # ssh_execute(node, "mkdir -p " + conf_path)
                    ssh_execute(node, "cp -r " + conf_link_tmp + "/*" + " " + conf_path)
                    ssh_copy(node, file, conf_path + os.path.basename(file))
                    ssh_execute(node, "rm -rf " + conf_link_tmp)
                    ssh_execute(node, "ln -s " + conf_path+ " " + conf_link_tmp)
                    continue
                ssh_copy(node, file, conf_path + os.path.basename(file))
            ssh_execute(node, "rm -rf " + conf_link)
            ssh_execute(node, "ln -s " + conf_path + " " + conf_link)

# Calculate statistics of hardware information for each node
# def calculate_hardware():
#     hardware_dict = {}
#     for node in slaves:
#         list = []
#         cmd = "cat /proc/cpuinfo | grep \"processor\" | wc -l"
#         stdout = ssh_execute_withReturn(node, cmd)
#         for line in stdout:
#             vcore_num = line
#             list.append(vcore_num)
#         cmd = "cat /proc/meminfo | grep \"MemTotal\""
#         stdout = ssh_execute_withReturn(node, cmd)
#         for line in stdout:
#             memory = int(int(line.split()[1])/1024*0.85)
#             list.append(memory)
#         hardware_dict[node.hostname] = list
#     return hardware_dict

def calculate_hardware():
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

def check_env(component, version):
    cmd = "ls /opt/Beaver | grep -x " + component + "-" + version
    installed = ""
    for line in os.popen(cmd).readlines():
        installed += line.strip()
    if installed != component + "-" + version:
        return False
    else:
        print colors.LIGHT_BLUE + component + " has exists, we do not have to deploy it." + colors.ENDC
        return True