import glob
import re
import time
import os
from utils.config_utils import *
from utils.colors import *
from utils.ssh import *

download_server = "10.239.47.156"
current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
package_path = os.path.join(project_path, "package")
config_path = os.path.join(project_path, "conf")
runtime_path = os.path.join(project_path, "runtime")

# Execute command on slave nodes
def execute_command_dist(slaves, command):
    print ("Execute commands over slaves")
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

# Set environment variables
def setup_env_dist(slaves, envs, component):
        print (colors.LIGHT_BLUE+ "Set environment variables over all nodes" + colors.ENDC)
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
            # for python 2.7
            #for key, value in envs.iteritems():
            for key, value in envs.items():
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

def update_etc_hosts(slaves):
    print (colors.LIGHT_BLUE + "Update \"/etc/hosts\" file" + colors.ENDC)
    str_hosts = "127.0.0.1 localhost\n"
    for node in slaves:
        str_hosts += node.ip + "  " + node.hostname + "\n"

    for node in slaves:
        ssh_execute(node, "echo \"" + str_hosts + "\">/etc/hosts;")

# Add binary files of different components into PATH
def set_path(component, slaves, path):
    print (colors.LIGHT_BLUE+ "Add binary files of " + component + " into PATH env" + colors.ENDC)
    for node in slaves:
        cmd = "echo \"export PATH=" + path + "/bin:" + path + "/sbin:\$PATH\" >> /opt/Beaver/" + component + "rc"
        ssh_execute(node, cmd)
        ssh_execute(node, "source ~/.bashrc")

# Copy and unpack a package to slave nodes
def copy_package_dist(slaves, file, component, version):
    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy " + component + " to " + node.hostname + "..." + colors.ENDC)
        ssh_execute(node, "mkdir -p /opt/Beaver/")
        ssh_copy(node, file, "/opt/Beaver/" + os.path.basename(file))
        print (colors.LIGHT_BLUE + "\tUnzip " + component + " on " + node.hostname + "..." + colors.ENDC)
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
def copy_packages(nodes, component, version):
    print (colors.LIGHT_BLUE + "Distrubte " + "tar.gz file" + " for " + component + colors.ENDC)
    download_url = "http://" + download_server + "/" + component
    package = component + "-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        print (colors.LIGHT_BLUE + "\tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist(nodes, os.path.join(package_path, package), component, version)

# Copy "spark-<version>-yarn-shuffle.jar" to all of Hadoop nodes
def copy_spark_shuffle(slaves, spark_version, hadoop_home):
    package = "spark-" + spark_version + "-yarn-shuffle.jar"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print (colors.LIGHT_BLUE + "Downloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    des_path = os.path.join(hadoop_home, "share/hadoop/yarn/lib")
    cmd = "rm -rf " + des_path + "/spark-*-yarn-shuffle.jar"
    des = os.path.join(des_path, package)
    for node in slaves:
        print (colors.LIGHT_BLUE + "\tCopy spark-shuffle jar to " + node.hostname + "..." + colors.ENDC)
        ssh_execute(node, cmd)
        ssh_copy(node, os.path.join(package_path, package), des)

# Generate final configuration file and copy this files to destination node
def copy_configurations(nodes, config_path, component, home_path):
    print (colors.LIGHT_BLUE + "Distribute configuration files for " + component + ":" + colors.ENDC)
    print (colors.LIGHT_BLUE + "\tGenerate final configuration files of " + component + colors.ENDC)
    path = config_path + "/*"
    final_config_files = glob.glob(path)
    print (final_config_files)
    copy_final_configs(nodes, final_config_files, component, home_path)

#Copy final configuration files to destination
def copy_final_configs(nodes, config_files, component, home_path):
    print (colors.LIGHT_BLUE + "\tCopy configuration files of " + component + " to all nodes" + colors.ENDC)
    if component == "hadoop":
        conf_link = os.path.join(home_path, "etc/hadoop")
        conf_path = os.path.join(home_path, "etc/") + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    if component == "hive":
        conf_link = os.path.join(home_path, "conf")
        conf_path = home_path + "/config/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    if component == "spark":
        conf_link = os.path.join(home_path, "conf")
        conf_path = home_path + "/config/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
    for node in nodes:
        if component == "BB":
            break
        ssh_execute(node, "mkdir -p " + conf_path)
        ssh_execute(node, "cp -r " + conf_link + "/*" + " " + conf_path)
        for file in config_files:
            ssh_copy(node, file, conf_path + os.path.basename(file))
        ssh_execute(node, "rm -rf " + conf_link)
        ssh_execute(node, "ln -s " + conf_path + " " + conf_link)
    if component == "BB":
        conf_link = os.path.join(home_path, "conf")
        conf_path = os.path.join(home_path, "config/") + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())) + "/"
        for node in nodes:
            ssh_execute(node, "mkdir -p " + conf_path)
            ssh_execute(node, "cp -r " + conf_link + "/*" + " " + conf_path)
            for file in config_files:
                if os.path.basename(file) == "engineSettings.sql":
                    conf_link_tmp = os.path.join(home_path, "engines/hive/conf")
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
        print (colors.LIGHT_BLUE + component + " has exists, we do not have to deploy it." + colors.ENDC)
        return True

def stop_firewall(slaves):
    print (colors.LIGHT_BLUE + "Stop firewall service" + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "systemctl stop firewalld")

def install_mysql(node, user, password):
    package = "mysql-community-release-el7-9.noarch.rpm"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    ssh_copy(node, download_package, "/opt/" + package)

    repo_package = "mysql-community.repo"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, repo_package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + repo_package)
    ssh_copy(node, download_package, "/etc/yum.repos.d/" + repo_package)

    cmd = "rpm -qa | grep mysql-community-server;"
    installed = ""
    for line in os.popen(cmd).readlines():
        installed += line.strip('\r\n')
    # For mysql5.7, command "mysqladmin -u username password pass" is not effective.
    install_cmd = "cd /opt;rpm -ivh " + package + ";yum -y install mysql-community-server;" \
                  + "systemctl start mysqld;mysqladmin -u " + user + " password " + password
    if installed.find("mysql-community-server") != -1:
        cmd = "systemctl stop mysqld;yum -y remove mysql-*;rm -rf /var/lib/mysql;"
        cmd += install_cmd
    else:
        cmd = install_cmd
    ssh_execute(node, cmd)
    cmd_grant_privilege = "mysql -u root -p123456 -Bse \"GRANT ALL PRIVILEGES ON *.* TO '" + user \
        + "'@'" + node.hostname + "' IDENTIFIED BY '" + password + "' with grant option;FLUSH PRIVILEGES;\""
    ssh_execute(node, cmd_grant_privilege)