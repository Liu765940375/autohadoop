import glob
import re

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

# Set environment variables
def setup_env_dist(slaves, envs, component):
    print colors.LIGHT_GREEN + "Set environment variables over all nodes" + colors.ENDC
    for node in slaves:
        cmd = ""
        cmd += "rm -f /opt/Beaver/" + component + "rc;"
        for key, value in envs.iteritems():
            cmd += "echo \"export " + key + "=" + value + "\">> /opt/Beaver/" + component + "rc;"
        if detect_rcfile(node, component):
            cmd += "echo \". /opt/Beaver/" + component + "rc" + "\" >> ~/.bashrc;"
        ssh_execute(node, cmd)
    print colors.LIGHT_BLUE + "\tEnvironment variables are stored in " + "\"/opt/Beaver/" + component + "rc\" file"+ colors.ENDC

# Get all the defined envs
def get_env_list(filename):
    # print colors.GREEN + "Get env list from " + filename + colors.ENDC
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

spark_env = get_env_list(os.path.join(project_path, "conf/spark/env"))
hive_env = get_env_list(os.path.join(project_path, "conf/hive/env"))
hadoop_env = get_env_list(os.path.join(project_path, "conf/hadoop/env"))
hadoop_home = hadoop_env.get("HADOOP_HOME")
java_home = hadoop_env.get("JAVA_HOME")
hive_home = hive_env.get("HIVE_HOME")
spark_home = spark_env.get("SPARK_HOME")

# Wirte IP address of all nodes to "/etc/hosts" file
def set_hosts(slaves):
    print colors.LIGHT_GREEN + "Update \"/etc/hosts\" file" + colors.ENDC
    str_hosts = "127.0.0.1 localhost\n"
    for node in slaves:
        str_hosts += node.ip + "  " + node.hostname + "\n"

    for node in slaves:
        ssh_execute(node, "echo \"" + str_hosts + "\">/etc/hosts;")

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

# Add binary files of different components into PATH
def set_path(component, slaves):
    print colors.LIGHT_GREEN + "Add binary files of " + component + " into PATH env" + colors.ENDC
    for node in slaves:
        if component == "hadoop":
            cmd = "echo \"export PATH=" + java_home + "/bin:" + hadoop_home + "/bin:" + hadoop_home + "/sbin:$PATH\" >> /opt/Beaver/" + component + "rc"
        if component == "spark":
            cmd = "echo \"export PATH=" + spark_home + "/bin:" + spark_home + "/sbin:$PATH\" >> /opt/Beaver/" + component + "rc"
        if component == "hive":
            cmd = "echo \"export PATH=" + hive_home + "/bin:$PATH\" >> /opt/Beaver/" + component + "rc"
        ssh_execute(node, cmd)

# Get the list of configuration files for different components
def get_config_files(component, config_path):
    config_file_names = ""
    if component == "hadoop":
        config_file_names = ["hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml", "capacity-scheduler.xml"]
        print colors.LIGHT_GREEN + "Generate \"slaves\" configuration file of hadoop" + colors.ENDC
        with open(os.path.join(config_path, "slaves"), "w") as f:
            for node in slaves:
                f.write(node.ip + "\n")
        # Setup Nopass for slave nodes
        print colors.LIGHT_GREEN + "Set password-free login for clusters" + colors.ENDC
        setup_nopass(slaves)
    if component == "spark":
        config_file_names = ["spark-defaults.conf"]
    if component == "hive":
        config_file_names = ["hive-site.xml", "hive-log4j2.properties"]
    if component == "BB":
        config_file_names = ["engineSettings.sql"]
    return config_file_names

# Copy and unpack a package to slave nodes
def copy_package_dist(slaves, file, component, version):
    for node in slaves:
        print colors.LIGHT_BLUE + "\tCopy " + component + " to " + node.hostname + colors.ENDC
        ssh_execute(node, "mkdir -p /opt/Beaver/")
        ssh_copy(node, file, "/opt/Beaver/" + os.path.basename(file))
        print colors.LIGHT_BLUE + "\tUnzip " + component + " on " + node.hostname + colors.ENDC
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
def copy_packages(component, version, slaves):
    print colors.LIGHT_GREEN + "Distrubte " + "tar.gz file" + " for " + component + colors.ENDC
    download_url = "http://" + download_server + "/" + component
    package = component + "-" + version + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        print colors.LIGHT_BLUE + "\tDownloading " + package + " from our repo..." + colors.ENDC
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print colors.YELLOW + "\t" + package + " has already exists in Beaver package" + colors.ENDC
    copy_package_dist(slaves, os.path.join(package_path, package), component, version)

# Copy JDK
# TODO: It should only setup one time.
def copy_jdk(version, slaves):
    package = "jdk-" + version + "-linux-x64.tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print colors.YELLOW + "\t" + package + " has already exists in Beaver package" + colors.ENDC
    copy_package_dist(slaves, os.path.join(package_path, package), "jdk", version)


# Copy "spark-<version>-yarn-shuffle.jar" to all of Hadoop nodes
def copy_spark_shuffle(spark_version, hadoop_home):
    package = "spark-" + spark_version + "-yarn-shuffle.jar"
    print colors.LIGHT_GREEN + "\tCopy " + package + " to all nodes" + colors.ENDC
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print colors.LIGHT_BLUE + "Downloading " + package + " from our repo..." + colors.ENDC
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print colors.YELLOW + "\t" + package + " has already exists in Beaver package" + colors.ENDC
    des_path = os.path.join(hadoop_home, "share/hadoop/yarn/lib")
    cmd = "rm -rf " + des_path + "/spark-*-yarn-shuffle.jar"
    des = os.path.join(des_path, package)
    for node in slaves:
        ssh_execute(node, cmd)
        ssh_copy(node, os.path.join(package_path, package), des)

# Install mysql, start mysql service; configure username and password
def install_mysql(node, user, password):
    print colors.LIGHT_GREEN + "Install mysql:" + colors.ENDC
    package = "mysql-community-release-el7-9.noarch.rpm"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, package)
    if not os.path.isfile(download_package):
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
        print colors.LIGHT_BLUE+ "\tDownload mysql rpm file from repo:" + colors.ENDC
    ssh_copy(node, download_package, "/opt/Beaver/" + package)

    repo_package = "mysql-community.repo"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, repo_package)
    if not os.path.isfile(download_package):
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + repo_package)
    ssh_copy(node, download_package, "/etc/yum.repos.d/" + repo_package)

    cmd = "rpm -qa | grep mysql-community-server;"
    installed = ""
    for line in os.popen(cmd).readlines():
         installed += line.strip('\r\n')
    # For mysql5.7, command "mysqladmin -u username password pass" is not effective.
    install_cmd = "cd /opt/Beaver/;rpm -ivh " + package + ";yum -y install mysql-community-server;"\
                  + "systemctl start mysqld;mysqladmin -u " + user + " password " + password
    if installed.find("mysql-community-server") != -1:
        print colors.LIGHT_BLUE + "\tMysql has been installed, mysql service and related folders will be removed" + colors.ENDC
        cmd = "systemctl stop mysqld;yum -y remove mysql-*;rm -rf /var/lib/mysql;"
        cmd += install_cmd
    else:
        cmd = install_cmd
    os.system(cmd)
    print colors.LIGHT_BLUE + "\tComplete mysql installation, and mysql service has been started" + colors.ENDC

#Copy final configuration files to destination
def setup_config_dist(slaves, config_files, component):
    print colors.LIGHT_BLUE + "\tCopy configuration files of " + component + " to all nodes" + colors.ENDC
    if component == "hadoop":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/Beaver/hadoop/etc/hadoop/" + os.path.basename(file))
    if component == "spark":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/Beaver/spark/conf/" + os.path.basename(file))
    if component == "hive":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/Beaver/hive/conf/" + os.path.basename(file))
    if component == "BB":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/Beaver/BB/engines/hive/conf/" + os.path.basename(file))

# Generate final configuration file and copy this files to destination node
def copy_configurations(slaves, config_file_names, config_path, component, action):
    print colors.LIGHT_GREEN + "Distribute configuration files for " + component + ":" + colors.ENDC
    print colors.LIGHT_BLUE + "\tGenerate final configuration files of " + component + colors.ENDC
    for config_file in config_file_names:
        template_config = os.path.join(config_path, config_file) + ".template"
        custom_config = os.path.join(config_path, config_file) + ".custom"
        if config_file == "hdfs-site.xml" and action == "deploy":
            custom_configs = {}
            get_configs_from_kv(custom_config, custom_configs)
            data_dir = "/home/tmp/hadoop/name"
            name_dir = "/home/tmp/hadoop/data"
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
    setup_config_dist(slaves, final_config_files, component)

# Stop hadoop related services
def stop_hadoop_service():
    print colors.LIGHT_BLUE + "\tStop hadoop related services, it may take a while..." + colors.ENDC
    process_list = ["NodeManager", "DataNode", "NameNode", "ResourceManager", "SecondaryNameNode",
                    "WebAppProxyServer", "JobHistoryServer"]
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
    print colors.LIGHT_BLUE + "\tStart hadoop related services,  it may take a while..." + colors.ENDC
    ssh_execute(master, "$HADOOP_HOME/sbin/start-all.sh")
    ssh_execute(master, "$HADOOP_HOME/sbin/yarn-daemon.sh start proxyserver")
    ssh_execute(master, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver")

# Stop Hive metastore service
def stop_metastore_service():
    print colors.LIGHT_GREEN + "Stop metastore service" + colors.ENDC
    stdout = ssh_execute_withReturn(master, "ps -ef | grep metastore")
    for line in stdout:
        if "org.apache.hadoop.hive.metastore.HiveMetaStore" in line:
            process_id = line.split()[1]
            os.system("kill -9 " + process_id)

# Start Spark history server
def start_spark_history(spark_conf):
    print colors.LIGHT_GREEN + "Start spark history server" + colors.ENDC
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
    if spark_eventLog is not None:
        print colors.LIGHT_BLUE+ "\nCreate spark eventlog HDFS directory" + colors.ENDC
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir -p " + spark_eventLog)
    if spark_history is not None:
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir -p " + spark_history)
    ssh_execute(master, "$SPARK_HOME/sbin/stop-history-server.sh")
    ssh_execute(master, "$SPARK_HOME/sbin/start-history-server.sh")

# Calculate statistics of hardware information for each node
def calculate_hardware():
    hardware_dict = {}
    for node in slaves:
        list = []
        cmd = "cat /proc/cpuinfo | grep \"processor\" | wc -l"
        stdout = ssh_execute_withReturn(node, cmd)
        for line in stdout:
            vcore_num = line
            list.append(vcore_num)
        cmd = "cat /proc/meminfo | grep \"MemTotal\""
        stdout = ssh_execute_withReturn(node, cmd)
        for line in stdout:
            memory = int(int(line.split()[1])/1024*0.85)
            list.append(memory)
        hardware_dict[node.hostname] = list
    return hardware_dict