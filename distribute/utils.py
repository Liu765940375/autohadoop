import glob

from config import *
from node import *
from ssh import *

download_server = "10.239.47.156"

project_path = os.path.dirname(os.path.abspath('__file__'))
package_path = os.path.join(project_path, "package")

slaves = get_slaves(os.path.join(project_path, "conf/hadoop/slaves.custom"))

def get_env_list(filename):
    envs = {}
    print "Get env list from " + filename
    if not os.path.isfile(filename):
        return envs
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            envs[key.strip()] = value.strip()

    return envs

hadoop_env = get_env_list(os.path.join(project_path, "conf/hadoop/env"))
spark_env = get_env_list(os.path.join(project_path, "conf/spark/env"))
hive_env = get_env_list(os.path.join(project_path, "conf/hive/env"))

spark_home = spark_env.get("SPARK_HOME")
hive_home = hive_env.get("HIVE_HOME")

jdk_version = hadoop_env["JDK_VERSION"]

# Add binary path to PATH
def set_path(component, slaves):
    for node in slaves:
        if component == "hadoop":
            cmd = "echo \"export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin\" >> /opt/" + component + "rc"
        if component == "spark":
            cmd = "echo \"export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin\" >> /opt/" + component + "rc"
        if component == "hive":
            cmd = "echo \"export PATH=$PATH:$HIVE_HOME/bin\" >> /opt/" + component + "rc"
        ssh_execute(node, cmd)

def get_config_files(component, config_path):
    config_file_names = ""
    if component == "hadoop":
        config_file_names = ["hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml"]
        with open(os.path.join(config_path, "slaves"), "w") as f:
            for node in slaves:
                f.write(node.ip + "\n")
        # Setup Nopass for slave nodes
        setup_nopass(slaves)
    if component == "spark":
        config_file_names = ["spark-defaults.conf"]
    if component == "hive":
        config_file_names = ["hive-site.xml", "hive-log4j2.properties"]
    return config_file_names

# Copy and unpack a package to slave nodes
def copy_package_dist(slaves, file, component):
    print "Distrubte package for " + component
    for node in slaves:
        print "\tCopy packge to " + node.hostname + ":" + node.ip
        ssh_execute(node, "mkdir -p /opt")
        ssh_copy(node, file, "/opt/" + os.path.basename(file))
        cmd = "mkdir -p /opt/" + component + ";tar zxf /opt/" + os.path.basename(file) + \
              " -C /opt/" + component + " --strip-components=1 > /dev/null &"
        ssh_execute(node, cmd)

# Copy component package to slave nodes
def copy_packages(component, version, slaves):
    download_url = "http://" + download_server + "/" + component
    package = component + "-" + version + ".tar.gz"
    if component == "hive":
        package = "apache-" + component + "-" + version + "-SNAPSHOT-bin.tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    copy_package_dist(slaves, os.path.join(package_path, package), component)


# Copy JDK
# TODO: It should only setup one time.
def copy_jdk():
    package = "jdk-" + jdk_version + "-linux-x64.tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    copy_package_dist(slaves, os.path.join(package_path, package), "jdk")


# Copy saprk-<version>-yarn-shuffle.jar
def copy_spark_shuffle():
    spark_version = spark_env.get("SPARK_VERSION")
    package = "spark-" + spark_version + "-yarn-shuffle.jar"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    hadoop_home = hadoop_env.get("HADOOP_HOME")
    des = os.path.join(hadoop_home, "share/hadoop/yarn/lib", package)
    for node in slaves:
        ssh_copy(node, os.path.join(package_path, package), des)

def install_mysql(node, user, password):
    package = "mysql-community-release-el7-9.noarch.rpm"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    ssh_copy(node, download_package, "/opt/" + package)

    package = "mysql-community.repo"
    download_url = "http://" + download_server + "/" + "mysql"
    download_package = os.path.join(package_path, package)
    if not os.path.isfile(download_package):
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    ssh_copy(node, download_package, "/etc/yum.repos.d/" + package)

    cmd = "rpm -qa | grep mysql-community-server;"
    installed = ""
    for line in os.popen(cmd).readlines():
         installed += line.strip('\r\n')
    # For mysql5.7, command "mysqladmin -u username password pass" is not effective.
    install_cmd = "cd /opt;rpm -ivh " + package + ";yum -y install mysql-community-server;"\
                  + "systemctl start mysqld;mysqladmin -u " + user + " password " + password
    if installed.find("mysql-community-server") != -1:
        cmd = "systemctl stop mysqld;yum -y remove mysql-*;rm -rf /var/lib/mysql;"
        cmd += install_cmd
    else:
        cmd = install_cmd
    # TODO: This may be replaced by ssh.execute(node, cmd) to install mysql on any node
    os.system(cmd)

def setup_config_dist(slaves, config_files, component):
    print "Distribute config xml for " + component
    if component == "hadoop":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/hadoop/etc/hadoop/" + os.path.basename(file))
    if component == "spark":
        for file in config_files:
                for node in slaves:
                    ssh_copy(node, file, "/opt/spark/conf/" + os.path.basename(file))
    if component == "hive":
        for file in config_files:
                for node in slaves:
                    ssh_copy(node, file, "/opt/hive/conf/" + os.path.basename(file))

def copy_configurations(slaves, config_file_names, config_path, component):
    # Generate configration XML files
    if component == "hadoop":
        master = get_master_node(slaves)
        yarn_custom_file = os.path.join(config_path, "yarn-site.xml.custom")
        if not os.path.isfile(yarn_custom_file):
            with open(yarn_custom_file, "w") as f:
                f.wirte("yarn.resourcemanager,hostname=" + master.ip + "\n")

        core_custom_file = os.path.join(config_path, "core-site.xml.custom")
        if not os.path.isfile(core_custom_file):
            with open(core_custom_file, "w") as f:
                f.write("fs.defaultFS=hdfs://" + master.ip + ":9000")

    for config_file in config_file_names:
        template_config = os.path.join(config_path, config_file) + ".template"
        custom_config = os.path.join(config_path, config_file) + ".custom"
        if config_file == "hdfs-site.xml":
            custom_configs = {}
            get_custom_configs(custom_config, custom_configs)
            data_dir = custom_configs.get("dfs.datanode.data.dir")
            name_dir = custom_configs.get("dfs.namenode.name.dir")
            for node in slaves:
                ssh_execute(node, "rm -rf /" + data_dir)
                ssh_execute(node, "rm -rf " + name_dir)

        target_config = os.path.join(config_path, config_file)
        if component == "hadoop" or component == "hive" and config_file != "hive-log4j2.properties":
            generate_configuration(template_config, custom_config, target_config)
        if component == "spark":
            generate_conf(template_config, custom_config, target_config)
        if component == "hive" and config_file == "hive-log4j2.properties":
            generate_conf(template_config, custom_config, target_config)
    path = config_path + "/*"
    final_config_files = glob.glob(path)
    setup_config_dist(slaves, final_config_files, component)

