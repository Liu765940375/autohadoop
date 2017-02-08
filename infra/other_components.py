#!/usr/bin/python

from utils.util import *
from utils.ssh import *

JAVA_COMPONENT = "jdk"

def deploy_jdk(slaves, beaver_env):
    jdk_version = beaver_env.get("JDK_VERSION")
    clean_jdk(slaves)
    copy_jdk(jdk_version, slaves)
    setup_env_dist(slaves, beaver_env, JAVA_COMPONENT)
    set_path(JAVA_COMPONENT, slaves, beaver_env.get("JAVA_HOME"))


def clean_jdk(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/jdk*")

def copy_jdk(version, slaves):
    package = "jdk-" + version + "-linux-x64.tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        print (colors.LIGHT_BLUE + "/tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    copy_package_dist(slaves, os.path.join(package_path, package), JAVA_COMPONENT, version)

def deploy_mysql(master, default_conf):
    default_hive_conf_file = os.path.join(default_conf, "hive/hive-site.xml")
    username = get_config_value(default_hive_conf_file, "javax.jdo.option.ConnectionUserName")
    password = get_config_value(default_hive_conf_file, "javax.jdo.option.ConnectionPassword")
    install_mysql(master, username, password)

def copy_lib_for_spark(master, beaver_env, hos):
    spark_version = beaver_env.get("SPARK_VERSION")
    if spark_version[0:3] == "1.6" and hos:
        ssh_execute(master, "cp -f " + beaver_env.get("SPARK_HOME") + "/lib/*" + " " + beaver_env.get("HIVE_HOME") + "/lib")
    elif spark_version[0:3] == "2.0":
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir /spark-2.0.0-bin-hadoop")
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -copyFromLocal $SPARK_HOME/jars/* /spark-2.0.0-bin-hadoop")
        ssh_execute(master,
                    "echo \"spark.yarn.jars hdfs://" + master.hostname + ":9000/spark-2.0.0-bin-hadoop/*\" >> $SPARK_HOME/conf/spark-defaults.conf")