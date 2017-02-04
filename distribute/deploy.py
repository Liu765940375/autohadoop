import sys
import os
from distribute.utils import *

def deploy_hadoop(project_path):
    config_path = os.path.join(project_path, "conf/hadoop")
    config_file_names = get_config_files("hadoop", config_path)
    slaves = get_slaves(os.path.join(project_path, "conf/hadoop/slaves.custom"))

    for node in slaves:
        cmd = "which hadoop;"
        cmd += "echo $HADOOP_HOME"
        bin = ssh_execute(node, cmd)
        if bin is not None:
            print "You have deployed hadoop on " + node.ip + ",please remove your previous hadoop first!"
            sys.exit(1)

    # Setup ENV on slave nodes
    envs = get_env_list(os.path.join(config_path, "env"))
    hadoop_version = envs.get("HADOOP_VERSION")
    setup_env_dist(slaves, envs, "hadoop")
    set_path("hadoop", slaves)

    # Set /etc/hosts
    set_hosts(slaves)

    copy_packages("hadoop", hadoop_version, slaves)

    copy_jdk(envs.get("JDK_VERSION"))

    copy_configurations(slaves, config_file_names, config_path, "hadoop")


def deploy_hive(project_path):
    config_path = os.path.join(project_path, "conf/hive")
    config_file_names = get_config_files("hive", config_path)
    slaves = get_slaves(os.path.join(project_path, "conf/hadoop/slaves.custom"))
    master = get_master_node(slaves)
    master_host = master.hostname

    hive_env = get_env_list(os.path.join(project_path, "conf/hive/env"))
    spark_env = get_env_list(os.path.join(project_path, "conf/spark/env"))
    hive_version = hive_env.get("HIVE_VERSION")
    hive_home = hive_env.get("HIVE_HOME")
    spark_home = spark_env.get("SPARK_HOME")
    spark_version = spark_env.get("SPARK_VERSION")

    setup_env_dist(slaves, hive_env, "hive")
    set_path("hive", slaves)

    copy_packages("hive", hive_version, slaves)

    mysql_configs = {}
    get_configs_from_kv(os.path.join(config_path, "hive-site.xml.custom"), mysql_configs)
    username = mysql_configs.get("javax.jdo.option.ConnectionUserName")
    password = mysql_configs.get("javax.jdo.option.ConnectionPassword")
    install_mysql(slaves[0], username, password)

    if spark_version[0:3] == "1.6":
        os.system("cp -f " + spark_home + "/lib/*" + " " + hive_home + "/lib")
    elif spark_version[0:3] == "2.0":
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir /spark-2.0.0-bin-hadoop")
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -copyFromLocal $SPARK_HOME/jars/* /spark-2.0.0-bin-hadoop")
        ssh_execute(master,
                    "echo \"spark.yarn.jars hdfs://" + master_host + ":9000/spark-2.0.0-bin-hadoop/*\" >> $SPARK_HOME/conf/spark-defaults.conf")

    copy_configurations(slaves, config_file_names, config_path, "hive")
    ssh_execute(slaves[0], hive_home + "/bin/schematool --initSchema -dbType mysql")
    ssh_execute(slaves[0], "nohup " + hive_home + "/bin/hive --service metastore &")

def deploy_spark(project_path):
    config_path = os.path.join(project_path, "conf/spark")
    config_file_names = get_config_files("spark", config_path)

    hadoop_config_path = os.path.join(project_path, "conf/hadoop")
    hadoop_env = get_env_list(os.path.join(hadoop_config_path, "env"))
    slaves = get_slaves(os.path.join(project_path, "conf/hadoop/slaves.custom"))

    spark_env = get_env_list(os.path.join(config_path, "env"))
    spark_version = spark_env.get("SPARK_VERSION")
    setup_env_dist(slaves, spark_env, "spark")
    set_path("spark", slaves)
    #master = get_master_node(slaves)
    #copy_packages("spark", spark_version, master)
    copy_packages("spark", spark_version, slaves)
    copy_spark_shuffle(spark_env.get("SPARK_VERSION"), hadoop_env.get("HADOOP_HOME"))
    copy_configurations(slaves, config_file_names, config_path, "spark")

def deploy_BB(project_path):
    config_path = os.path.join(project_path, "conf/BB")
    slaves = get_slaves(os.path.join(project_path, "conf/hadoop/slaves.custom"))
    master = get_master_node(slaves)
    copy_packages("BB", "1.0", [master])

    BB_config_files = ["bigBench.properties.template", "userSettings.conf"]
    BB_hive_config_files = ["engineSettings.conf", "engineSettings.sql"]

        
    while not os.path.isdir("/opt/BB/conf"):
       pass 
    for file in BB_config_files:
        os.system("cp " + os.path.join(config_path, file) + " /opt/BB/conf/"+file)

    while not os.path.isdir("/opt/BB/engines/hive/conf"):
       pass 
    for file in BB_hive_config_files:
        os.system("cp " + os.path.join(config_path, "hive/"+file) + " /opt/BB/engines/hive/conf/"+file)




