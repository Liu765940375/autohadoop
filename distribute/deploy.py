import sys
import os

from distribute.utils import *

def deploy_hadoop(project_path):
    config_path = os.path.join(project_path, "conf/hadoop")
    config_file_names = get_config_files("hadoop", config_path)

    # Setup ENV on slave nodes
    envs = get_env_list(os.path.join(config_path, "env"))
    hadoop_version = envs.get("HADOOP_VERSION")
    setup_env_dist(slaves, envs, "hadoop")
    set_path("hadoop", slaves)

    # Set /etc/hosts
    set_hosts(slaves)

    copy_packages("hadoop", hadoop_version, slaves)
    spark_env = get_env_list(os.path.join(project_path, "conf/spark/env"))
    copy_spark_shuffle(spark_env.get("SPARK_VERSION"), hadoop_env.get("HADOOP_HOME"))
    copy_jdk(envs.get("JDK_VERSION"), slaves)

    copy_configurations(slaves, config_file_names, config_path, "hadoop", "deploy")
    hardware_dict = calculate_hardware()
    for node in slaves:
        with open(os.path.join(config_path, "yarn-site.xml.custom"), 'w') as wf:
            wf.write("yarn.nodemanager.resource.cpu-vcores=" + str(hardware_dict.get(node.hostname)[0]))
            wf.write("yarn.nodemanager.resource.memory-mb=" + str(hardware_dict.get(node.hostname)[1]))
            generate_configuration_xml(master, os.path.join(config_path, "yarn-site.xml.template"), "yarn-site.xml.custom", "yarn-site.xml")
            ssh_copy(node, os.path.join(config_path, "yarn-site.xml"), "/opt/Beaver/hadoop/etc/hadoop/" + "yarn-site.xml")

def deploy_hive(project_path):
    config_path = os.path.join(project_path, "conf/hive")
    config_file_names = get_config_files("hive", config_path)

    hive_version = hive_env.get("HIVE_VERSION")
    hive_home = hive_env.get("HIVE_HOME")

    setup_env_dist([master], hive_env, "hive")
    set_path("hive", slaves)

    copy_packages("hive", hive_version, [master])

    mysql_configs = {}
    get_configs_from_kv(os.path.join(config_path, "hive-site.xml.custom"), mysql_configs)
    username ="root"
    password = "123456"
    if mysql_configs.has_key("javax.jdo.option.ConnectionUserName"):
        username = mysql_configs.get("javax.jdo.option.ConnectionUserName")
    if mysql_configs.has_key("javax.jdo.option.ConnectionPassword"):
        password = mysql_configs.get("javax.jdo.option.ConnectionPassword")
    install_mysql(slaves[0], username, password)

    copy_configurations([master], config_file_names, config_path, "hive", "deploy")
    ssh_execute(slaves[0], hive_home + "/bin/schematool --initSchema -dbType mysql")
    stop_metastore_service()
    ssh_execute(slaves[0], "nohup " + hive_home + "/bin/hive --service metastore &")

def deploy_spark(project_path):
    config_path = os.path.join(project_path, "conf/spark")
    config_file_names = get_config_files("spark", config_path)

    hadoop_config_path = os.path.join(project_path, "conf/hadoop")
    hadoop_env = get_env_list(os.path.join(hadoop_config_path, "env"))

    spark_env = get_env_list(os.path.join(project_path, "conf/spark/env"))
    setup_env_dist([master], spark_env, "spark")
    set_path("spark", slaves)

    spark_version = spark_env.get("SPARK_VERSION")
    copy_packages("spark", spark_version, [master])
    copy_spark_shuffle(spark_version, hadoop_env.get("HADOOP_HOME"))

    hive_home = hive_env.get("HIVE_HOME")
    spark_home = spark_env.get("SPARK_HOME")
    spark_version = spark_env.get("SPARK_VERSION")
    if spark_version[0:3] == "1.6":
        os.system("cp -f " + spark_home + "/lib/*" + " " + hive_home + "/lib")
    elif spark_version[0:3] == "2.0":
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir /spark-2.0.0-bin-hadoop")
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -copyFromLocal $SPARK_HOME/jars/* /spark-2.0.0-bin-hadoop")
        ssh_execute(master,
                    "echo \"spark.yarn.jars=hdfs://" + master.hostname + ":9000/spark-2.0.0-bin-hadoop/*\" >> $SPARK_HOME/conf/spark-defaults.conf")

    copy_configurations([master], config_file_names, config_path, "spark", "deploy")
    stop_hadoop_service()
    start_hadoop_service()

def deploy_BB(project_path):
    config_path = os.path.join(project_path, "conf/BB")
    copy_packages("BB", "1.0", [master])

    # BB_config_files = ["bigBench.properties.template", "userSettings.conf"]
    # BB_hive_config_files = ["engineSettings.conf", "engineSettings.sql"]

    # while not os.path.isdir("/opt/Beaver/BB/conf"):
    #    pass
    # for file in BB_config_files:
    #     os.system("cp " + os.path.join(config_path, file) + " /opt/Beaver/BB/conf/"+file)
    #
    # while not os.path.isdir("/opt/Beaver/BB/engines/hive/conf"):
    #    pass
    # for file in BB_hive_config_files:
    #     os.system("cp " + os.path.join(config_path, "hive/"+file) + " /opt/Beaver/BB/engines/hive/conf/"+file)