from utils import *

# Start Spark history server
def start_spark_history(slaves, spark_conf):
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
    master = get_master_node(slaves)
    if spark_eventLog is not None:
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir -p " + spark_eventLog)
    if spark_history is not None:
        ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir -p " + spark_history)
    ssh_execute(master, "$SPARK_HOME/sbin/stop-history-server.sh")
    ssh_execute(master, "$SPARK_HOME/sbin/start-history-server.sh")

# Execute command on slave nodes
def execute_command_dist(slaves, command, component):
    print "Execute commands over slaves"
    for node in slaves:
        ssh_execute(node, command)

def setup_env_dist(slaves, envs, component):
    print "Setup Environment over slaves"
    cmd = ""
    for node in slaves:
        cmd += "rm -f /opt/" + component + "rc;"
        for key, value in envs.iteritems():
            cmd += "echo \"export " + key + "=" + value + "\">> /opt/" + component + "rc;"
        cmd += "echo \". /opt/" + component + "rc" + "\" >> ~/.bashrc;"
        ssh_execute(node, cmd)

def deploy_general(component, version, project_path):
    config_path = project_path + "/conf/" + component
    config_file_names = get_config_files(component, config_path)
    slaves = get_slaves(os.path.join(project_path, "conf/hadoop/slaves.custom"))
    master = get_master_node(slaves)
    master_host = master.hostname
    if component == "spark" or component == "hive":
        slaves = [master]

    # Setup ENV on slave nodes
    envs = get_env_list(os.path.join(config_path, "env"))
    setup_env_dist(slaves, envs, component)
    set_path(component, slaves)

    copy_packages(component, version, slaves)

    if component == "hadoop":
        set_hosts(slaves)
        copy_jdk()
        copy_spark_shuffle()
    spark_version = spark_env.get("SPARK_VERSION")
    if component == "hive":
        mysql_configs = {}
        get_custom_configs(os.path.join(config_path, "hive-site.xml.custom"), mysql_configs)
        username = mysql_configs.get("javax.jdo.option.ConnectionUserName")
        password = mysql_configs.get("javax.jdo.option.ConnectionPassword")
        install_mysql(slaves[0], username, password)
        if spark_version[0:3] == "1.6":
            os.system("cp -f " + spark_home + "/lib/*" + " " + hive_home + "/lib")
        if spark_version[0:3] == "2.0":
            #ssh_execute(master, "zip spark-archive.zip $SPARK_HOME/jars/*")
            #ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -copyFromLocal spark-archive.zip /spark-archive.zip")
            #ssh_execute(master, "echo \"spark.yarn.archive=hdfs://" + master_host + ":9000/spark-archive.zip\" >> $SPARK_HOME/conf/spark-defaults.conf")

            ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -mkdir /spark-2.0.0-bin-hadoop")
            ssh_execute(master, "$HADOOP_HOME/bin/hadoop fs -copyFromLocal $SPARK_HOME/jars/* /spark-2.0.0-bin-hadoop")
            ssh_execute(master, "echo \"spark.yarn.jars=hdfs://"+master_host+":9000/spark-2.0.0-bin-hadoop/*\" >> $SPARK_HOME/conf/spark-defaults.conf")

    copy_configurations(slaves, config_file_names, config_path, component)

    if component == "hive":
        ssh_execute(slaves[0], hive_home + "/bin/schematool --initSchema -dbType mysql")
        # ssh_execute(slaves[0], hive_home + "/bin/hive --service metastore &")
        os.system("nohup $HIVE_HOME/bin/hive --service metastore &")

