#!/usr/bin/python
from cluster import *

def check_env(component):
    cmd = "ls /opt/Beaver | grep -x " + component
    # if component == "spark":
    #     switch_spark_version(version)
    #     cmd = "ls /opt/Beaver | grep -x spark-" + version
    installed = ""
    for line in os.popen(cmd).readlines():
        installed += line.strip('\r\n')
    if installed != component:
        manage_cluster(component, "deploy")
    else:
        print colors.LIGHT_GREEN + component + " has exists, we do not have to deploy it." + colors.ENDC
    # if component == "spark":
    #     switch_spark_version(version)
    #     manage_cluster(component, "deploy")
    # if installed != "spark-" + version and component == "spark":
    #     manage_cluster(component, "deploy")

def switch_spark_version(version):
    spark_env_file = os.path.join(config_path, "spark/env")
    with open(spark_env_file) as rf:
        line = rf.read()
    with open(spark_env_file, 'w') as wf:
        if version == "1.6.2":
            line = line.replace("SPARK_VERSION=2.0.0", "SPARK_VERSION=1.6.2")
        if version == "2.0.0":
            line = line.replace("SPARK_VERSION=1.6.2", "SPARK_VERSION=2.0.0")
        wf.write(line)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        version = sys.argv[2]
        service = sys.argv[1]
    if len(sys.argv) ==2:
        service = sys.argv[1]

    list = ["hadoop", "hive", "spark", "BB"]

    if service == "deploy":
        for component in list:
            print colors.LIGHT_PURPLE + component + " is deploying..." + colors.ENDC
            manage_cluster(component, "deploy")

    if service == "run":
        for component in list:
            check_env(component)
        # os.system("rm -rf /opt/Beaver/spark")
        # os.system("ln -s /opt/Beaver/spark-" + version + " " + "/opt/Beaver/spark")
        cmd = "/opt/Beaver/BB/bin/bigBench runBenchmark"
        os.system(cmd)