#!/usr/bin/python

import os

from bb import deploy_BB, undeploy_BB, run_BB
from hive import deploy_hive, undeploy_hive
from infra.hadoop import deploy_hadoop_internal, undeploy_hadoop
from infra.spark import deploy_spark_internal, undeploy_spark
from utils.util import hadoop_version, check_env

# hive_env_file = os.path.join(config_path, "hive/env")
# spark_env_file = os.path.join(config_path, "spark/env")

def deploy_hosBB(version):
    if version == "1.6":
        hive_version = "hos1.6"
        spark_version = "1.6.2"
    else:
        hive_version = "2.2.0"
        spark_version = "2.0.0"

    installed = check_env("hadoop", hadoop_version)
    if not installed:
        deploy_hadoop_internal(hadoop_version)
    installed = check_env("hive", hive_version)
    if not installed:
        deploy_hive(hive_version)
    installed = check_env("spark", spark_version)
    if not installed:
        deploy_spark_internal(spark_version)
    installed = check_env("BB", "1.0")
    if not installed:
        deploy_BB()

    dict = {'hive':hive_version, 'spark':spark_version}
    for key,val in dict.iteritems():
        component = key
        version = val
        link_home = "/opt/Beaver/" + component
        cmd = "readlink -f " + link_home
        for line in os.popen(cmd).readlines():
            component_home = line.strip()
        if os.path.basename(component_home) != component + "-" + version:
            cmd = "rm -rf " + link_home + ";" + "ln -s " + os.path.join("/opt/Beaver",
                                                                        component + "-" + version) + " " + link_home + ";"
            os.system(cmd)

    # stop_history_server()
    # start_spark_history()

def run_hosBB(version):
    deploy_hosBB(version)
    run_BB()

def undeploy_hosBB(version):
    if version == "1.6":
        hive_version = "hos1.6"
        spark_version = "1.6.2"
    else:
        hive_version = "2.2.0"
        spark_version = "2.0.0"

    undeploy_hadoop(hadoop_version)
    undeploy_hive(hive_version)
    undeploy_spark(spark_version)
    undeploy_BB()

# def modify_env(version):
#     if version == "1.6":
#         hive_version = "hos1.6"
#         spark_version = "1.6.2"
#     if version == "2.0":
#         hive_version = "2.2.0"
#         spark_version = "2.0.0"
#     dict = {hive_env_file:'HIVE_VERSION=', spark_env_file:'SPARK_VERSION='}
#     for key, val in dict.iteritems():
#         env_file = key
#         str = val
#         if val == "HIVE_VERSION=":
#             version = hive_version
#         if val == "SPARK_VERSION=":
#             version = spark_version
#         with open(env_file) as rf:
#             content = rf.readlines()
#         with open(env_file, 'w') as wf:
#             for line in content:
#                 if re.search(str, line):
#                     line = str + version
#                 wf.write(line)