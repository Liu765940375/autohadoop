#!/usr/bin/python
import os
import sys
current_path = os.path.dirname(os.path.abspath(__file__))
project_path = os.path.dirname(current_path)
sys.path.append(project_path)

from distribute.deploy import *
from distribute.utils import *
from distribute.node import *
from distribute.colors import *

def manage_cluster(component, service):
    if component == "hadoop" and service == "deploy":
        deploy_hadoop(project_path)
    if component == "hadoop" and service == "restart":
        config_file_names = get_config_files("hadoop", config_path)
        copy_configurations(slaves, config_file_names, os.path.join(config_path, "hadoop"), "hadoop", "restart")
        stop_hadoop_service()
        start_hadoop_service()

    if component == "spark" and service == "deploy":
        deploy_spark(project_path)
    if component == "hive" and service == "deploy":
        deploy_hive(project_path)
    if component == "BB" and service == "deploy":
        deploy_BB(project_path)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: bin/cluster.py <component> <service>")
        sys.exit(1)

    component = sys.argv[2]
    if component != "hadoop" and component != "spark" \
            and component != "hive" and component != "BB":
        print("Beaver now only support deploy hadoop, spark, hive, BB")
        sys.exit(1)

    service = sys.argv[1]
    if service != "deploy" and service != "restart":
        print("Beaver now only support deploy and restart service")
        sys.exit(1)

    manage_cluster(component, service)
