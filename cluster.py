import optparse
import os
import sys

import distribute.deploy_slave as dist

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("--version", dest="version", help="specify version")
    parser.add_option("--component", dest="component", help="specify which component you want to setup")

    (options, args) = parser.parse_args()
    component = options.component
    version = options.version

    if not options.component:
        parser.error("component not found")
        sys.exit(1)

    if not options.version:
        parser.error("version not specified")
        sys.exit(1)

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = current_path
    script_path = os.path.join(project_path, "scripts")
    config_path = os.path.join(project_path, "conf")
    package_path = os.path.join(project_path, "packages")
    os.system("export PYTHONPATH=$PYTHONPATH:"+project_path)
    #sys.path.append(os.path.join(project_path, "distribute"))

    # dist.deploy(component, version, project_path)
    dist.deploy_general(component, version, project_path)

    print "Running commands on master"
    slaves = dist.get_slaves(os.path.join(config_path, "hadoop/slaves.custom"))
    master = dist.get_master_node(slaves)
    if component == "hadoop":
        for node in slaves:
            dist.ssh_execute(node, "systemctl stop firewalld")
        dist.ssh_execute(master, "yes | $HADOOP_HOME/bin/hdfs namenode -format")
        dist.ssh_execute(master, "$HADOOP_HOME/sbin/stop-all.sh")
        dist.ssh_execute(master, "$HADOOP_HOME/sbin/start-all.sh")
        dist.ssh_execute(master, "$HADOOP_HOME/sbin/yarn-daemon.sh start proxyserver")
        dist.ssh_execute(master, "$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver")
    if component == "spark":
        dist.start_spark_history(slaves, os.path.join((config_path), "spark/spark-defaults.conf"))