import optparse
import os
import distribute.deploy_slave as dist

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("--version", dest="version", help="specify version")
    parser.add_option("--component", dest="component", help="specify which component you want to setup")

    (options, args) = parser.parse_args()
    component = options.component
    version = options.version

    current_path = os.path.dirname(os.path.abspath(__file__))
    project_path = os.path.dirname(current_path)
    script_path = os.path.join(project_path, "scripts")
    config_path = os.path.join(project_path, "conf")
    package_path = os.path.join(project_path, "packages")

    dist.deploy(component, version, project_path)

    slaves = dist.get_slaves(os.path.join(config_path, "slaves.custom"))
    master = dist.get_master_node(slaves)
    dist.ssh_execute(master, "$HADOOPHOME/bin/hdfs namenode -format")
    dist.ssh_execute(master, "$HADOOPHOME/sbin/stop-all.sh")
    dist.ssh_execute(master, "$HADOOPHOME/sbin/start-all.sh")