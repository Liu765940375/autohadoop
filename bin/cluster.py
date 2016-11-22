import optparse
import os
import distribute.deploy_slave

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

    distribute.deploy_slave.deploy(component, version, project_path)

    #slaves = distribute.deploy_slave.get_slaves(os.path.join(config_path, "slaves.property"))
    #master = get_master(slaves)
    #distribute.deploy_slave.ssh_execute(master, "$HADOOPHOME/bin/hdfs namenode -format")
    #distribute.deploy_slave.ssh_execute(master, "$HADOOPHOME/sbin/start-all.sh")