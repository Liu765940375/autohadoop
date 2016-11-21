#!/usr/bin/python

import os
import paramiko
import glob
import optparse
import xml.etree.cElementTree as ET

class Node:
    def __init__(self, hostname, ip, username, password):
        self.hostname = hostname
        self.ip = ip
        self.username = username
        self.password = password

def get_custom_configs (filename, custom_configs):
    with open(filename) as f:
        for line in f:
            key, value = line.partition("=")[::2]
            custom_configs[key.strip()] = value.strip()

def ssh_execute(node, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username,password=node.password)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    for line in ssh_stdout:
        print '...' + line.strip('\n')
    ssh.close()

def ssh_copy(node, src, dst):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username, password=node.password)
    sftp = ssh.open_sftp()
    sftp.put(src, dst)
    sftp.close()
    ssh.close()

def setup_nopass(slaves):
    home = os.path.expanduser("~")
    rsa_file = home + "/.ssh/id_rsa.pub"
    if not os.path.isfile(rsa_file):
        os.system("ssh-keygen -t rsa -P '' -f " + rsa_file)

    for node in slaves:
        ssh_copy(node, rsa_file, "/tmp/id_rsa.pub")
        ssh_execute(node, "cat /tmp/id_rsa.pub >> ~/.ssh/authorized_keys")
        ssh_execute(node, "chmod 0600 ~/.ssh/authorized_keys")

def get_config(filename, key):
    doc = ET.parse(filename)
    root = doc.getroot()
    configs = root.getchildren()
    value = ""
    for config in configs:
        attrs = config.getchildren()
        if value != "":
            break;
        hit = False;
        for attr in attrs:
            if attr.tag == "name" and attr.text == key:
                hit = True
            if attr.tag == "value" and hit:
                value = attr.text
                break;
    return value

def get_slaves(filename):
    slaves = []
    if not os.path.isfile(filename):
        return slaves
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            val = line.split()
            if len(val) != 4:
                print "Wrong format of slave config"
                break
            else:
                node = Node(val[0], val[1], val[2], val[3])
                slaves.append(node)

    return slaves

def setup_config_dist(slaves, config_files, component):
    print "Distribute config xml for " + component
    if component == "hadoop":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/hadoop/etc/hadoop/" + os.path.basename(file))

# Copy and unpack a package to slave nodes
def copy_package_dist(slaves, file, component):
    print "Distrubte package for " + component
    for node in slaves:
        ssh_execute(node, "mkdir -p /opt")
        ssh_copy(node, file, "/opt/" + os.path.basename(file))
        cmd = "mkdir -p /opt/" + component + ";tar zxf /opt/" + os.path.basename(file) +\
              " -C /opt/" + component + " --strip-components=1"
        ssh_execute(node, cmd)

# Execute command on slave nodes
def execute_command_dist(slaves, command, component):
    print "Execute commands over slaves"
    for node in slaves:
        ssh_execute(node, command)

def get_env_list(filename):
    envs = {}
    print "Get env list from " + filename
    if not os.path.isfile(filename):
        return envs
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            envs[key.strip()] = value.strip()

    return envs

def setup_env_dist(slaves, envs, component):
    print "Setup Environment over slaves"

    for node in slaves:
        for key, value in envs.iteritems():
            cmd = "echo \"export " + key + "=" + value + "\"> /opt" + component + "rc";
            cmd += "echo \". /opt/hadooprc\" >> ~/.bashrc"
            ssh_execute(node, cmd)


def generate_configuration(config_template_file, custom_config_file, target_config_file):
    default_configs = {}
    custom_configs = {}

    configs = ET.parse(config_template_file)
    root = configs.getroot()

    # No custom file exsit
    if not os.path.isfile(custom_config_file):
        tree = ET.ElementTree(root)
        with open(target_config_file, "w") as f:
            tree.write(f)
        return

    get_custom_configs(custom_config_file, custom_configs)

    properties = root.getchildren()
    for prop in properties:
        attributes = prop.getchildren()
        key = ""
        value = ""
        custom = False
        for attribute in attributes:
            if attribute.tag == "name":
                key = attribute.text
                if custom_configs.has_key(key):
                    custom = True;

            if attribute.tag == "value" and custom == True:
                attribute.text = custom_configs[key]
                value = attribute.text
                custom_configs.pop(key, None)
                custom = False

        default_configs[key] = value

    for key, val in custom_configs.iteritems():
        prop = ET.Element('property')
        name = ET.SubElement(prop, "name")
        name.text = key
        value = ET.SubElement(prop, "value")
        value.text = val
        root.append(prop)

    tree = ET.ElementTree(root)
    with open(target_config_file, "w") as f:
        tree.write(f)



if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("--version", dest="version", help="specify version")
    parser.add_option("--mode", dest="mode", help="specify operation mode")
    parser.add_option("--component", dest="component", help="specify which component you want to setup")

    (options, args) = parser.parse_args()
    component = options.component
    version = options.version
    mode = options.mode

    current_path = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.dirname(current_path)
    project_path = os.path.dirname(script_path)
    config_path = project_path + "/conf"
    package_path = project_path +"/packages"
    config_file_names = ["hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml"]

    # Setup Nopass for slave nodes
    slaves = get_slaves(os.path.join(config_path, "slaves"))
    setup_nopass(slaves)

    # Setup ENV on slave nodes
    envs = get_env_list(os.path.join(config_path, "env"))
    setup_env_dist(slaves, envs, component)

    # Copy component package to slave nodes
    download_server = "10.239.47.53"
    package = component + "-" + version + ".tar.gz";
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/" + component
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    copy_package_dist(slaves, os.path.join(package_path, package), component)

    # Copy JDK
    # TODO: It should only setup one time.
    jdk_version = envs["JDK_VERSION"]
    package = "jdk-" + jdk_version + "-linux-x64.tar.gz";
    if not os.path.isfile(os.path.join(package_path, package)):
        download_url = "http://" + download_server + "/software"
        os.system("wget -P " + package_path + " " + download_url + "/" + package)
    copy_package_dist(slaves, os.path.join(package_path, package), "jdk")

    # Add binary path to PATH
    for node in slaves:
        cmd="echo \"export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin\" >> /opt/" + component +"rc"
        ssh_execute(node, cmd)

    # Generate configration XML files
    for config_file in config_file_names:
        template_config = os.path.join(config_path, config_file) + ".template"
        custom_config = os.path.join(config_path, config_file) + ".custom"
        target_config = os.path.join(config_path, config_file)
        generate_configuration(template_config, custom_config, target_config)

    path = config_path + "/*"
    final_config_files = glob.glob(path)
    setup_config_dist(slaves, final_config_files, component)

    # Create namenode and datanode direcotry on slave nodes
    cmd = ""
    if component == "hadoop":
        datanode_dir = get_config(os.path.join(config_path, "hdfs-site.xml"), "dfs.namenode.name.dir").split(':')[1]
        namenode_dir = get_config(os.path.join(config_path, "hdfs-site.xml"), "dfs.datanode.data.dir").split(':')[1]
        cmd = "mkdir -p " + namenode_dir + ";"
        cmd += "mkdir -p " + datanode_dir + ";"
    execute_command_dist(slaves, cmd, component)

