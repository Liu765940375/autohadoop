#!/usr/bin/python

import os
import sys
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

def ssh_execute(node, lcmd):
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
            val = line.split()
            if len(val) != 4:
                print "Wrong format of slave config"
            node = Node(val[0], val[1], val[2], val[3])
            slaves.append(node)

    return slaves

def setup_config_dist(slaves, config_files, component):
    print "distribute config xml for" + component
    if component == "hadoop":
        for file in config_files:
            for node in slaves:
                ssh_copy(node, file, "/opt/hadoop/etc/hadoop/" + os.path.basename(file))

def download_package_dist(slaves, packages, component):
    print "distrubte packages for " + component
    for file in packages:
        for node in slaves:
            ssh_execute(node, "mkdir -p /opt")
            ssh_copy(node, file, "/opt/" + os.path.basename(file))
            ssh_execute(node, "cd /opt/; mkdir -p hadoop")
            ssh_execute(node, "cd /opt/; tar zxf " + os.path.basename(file) + "-C /opt/hadoop --strip-components=1")

def execute_command_dist(slaves, command, component):
    print "Execute commands over slaves"
    for node in slaves:
        ssh_execute(node, command)

def setup_env_dist(slaves, envs):
    print "Setup Environment over slaves"

    for node in slaves:
        for key, value in envs.iteritems:
            ssh_execute(node, "export " + key + "=" + value)

def generate_configuration(config_template_file, custom_config_file, target_config_file):
    default_configs = {}
    custom_configs = {}

    configs = ET.parse(config_template_file)
    root = configs.getroot();

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


def setup_nopass(slaves):
    home = os.path.expanduser("~")
    rsa_file = home + "/.ssh/id_rsa.pub"
    if not os.path.isfile(rsa_file):
        os.system("ssh-keygen -t rsa -P '' -f " + rsa_file)

    for node in slaves:
        ssh_copy(node, rsa_file, "/tmp/id_rsa.pub")
        ssh_execute(node, "cat /tmp/id_rsa.pub > ~/.ssh/authorized_keys")
        ssh_execute(node, "chmod 0600 ~/.ssh/authorized_keys")



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

    # Distribute component package to slave nodes
    slaves = get_slaves(os.path.join(config_path, "slaves"))
    setup_nopass(slaves)

    # Download component package
    download_server = "10.239.47.53"
    package = component + "-" + version + ".tar.gz";
    download_url = "http://" + download_server + "/" + component
    os.system("wget -P " + package_path + " " + download_url + "/" + package)

    # TODO: only copy component package or all?
    path = package_path + "/*.tar.gz"
    packages = glob.glob(path)
    download_package_dist(slaves, packages, component)

    # Generate configration XML files
    for config_file in config_file_names:
        template_config = os.path.join(config_path, config_file) + ".template"
        custom_config = os.path.join(config_path, config_file) + ".custom"
        target_config = os.path.join(config_path, config_file)
        generate_configuration(template_config, custom_config, target_config)

    path = config_path + "/*"
    final_config_files = glob.glob(path)
    setup_config_dist(slaves, final_config_files, component)

    cmd = ""
    # Generate command to execute on slave nodes
    if component == "hadoop":
        datanode_dir = get_config(os.path.join(config_path, "hdfs-site.xml"), "dfs.namenode.name.dir").split(':')[1]
        namenode_dir = get_config(os.path.join(config_path, "hdfs-site.xml"), "dfs.datanode.data.dir").split(':')[1]
        cmd = "mkdir -p " + namenode_dir + ";"
        cmd += "mkdir -p " + datanode_dir + ";"
    execute_command_dist(slaves, cmd, component)


