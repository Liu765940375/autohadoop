#!/usr/bin/python

import sys
import os
import paramiko
import xml.etree.cElementTree as ET

def get_custom_configs (filename, custom_configs):
    with open(filename) as f:
        for line in f:
            key, value = line.partition("=")[::2]
            custom_configs[key.strip()] = value.strip()

def ssh_execute(server, user, psw, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(server, username=user, password=psw)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    for line in ssh_stdout:
        print '...' + line.strip('\n')
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
            slave = line.split()
            slaves.append(slave)

    return slaves

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



if __name__ == '__main__':
    current_path = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.dirname(current_path)
    project_path = os.path.dirname(script_path)
    config_path = project_path + "/conf/"

    config_files = ["hdfs-site.xml", "core-site.xml", "mapred-site.xml", "yarn-site.xml"]

    # Generate configration XML files
    for config_file in config_files:
        template_config = config_path + config_file + ".template"
        custom_config = config_path + config_file + ".custom"
        target_config = config_path + config_file
        generate_configuration(template_config, custom_config, target_config)

    #Get namenode and datanode dir
    datanode_dir = get_config(config_path + "hdfs-site.xml", "dfs.namenode.name.dir").split(':')[1]
    namenode_dir = get_config(config_path + "hdfs-site.xml", "dfs.datanode.data.dir").split(':')[1]

    print datanode_dir
    print namenode_dir

    ##
    slaves = get_slaves(config_path+"slaves")
    cmd = "mkdir -p " + namenode_dir + ";"
    cmd += "mkdir -p " + datanode_dir + ";"

    for slave in slaves:
        print "deploy " + slave[0]
        ssh_execute(slave[1], slave[2], slave[3], cmd)
