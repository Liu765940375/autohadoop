#!/usr/bin/python

from utils.util import *

def deploy_tez_internal(default_conf, custom_conf, master, beaver_env):
    tez_vesrion = beaver_env.get("TEZ_VERSION")
    download_tez(master, tez_vesrion)
    copy_tez_lib_to_hive(master)
    copy_tez_conf_to_hadoop(default_conf, custom_conf, [master], beaver_env)

def download_tez(node, version):
    print (colors.LIGHT_BLUE + "Distribute " + "tar.gz file" + " for apache-tez-" + version + "-bin" + colors.ENDC)
    download_url = "http://" + download_server + "/software"
    package = "apache-tez-" + version + "-bin" + ".tar.gz"
    if not os.path.isfile(os.path.join(package_path, package)):
        print (colors.LIGHT_BLUE + "\tDownloading " + package + " from our repo..." + colors.ENDC)
        os.system("wget --no-proxy -P " + package_path + " " + download_url + "/" + package)
    else:
        print (colors.LIGHT_GREEN + "\t" + package + " has already exists in Beaver package" + colors.ENDC)
    print (colors.LIGHT_BLUE + "\tCopy " + package + " to " + node.hostname + "..." + colors.ENDC)
    ssh_execute(node, "mkdir -p /opt/Beaver/")
    ssh_copy(node, os.path.join(package_path, package), "/opt/Beaver/" + package)
    print (colors.LIGHT_BLUE + "\tUnzip " + package + " on " + node.hostname + "..." + colors.ENDC)
    softlink = "/opt/Beaver/tez"
    cmd = "rm -rf " + softlink + ";"
    cmd += "rm -rf /opt/Beaver/tez-*;"
    cmd += "mkdir /opt/Beaver/tez-" + version + ";"
    cmd += "tar zxf /opt/Beaver/" + package + " -C /opt/Beaver/tez-" + version + " --strip-components=1 > /dev/null"
    ssh_execute(node, cmd)
    cmd = "ln -s /opt/Beaver/tez-" + version + " "  + softlink + ";"\
        + "rm -rf /opt/Beaver/" + package
    ssh_execute(node, cmd)

def copy_tez_lib_to_hive(node):
    print (colors.LIGHT_BLUE + "Copy Tez lib to Hive" + colors.ENDC)
    cmd = "yes|cp /opt/Beaver/tez/*.jar /opt/Beaver/hive/lib;"
    cmd += "yes|cp /opt/Beaver/tez/lib/*.jar /opt/Beaver/hive/lib"
    ssh_execute(node, cmd)

def copy_tez_package_to_hadoop(node):
    print (colors.LIGHT_BLUE + "Copy Tez package to Hadoop" + colors.ENDC)
    cmd = "$HADOOP_HOME/bin/hadoop fs -mkdir /apps;"
    cmd += "$HADOOP_HOME/bin/hadoop fs -copyFromLocal /opt/Beaver/tez/share/tez.tar.gz /apps/"
    ssh_execute(node, cmd)

def undeploy_tez(master):
    ssh_execute(master, "rm -rf /opt/Beaver/tez*")

def copy_tez_conf_to_hadoop(default_conf, custom_conf, master, beaver_env):
    output_tez_conf = update_tez_conf(default_conf, custom_conf, master)
    copy_configurations(master, output_tez_conf, "hadoop", beaver_env.get("HADOOP_HOME"))

def update_tez_conf(default_conf, custom_conf, master):
    output_tez_conf = update_conf("tez", default_conf, custom_conf)
    # for all conf files, replace the related value, eg, replace master_hostname with real hostname
    for conf_file in [file for file in os.listdir(output_tez_conf) if fnmatch.fnmatch(file, '*.xml')]:
        output_conf_file = os.path.join(output_tez_conf, conf_file)
        for node in master:
            dict = {'master_hostname': node.hostname}
        replace_conf_value(output_conf_file, dict)
        format_xml_file(output_conf_file)
    return output_tez_conf