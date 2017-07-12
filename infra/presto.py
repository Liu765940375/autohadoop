#!/usr/bin/python

from infra.jdk import *
import fileinput

PRESTO_COMPONENT="presto"
def deploy_presto(default_conf, custom_conf, master, slaves, beaver_env):
    # Deploy Presto
    deploy_jdk(slaves, beaver_env)
    stop_presto_service(slaves)
    deploy_presto_internal(default_conf, custom_conf, master, slaves)

def undeploy_presto(slaves):
    stop_presto_service(slaves)
    clean_presto(slaves)

def stop_presto_service(slaves):
    print (colors.LIGHT_BLUE + "Stop presto related services, it may take a while..." + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "$PRESTO_HOME/bin/launcher stop")

def clean_presto(slaves):
    for node in slaves:
        ssh_execute(node, "rm -rf /opt/Beaver/presto*")
        ssh_execute(node, "source ~/.bashrc")

def deploy_presto_internal(default_conf, custom_conf, master, slaves):
    setup_nopass(slaves)
    update_etc_hosts(slaves)
    beaver_env = get_env_list(os.path.join(custom_conf, "env"))
    clean_presto(slaves)
    setup_env_dist(slaves, beaver_env, PRESTO_COMPONENT)
    copy_packages(slaves, PRESTO_COMPONENT, beaver_env.get("PRESTO_VERSION"))
    update_copy_presto_conf(default_conf, custom_conf, master, slaves)
    stop_firewall(slaves)
    #download presto command line interface
    ssh_execute(master, "wget --no-proxy -P /opt/Beaver/presto/ "+"http://" + download_server + "/presto/presto")
    # download presto benchmark driver
    ssh_execute(master, "wget --no-proxy -P /opt/Beaver/presto/ " + "http://" + download_server + "/presto/presto-benchmark-driver")

def update_copy_presto_conf(default_conf, custom_conf, master, slaves):
    output_presto_conf = update_conf(PRESTO_COMPONENT, default_conf, custom_conf)
    totalmemory = 0
    for node in slaves:
        totalmemory += calculate_memory(node)
    for node in slaves:
        if node is master:
            conf_path = "/opt/Beaver/presto/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
            cmd = "mkdir -p " + conf_path + ";ln -s " + conf_path + " /opt/Beaver/presto/etc"
            ssh_execute(node, cmd)
            ssh_copy(node, os.path.join(output_presto_conf, "log.properties"), "/opt/Beaver/presto/etc/log.properties")
            for file in os.listdir(output_presto_conf):
                if file.__eq__("jvm.config"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"-XmxxxG":"-Xmx" + str(calculate_memory(node)) + "G"})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/jvm.config")
                if file.__eq__("config.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"${xx}":str(totalmemory*0.6), "${xxx}":str(calculate_memory(node)*0.6), "master_hostname":master.ip})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/config.properties")
                if file.__eq__("node.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"xxxxxx":node.hostname})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/node.properties")
        else:
            conf_path = "/opt/Beaver/presto/" + str(time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()))
            cmd = "mkdir -p " + conf_path + ";ln -s " + conf_path + " /opt/Beaver/presto/etc"
            ssh_execute(node, cmd)
            ssh_copy(node, os.path.join(output_presto_conf, "log.properties"), "/opt/Beaver/presto/etc/log.properties")
            for file in os.listdir(output_presto_conf):
                if file.__eq__("jvm.config"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"-XmxxxG":"-Xmx" + str(calculate_memory(node)) + "G"})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/jvm.config")
                if file.__eq__("nodeconfig.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"${xx}":str(totalmemory * 0.6), "${xxx}":str(calculate_memory(node) * 0.6), "master_hostname":master.ip})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/config.properties")
                if file.__eq__("node.properties"):
                    replace_line_in_bak(os.path.join(output_presto_conf, file), os.path.join(output_presto_conf, file + ".bak"), {"xxxxxx":node.hostname})
                    ssh_copy(node, os.path.join(output_presto_conf, file + ".bak"), "/opt/Beaver/presto/etc/node.properties")

def replace_line_in_bak(infilepath, outfilepath, replacements):
    with open(infilepath) as infile, open(outfilepath, 'w') as outfile:
        for line in infile:
            for old,new in replacements.iteritems():
                line = line.replace(old, new)
            outfile.write(line)

def calculate_memory(node):
    cmd = "cat /proc/meminfo | grep \"MemTotal\""
    stdout = ssh_execute_withReturn(node, cmd)
    for line in stdout:
        memory = int(int(line.split()[1]) / 1024 / 1024 * 0.8)
    return memory

def start_presto_service(slaves):
    print (colors.LIGHT_BLUE + "Start presto related services, it may take a while..." + colors.ENDC)
    for node in slaves:
        ssh_execute(node, "$PRESTO_HOME/bin/launcher start")