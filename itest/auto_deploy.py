#!/usr/bin/python

import os
import sys

#get this node IP
def git_local_IP():
    check_this_IP = "ifconfig | grep inet"
    this_IP=os.popen(check_this_IP).readline().strip('\r\n').split(" ")[9]
    return this_IP

#get this node hostname
def git_local_hostname():
    check_this_hostname = "hostname"
    this_hostname=os.popen(check_this_hostname).readline().strip('\r\n')
    return this_hostname

#cover slave.custom
def auto_deploy_slave(file_path):
    # host_name=auto_deploy_hostname()
    # host_ip=auto_deploy_IP()
    # deploy_slave_master = host_name+" "+host_ip +" root bdpe123 master"
    deploy_slave_master = git_local_hostname()+" "+git_local_IP() +" root bdpe123 master"
    slave_custom_fld=file_path+"/slaves.custom"
    slave_custom_cover="echo "+deploy_slave_master+" > "+slave_custom_fld
    os.popen(slave_custom_cover)
    return deploy_slave_master

#change engine
def auto_deploy_engine(engine_name,file_path):

    engine_conf_fld=file_path+"/BB/conf/userSettings.conf"
    f = file(engine_conf_fld,"a+")
    engine_conf_act="cat "+engine_conf_fld
    engine_conf_in=traverse_file(engine_conf_act)
    replace_name="export BIG_BENCH_DEFAULT_ENGINE"
    engine_flg=False
    if engine_name == "HoS":
        os.popen("\"\" > "+engine_conf_fld)
        for line in engine_conf_in:
            if replace_name in line:
                line = replace_name + "=\"hive\"\n"
            f.writelines(line)
        engine_flg = True
    elif engine_name=="spark":
        os.popen("\"\" > " + engine_conf_fld)
        for line in engine_conf_in:
            if replace_name in line:
                line = replace_name + "=\"spark_sql\"\n"
            #print line
            f.writelines(line)
        engine_flg=True
    # elif engine_name == "TPCDS":
    #     engine_flg=True
    return engine_flg

def traverse_file(file_act):
    file_in=[]
    for line in os.popen(file_act).readlines():
        file_in.append(line)
    return file_in


if __name__ == '__main__':
    slave_custom_cat="cat ../conf/slaves.custom"
    slave_custom_default=os.popen(slave_custom_cat).readline().strip('\r\n')
    conf_deploy_act="cp -rf ../conf /home/custom"
    conf_deploy_act1="cp -rf ../conf /home/custom1"
    #bdpe30-cjj 10.239.47.120 root bdpe123 master
    args = sys.argv
    if len(args)<2:
        engine_name = "HoS"
        os.popen("rm -rf /home/custom")
        os.popen(conf_deploy_act)
        engine_flg = auto_deploy_engine(engine_name,"/home/custom")
        slave_custom_covered = auto_deploy_slave("/home/custom")

        engine_name1="spark"
        os.popen("rm -rf /home/custom1")
        os.popen(conf_deploy_act1)
        engine_flg1 = auto_deploy_engine(engine_name1,"/home/custom1")
        slave_custom_covered = auto_deploy_slave("/home/custom1")
    else :
        engine_name = args[1]
        os.popen("rm -rf /home/custom")
        engine_flg=auto_deploy_engine(engine_name,"/home/custom")
        if engine_flg :
            os.popen(conf_deploy_act)

