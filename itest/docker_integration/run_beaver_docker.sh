project_name=$1
image_name=$2
project_path=$3

repo_url=http://10.239.47.156
os_repo_path=$repo_url/repodata/os.repo

##delete container if exit slave or master
docker ps -a|grep -E 'slave|master' |awk '{print $1}'|xargs docker stop;
docker ps -a|grep -E 'slave|master' |awk '{print $1}'|xargs docker rm;

##run image for master
masterId=$(docker run --privileged --name=master -dti -v /sys/fs/cgroup:/sys/fs/cgroup:ro -p 50002:22 $image_name)
echo "containerId(master):$masterId"
master_hostname=$(docker ps -a|grep master|awk '{print $1}')
echo "hostname:$master_hostname"
master_ip=$(docker inspect --format='{{.NetworkSettings.IPAddress}}' $masterId|awk '{print $1}')
echo "master IP:$master_ip"

wget -P /opt/ $os_repo_path
docker cp  $project_path/$project_name $masterId:/home/
docker cp /opt/os.repo $masterId:/etc/yum.repos.d/

##run image for slave1
slave1Id=$(docker run --privileged --name=slave1 -dti -v /sys/fs/cgroup:/sys/fs/cgroup:ro -p 50003:22 $image_name)
echo "containerId(slave1):$slave1Id"
slave1_hostname=$(docker ps -a|grep slave1|awk '{print $1}')
echo "hostname:$slave1_hostname"
slave1_ip=$(docker inspect --format='{{.NetworkSettings.IPAddress}}' $slave1Id|awk '{print $1}')
echo "slave1 IP:$slave1_ip"

##The master need several seconds to start sshserver
sleep 30s

/usr/bin/expect<<-EOF
set timeout 7200
set pass bdpe123
spawn sed -i "/$master_ip/d" /root/.ssh/known_hosts
spawn ssh $master_ip
expect {
        "(yes/no)" {send "yes\r"; exp_continue}
        "password:" {send "bdpe123\r"; exp_continue}
        "~]#" {send "source /home/$project_name/bin/Python_install.sh;source /home/$project_name/bin/setup-env.sh\r"}
}
expect {
        "~]#" {send "cp -r /home/$project_name/conf/ /opt/conf1;echo \"$master_hostname $master_ip root bdpe123 master\" > /opt/conf1/slaves.custom;echo \"$slave1_hostname $slave1_ip root bdpe123 slave\" >> /opt/conf1/slaves.custom;sed -i 's/power_test_0=1-30/power_test_0=1-30/g' /opt/conf1/BB/conf/bigBench.properties\r"}
}
expect {
        "~]#" {send "sed -i 's/{\%yarn.nodemanager.resource.memory-mb\%}/20480/g' /opt/conf1/hadoop/yarn-site.xml;sed -i 's/{\%yarn.nodemanager.resource.cpu-vcores\%}/30/g' /opt/conf1/hadoop/yarn-site.xml;sed -i 's/{\%yarn.scheduler.maximum-allocation-mb\%}/20000/g' /opt/conf1/hadoop/yarn-site.xml\r"}
}
expect {
        "~]#" {send "rm -rf /etc/yum.repos.d/CentOS-*;cd /home/$project_name/;bin/runBBonHoS.py deploy_run /opt/conf1/\r"}
}
expect {
        "]#" {send "cd /home/$project_name/itest/;source test_utils.sh;service_check;cp log.txt /opt/Beaver/result/;cat log.txt;exit\r"}
}
expect eof
EOF

##copy the result to localhost
nowdate=$(date +%Y-%-m-%-d-%-H-%-M-%-S)
mkdir -p /opt/Beaver/result/docker/
docker cp $masterId:/opt/Beaver/result/ /opt/Beaver/result/docker/$nowdate
