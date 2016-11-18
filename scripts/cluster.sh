#!/bin/bash
set -x
source ../conf/env.sh
CONF_PATH="`dirname $0`/../conf/"

function setup_login_without_password(){
  echo "setup_login_without_password"

  echo $2 $1 >> /etc/hosts
  hostname=$1
  user=$3
  password=$4
  rsafile=~/.ssh/id_rsa

  if [ ! -f "$rsafile" ]
  then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  fi
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys

  wget http://$DOWNLOAD_SERVER/software/sshpass-1.05-5.el7.x86_64.rpm
  rpm -ivh sshpass-1.05-5.el7.x86_64.rpm
  rm -rf sshpass-1.05-5.el7.x86_64.rpm
  sshpass -p $password ssh-copy-id -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa.pub $user@$hostname
}

function setup_hadoop_environment(){
  package_path="`dirname $0`/../packages"
  hadoop_tar_name=$(ls $package_path | grep hadoop-.*.tar.gz)
  hadoop_version=$(basename $hadoop_tar_name .tar.gz)

  hadoop_tar=$package_path/${hadoop_version}.tar.gz
  username=$3
  hostname=$1

  scp $hadoop_tar $username@$hostname:/opt
  ssh $username@$hostname 'tar -zxvf /opt/'${hadoop_version}'.tar.gz -C /opt'
  ssh $username@$hostname 'rm -rf /opt/'${hadoop_version}'.tar.gz'

  CONF_PATH="`dirname $0`/../conf"
  scp -r $CONF_PATH/* $username@$hostname:/opt/$hadoop_version/etc/hadoop
}

function get_hardware_info(){
  echo "CPU info"
  pssh -i -h /home/hosts.txt  cat /proc/cpuinfo
  echo "Memory info"
  pssh -i -h /home/hosts.txt  free -m
  echo "Disk info"
  pssh -i -h /home/hosts.txt  fdisk -l
}

function install_pssh(){
  wget http://$DOWNLOAD_SERVER/software/pssh-2.3.1.tar.gz
  tar -zxvf pssh-2.3.1.tar.gz
  cd pssh-2.3.1/
  python setup.py build
  python setup.py install
# you can add the hostname or IP in hosts.txt
  vim hosts.txt
}

function config_repo(){
  echo "config_repo"
  #install httpd
  yum -y install httpd

  DEFAULTDIR1="\"/var/www/html\""
  DEFAULTDIR2="\"/var/www\""
  DIR="\"${1:-/srv/my/repo}\""
  DEF_FILE="/etc/httpd/conf/httpd.conf"
  PSSH_HOST="${2:-/home/hosts.txt}"

  sed -i "s%$DEFAULTDIR1%$DIR%g" $DEF_FILE
  sed -i "s%$DEFAULTDIR2%$DIR%g" $DEF_FILE
  sed -i 's/#ServerName[[:space:]]www.example.com:80/ServerName localhost:80/g' $DEF_FILE

  mkdir -p $DIR

  mv /etc/httpd/conf.d/welcome.conf /etc/httpd/conf.d/welcome.conf.origin
  chcon -R -t httpd_sys_content_t $DIR
  chmod 755 $DIR
  systemctl stop firewalld.service
  systemctl enable httpd.service
  systemctl start httpd.service 

  # install createrepo
  yum -y install createrepo
  createrepo $DIR

  #before this command you should confirm the machine have installed pssh
  echo -e "[bdperepo]\nname = This is my repo\nbaseurl = http://"$DOWNLOAD_SERVER/" > /etc/yum.repos.d/bdperepo.repo
  pscp -h $PSSH_HOST /etc/yum.repos.d/bdperepo.repo /etc/yum.repos.d/
}

function add_node(){
  echo "Add node with hostname $1, IP $2, username $3 and password $4"
  setup_login_without_password $1 $2 $3 $4
  setup_hadoop_environment $1 $2 $3 $4
}

function add_nodes(){
  while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ ${line:0:1} != \# ]];then
      IFS=' ' read -r -a node <<< "$line"
      echo "Add node with hostname ${node[0]}, IP ${node[1]}, username ${node[2]} and password ${node[3]}"
      add_node ${node[@]}
    fi
  done < "$1"


  # deploy configurations to all nodes
}
