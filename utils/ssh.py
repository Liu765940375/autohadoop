import paramiko
import os
import select
import time

def ssh_execute(node, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username,password=node.password)
    # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    channel = ssh.get_transport().open_session()
    stdin, stdout, stderr = ssh.exec_command(cmd)
    for line in stdout:
        print ('....' + line.strip('\n'))
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            rl, wl, xl = select.select([channel], [], [], 0.0)
            if len(rl) > 0:
                print (channel.recv(1024))
    ssh.close()

def ssh_execute_withReturn(node, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username,password=node.password)
    # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    channel = ssh.get_transport().open_session()
    stdin, stdout, stderr = ssh.exec_command(cmd)
    res = []
    for l in stdout.readlines():
        res.append(str(l).strip())
    ssh.close()
    return res


def ssh_execute_forMetastore(node, cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(node.ip, username=node.username,password=node.password)
    # ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd)
    channel = ssh.get_transport().open_session()
    stdin, stdout, stderr = ssh.exec_command(cmd)
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            rl, wl, xl = select.select([channel], [], [], 0.0)
            if len(rl) > 0:
                print (channel.recv(1024))
    # print "Metastore is starting, it may take a while..."
    time.sleep(10)
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

def ssh_download(node, remote_path, local_path):
    transport = paramiko.Transport((node.ip,22))
    transport.connect(username=node.username, password=node.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.get(remote_path, local_path)
    transport.close()


def setup_nopass(master,slaves):
    pubkey = "/tmp/id_rsa.pub"
    if os.path.isfile(pubkey) :
        os.system("rm -rf "+pubkey)

    ssh_execute(master, "yes n| ssh-keygen -f ~/.ssh/id_rsa -t rsa -P ''")
    ssh_execute(master, "rm ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -H `hostname -f` > ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts")
    ssh_execute(master, "ssh-keyscan -H localhost >> ~/.ssh/known_hosts")
    ssh_download(master, "/root/.ssh/id_rsa.pub", "/tmp/id_rsa.pub")

    for node in slaves:
        ssh_execute(master, "ssh-keyscan -H " + node.hostname + " >> ~/.ssh/known_hosts")
        ssh_execute(master, "ssh-keyscan -H " + node.ip + " >> ~/.ssh/known_hosts")

        ssh_copy(node, pubkey, "/tmp/id_rsa.pub")
        ssh_execute(node, "mkdir -p ~/.ssh")
        ssh_execute(node, "cat /tmp/id_rsa.pub >> ~/.ssh/authorized_keys")
        ssh_execute(node, "chmod 0600 ~/.ssh/authorized_keys")
        ssh_execute(node, "yes n|ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa ")

