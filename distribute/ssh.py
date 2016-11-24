import paramiko
import os

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
    privkey = home + "/.ssh/id_rsa"
    pubkey = privkey + ".pub"
    if not os.path.isfile(pubkey):
        os.system("ssh-keygen -t rsa -P '' -f " + privkey)

    for node in slaves:
        os.system("ssh-keyscan -H " + node.hostname  + " >> ~/.ssh/known_hosts")
        os.system("ssh-keyscan -H " + node.ip + " >> ~/.ssh/known_hosts")
        ssh_copy(node, pubkey, "/tmp/id_rsa.pub")
        ssh_execute(node, "mkdir -p ~/.ssh")
        ssh_execute(node, "cat /tmp/id_rsa.pub >> ~/.ssh/authorized_keys")
        ssh_execute(node, "chmod 0600 ~/.ssh/authorized_keys")
