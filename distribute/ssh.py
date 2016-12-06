import paramiko
import os
import select

def ssh_execute(node, cmd):
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
                print channel.recv(1024)
    ssh.close()
    return str

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

    os.system("ssh-keyscan -H `hostname -f` > ~/.ssh/known_hosts")
    os.system("ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts")
    os.system("ssh-keyscan -H 127.0.0.1 >> ~/.ssh/known_hosts")
    os.system("ssh-keyscan -H localhost >> ~/.ssh/known_hosts")
    for node in slaves:
        os.system("ssh-keyscan -H " + node.hostname  + " >> ~/.ssh/known_hosts")
        os.system("ssh-keyscan -H " + node.ip + " >> ~/.ssh/known_hosts")
        ssh_copy(node, pubkey, "/tmp/id_rsa.pub")
        ssh_execute(node, "mkdir -p ~/.ssh")
        ssh_execute(node, "cat /tmp/id_rsa.pub >> ~/.ssh/authorized_keys")
        ssh_execute(node, "chmod 0600 ~/.ssh/authorized_keys")
