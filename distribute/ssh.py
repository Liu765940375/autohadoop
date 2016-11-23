import paramiko

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