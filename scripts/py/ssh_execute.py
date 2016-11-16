#!/usr/bin/python
import sys
import paramiko


#Usage: ssh_execute server username password command
if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: ssh_execute.py server username password command"
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    ssh.connect(sys.argv[1], username=sys.argv[2], password=sys.argv[3], port=22)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(sys.argv[4])
    for line in ssh_stdout:
        print '...' + line.strip('\n')
    ssh.close()