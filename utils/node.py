import os

class Node:
    def __init__(self, hostname, ip, username, password, role):
        self.hostname = hostname
        self.ip = ip
        self.username = username
        self.password = password
        self.role = role


def get_master_node(slaves):
    for node in slaves:
        if node.role == "master":
            return node
    return None

def get_slaves(filename):
    slaves = []
    if not os.path.isfile(filename):
        return slaves
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            val = line.split()
            if len(val) != 5:
                print ("Wrong format of slave config")
                break
            else:
                node = Node(val[0], val[1], val[2], val[3], val[4])
                slaves.append(node)

    return slaves
