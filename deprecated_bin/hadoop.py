import optparse
import os
import sys

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("--action", dest="action", help="start/stop hadoop service")

    (options, args) = parser.parse_args()
    action = options.action

    if not options.action:
        parser.error("action not found")
        sys.exit(1)

    project_path = os.path.abspath('..')
    conf_path = os.path.join(project_path, "conf/hadoop")
    # if action == "start":

# def check_conf(conf_patjh):
