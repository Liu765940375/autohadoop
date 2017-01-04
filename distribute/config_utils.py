import os
import xml.etree.cElementTree as ET
from xml.dom.minidom import parseString

pretty_print = lambda data: '\n'.join(
    [line for line in parseString(data).toprettyxml(indent=' ' * 2).split('\n') if line.strip()])

def get_config_from_xml(filename, key):
    doc = ET.parse(filename)
    root = doc.getroot()
    configs = root.getchildren()
    value = ""
    for config in configs:
        attrs = config.getchildren()
        if value != "":
            break;
        hit = False;
        for attr in attrs:
            if attr.tag == "name" and attr.text == key:
                hit = True
            if attr.tag == "value" and hit:
                value = attr.text
                break;
    return value

def get_configs_from_kv (filename, custom_configs):
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            custom_configs[key.strip()] = value.strip()

def generate_configuration_kv (master, config_template_file, custom_config_file, target_config_file):
    custom_configs = {}
    get_configs_from_kv(custom_config_file, custom_configs)
    with open(config_template_file) as f, open(target_config_file, "w") as target:
        for line in f:
            if line.startswith('#') or not line.split():
                target.writelines(line)
            else:
                list = line.split(" ")
                if custom_configs.has_key(list[0]):
                    line = list[0] + " " + custom_configs.get(list[0]) + "\n"
                    target.writelines(line)
                    del custom_configs[list[0]]
                else:
                    line = line.replace("<master_hostname>", master.hostname)
                    target.writelines(line)
        for key, value in custom_configs.iteritems():
           line = key + "      " + value + "\n"
           target.writelines(line)

def generate_configuration_xml(master, config_template_file, custom_config_file, target_config_file):
    default_configs = {}
    custom_configs = {}

    configs = ET.parse(config_template_file)
    root = configs.getroot()

    # No custom file exsit
    if not os.path.isfile(custom_config_file):
        tree = ET.ElementTree(root)
        with open(target_config_file, "w") as f:
            tree.write(f)
        return

    get_configs_from_kv(custom_config_file, custom_configs)

    properties = root.getchildren()
    for prop in properties:
        attributes = prop.getchildren()
        key = ""
        value = ""
        custom = False
        for attribute in attributes:
            if attribute.tag == "name":
                key = attribute.text
                if custom_configs.has_key(key):
                    custom = True;

            if attribute.tag == "value" and custom == False:
                value = attribute.text
                if value.find("master_hostname") != -1:
                    value = value.replace("master_hostname", master.hostname)
                    attribute.text = value
            if attribute.tag == "value" and custom == True:
                attribute.text = custom_configs[key]
                value = attribute.text
                custom_configs.pop(key, None)
                custom = False

        default_configs[key] = value

    for key, val in custom_configs.iteritems():
        prop = ET.Element('property')
        name = ET.SubElement(prop, "name")
        name.text = key
        value = ET.SubElement(prop, "value")
        value.text = val
        root.append(prop)

    xmlstr = pretty_print(ET.tostring(root))
    with open(target_config_file, "w") as f:
        f.write(xmlstr)
