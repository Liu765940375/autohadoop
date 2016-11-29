import os
import xml.etree.cElementTree as ET

def get_config(filename, key):
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

def get_custom_configs (filename, custom_configs):
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            custom_configs[key.strip()] = value.strip()

def generate_spark_conf(config_template_file, custom_config_file, target_config_file):
    custom_configs = {}
    get_custom_configs(custom_config_file, custom_configs)
    with open(config_template_file) as f, open(target_config_file, "w") as target:
        for line in f:
            if line.startswith('#') or not line.split():
                target.writelines(line)
            else:
                list = line.split()
                if custom_configs.has_key(list[0]):
                    line = list[0] + " " + custom_configs.get(list[0]) + "\n"
                    target.writelines(line)
                    del custom_configs[list[0]]
                else:
                    target.writelines(line)
        for key, value in custom_configs.iteritems():
           line = key + " " + value + "\n"
           target.writelines(line)

def generate_configuration(config_template_file, custom_config_file, target_config_file):
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

    get_custom_configs(custom_config_file, custom_configs)

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

    tree = ET.ElementTree(root)
    with open(target_config_file, "w") as f:
        tree.write(f)
