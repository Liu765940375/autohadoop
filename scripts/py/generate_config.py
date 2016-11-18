#!/usr/bin/python
import sys
import os.path
import xml.etree.cElementTree as ET

def get_custom_configs (filename, custom_configs):
    with open(filename) as f:
        for line in f:
            key, value = line.partition("=")[::2]
            custom_configs[key.strip()] = value.strip()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Usage: generate_config.py abcd.xml.template abcd.xml.custom abcd.xml"
        sys.exit(1)
    default_config_filename = sys.argv[1]
    custom_config_filename = sys.argv[2]
    output_filename = sys.argv[3]
    default_configs = {}
    custom_configs = {}
    doc = ET.parse(default_config_filename)
    root = doc.getroot();

    # No custom file exsit
    if os.path.isfile(custom_config_filename) == False:
        tree = ET.ElementTree(root)
        with open(output_filename, "w") as f:
            tree.write(f)
        sys.exit(0)


    get_custom_configs(custom_config_filename, custom_configs)
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
    with open(output_filename, "w") as f:
        tree.write(f)
    sys.exit(0)
