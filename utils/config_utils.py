import os
import xml.etree.cElementTree as ET
from xml.dom.minidom import parseString
from shutil import copyfile

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

def get_configs_from_xml(filename, custom_configs):
    doc = ET.parse(filename)
    root = doc.getroot()
    configs = root.getchildren()
    value = ""
    for config in configs:
        attrs = config.getchildren()
        if value != "":
            break;
        key = ""
        for attr in attrs:
            if attr.tag == "name":
                key = attr.text.strip()
            if attr.tag == "value":
                custom_configs[key] = attr.text.strip()
    pass

def get_configs_from_kvfile (filename, custom_configs):
    with open(filename) as f:
        for line in f:
            if line.startswith('#') or not line.split():
                continue
            key, value = line.partition("=")[::2]
            custom_configs[key.strip()] = value.strip()

# TODO: need to be more smart and accurate
def isxml(filename):
    return "xml" in filename
    try:
        ET.parse(filename)
    except os.error:
        return False

    return True

# Merge configuration
def merge_configuration (config_files, default_dir, custom_dir, target_dir):
    if not os.path.isdir(custom_dir):
        os.system("cp -r "+default_dir + " " + target_dir)
        return

    sub_dirs = os.listdir(default_dir)

    for file in sub_dirs:
        src = os.path.join(default_dir, file)
        custom = os.path.join(custom_dir, file)
        dest = os.path.join(target_dir, file)
        if os.path.isfile(src) and os.path.basename(src) in config_files:
            generate_configuration(src, custom, dest)
        elif os.path.isdir(src):
            merge_configuration ( src, custom, dest)

def merge_configs(default_configs, custom_configs):
    for key, value in custom_configs.iteritems():
        default_configs[key] = custom_configs[key]
    return default_configs

def generate_xml(configs, target_file):
    xml_root = ET.Element("configuration")
    for key, val in configs.iteritems():
        prop = ET.Element('property')
        name = ET.SubElement(prop, "name")
        name.text = key
        value = ET.SubElement(prop, "value")
        value.text = val
        xml_root.append(prop)

    xmlstr = pretty_print(ET.tostring(xml_root))
    with open(target_file, "w") as f:
        f.write(xmlstr)

def generate_kvfile(configs, target_file):
    with open(target_file, "w") as f:
        for key, value in configs.iteritems():
            f.wirte(key + "=" + value + "\n")

def get_configs(filename):
    configs = {}
    if isxml(filename):
        get_configs_from_xml(filename, configs)
    else:
        get_configs_from_kvfile(filename, configs)

    return configs

def generate_configuration(config_template_file, custom_config_file, target_config_file):
    if not os.path.isfile(custom_config_file):
        copyfile(config_template_file, target_config_file)
        return
    default_config = get_configs(config_template_file)
    custom_configs = get_configs(custom_config_file)
    final_configs = merge_configs(default_config, custom_configs)
    if isxml(target_config_file):
        generate_xml(final_configs, target_config_file)
    else:
        generate_kvfile(final_configs, target_config_file)

def replace_conf_value(conf_file, old_value, new_value):
    tree = ET.parse(conf_file)
    root = tree.getroot()
    for value_tag in root.findall("./property/value"):
        value = str(value_tag.text)
        value_tag.text = value.replace(old_value, new_value)

    tree.write(conf_file, encoding="UTF-8", xml_declaration=True)

def merge_conf_file(default_conf_file, custom_conf_file, output_conf_file):
    tree_default = ET.parse(default_conf_file)
    tree_custom = ET.parse(custom_conf_file)
    root_default = tree_default.getroot()
    root_custom = tree_custom.getroot()
    root_output = ET.Element("configuration")
    for property_tag in root_custom.findall("./property"):
        property_name = property_tag.find("name").text
        add_property_element(root_output, property_name, property_tag.find("value").text)
        tags_in_default = root_default.findall("*[name='" + property_name + "']")
        if len(tags_in_default) > 0:
            root_default.remove(tags_in_default[0])

    for property_tag in root_default.findall("./property"):
        add_property_element(root_output, property_tag.find("name").text,
            property_tag.find("value").text)

    tree_output = ET.ElementTree(root_output)
    tree_output.write(output_conf_file, encoding="UTF-8", xml_declaration=True)

def add_property_element(root_elemnt, name, value):
    property_element = ET.SubElement(root_elemnt, "property")
    name_element = ET.SubElement(property_element, "name")
    value_element = ET.SubElement(property_element, "value")
    name_element.text = name
    value_element.text = value

def format_xml_file(xml_file):
    xmlstr = ""
    with open(xml_file, 'r') as f:
        xmlstr = pretty_print(f.read())
    with open(xml_file, "w") as f:
        f.write(xmlstr)