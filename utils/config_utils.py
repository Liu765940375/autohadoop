import os
import fnmatch
import xml.etree.cElementTree as ET
from xml.dom.minidom import parseString


pretty_print = lambda data: '\n'.join(
    [line for line in parseString(data).toprettyxml(indent=' ' * 2).split('\n') if line.strip()])


def merge_conf_xml_file(default_conf_file, custom_conf_file, output_conf_file):
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


def replace_conf_value(conf_file, dict):
    with open(conf_file) as f:
        read = f.read()
    with open(conf_file, 'w') as f:
        for key,val in dict.items():
            read = read.replace(key, val)
        f.write(read)


def replace_conf_value(conf_file, dict):
    with open(conf_file) as f:
        read = f.read()
    with open(conf_file, 'w') as f:
        for key,val in dict.items():
            read = read.replace(key, val)
        f.write(read)


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


def get_config_value(conf_file, name):
    tree = ET.parse(conf_file)
    root = tree.getroot()
    for property_tag in root.findall("property/[name='" + name + "']"):
        return property_tag.find("value").text


# merge configuration file
def update_conf(component, default_conf, custom_conf):
    custom_component_conf = os.path.join(custom_conf, component)
    default_component_conf = os.path.join(default_conf, component)
    output_component_conf = os.path.join(custom_conf, "output/" + component)
    # create output dir for merged configuration file
    os.system("rm -rf " + output_component_conf)
    os.system("mkdir -p " + output_component_conf)
    processed_file = {}
    # loop in default_conf, merge with custom conf file and copy to output_conf
    for conf_file in [file for file in os.listdir(default_component_conf) if fnmatch.fnmatch(file, '*.xml')]:
        custom_conf_file = os.path.join(custom_component_conf, conf_file)
        default_conf_file = os.path.join(default_component_conf, conf_file)
        output_conf_file = os.path.join(output_component_conf, conf_file)
        if os.path.isfile(custom_conf_file):
            merge_conf_xml_file(default_conf_file, custom_conf_file, output_conf_file)
        else:
            os.system("cp " + default_conf_file + " " + output_conf_file)
        processed_file[conf_file] = ""

    # loop in default_conf, merge with custom conf properties file and copy to output_conf
    for conf_file in [file for file in os.listdir(default_component_conf) if fnmatch.fnmatch(file, '*.conf')]:
        custom_conf_file = os.path.join(custom_component_conf, conf_file)
        default_conf_file = os.path.join(default_component_conf, conf_file)
        output_conf_file = os.path.join(output_component_conf, conf_file)
        if os.path.isfile(custom_conf_file):
            merge_conf_properties_file(default_conf_file, custom_conf_file, output_conf_file)
        else:
            os.system("cp " + default_conf_file + " " + output_conf_file)
        processed_file[conf_file] = ""

    # copy unprocessed file in default_component_conf to output_component_conf
    copy_unprocessed_file(processed_file, default_component_conf, output_component_conf)
    # copy unprocessed file in custom_component_conf to output_component_conf
    copy_unprocessed_file(processed_file, custom_component_conf, output_component_conf)
    return output_component_conf


def copy_unprocessed_file(processed_file, conf_dir, output_dir):
    for conf_file in os.listdir(conf_dir):
        if conf_file not in processed_file:
            src_conf_file = os.path.join(conf_dir, conf_file)
            output_conf_file = os.path.join(output_dir, conf_file)
            os.system("cp " + src_conf_file + " " + output_conf_file)


def merge_conf_properties_file (default_filename, custom_filename, output_filename):
    props_result = get_configs_from_properties(default_filename)
    props_custom = get_configs_from_properties(custom_filename)
    for k, v in props_custom.items():
        props_result[k] = v
    with open(output_filename, 'w') as f:
        for k, v in props_result.items():
            f.write(k + " " + v + "\n")


def get_configs_from_properties (filename):
    result = {}
    with open(filename, 'r') as f:
        for line in f:
            kv = line.split()
            if line.startswith('#') or len(kv) != 2:
                continue
            result[kv[0]] = kv[1]
    return result
