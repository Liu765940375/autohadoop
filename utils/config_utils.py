import os
import fnmatch
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

def replace_properties_conf_value(conf_file, old_value, new_value):
    tmp_filename = conf_file + '.tmp'
    os.system("mv " + conf_file + " " + tmp_filename)
    with open(tmp_filename, 'r') as file_read:
            with open(conf_file, 'w') as file_write:
                for line in file_read:
                    file_write.write(line.replace(old_value, new_value))
    os.system("rm -rf " + tmp_filename)

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