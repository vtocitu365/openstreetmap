"""
Your task in this exercise has two steps:

- audit the OSMFILE and change the variable 'mapping' to reflect the changes needed to fix
    the unexpected street types to the appropriate ones in the expected list.
    You have to add mappings only for the actual problems you find in this OSMFILE,
    not a generalized solution, since that may and will depend on the particular area you are auditing.
- write the update_name function, to actually fix the street name.
    The function takes a string with street name as an argument and should return the fixed name
    We have provided a simple test so that you see what exactly is expected
"""
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

#OSMFILE = "example.osm"
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons", "Circle", "Trace", "Parkway", "Suite"]



def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)
            return True
        return False


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def is_zipcode(elem):
    """ returns if zip-code like """
    return 'zip' in elem.attrib['k']

def is_phone(elem):
    """ returns if zip-code like """
    return 'phone' in elem.attrib['k']

def phone_format(phone_number):
    clean_phone_number = re.sub('[^0-9]+', '', phone_number)
    formatted_phone_number = re.sub("(\d)(?=(\d{3})+(?!\d))", r"\1-", "%d" % int(clean_phone_number[:-1])) + \
    clean_phone_number[-1]
    return formatted_phone_number

def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    zipcode = defaultdict(int)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
                if is_zipcode(tag):
                    audit_zipcodes()
    osm_file.close()
    return street_types


def update_name(name, mapping):
    dict_map = sorted(mapping.keys(), key=len, reverse=True)
    for key in dict_map:
        if name.find(key) != -1:
            name = name.replace(key, mapping[key])
    return name

def audit_zipcodes(osmfile):
    """Iterate through all zip codes, collect all the zip codes that does not start with 3"""
    osm_file = open(osmfile, "r")
    zip_codes = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if tag.attrib['k'] == "addr:postcode" and tag.attrib['v'].startswith('300'):
                        zip_codes[tag.attrib['v']].add(tag.attrib["v"])
    osm_file.close()
    return zip_codes

def audit_phone(osmfile):
    """Iterate through all phone numbers, collect all the phone numbers that don't start with 4, 6, or 7"""
    osm_file = open(osmfile, "r")
    phone_num = defaultdict(set)
    area_code = ['6','7','4','(']
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                for code in area_code:
                    if 'phone' in tag.attrib['k'].lower() and tag.attrib['v'].startswith(code):
                        formatted_phone_number =  phone_format(tag.attrib['v'])
                        phone_num[tag.attrib['v']].add(formatted_phone_number)
    return phone_num

def clean_phone(name, zip_codes):
    dict_map = sorted(zip_codes.keys(), key=len, reverse=True)
    for key in dict_map:
        if name.find(key) != -1:
            name = name.replace(key, zip_codes[key])
    return name

def test():
    st_types, zipcode = audit(OSMFILE)
    assert len(st_types) == 3
    pprint.pprint(dict(st_types))

    for st_type, ways in st_types.iteritems():
        for name in ways:
            better_name = update_name(name, mapping)
            pprint.pprint(dict(st_types))
            print name, "=>", better_name
            if name == "West Lexington St.":
                assert better_name == "West Lexington Street"
            if name == "Baldwin Rd.":
                assert better_name == "Baldwin Road"


if __name__ == '__main__':
    test()