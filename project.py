import sys
import os
import collections
import pprint
import xml.etree.cElementTree as ET
import re
import codecs
import csv
import cerberus
import copy
import xml.etree.cElementTree as ET
from data import *
import pprint
import sqlite3
#import pymongo
OSM_FILE = "lawrenceville.osm"
k = 40 # Parameter: take every k-th top level element

#import extra.py

from mapparser import count_tags
ct = count_tags(OSM_FILE)
pprint.pprint(ct)
import tags as tg
print "c1"
import users
print "c1"
from audit import audit
print "c1"
import data
print "c1"
data = process_map(OSM_FILE, True)
print "c1"
st_types = audit(OSM_FILE)
pprint.pprint(dict(st_types))
tg.process_map(OSM_FILE)
#pprint.pprint(data[0:6])
conn = sqlite3.connect('savannah.db')

c = conn.cursor()
c.execute('DROP TABLE IF EXISTS ways_tags')
c.execute('DROP TABLE IF EXISTS ways_nodes')
c.execute('DROP TABLE IF EXISTS nodes_tags')
c.execute('DROP TABLE IF EXISTS ways')
c.execute('DROP TABLE IF EXISTS nodes')

#id	user	uid	version	changeset	timestamp
c.execute('CREATE TABLE ways(id text, user text, uid text, version text, changeset text, timestamp text)')
with open('ways.csv') as f:
    reader = csv.reader(f)
    for field in reader:
        c.execute("INSERT INTO ways VALUES (?,?,?,?,?,?);", field)
print "there"


#id	node_id	position
c.execute('CREATE TABLE ways_nodes(id text, node_id text, position text)')
with open('ways_nodes.csv') as f:
    reader = csv.reader(f)
    for field in reader:
        c.execute("INSERT INTO ways_nodes VALUES (?,?,?);", field)
print "there"


#id	key	value	type
c.execute('CREATE TABLE ways_tags(id text, key text, value text , type text)')
with open('ways_tags.csv') as f:
    reader = csv.reader(f)
    for field in reader:
        c.execute("INSERT INTO ways_tags VALUES (?,?,?,?);", field)
print "there"


c.execute('CREATE TABLE nodes_tags(id text, key text, value text , type text)')
with open('nodes_tags.csv') as f:
    reader = csv.reader(f)
    for field in reader:
        c.execute("INSERT INTO nodes_tags VALUES (?,?,?,?);", field)
print "there"


c.execute('CREATE TABLE nodes(id text, lat text, lon text , user text, uid text, version text, changeset text, timestamp text)')
with open('nodes.csv') as f:
    reader = csv.reader(f)
    for field in reader:
        c.execute("INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?);", field)
conn.commit()

# Number of Nodes
results = c.execute('''SELECT COUNT(*) FROM nodes''');
print list(results)

# Number of Ways
results = c.execute('''SELECT COUNT(*) as count FROM ways''');
print list(results)

# Top 10 contributing users
results = c.execute('''SELECT e.user, COUNT(*) as num \
FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e \
GROUP BY e.user \
ORDER BY num DESC \
LIMIT 10\
''');
print list(results)

# Number of users appearing only once
results = c.execute('''SELECT COUNT(*) \
FROM\
    (SELECT e.user, COUNT(*) as num \
     FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e \
     GROUP BY e.user \
     HAVING num=1)  u\
''');
print list(results)

# Top 10 amenities
results = c.execute('''SELECT value, COUNT(*) as num \
FROM nodes_tags \
WHERE key='amenity' \
GROUP BY value \
ORDER BY num DESC \
LIMIT 10\
''');
print list(results)

# Top religions
results = c.execute('''SELECT nodes_tags.value, COUNT(*) as num \
FROM nodes_tags \
    JOIN (SELECT DISTINCT(id) FROM nodes_tags \
    WHERE value='place_of_worship') i \
    ON nodes_tags.id=i.id \
WHERE nodes_tags.key='religion' \
GROUP BY nodes_tags.value \
ORDER BY num DESC \
''');
print list(results)

# Most popular cuisines
results = c.execute('''SELECT nodes_tags.value, COUNT(*) as num \
FROM nodes_tags \
    JOIN (SELECT DISTINCT(id) FROM nodes_tags WHERE value='restaurant') i \
    ON nodes_tags.id=i.id \
WHERE nodes_tags.key='cuisine' \
GROUP BY nodes_tags.value \
ORDER BY num DESC\
LIMIT 10\
''');
print list(results)

conn.close()