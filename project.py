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
OSM_FILE = 'example.osm'

from mapparser import count_tags
ct = count_tags(OSM_FILE)
pprint.pprint(ct)
import tags as tg

import users

from audit import audit

import data

data = process_map(OSM_FILE, True)
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
c.execute('DROP TABLE nodes')

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

# Sort Postal codes by count, descending
c.execute('SELECT tags.value, COUNT(*) as count \
FROM (SELECT * FROM nodes_tags \
      UNION ALL \
      SELECT * FROM ways_tags) tags\
WHERE tags.key="postcode"\
GROUP BY tags.value \
ORDER BY count DESC');
print c.fetchall()

#Sort cities by count, descending
c.execute('SELECT tags.value, COUNT(*) as count \
FROM (SELECT * FROM nodes_tags UNION ALL \
      SELECT * FROM ways_tags) tags\
WHERE tags.key LIKE "%city"\
GROUP BY tags.value \
ORDER BY count DESC');
print c.fetchall()

# Number of Nodes
c.execute('SELECT COUNT(*), FROM nodes')
print c.fetchall()

# Number of Ways
c.execute('SELECT COUNT(*), FROM ways')
print c.fetchall()

# Number of unique users
c.execute('SELECT COUNT(DISTINCT(e.uid)) FROM (SELECT uid FROM nodes \
UNION ALL SELECT uid FROM ways) e');
print c.fetchall()

# Top 10 contributing users
c.execute('SELECT e.user, COUNT(*) as num \
FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e \
GROUP BY e.user \
ORDER BY num DESC \
LIMIT 10')
print c.fetchall()

# Number of users appearing only once
c.execute('SELECT COUNT(*) \
FROM\
    (SELECT e.user, COUNT(*) as num \
     FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e \
     GROUP BY e.user \
     HAVING num=1)  u')
print c.fetchall()

# Top 10 amenities
c.execute('SELECT value, COUNT(*) as num \
FROM nodes_tags \
WHERE key="amenity" \
GROUP BY value \
ORDER BY num DESC \
LIMIT 10')
print c.fetchall()

# Top religions
c.execute('SELECT nodes_tags.value, COUNT(*) as num \
FROM nodes_tags \
    JOIN (SELECT DISTINCT(id) FROM nodes_tags \
    WHERE value="place_of_worship") i \
    ON nodes_tags.id=i.id \
WHERE nodes_tags.key="religion" \
GROUP BY nodes_tags.value \
ORDER BY num DESC ')
print c.fetchall()

# Most popular cuisines
c.execute('SELECT nodes_tags.value, COUNT(*) as num \
FROM nodes_tags \
    JOIN (SELECT DISTINCT(id) FROM nodes_tags WHERE value="restaurant") i \
    ON nodes_tags.id=i.id \
WHERE nodes_tags.key="cuisine" \
GROUP BY nodes_tags.value \
ORDER BY num DESC')
print c.fetchall()

conn.close()