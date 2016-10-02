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
OSM_FILE = "lawrenceville.osm"
k = 10 # Parameter: take every k-th top level element

#import extra.py

import csv, codecs, cStringIO

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

from mapparser import count_tags
ct = count_tags(OSM_FILE)
pprint.pprint(ct)
import tags as tg
import users
from audit import audit_phone, audit_zipcodes
import data as dt
data = process_map(OSM_FILE, True)
zip_codes = audit_zipcodes(OSM_FILE)
pprint.pprint(dict(zip_codes))
phone_num = audit_phone(OSM_FILE)
pprint.pprint(dict(phone_num))

tg.process_map(OSM_FILE)
#dt.process_map(OSM_FILE,validate=True)
#pprint.pprint(data[0:6])
conn = sqlite3.connect('lawrenceville.db')


c = conn.cursor()
c.execute('DROP TABLE IF EXISTS ways_tags')
c.execute('DROP TABLE IF EXISTS ways_nodes')
c.execute('DROP TABLE IF EXISTS nodes_tags')
c.execute('DROP TABLE IF EXISTS ways')
c.execute('DROP TABLE IF EXISTS nodes')

#id	user	uid	version	changeset	timestamp
c.execute('CREATE TABLE ways(id text, user text, uid text, version text, changeset text, timestamp text)')
with open('ways.csv') as f:
    reader = UnicodeReader(f)
    for field in reader:
        c.execute("INSERT INTO ways VALUES (?,?,?,?,?,?);", field)


#id	node_id	position
c.execute('CREATE TABLE ways_nodes(id text, node_id text, position text)')
with open('ways_nodes.csv') as f:
    reader = UnicodeReader(f)
    for field in reader:
        c.execute("INSERT INTO ways_nodes VALUES (?,?,?);", field)


#id	key	value	type
c.execute('CREATE TABLE ways_tags(id text, key text, value text , type text)')
with open('ways_tags.csv') as f:
    reader = UnicodeReader(f)
    for field in reader:
        c.execute("INSERT INTO ways_tags VALUES (?,?,?,?);", field)

c.execute('CREATE TABLE nodes_tags(id text, key text, value text , type text)')
with open('nodes_tags.csv') as f:
    reader = UnicodeReader(f)
    for field in reader:
        c.execute("INSERT INTO nodes_tags VALUES (?,?,?,?);", field)

c.execute('CREATE TABLE nodes(id text, lat text, lon text , user text, uid text, version text, changeset text, timestamp text)')
with open('nodes.csv') as f:
    reader = UnicodeReader(f)
    for field in reader:
        c.execute("INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?);", field)
conn.commit()

# Sort Postal codes by count, descending
results = c.execute('''SELECT tags.value, COUNT(*) as count \
FROM (SELECT * FROM nodes_tags \
      UNION ALL \
      SELECT * FROM ways_tags) tags\
    WHERE tags.key='postcode'\
    GROUP BY tags.value \
    ORDER BY count DESC\
''');
print list(results)

# Sort Phone numbers by count, descending
results = c.execute('''SELECT tags.value, COUNT(*) as count \
FROM (SELECT * FROM nodes_tags \
      UNION ALL \
      SELECT * FROM ways_tags) tags\
    WHERE tags.key='phone'\
    GROUP BY tags.value \
    ORDER BY count DESC\
''');
print list(results)

#Sort cities by count, descending
results = c.execute('''SELECT tags.value, COUNT(*) as count \
FROM (SELECT * FROM nodes_tags UNION ALL \
      SELECT * FROM ways_tags) tags\
    WHERE tags.key LIKE '%city'\
    GROUP BY tags.value \
    ORDER BY count DESC\
''');
print list(results)

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
''');
print list(results)

conn.close()