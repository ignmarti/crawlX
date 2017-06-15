from io import BytesIO
import sys
import feedparser
import pycurl
import re
import time
import random
from pymongo import MongoClient
import pprint

if(len(sys.argv)!=3):
	print("ERROR; USAGE: python {} <dbName> <collectionName>".format(sys.argv[0]))
	sys.exit()

DB=sys.argv[1]
collection=sys.argv[2]


db = MongoClient()[DB]


PRIMARY_URL="https://www.edx.org/api/v2/report/course-feed/rss?page="

def inputData(parser):
	for entry in parser.get("entries", []):
		entry["published_parsed"]=""
		db[collection].insert_one(entry)
	return (True)


def getContent(url):
	time.sleep(3*random.random())
	try:
		c = pycurl.Curl()
		storage = BytesIO()
		c.setopt(c.URL, url)
		c.setopt(c.WRITEFUNCTION, storage.write)
		c.setopt(c.CAPATH, "/etc/ssl/certs/")
		c.setopt(c.CAINFO, "/etc/ssl/certs/ca-certificates.crt")
		c.setopt(c.FOLLOWLOCATION, True)
		print("%s - Obtaining courses from -> %s" % (time.strftime("[%d-%m-%Y -> %H:%M:%S]"), url))
		c.perform()

		content = storage.getvalue()
		parser = feedparser.parse(content)
		print("Getting Content into DB")
		inputData(parser)
	except Exception as e:
		print("%s - ERROR, Get Random Wait -> %s" % (time.strftime("[%d-%m-%Y -> %H:%M:%S]"), str(e)))

for i in range(11):
	getContent(PRIMARY_URL+str(i))