#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 21:54:31 2017

@author: varshith

Steps:
    1. Get index map url from index_urls table for the given site
    2. Perform a get on the URL
    3. Connect to index table of given site
    4. Compare the get last_modified_date with last_modified_date present in 
    site_index db
    5. If the date is changed, perform a get on sitemap url
    -. Perform a checksum op on the get response content and compare it with the table get
    response
    6. 
"""
from argparse import ArgumentParser
from psycopg2 import connect
from requests import get
from BeautifulSoup import BeautifulStoneSoup as Soup

conn = connect(database="advice", user="postgres",
                        password="scriptbees1$", host="127.0.0.1", port="5432")
cursor = conn.cursor()


def updatesitelist(tablename):
    resp = open('pcmag.xml','r').read()
    	# we didn't get a valid response, bail
    # BeautifulStoneSoup to parse the document 
    soup = Soup(resp)
    urls = soup.findAll('url')
    #print urls
    	# find all the <url> tags in the document 
    	# no urls? bail
    if not urls:
        print 'no urls exist in the sitemap '
        return False
    	#extract what we need from the url
    for u in urls:
        try:
           loc = u.find('loc').string
        except AttributeError:
            loc = ''
        query = """INSERT INTO """ + tablename + """ (url) values (%s)"""
        print loc
        cursor.execute(query,(loc,))
        u = None
        del u
    print 'committing data from sitemap'
    conn.commit()
    return True

if __name__ == '__main__':
    options = ArgumentParser()
    options.add_argument('-s', '--sitename', action='store', dest='site', 
                         help='this program will take the content present in url')
    args = options.parse_args()
    #change to work with robots here
    site = args.site
    #preparing site index data in the start from robots.txt file by just 
    result = updatesitelist('pcmag')
    if not result:
         print 'There was an error!'
    else:
         print ('Index tables for the site :' + site + ' is updated to ' + site 
                + ' db')
