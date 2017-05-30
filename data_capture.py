#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  1 16:39:43 2017

@author: venkatesh
"""
from BeautifulSoup import BeautifulSoup as Soup
from psycopg2 import connect
from requests import get
import requests.packages.urllib3
from concurrent.futures import ProcessPoolExecutor
from argparse import ArgumentParser

requests.packages.urllib3.disable_warnings()


def scraper(item):
    url = item[0]
    # url =
    # 'https://www.unbiased.co.uk/profile/financial-adviser/tristan-brodbeck-financial-planning-ltd-519866?hash=8197451'
    print url
    page = get(url)
    if 200 != page.status_code:
        print 'failed getting site url data ' + url
        pass
    soup = Soup(page.content)
    try:
        name = soup.find('span', {'itemprop': 'name'}).text
        name = name.split(' ')
        firstname = name[0]
        lastname = name[1]
        print firstname, lastname
    except AttributeError:
        firstname = ''
        lastname = ''
    try:
        jobtitle = soup.find('span', {'itemprop': 'jobTitle'}).text
        print jobtitle
    except AttributeError:
        jobtitle = ''
    try:
        followme = soup.find('div', {'class': 'followme'})
        followme = str(followme)
        fsoup = Soup(followme)
        try:
            tlink = fsoup.find('a', {'class': 'twitter'})
            tlink = tlink['href']
        except AttributeError:
            tlink = ''
        try:
            llink = fsoup.find('a', {'class': 'linkedin'})
            llink = llink['href']
        except AttributeError:
            llink = ''
        try:
            mail = fsoup.find('a', {'class': 'mail'})
            mail = mail['href'].split(':')[-1]
        except AttributeError:
            mail = ''
    except AttributeError:
        tlink = ''
        llink = ''
        mail = ''
    print 'faulty'
    print firstname, lastname, 'pcmag', llink, tlink, mail
    updatequery = """insert into pcmagdata  (Firstname, Lastname, Website, Linkedin, Twitter, Mail)
            values (%s,%s, %s, %s,%s, %s)"""
    conn = connect(database="advice", user="postgres",
                   password="scriptbees1$", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    print firstname, lastname, 'pcmag', llink, tlink, mail
    cursor.execute(
        updatequery,
        (firstname,
         lastname,
         'pcmag',
         llink,
         tlink,
         mail))
    conn.commit()
    conn.close()


def scrape(table):
    # Change url_inserted_date every week here
    conn = connect(database="advice", user="postgres",
                   password="scriptbees1$", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    query1 = """select url from """ + table + """ where s_no <= 104"""
    cursor.execute(query1)
    items = cursor.fetchall()
    conn.close()
    e = ProcessPoolExecutor()
    e.map(scraper, items)
    # map(scraper, items)
    return True


def results(table):
    result = scrape(table)
    if not result:
        print 'There was an error!'
    else:
        print(r"scrape job for the site www.pcmag.com has been done")

if __name__ == '__main__':
    options = ArgumentParser()
    options.add_argument('-t', '--table', action='store', dest='site',
                         help='this program will take the content present in url')
    args = options.parse_args()
    # change to work with robots here
    site = args.site
    # preparing site index data in the start from robots.txt file by just
    result = results(site)
    if not result:
        print 'There was an error!'
    else:
        print('Index tables for the site :' + site +
               ' is updated to ' + site + ' db')
