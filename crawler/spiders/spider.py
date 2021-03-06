import scrapy
from scrapy.conf import settings
from scrapy.http import Request
import os
import os.path
from crawler.items import CrawlerItem
import json
import js2xml
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib
import pymysql
import emoji

db = pymysql.connect("localhost","root","belle","thsst_db" )

class Spider(scrapy.Spider):

    name = "crawler"
    urls = []
    html_ctr = 0

    cursor = db.cursor()
    cursor.execute("SELECT distinct(link) FROM link WHERE link NOT IN (SELECT url FROM page)")
    db.commit()

    for row in cursor.fetchall():
        urls.append("".join(row))

    cursor.close()
    start_urls = urls

    def getLastId(self):
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(*) FROM page")
        db.commit()
        rows = cursor.fetchone()[0]
        cursor.close()
        self.html_ctr = rows

    def parse(self, response):

        self.getLastId()

        for rapplerStory in response.css('div.story-area'):

            item = CrawlerItem()

            title = response.css('title::text').extract_first()

            self.html_ctr += 1

            #make dir 
            if not os.path.exists(settings['HTML_OUTPUT_DIRECTORY']):
                os.makedirs(settings['HTML_OUTPUT_DIRECTORY'])

            #save html file
            filename = '%s/%d.html' % (settings['HTML_OUTPUT_DIRECTORY'], self.html_ctr)
            with open(filename, 'wb') as f:
               f.write(response.body)

            page = urllib.request.urlopen(response.url).read()
            soup = BeautifulSoup(page, "lxml")

            #metadata (author & date)
            author = soup.find('meta', attrs={'name':'bt:author'}) 
            date = soup.find('meta', attrs={'name':'bt:pubDate'}) 

            if author:
                item["author"] = author.get('content')
            if date:
                item["date"] = date.get('content')

            try:
                #content located in script
                js = response.xpath('//script/text()').extract()
                jstree = js2xml.parse(js[1])
                content = js2xml.jsonlike.make_dict(jstree.xpath('//var[@name="r4articleData"]//object//property[@name="fulltext"]')[0])
                cleantext = BeautifulSoup(str(content), "lxml").text
                content = re.sub('fulltext', '', cleantext, 1)
                content = re.sub('[^A-Za-z0-9\.]+', ' ', content)

                patt = re.compile(u"[^\U00000000-\U0000d7ff\U0000e000-\U0000ffff]", flags=re.UNICODE)
                item["content"] = patt.sub(u'', content)
                
            except:
                try:
                    #content located in <p> (scripts not included) or <p><span> 
                    content = response.xpath('//div[starts-with(@class,"story-area")]//p//text()[not(ancestor::script|ancestor::style|ancestor::noscript)] | //div[starts-with(@class,"story-area")]//p/span//text()').extract()
                    
                    item["content"] = u','.join(content)

                    patt = re.compile(u"[^\U00000000-\U0000d7ff\U0000e000-\U0000ffff]", flags=re.UNICODE)
                    item["content"] = patt.sub(u'', item["content"])

                except:
                    item["content"] = ""  

            item["url"]= response.url
            item["title"] = title

            yield item
