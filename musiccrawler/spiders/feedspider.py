# This is a spider that can crawl RSS feeds in a version independent manner. it uses Mark pilgrim's excellent feedparser utility to parse RSS feeds. You can read about the nightmares of  RSS incompatibility [here](http://diveintomark.org/archives/2004/02/04/incompatible-rss) and  download feedparser that strives to resolve it from [here](http://feedparser.org/docs/)
# The scripts processes only certain elements in the feeds(title, link and summary)
# The items may be saved in the Item pipeline which I leave to you.
#
# Please let me know about any discrepencies you may find in the technical and functional aspects of this script.
#
# -Sid

from scrapy.spider import BaseSpider
from scrapy import log
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

import pymongo
import feedparser
import re
import json
import math
import musiccrawler.settings
import monthdelta
from datetime import datetime
from time import mktime
from musiccrawler.items import DownloadLinkItem
from scrapy.http import Request

class FeedSpider(BaseSpider):        
    name = "feedspider"
    
    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db['sites']
        self.site = self.collection.find_one({"feedurl": kwargs.get('feedurl')})
        log.msg("Received Site from Database:" + self.site, level=log.INFO)
        
        hosts = json.load(open(musiccrawler.settings.HOSTS_FILE_PATH))
        decrypters = json.load(open(musiccrawler.settings.DECRYPTER_FILE_PATH))
        regex_group_count = 40
        self.regexes = []
        
        self.start_urls = [self.site['feedurl']];
        
        for i in range(int(math.ceil(len(hosts) / regex_group_count))):
            hosterregex = ''
    
            for hoster in hosts[(i + 1) * regex_group_count - regex_group_count:(i + 1) * regex_group_count]:
                hosterpattern = unicode(hoster['pattern']).rstrip('\r\n').replace("/", "\/",99).replace(":", "\:",99).replace("\d+{", "\d{",10).replace("++", "+",10).replace("\r\n", "",10).replace("|[\p{L}\w-%]+\/[\p{L}\w-%]+", "",10).replace("decrypted","",10).replace("httpJDYoutube","http",10) + '|'
                hosterregex += hosterpattern.encode('utf-8')
            
            self.regexes.append(re.compile("'" + hosterregex[:-1] + "'", re.IGNORECASE))
            
        for i in range(int(math.ceil(len(decrypters) / regex_group_count))):
            hosterregex = ''
    
            for decrypter in decrypters[(i + 1) * regex_group_count - regex_group_count:(i + 1) * regex_group_count]:
                hosterpattern = unicode(decrypter['pattern']).rstrip('\r\n') + '|'
                hosterregex += hosterpattern.encode('utf-8')
            self.regexes.append(re.compile("\"" + hosterregex[:-1] + "\"", re.IGNORECASE))
                
    def parse(self, response):
            rssFeed = feedparser.parse(response.url)
            
            if rssFeed.bozo == 1:
                log.msg(("Feed kann nicht verarbeitet werden:" + str(response.url)), level=log.WARNING)     
            else:
                for entry in rssFeed.entries:
                    if (datetime.now() - monthdelta.MonthDelta(3)) < datetime.fromtimestamp(mktime(rssFeed.get('updated_parsed', ''))):
                        log.msg(("Verarbeite Feed:" + rssFeed.get('title', response.url)), level=log.INFO)
                        if (datetime.now() - monthdelta.MonthDelta(2)) < datetime.fromtimestamp(mktime(entry.get('published_parsed', ''))):
                            log.msg(("Verarbeite Eintrag:" + entry.get('title', "unnamed entry")), level=log.DEBUG)
                            for regexpr in self.regexes:
                                if 'summary' in entry:
                                    iterator = regexpr.finditer(str(entry.summary))
                                    for match in iterator:
                                        linkitem = DownloadLinkItem()
                                        linkitem['url'] = match.group().split('" ')[0]
                                        linkitem['source'] = self.start_urls[0]
                                        if entry.get('published_parsed',None) is None:
                                            linkitem['date_published'] = datetime.now()
                                        else:
                                            linkitem['date_published'] = datetime.fromtimestamp(mktime(entry.get('published_parsed')))
                                        linkitem['date_discovered'] = datetime.now()
                                        yield linkitem
                                if 'content' in entry:
                                    iterator = regexpr.finditer(str(entry.content))
                                    for match in iterator:
                                        linkitem = DownloadLinkItem()
                                        linkitem['url'] = match.group().split('" ')[0]
                                        linkitem['source'] = self.start_urls[0]
                                        if entry.get('published_parsed',None) is None:
                                            linkitem['date_published'] = datetime.now()
                                        else:
                                            linkitem['date_published'] = datetime.fromtimestamp(mktime(entry.get('published_parsed')))
                                        linkitem['date_discovered'] = datetime.now()
                                        yield linkitem
                                if 'links' in entry:
                                    iterator = regexpr.finditer(str(entry.links))
                                    for match in iterator:
                                        linkitem = DownloadLinkItem()
                                        linkitem['url'] = match.group().split('" ')[0]
                                        linkitem['source'] = self.start_urls[0]
                                        if entry.get('published_parsed',None) is None:
                                            linkitem['date_published'] = datetime.now()
                                        else:
                                            linkitem['date_published'] = datetime.fromtimestamp(mktime(entry.get('published_parsed')))
                                        linkitem['date_discovered'] = datetime.now()
                                        yield linkitem
                            for link in entry.links:
                                request = Request(url=unicode(link.href),callback=self.parse_entry_html)
                                if entry.get('published_parsed',None) is None:
                                    request.meta['date_published'] = datetime.now()
                                else:
                                    request.meta['date_published'] = datetime.fromtimestamp(mktime(entry.get('published_parsed')))
                                yield request 
                        else:
                            log.msg(("Feed-Entry is older than 2 month:" + entry.get('title', "unnamed entry")), level=log.DEBUG)  
                    else:
                        log.msg(("Feed has not been updated within 3 months:" + entry.get('title', "unnamed entry")), level=log.INFO)
        
    def parse_entry_html(self, response):
        for regexpr in self.regexes:
            iterator = regexpr.finditer(response.body)
            for match in iterator:
                linkitem = DownloadLinkItem()
                linkitem['url'] = match.group().split('" ')[0]
                linkitem['source'] = self.start_urls[0]
                linkitem['date_published'] = response.meta['date_published']
                linkitem['date_discovered'] = datetime.now()
                yield linkitem
                
    def handle_spider_closed(self, spider, reason):
        if reason == "finished":
            self.collection.update({"feedurl" : self.source},{"$set" : {"last_crawled" : datetime.now(), "next_crawl" : None}})
