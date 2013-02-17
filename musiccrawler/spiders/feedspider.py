# This is a spider that can crawl RSS feeds in a version independent manner. it uses Mark pilgrim's excellent feedparser utility to parse RSS feeds. You can read about the nightmares of  RSS incompatibility [here](http://diveintomark.org/archives/2004/02/04/incompatible-rss) and  download feedparser that strives to resolve it from [here](http://feedparser.org/docs/)
# The scripts processes only certain elements in the feeds(title, link and summary)
# The items may be saved in the Item pipeline which I leave to you.
#
# Please let me know about any discrepencies you may find in the technical and functional aspects of this script.
#
# -Sid

from datetime import datetime
from musiccrawler.items import DownloadLinkItem
from pkgutil import get_loader
from scrapy import log, signals
from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.xlib.pydispatch import dispatcher
from time import mktime
import feedparser
import json
import math
import monthdelta
import musiccrawler.settings
import os
import sys
import pymongo
import re
import pkgutil


class FeedSpider(BaseSpider):        
    name = "feedspider"
    
    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT,tz_aware=True)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db['sites']
        self.site = self.collection.find_one({"feedurl": kwargs.get('feedurl')})
        self.source = self.site['feedurl']
        
        if self.site['last_crawled'] is None:
            self.last_crawled = datetime.now() - monthdelta.MonthDelta(12)
        else:
            self.last_crawled = self.site['last_crawled']
            
        log.msg("Received Site from Database:" + str(self.site), level=log.INFO)
        
        hosts = json.load(pkgutil.get_data("spiders","hosts.json"))
        decrypters = json.load(pkgutil.get_data("spiders","decrypter.json"))
        regex_group_count = 35
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
                            if self.last_crawled < datetime.fromtimestamp(mktime(entry.get('published_parsed', ''))):
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
                                log.msg(("Feed-Entry was already spidered at last run:" + entry.get('title', "unnamed entry")), level=log.DEBUG)  
                        else:
                            log.msg(("Feed-Entry is older than 2 month:" + entry.get('title', "unnamed entry")), level=log.DEBUG)  
                    else:
                        log.msg(("Feed has not been updated within 3 months:" + entry.get('title', "unnamed entry")) + ", DEACTIVATING FEED!", level=log.WARNING)
                        self.collection.update({"feedurl" : self.source},{"$set" : {"active" : False}})
        
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
            
def get_data_smart(package, resource, as_string=True):
    """Rewrite of pkgutil.get_data() that actually lets the user determine if data should
    be returned read into memory (aka as_string=True) or just return the file path.
    """
    
    loader = get_loader(package)
    if loader is None or not hasattr(loader, 'get_data'):
        return None
    mod = sys.modules.get(package) or loader.load_module(package)
    if mod is None or not hasattr(mod, '__file__'):
        return None
    
    # Modify the resource name to be compatible with the loader.get_data
    # signature - an os.path format "filename" starting with the dirname of
    # the package's __file__
    parts = resource.split('/')
    parts.insert(0, os.path.dirname(mod.__file__))
    resource_name = os.path.join(*parts)
    if as_string:
        return loader.get_data(resource_name)
    else:
        return resource_name
