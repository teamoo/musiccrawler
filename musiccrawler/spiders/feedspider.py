# This is a spider that can crawl RSS feeds in a version independent manner. it uses Mark pilgrim's excellent feedparser utility to parse RSS feeds. You can read about the nightmares of  RSS incompatibility [here](http://diveintomark.org/archives/2004/02/04/incompatible-rss) and  download feedparser that strives to resolve it from [here](http://feedparser.org/docs/)
# The scripts processes only certain elements in the feeds(title, link and summary)
# The items may be saved in the Item pipeline which I leave to you.
#
# Please let me know about any discrepencies you may find in the technical and functional aspects of this script.
#
# -Sid
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

from datetime import datetime
from musiccrawler.items import DownloadLinkItem
from pytz import timezone
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
import pkg_resources
import pymongo
import re

class FeedSpider(BaseSpider):        
    name = "feedspider"
    
    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT, tz_aware=True)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        if musiccrawler.settings.__dict__.has_key('MONGODB_USER') and musiccrawler.settings.__dict__.has_key('MONGODB_PASSWORD') and musiccrawler.settings.MONGODB_USER is not None:
            self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db['sites']
        self.site = self.collection.find_one({"feedurl": kwargs.get('feedurl')})
        
        if self.site is not None:
            self.source = self.site['feedurl']
            self.active = self.site['active']
            self.tz = timezone("Europe/Berlin")
            self.start_urls = [self.site['feedurl']]
            
            
            if "last_post" in self.site and not self.site['last_post'] is None:
                self.last_post = self.site['last_post']
            else:
                self.last_post = None
            
            if self.site['last_crawled'] is None:
                self.last_crawled = self.tz.localize(datetime.now() - monthdelta.MonthDelta(12))
            else:
                self.last_crawled = self.site['last_crawled']
                
            log.msg("Received Site from Database:" + str(self.site), level=log.INFO)
            
            if self.active == False:
                log.msg("Site is deactivated, not crawling.", level=log.ERROR);
            else:
                hosts = json.loads(pkg_resources.resource_string('musiccrawler.config', musiccrawler.settings.HOSTS_FILE_PATH))
                log.msg("Loaded " + str(len(hosts)) + " hoster", level=log.INFO)
                
                decrypters = json.loads(pkg_resources.resource_string('musiccrawler.config', musiccrawler.settings.DECRYPTERS_FILE_PATH))
                log.msg("Loaded " + str(len(decrypters)) + " decrypter", level=log.INFO)
                
                regex_group_count = 35
                self.regexes = []
                
                for i in range(int(math.ceil(len(hosts) / regex_group_count))):
                    hosterregex = ''
            
                    for hoster in hosts[(i + 1) * regex_group_count - regex_group_count:(i + 1) * regex_group_count]:
                        hosterpattern = unicode(hoster['pattern']).rstrip('\r\n').replace("/", "\/", 99).replace(":", "\:", 99).replace("\d+{", "\d{", 10).replace("++", "+", 10).replace("\r\n", "", 10).replace("|[\p{L}\w-%]+\/[\p{L}\w-%]+", "", 10).replace("decrypted", "", 10).replace("httpJDYoutube", "http", 10) + '|'
                        hosterregex += hosterpattern.encode('utf-8')
                    
                    self.regexes.append(re.compile("'" + hosterregex[:-1] + "'", re.IGNORECASE))
                    
                for i in range(int(math.ceil(len(decrypters) / regex_group_count))):
                    hosterregex = ''
            
                    for decrypter in decrypters[(i + 1) * regex_group_count - regex_group_count:(i + 1) * regex_group_count]:
                        hosterpattern = unicode(decrypter['pattern']).rstrip('\r\n') + '|'
                        hosterregex += hosterpattern.encode('utf-8')
                    self.regexes.append(re.compile("\"" + hosterregex[:-1] + "\"", re.IGNORECASE))
    
                log.msg("Spider initialized.", level=log.INFO)
        else:
            log.msg("Site not found, shutting down Spider.", level=log.ERROR)
     
                
    def parse(self, response):
        if response.status < 400:
            if not self.site is None:
                if self.active == True:
                    rssFeed = feedparser.parse(response.body.strip())
                    
                    if rssFeed.bozo == 1:
                        log.msg(("Feed is corrupted: " + str(response.url) + ", DEACTIVATING FEED!"), level=log.WARNING)
                        self.collection.update({"feedurl" : self.source}, {"$set" : {"active" : False, "next_crawl" : None}})     
                    else:
                        log.msg(("Processing Feed: " + rssFeed.get('title', response.url)), level=log.INFO)
                        if isinstance(rssFeed.entries, (list, tuple)) and len(rssFeed.entries) >= 1:
                            self.last_post = datetime.fromtimestamp(mktime(rssFeed.entries[0].get('published_parsed', rssFeed.get('updated_parsed', datetime.now().timetuple()))))
                            for entry in rssFeed.entries:
                                if (datetime.now() - monthdelta.MonthDelta(3)) < datetime.fromtimestamp(mktime(rssFeed.get('updated_parsed', (datetime.now() - monthdelta.MonthDelta(2)).timetuple()))):
                                    if (datetime.now() - monthdelta.MonthDelta(2)) < datetime.fromtimestamp(mktime(entry.get('published_parsed', (datetime.now() - monthdelta.MonthDelta(1)).timetuple()))):
                                        if self.last_crawled < self.tz.localize(datetime.fromtimestamp(mktime(entry.get('published_parsed', (datetime.now() - monthdelta.MonthDelta(1)).timetuple())))):
                                            log.msg(("Processing Feed-Entry:" + entry.get('title', "unnamed entry")), level=log.INFO)
                                            for regexpr in self.regexes:
                                                if 'description' in entry:
                                                    iterator = regexpr.finditer(str(entry.description).encode('utf-8'))
                                                    for match in iterator:
                                                        linkitem = DownloadLinkItem()
                                                        linkitem['url'] = match.group()
                                                        linkitem['source'] = self.start_urls[0]
                                                        if entry.get('published_parsed', None) is None:
                                                            linkitem['date_published'] = self.tz.localize(datetime.now())
                                                        else:
                                                            linkitem['date_published'] = self.tz.localize(datetime.fromtimestamp(mktime(entry.get('published_parsed'))))
                                                        linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                                        yield linkitem
                                                if 'summary' in entry:
                                                    iterator = regexpr.finditer(str(entry.summary).encode('utf-8'))
                                                    for match in iterator:
                                                        linkitem = DownloadLinkItem()
                                                        linkitem['url'] = match.group()
                                                        linkitem['source'] = self.start_urls[0]
                                                        if entry.get('published_parsed', None) is None:
                                                            linkitem['date_published'] = self.tz.localize(datetime.now())
                                                        else:
                                                            linkitem['date_published'] = self.tz.localize(datetime.fromtimestamp(mktime(entry.get('published_parsed'))))
                                                        linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                                        yield linkitem
                                                if 'content' in entry:
                                                    iterator = regexpr.finditer(str(entry.content).encode('utf-8'))
                                                    for match in iterator:
                                                        linkitem = DownloadLinkItem()
                                                        linkitem['url'] = match.group()
                                                        linkitem['source'] = self.start_urls[0]
                                                        if entry.get('published_parsed', None) is None:
                                                            linkitem['date_published'] = self.tz.localize(datetime.now())
                                                        else:
                                                            linkitem['date_published'] = self.tz.localize(datetime.fromtimestamp(mktime(entry.get('published_parsed'))))
                                                        linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                                        yield linkitem
                                                if 'links' in entry:
                                                    iterator = regexpr.finditer(str(entry.links).encode('utf-8'))
                                                    for match in iterator:
                                                        linkitem = DownloadLinkItem()
                                                        linkitem['url'] = match.group()
                                                        linkitem['source'] = self.start_urls[0]
                                                        if entry.get('published_parsed', None) is None:
                                                            linkitem['date_published'] = self.tz.localize(datetime.now())
                                                        else:
                                                            linkitem['date_published'] = self.tz.localize(datetime.fromtimestamp(mktime(entry.get('published_parsed'))))
                                                        linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                                        yield linkitem    
                                                    for link in entry.links:
                                                        request = Request(url=unicode(link.href), callback=self.parse_entry_html)
                                                        if entry.get('published_parsed', None) is None:
                                                            request.meta['date_published'] = self.tz.localize(datetime.now())
                                                        else:
                                                            request.meta['date_published'] = self.tz.localize(datetime.fromtimestamp(mktime(entry.get('published_parsed'))))
                                                        request.meta['entry_title'] = entry.get('title', "unnamed entry");
                                                        yield request
                                        else:
                                            log.msg(("Feed-Entry was already spidered at last run:" + entry.get('title', "unnamed entry")), level=log.INFO) 
                                    else:
                                        log.msg(("Feed-Entry is older than 2 month:" + entry.get('title', "unnamed entry")), level=log.INFO)
                                else:
                                    log.msg(("Feed has not been updated within 3 months:" + entry.get('title', "unnamed entry")) + ", DEACTIVATING FEED!", level=log.WARNING)
                                    self.collection.update({"feedurl" : self.source}, {"$set" : {"active" : False}})
                                    return
                        else:
                            log.msg("Feed has no entries, NOT crawling.", level=log.ERROR)
                else:
                    log.msg("Feed not active, NOT crawling.", level=log.WARNING) 
            else:
                log.msg("Feed not found, NOT crawling.", level=log.ERROR)
        else:
            log.msg("Feed not found, NOT crawling.", level=log.ERROR)  
        
    def parse_entry_html(self, response):        
        log.msg("Crawling Feed-Entry " + response.meta['entry_title'], level=log.INFO)  
        if response.status < 400:           
            self.regexeslinkengine = []
            self.regexeslinkengine.append(re.compile("http\:\/\/themusicfire\.com\/goto(\w|\/)*", re.IGNORECASE))
            self.regexeslinkengine.append(re.compile("http\:\/\/4djsonline\.com\/download(\w|\/)*", re.IGNORECASE))
            
            for regexpr in self.regexeslinkengine:
                iterator = regexpr.finditer(response.body)
                for match in iterator:
                    if "http://themusicfire.com/goto" in match.group():
                        request = Request(url=unicode(match.group().split(" ")[0].strip('"')), callback=self.parse_entry_html)
                        request.meta['date_published'] = response.meta['date_published']
                        request.meta['entry_title'] = response.meta['entry_title']
                        yield request
                    if "http://4djsonline.com/download" in match.group():
                        request = Request(url=unicode(match.group().split(" ")[0].strip('"')), callback=self.parse_entry_html)
                        request.meta['date_published'] = response.meta['date_published']
                        request.meta['entry_title'] = response.meta['entry_title']
                        yield request
                       
            for regexpr in self.regexes:
                iterator = regexpr.finditer(response.body)
                for match in iterator:
                    linkitem = DownloadLinkItem()
                    linkitem['url'] = match.group().split(" ")[0].strip('"')
                    linkitem['source'] = self.start_urls[0]
                    linkitem['date_published'] = response.meta['date_published']
                    linkitem['date_discovered'] = self.tz.localize(datetime.now())
                    yield linkitem
        else:
            log.msg("Feed-Entry not found.", level=log.ERROR)  
                
    def handle_spider_closed(self, spider, reason):
        if reason == "finished" and not self.site is None:
            discovered = self.db['links'].find({"source" : self.source}).count()
            
            if int(self._crawler.stats.get_value("log_count/ERROR", 0)) == 0:
                log.msg("Spider finished without errors, updating site record", level=log.INFO)
                if self.last_post and not self.last_post is None:
                    self.collection.update({"feedurl" : self.source},{"$set" : {"last_crawled" : datetime.now(), "next_crawl" : None, "discovered_links": discovered, "last_post" : self.last_post}})
            else:
                log.msg("Spider finished with errors, NOT updating site record", level=log.WARNING)
                if self.last_post and not self.last_post is None:
                    self.collection.update({"feedurl" : self.source}, {"$set" : {"next_crawl" : None, "discovered_links": discovered, "last_post" : self.last_post}})
        else:
            log.msg("Spider finished unexpectedly, NOT updating site record", level=log.WARNING)
