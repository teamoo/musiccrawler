from datetime import datetime
from dateutil.parser import parse
from musiccrawler.items import DownloadLinkItem
from pytz import timezone
from scrapy import log, signals
from scrapy.spider import BaseSpider
from scrapy.xlib.pydispatch import dispatcher
import json
import math
import monthdelta
import musiccrawler.settings
import pkg_resources
import pymongo
import re

class FacebookGroupSpider(BaseSpider):        
    name = "facebookgroupspider"

    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT, tz_aware=True)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        if musiccrawler.settings.__dict__.has_key('MONGODB_USER') and musiccrawler.settings.__dict__.has_key('MONGODB_PASSWORD'):
            self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db['sites']
        self.site = self.collection.find_one({"feedurl": kwargs.get('feedurl')})
        self.source = self.site['feedurl']
        self.active = self.site['active']
        self.tz = timezone("Europe/Berlin")
        
        self.groupid = str(self.site.get('groupid', ""))
        self.accesstoken = self.site.get('accesstoken', "")
        
        if self.site['last_crawled'] is None:
            self.last_crawled = self.tz.localize(datetime.now() - monthdelta.MonthDelta(12))
        else:
            self.last_crawled = self.site['last_crawled']

        log.msg("Received Site from Database:" + str(self.site), level=log.INFO)

        if self.active == False:
            log.msg("Site is deactivated, not crawling.", level=log.ERROR);
        elif self.accesstoken is None:
            self.collection.update({"feedurl" : self.source}, {"$set" : {"active" : False}})
            log.msg("Access Token is not set, spider cannot crawl.", level=log.ERROR);
        else:
            self.start_urls = ["https://graph.facebook.com/" + self.groupid + "/feed?access_token=" + self.accesstoken]                
            
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
                
    def parse(self, response):
        if self.active == True:
            if self.accesstoken is not None:
                groupfeed = json.loads(response.body)
                
                feed = groupfeed['data']
                self.last_post = parse(feed[0]['created_time'])
                                
                for item in feed:
                    if parse(item['created_time']) > self.last_post:
                        self.last_post = parse(item['created_time'])
                    
                    if self.last_crawled < parse(item['created_time']):
                        if 'message' in item:
                            for regexpr in self.regexes:
                                iterator = regexpr.finditer(str(item['message']))
                                for match in iterator:
                                    linkitem = DownloadLinkItem()
                                    linkitem['url'] = match.group()
                                    linkitem['source'] = str(self.source)
                                    linkitem['creator'] = item['from']['id']
                                    linkitem['date_published'] = parse(item['created_time'])
                                    linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                    yield linkitem   
                        if 'source' in item:
                            for regexpr in self.regexes:
                                iterator = regexpr.finditer(str(item['message']))
                                for match in iterator:
                                    linkitem = DownloadLinkItem()
                                    linkitem['url'] = match.group()
                                    linkitem['source'] = str(self.source)
                                    linkitem['creator'] = item['from']['id']
                                    linkitem['date_published'] = parse(item['created_time'])
                                    linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                    yield linkitem   
                        if 'link' in item:
                            for regexpr in self.regexes:
                                iterator = regexpr.finditer(str(item['message']))
                                for match in iterator:
                                    linkitem = DownloadLinkItem()
                                    linkitem['url'] = match.group()
                                    linkitem['source'] = str(self.source)
                                    linkitem['creator'] = item['from']['id']
                                    linkitem['date_published'] = parse(item['created_time'])
                                    linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                    yield linkitem   
                    if 'comments' in item:
                        for comment in item['comments']:
                            if 'message' in comment:
                                for regexpr in self.regexes:
                                    iterator = regexpr.finditer(str(item['message']))
                                    for match in iterator:
                                        linkitem = DownloadLinkItem()
                                        linkitem['url'] = match.group()
                                        linkitem['source'] = str(self.source)
                                        linkitem['creator'] = comment['from']['id']
                                        linkitem['date_published'] = parse(comment['created_time'])
                                        linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                        yield linkitem
            else:
                log.msg("Facebook-Gruppe kann nicht verarbeitet werden:" + str(response.url) + ", Access-Token nicht vorhanden.")
        else:
            log.msg("Facebook-Gruppe kann nicht verarbeitet werden:" + str(response.url) + ", Gruppe ist inaktiv.")
    
    def handle_spider_closed(self, spider, reason):
        if reason == "finished":
            discovered = int(self._crawler.stats.get_value("item_scraped_count", 0)) + self.db['links'].find({"source" : self.source}).count()
            
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

