from musiccrawler.items import DownloadLinkItem
from scrapy import log
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.spider import BaseSpider
from datetime import datetime
from dateutil.parser import parse
import pymongo
import json
import math
import musiccrawler.settings
import re


class FacebookGroupSpider(BaseSpider):        
    name = "facebookgroupspider"

    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db['sites']
        self.site = self.collection.find_one({"feedurl": kwargs.get('feedurl')})
        log.msg("Received Site from Database:" + str(self.site), level=log.INFO)
        self.source = self.site['feedurl']
        
        accesstoken = self.site['accesstoken']
        #TODO das muss noch bereinigt werden, die Gruppennummer aus dem Link holen
        self.start_urls = ["https://graph.facebook.com/137328326321645/feed?access_token=" + accesstoken]
        
        hosts = json.load(open(musiccrawler.settings.HOSTS_FILE_PATH))
        decrypters = json.load(open(musiccrawler.settings.DECRYPTER_FILE_PATH))
        regex_group_count = 40
        self.regexes = []

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
        
        log.msg("Spider initialized.", level=log.INFO)
                
    def parse(self, response):
        groupfeed = json.loads(response.body)
        
        feed = groupfeed['data']
        
        for item in feed:
            if 'message' in item:
                for regexpr in self.regexes:
                    iterator = regexpr.finditer(str(item['message']))
                    for match in iterator:
                        linkitem = DownloadLinkItem()
                        linkitem['url'] = match.group().split('" ')[0].split('\n')[0]
                        linkitem['source'] = str(self.source)
                        linkitem['creator'] = item['from']['id']
                        linkitem['date_published'] = parse(item['created_time'])
                        linkitem['date_discovered'] = datetime.now()
                        yield linkitem   
            if 'source' in item:
                for regexpr in self.regexes:
                    iterator = regexpr.finditer(str(item['message']))
                    for match in iterator:
                        linkitem = DownloadLinkItem()
                        linkitem['url'] = match.group().split('" ')[0].split('\n')[0]
                        linkitem['source'] = str(self.source)
                        linkitem['creator'] = item['from']['id']
                        linkitem['date_published'] = parse(item['created_time'])
                        linkitem['date_discovered'] = datetime.now()
                        yield linkitem   
            if 'link' in item:
                for regexpr in self.regexes:
                    iterator = regexpr.finditer(str(item['message']))
                    for match in iterator:
                        linkitem = DownloadLinkItem()
                        linkitem['url'] = match.group().split('" ')[0].split('\n')[0]
                        linkitem['source'] = str(self.source)
                        linkitem['creator'] = item['from']['id']
                        linkitem['date_published'] = parse(item['created_time'])
                        linkitem['date_discovered'] = datetime.now()
                        yield linkitem   
            if 'comments' in item:
                for comment in item['comments']:
                    if 'message' in comment:
                        for regexpr in self.regexes:
                            iterator = regexpr.finditer(str(item['message']))
                            for match in iterator:
                                linkitem = DownloadLinkItem()
                                linkitem['url'] = match.group().split('" ')[0].split('\n')[0]
                                linkitem['source'] = str(self.source)
                                linkitem['creator'] = comment['from']['id']
                                linkitem['date_published'] = parse(comment['created_time'])
                                linkitem['date_discovered'] = datetime.now()
                                yield linkitem
                                
    def handle_spider_closed(self, spider, reason):
        if reason == "finished":
            print "Spider finished, updating site record"
            self.collection.update({"feedurl" : self.source},{"$set" : {"last_crawled" : datetime.now(), "next_crawl" : None}})