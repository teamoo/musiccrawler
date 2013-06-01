'''
Created on 01.06.2013

@author: thimobrinkmann
'''
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
from scrapy.spider import BaseSpider
from scrapy.xlib.pydispatch import dispatcher
import json
import math
import time
import monthdelta
import musiccrawler.settings
import pkg_resources
import pymongo
import re
import vk_api

class VKontakteSpider(BaseSpider):        
    name = "vkontaktespider"
    
    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        dispatcher.connect(self.handle_spider_closed, signals.spider_closed)
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT, tz_aware=True)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        if musiccrawler.settings.__dict__.has_key('MONGODB_USER') and musiccrawler.settings.__dict__.has_key('MONGODB_PASSWORD'):
            self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db['sites']
        self.site = self.collection.find_one({"feedurl": kwargs.get('feedurl')})
        self.source = self.site
 
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
            
                
    def parse(self, response):
        if not self.site is None:
            if self.active == True:
                #vk = vkontakte.API('3685402','7RnJnG073kTdkK3Bf0F5')
                
                login = u'thimo.brinkmann@googlemail.com'
                password = u'geheimoder'

                try:
                    vk = vk_api.VkApi(login, password)
                except vk_api.authorization_error, error_msg:
                    print error_msg
                    return
            
                values = {
                    'source_ids': 'g28519525',
                    'filters' : 'post'
                }
                response = vk.method('newsfeed.get',values)
                for item in response['items']:
                    for attachment in item['attachments']:
                        if attachment['type'] == 'audio':
                            responseaudio = vk.method('audio.getById',{'audios': str(attachment['audio']['owner_id'])+'_'+str(attachment['audio']['aid'])})
                            print responseaudio[0]['artist'] + " - " + responseaudio[0]['title'] + " : " + responseaudio[0]['url']
                            time.sleep(2)
        else:
            log.msg("Feed not found, NOT crawling.", level=log.ERROR)
                    
    def parse_entry_html(self, response):
        for regexpr in self.regexes:
            iterator = regexpr.finditer(response.body)
            for match in iterator:
                linkitem = DownloadLinkItem()
                linkitem['url'] = match.group()
                linkitem['source'] = self.start_urls[0]
                linkitem['date_published'] = response.meta['date_published']
                linkitem['date_discovered'] = self.tz.localize(datetime.now())
                yield linkitem
                
    def handle_spider_closed(self, spider, reason):
        if reason == "finished" and not self.site is None:
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
