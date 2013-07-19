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
from datetime import datetime
from musiccrawler.items import DownloadLinkItem
from pytz import timezone
from scrapy import log, signals
from scrapy.spider import BaseSpider
from scrapy.xlib.pydispatch import dispatcher
import monthdelta
import musiccrawler.settings
import pymongo
import sys
import time
import vk_api

reload(sys)
sys.setdefaultencoding("utf-8")


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
        self.source = self.site['feedurl']
 
        self.active = self.site['active']
        self.tz = timezone("Europe/Berlin")
        self.start_urls = [self.site['feedurl']]
        self.groupid = self.site.get('groupid', None)
                       
        if "last_post" in self.site and not self.site['last_post'] is None:
            self.last_post = self.site['last_post']
        else:
            self.last_post = None
        
        if self.site['last_crawled'] is None:
            self.last_crawled = self.tz.localize(datetime.now() - monthdelta.MonthDelta(12))
        else:
            self.last_crawled = self.site['last_crawled']
            
        log.msg("Received Site from Database:" + str(self.site), level=log.INFO)
        log.msg("Spider initialized.", level=log.INFO)
                 
    def parse(self, response):
        if not self.site is None:
            if self.active == True:
                try:
                    vk = vk_api.VkApi(musiccrawler.settings.VKONTAKTE_USER, musiccrawler.settings.VKONTAKTE_PASSWORD)
                except vk_api.authorization_error, error_msg:
                    log.msg("Error authorizing to vk.com: " + error_msg,level=log.ERROR)
                    return
                
                if self.groupid is None:
                    responsegroup = vk.method('groups.search',{'q':self.source.split('http://vk.com/')[1]})
                
                    for groupinfo in responsegroup:
                        if isinstance(groupinfo, (dict)):
                            if groupinfo['screen_name'] == self.source.split('http://vk.com/')[1]:
                                log.msg("VKontakte Group ID is " + str(groupinfo['gid']) + " received for VKontakte Site " + self.source,level=log.INFO)
                                self.groupid = groupinfo['gid']
                                self.collection.update({"feedurl" : self.source},{"$set" : {"groupid" : groupinfo['gid']}})
                
                if self.groupid is not None:
                    values = {
                        'source_ids': 'g'+str(self.groupid),
                        'filters' : 'post',
                        'start_time' : time.mktime(self.last_crawled.timetuple()) 
                    }
                    response = vk.method('newsfeed.get',values)
                    
                    if len(response['items']) >= 1:
                        self.last_post = datetime.fromtimestamp(response['items'][0]['date'])
                    
                    for item in response['items']:
                        if datetime.fromtimestamp(item['date']) > self.last_post:
                            self.last_post = datetime.fromtimestamp(item['date'])
                        for attachment in item['attachments']:
                            if attachment['type'] == 'audio':
                                responseaudio = vk.method('audio.getById',{'audios': str(attachment['audio']['owner_id'])+'_'+str(attachment['audio']['aid'])})
                                time.sleep(2)
                                linkitem = DownloadLinkItem()
                                linkitem['url'] = responseaudio[0]['url']
                                linkitem['source'] = self.start_urls[0]
                                linkitem['date_published'] = self.tz.localize(datetime.fromtimestamp(item['date']))
                                linkitem['date_discovered'] = self.tz.localize(datetime.now())
                                linkitem['name'] = responseaudio[0]['artist'] + " - " + responseaudio[0]['title']
                                linkitem['metainfo'] = 'duration='+str(responseaudio[0]['duration'])
                                linkitem['aid'] = str(attachment['audio']['aid'])
                                linkitem['oid'] = str(attachment['audio']['owner_id'])
                                yield linkitem
                else:
                    log.msg("VKontakte Group ID could not be received for VKontakte-URL " + self.site.feedurl,level=log.ERROR)
            else:
                log.msg("Site not active, NOT crawling.", level=log.WARNING)                    
        else:
            log.msg("Site not found, NOT crawling.", level=log.ERROR)
                          
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
