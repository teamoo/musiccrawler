from datetime import datetime, timedelta
from musiccrawler.items import DownloadLinkItem
from pytz import timezone
from scrapy import log
from scrapy.spider import Spider
import musiccrawler.settings
import pymongo
import re

class CleanUpSpider(Spider):        
    name = "cleanupspider"
    
    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        self.start_urls = ['http://soundcloud.com']
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT, tz_aware=True)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        if musiccrawler.settings.__dict__.has_key('MONGODB_USER') and musiccrawler.settings.__dict__.has_key('MONGODB_PASSWORD') and musiccrawler.settings.MONGODB_USER is not None:
            self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.links = self.db['links']
        log.msg("Removing " + str(self.links.find({'status': 'off', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}}).count()) +  " links from Database that are OFFLINE OR UNKNOWN and older than 90 days.", level=log.INFO)
        self.links.remove({'status': 'off', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}},False)
        self.links.remove({'status': 'unknown', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}},False)
        #self.unknownlinks = list(self.links.find({'$query':{"status": 'unknown',"url": { "$not": re.compile(".*dwmp3\.com.*") } },'$orderby': {"url": 1}}))
        self.unknownlinks = list(self.links.find({'$query':{"status": 'on',"url": { "$not": re.compile(".*dwmp3\.com.*") } },'$orderby': {"date_discovered": -1}}))
        log.msg("Received " + str(len(self.unknownlinks)) +  " UNKNOWN links from Database", level=log.INFO)
                  
    def parse(self, request):   
        for unknownlink in self.unknownlinks:
            log.msg("Checking unknown link " + str(self.unknownlinks.index(unknownlink)+1)  + " of " + str(len(self.unknownlinks)), level=log.INFO)
            unknownitem = DownloadLinkItem()
            unknownitem['url'] = unknownlink.get('url', None)
            unknownitem['source'] = unknownlink.get('source', None)
        
            if unknownlink.get('date_discovered', None) is None:
                    unknownitem['date_discovered'] = timezone("Europe/Berlin").localize(datetime.now())
            else:
                unknownitem['date_discovered'] = unknownlink.get('date_discovered', None)
                
            if unknownlink.get('date_published', None) is None:
                    unknownitem['date_published'] = unknownitem['date_discovered']
            else:
                unknownitem['date_published'] = unknownlink.get('date_published', None)    
                             
            yield unknownitem