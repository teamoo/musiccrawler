from datetime import datetime, timedelta
from musiccrawler.items import DownloadLinkItem
from pytz import timezone
from scrapy import log
from scrapy.spider import BaseSpider
import musiccrawler.settings
import pymongo

class CleanUpSpider(BaseSpider):        
    name = "cleanupspider"
    
    def __init__(self, **kwargs):
        log.msg("Initializing Spider", level=log.INFO)
        self.start_urls = ['http://soundcloud.com']
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT, tz_aware=True)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        if musiccrawler.settings.__dict__.has_key('MONGODB_USER') and musiccrawler.settings.__dict__.has_key('MONGODB_PASSWORD'):
            self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.links = self.db['links']
        log.msg("Removing " + str(self.links.find({'status': 'off', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}}).count()) +  " links from Database that are OFFLINE OR UNKNOWN and older than 90 days.", level=log.INFO)
        self.links.remove({'status': 'off', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}},False)
        self.links.remove({'status': 'unknown', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}},False)
        log.msg("Removing " + str(self.links.find({'date_published': {'$lte': (datetime.now()-timedelta(days=365))}}).count()) +  " links from Database that are older than one year.", level=log.INFO)
        self.links.remove({'date_published': {'$lte': (datetime.now()-timedelta(days=365))}},False)
        self.unknownlinks = list(self.links.find({"status": 'unknown'}))
        log.msg("Received " + str(len(self.unknownlinks)) +  " UNKNOWN links from Database", level=log.INFO)
                  
    def parse(self, request):   
        for unknownlink in self.unknownlinks:
            log.msg("Checking unknown link " + str(self.unknownlinks.index(unknownlink)+1)  + " of " + str(len(self.unknownlinks)), level=log.INFO)
            unknownitem = DownloadLinkItem()
            unknownitem['url'] = unknownlink.get('url', None)
            unknownitem['source'] = unknownlink.get('source', None)
            if unknownlink.get('date_published', None) is None:
                    unknownitem['date_published'] = timezone("Europe/Berlin").localize(datetime.now())
            else:
                unknownitem['date_discovered'] = unknownlink.get('date_discovered', None)            
            if unknownlink.get('date_discovered', None) is None:
                    unknownitem['date_discovered'] = timezone("Europe/Berlin").localize(datetime.now())
            else:
                unknownitem['date_discovered'] = unknownlink.get('date_discovered', None)
                             
            yield unknownitem