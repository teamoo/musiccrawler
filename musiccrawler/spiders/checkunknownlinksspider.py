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

from datetime import datetime, timedelta
from musiccrawler.items import DownloadLinkItem
from pytz import timezone
from scrapy import log
from scrapy.http import Request
from scrapy.spider import BaseSpider
from time import mktime
import feedparser
import monthdelta
import musiccrawler.settings
import pkg_resources
import pymongo

class CheckUnknownLinksSpider(BaseSpider):        
    name = "checkunknownlinksspider"
    
    def __init__(self):
        self.start_urls = ['http://www.google.de']
        log.msg("Initializing Spider", level=log.INFO)
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT, tz_aware=True)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        if musiccrawler.settings.__dict__.has_key('MONGODB_USER') and musiccrawler.settings.__dict__.has_key('MONGODB_PASSWORD'):
            self.db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db['links']
        self.unknownlinks = self.collection.find({"status": 'unknown'})
        log.msg("Received " + str(self.unknownlinks.count) +  " unknown links from Database", level=log.INFO)
        self.oldofflinelinkes = self.collection.find({'status': 'off', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}}).count()
        log.msg("Removing " + str(self.oldofflinelinkes.count) +  " from Database that are OFFLINE and older than 90 days.", level=log.INFO)
        self.collection.remove({'status': 'off', 'date_published': {'$lte': (datetime.now()-timedelta(days=90))}},False)
        
        self.oldlinks = self.collection.find({'date_published': {'$lte': (datetime.now()-timedelta(days=365))}}).count()
        log.msg("Removing " + str(self.oldofflinelinkes.count) +  " from Database that are older than one year.", level=log.INFO)
        self.collection.remove({'date_published': {'$lte': (datetime.now()-timedelta(days=365))}},False)   
                  
    def parse(self, response):
        for unknownlink in self.unknownlinks:
            unknownitem = DownloadLinkItem()
            unknownitem['url'] = unknownlink.get('url', None)
            unknownitem['source'] = unknownlink.get('source', None)
            if unknownlink.get('date_published', None) is None:
                    unknownitem['date_published'] = self.tz.localize(datetime.now())
            else:
                unknownitem['date_discovered'] = unknownlink.get('date_discovered', None)
            
            if unknownlink.get('date_discovered', None) is None:
                    unknownitem['date_discovered'] = self.tz.localize(datetime.now())
            else:
                unknownitem['date_discovered'] = unknownlink.get('date_discovered', None) 
                    
            yield unknownitem