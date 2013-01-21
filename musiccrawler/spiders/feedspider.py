# This is a spider that can crawl RSS feeds in a version independent manner. it uses Mark pilgrim's excellent feedparser utility to parse RSS feeds. You can read about the nightmares of  RSS incompatibility [here](http://diveintomark.org/archives/2004/02/04/incompatible-rss) and  download feedparser that strives to resolve it from [here](http://feedparser.org/docs/)
# The scripts processes only certain elements in the feeds(title, link and summary)
# The items may be saved in the Item pipeline which I leave to you.
#
# Please let me know about any discrepencies you may find in the technical and functional aspects of this script.
#
# -Sid

from scrapy.spider import BaseSpider
from scrapy import log

import feedparser
import re
import json
import math
import musiccrawler.settings
import monthdelta
from datetime import datetime
from time import mktime
from musiccrawler.items import DownloadLinkItem

class FeedSpider(BaseSpider):        
    name = "feedspider"
    
    def __init__(self):
        
        log.msg("Initalizing Spider", level=log.INFO)
        hosts = json.load(open(musiccrawler.settings.HOSTS_FILE_PATH))
        feeds = json.load(open(musiccrawler.settings.FEEDS_FILE_PATH))
        regex_group_count = 50
        self.regexes = []
        
        feedurls = []
        
        for feed in feeds:
            feedurls.append(feed['feedurl'])
           
        self.start_urls = [feedurls[3]]
            
        for i in range(int(math.ceil(len(hosts) / regex_group_count))):
            
            hosterregex = ''
    
            for hoster in hosts[(i + 1) * regex_group_count - regex_group_count:(i + 1) * regex_group_count]:
                hosterpattern = unicode(hoster['pattern']).rstrip('\r\n').replace("/", "\/").replace(":", "\:").replace("\d+{", "\d{").replace("++", "+").replace("\r\n", "").replace("|[\p{L}\w-%]+\/[\p{L}\w-%]+", "") + '|'
                hosterregex += hosterpattern.encode('utf-8')
            
            self.regexes.append(re.compile("'" + hosterregex[:-1] + "'", re.IGNORECASE))
        
        log.msg("Spider initalized.", level=log.INFO)
                
    def parse(self, response):
            rssFeed = feedparser.parse(response.url)
            
            if rssFeed.bozo == 1:
                log.msg(("Feed kann nicht verarbeitet werden:" + str(response.url)), level=log.INFO)
            else:
                for entry in rssFeed.entries:
                    if (datetime.now() - monthdelta.MonthDelta(1)) < datetime.fromtimestamp(mktime(rssFeed.get('updated_parsed', ''))):
                        log.msg(("Verarbeite Feed:" + rssFeed.get('title', response.url)), level=log.INFO)
                        if (datetime.now() - monthdelta.MonthDelta(1)) < datetime.fromtimestamp(mktime(entry.get('published_parsed', ''))):
                            log.msg(("Verarbeite Eintrag:" + entry.get('title', "unnamed entry")), level=log.DEBUG)
                            for regexpr in self.regexes:
                                if 'summary' in entry:
                                    iterator = regexpr.finditer(str(entry.summary))
                                    for match in iterator:
                                        linkitem = DownloadLinkItem()
                                        linkitem['url'] = match.group().split('" ')[0]
                                        linkitem['source'] = str(rssFeed.get('link', 'unknown source'))
                                        yield linkitem
                                if 'content' in entry:
                                    iterator = regexpr.finditer(str(entry.content))
                                    for match in iterator:
                                        linkitem = DownloadLinkItem()
                                        linkitem['url'] = match.group().split('" ')[0]
                                        linkitem['source'] = str(rssFeed.get('link', 'unknown source'))
                                        yield linkitem
                                if 'links' in entry:
                                    iterator = regexpr.finditer(str(entry.links))
                                    for match in iterator:
                                        linkitem = DownloadLinkItem()
                                        linkitem['url'] = match.group().split('" ')[0]
                                        linkitem['source'] = str(rssFeed.get('link', 'unknown source'))
                                        yield linkitem
                        else:
                            log.msg(("Feed-Entry is older than 1 month:" + entry.get('title', "unnamed entry")), level=log.DEBUG)  
                    else:
                        log.msg(("Feed has not been updated within 6 months:" + entry.get('title', "unnamed entry")), level=log.INFO)
