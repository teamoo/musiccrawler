# This is a spider that can crawl RSS feeds in a version independent manner. it uses Mark pilgrim's excellent feedparser utility to parse RSS feeds. You can read about the nightmares of  RSS incompatibility [here](http://diveintomark.org/archives/2004/02/04/incompatible-rss) and  download feedparser that strives to resolve it from [here](http://feedparser.org/docs/)
# The scripts processes only certain elements in the feeds(title, link and summary)
# The items may be saved in the Item pipeline which I leave to you.
#
# Please let me know about any discrepencies you may find in the technical and functional aspects of this script.
#
# -Sid

from musiccrawler.items import DownloadLinkItem
from scrapy import log
from scrapy.spider import BaseSpider
import json
import math
import musiccrawler.settings
import re


class FacebookGroupSpider(BaseSpider):        
    name = "facebookgroupspider"
    
    accesstoken = "AAAHh61ahtmABAKV6uk1LfReD8fjC3gNVZC5FdQCQEjgaZAsDo82WOMRiKZAVO4fYyaCIroR1YzTAromD19aDKYPJ0x7uRn8nJN9JZBDxGMzAYF2Ewcp6"
    
    source = 'https://www.facebook.com/groups/137328326321645'
    
    start_urls = ["https://graph.facebook.com/137328326321645/feed?access_token=" + accesstoken]
    
    def __init__(self):
        
        log.msg("Initalizing Spider", level=log.INFO)
        hosts = json.load(open(musiccrawler.settings.HOSTS_FILE_PATH))
        decrypters = json.load(open(musiccrawler.settings.DECRYPTER_FILE_PATH))
        regex_group_count = 50
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
                hosterpattern = unicode(decrypter['pattern']).rstrip('\r\n').replace("/", "\/",99).replace(":", "\:",99).replace("\d+{", "\d{",10).replace("++", "+",10).replace("\r\n", "",10).replace("|[\p{L}\w-%]+\/[\p{L}\w-%]+", "",10).replace("decrypted","",10).replace("httpJDYoutube","http",10) + '|'
                hosterregex += hosterpattern.encode('utf-8')
            
            self.regexes.append(re.compile("'" + hosterregex[:-1] + "'", re.IGNORECASE))
        
        
        
        log.msg("Spider initalized.", level=log.INFO)
                
    def parse(self, response):
        groupfeed = json.loads(response.body)
        
        feed = groupfeed['data']
        
        for item in feed:
            if 'message' in item:
                for regexpr in self.regexes:
                    iterator = regexpr.finditer(str(item['message']))
                    for match in iterator:
                        linkitem = DownloadLinkItem()
                        linkitem['url'] = match.group().split('" ')[0]
                        print linkitem['url']
                        linkitem['source'] = str(self.source)
                        yield linkitem   
            if 'source' in item:
                linkitem = DownloadLinkItem()
                linkitem['url'] = match.group().split('" ')[0]
                print linkitem['url']
                linkitem['source'] = str(self.source)
                yield linkitem   
            if 'link' in item:
                linkitem = DownloadLinkItem()
                linkitem['url'] = match.group().split('" ')[0]
                print linkitem['url']
                linkitem['source'] = str(self.source)
                yield linkitem   

            if 'comments' in item:
                for comment in item['comments']:
                    if 'message' in comment:
                        for regexpr in self.regexes:
                            iterator = regexpr.finditer(str(item['message']))
                            for match in iterator:
                                linkitem = DownloadLinkItem()
                                linkitem['url'] = match.group().split('" ')[0]
                                print linkitem['url']
                                linkitem['source'] = str(self.source)
                                yield linkitem   