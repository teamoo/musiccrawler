# This is a spider that can crawl RSS feeds in a version independent manner. it uses Mark pilgrim's excellent feedparser utility to parse RSS feeds. You can read about the nightmares of  RSS incompatibility [here](http://diveintomark.org/archives/2004/02/04/incompatible-rss) and  download feedparser that strives to resolve it from [here](http://feedparser.org/docs/)
# The scripts processes only certain elements in the feeds(title, link and summary)
# The items may be saved in the Item pipeline which I leave to you.
#
# Please let me know about any discrepencies you may find in the technical and functional aspects of this script.
#
# -Sid

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import XmlXPathSelector
from scrapy import log
import json
import re
import math
import musiccrawler.settings
from musiccrawler.linkextractors import LxmlParserTreeLinkExtractor

class MyCustomXMLFeedSpider(CrawlSpider):
    name = "xmlfeedspider"

    #allowed_domains = ['electropeople.org']
    start_urls = ["http://www.bestclubsound.com/feed"]

    hosts = json.load(open(musiccrawler.settings.HOSTS_FILE_PATH))
    regex_group_count = 50
    regexes = []
        
    for i in range(int(math.ceil(len(hosts) / regex_group_count))):
            
        hosterregex = ''
    
        for hoster in hosts[(i + 1) * regex_group_count - regex_group_count:(i + 1) * regex_group_count]:
            hosterpattern = unicode(hoster['pattern']).rstrip('\r\n').replace("/", "\/").replace(":", "\:").replace("\d+{", "\d{").replace("++", "+").replace("\r\n", "").replace("|[\p{L}\w-%]+\/[\p{L}\w-%]+", "") + '|'
            hosterregex += hosterpattern.encode('utf-8')
            
        regexes.append(re.compile("'" + hosterregex[:-1] + "'", re.IGNORECASE))


    rules = (
        # Extract links matching 'category.php' (but not matching 'subsection.php')
        # and follow links from them (since no callback means follow=True by default).
            Rule(
                    SgmlLinkExtractor(allow=regexes),callback='parse_item'
                 ), 
            Rule(
                    LxmlParserTreeLinkExtractor(tag="link"),follow=True
                 ),
            Rule(
                    SgmlLinkExtractor(allow=(),allow_domains=("bestclubsound.com")),follow=True
                 ),

             )
    
    def parse_item(self, response):
        print "JO"
        self.log('Hi, this is an item page! %s' % response)


#class XMLFeedSpider(CrawlSpider):        
#    name = "xmlfeedspider"
#
#    allowed_domains = ['electropeople.org']
#    start_urls = ["http://electropeople.org/rss.xml"]
#    
#
#        
#    log.msg("Initalizing Spider", level=log.INFO)
#    hosts = json.load(open(musiccrawler.settings.HOSTS_FILE_PATH))
#    feeds = json.load(open(musiccrawler.settings.FEEDS_FILE_PATH))
#    regexestrings = []
#        
#    #feedurls = []
#        
#    #for feed in feeds:
#    #    feedurls.append(feed['feedurl'])
#           
#    #self.start_urls = feedurls[0]    
#            
#    for hoster in hosts:    
#        hosterregex =''
#        hosterpattern = unicode(hoster['pattern']).rstrip('\r\n').replace("/","\/").replace(":","\:").replace("\d+{","\d{").replace("++","+").replace("\r\n","").replace("|[\p{L}\w-%]+\/[\p{L}\w-%]+","")
#        hosterregex = hosterpattern.encode('utf-8')
#        regexestrings.append(hosterregex)
#
#    itertag = 'item'
#
#    def ____parse_node(self, response, node):
#        log.msg('Hi, this is a <%s> node!: %s' % (self.itertag, ''.join(node.extract())))
#        print "DESC:",node.select("description/text()").extract()
#        print "LINK:",node.select("link/text()").extract()