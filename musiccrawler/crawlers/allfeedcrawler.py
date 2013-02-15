import musiccrawler.settings
import pymongo
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import log
from scrapy import signals
from musiccrawler.spiders.feedspider import FeedSpider
from musiccrawler.spiders.facebookgroupspider import FacebookGroupSpider

def setup_facebook_crawler():
    spider = FacebookGroupSpider()
    crawler = Crawler(Settings())
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()

def setup_crawler(feedurl):
    spider = FeedSpider(feedurl)
    crawler = Crawler(Settings())
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()

log.msg("Initializing Crawler", level=log.INFO)
connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT)
db = connection[musiccrawler.settings.MONGODB_DB]
db.authenticate(musiccrawler.settings.MONGODB_USER, musiccrawler.settings.MONGODB_PASSWORD)
collection = db['sites']
feeds = collection.findOne({})
log.msg("Received" + feeds.count +  " Sources from Database:", level=log.INFO)

for feed in feeds:
    if feed['type'] == "feed":
        print "Setting up FeedSpider for URL", feed['feedurl']
        setup_crawler(feed['feedurl'])
    elif feed['type'] == 'facebook-group':
        print "Setting up FacebookGroupSpider for URL", feed['feedurl']
        setup_facebook_crawler()
log.start()
reactor.run()