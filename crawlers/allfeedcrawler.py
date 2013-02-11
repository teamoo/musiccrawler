import musiccrawler.settings
import json
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import log
from musiccrawler.spiders.feedspider import FeedSpider

def setup_crawler():
    feedurls = json.load(open(musiccrawler.settings.FEEDS_FILE_PATH))
    spider = FeedSpider()
    crawler = Crawler(Settings())
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()

    for feed in feedurls:
        setup_crawler(feed['feedurl'])
    log.start()
    reactor.run()