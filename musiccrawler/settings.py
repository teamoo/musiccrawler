# Scrapy settings for musiccrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'musiccrawler'

SPIDER_MODULES = ['musiccrawler.spiders.xmlfeedspider']
NEWSPIDER_MODULE = 'musiccrawler.spiders.xmlfeedspider'

HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 43200

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'musiccrawler (+http://www.yourdomain.com)'

LOG_LEVEL = 'DEBUG'

MEMUSAGE_ENABLED = False
MEMUSAGE_NOTIFY_MAIL = ['thimo.brinkmann@googlemail.com']
MEMUSAGE_WARNING_MB = 800

ITEM_PIPELINES = [
    'musiccrawler.pipelines.DuplicateURLsPipeline',
    'musiccrawler.pipelines.CheckMusicDownloadLinkPipeline',
    #'musiccrawler.pipelines.SOAPWSExportPipeline',
    #'musiccrawler.pipelines.RESTWSExportPipeline',
    'musiccrawler.pipelines.MongoDBExportPipeline'
]

FEED_EXPORTERS = [
    'musiccrawler.exporters.SOAPWSExporter',
    'musiccrawler.exporters.RESTWSExporter',
    'musiccrawler.exporters.MongoDBExporter'
]

SPIDER_CONTRACTS = [

]

WSDL_FILE = 'http://musiclink.webcomsult.de/v2/index.php?wsdl'
REST_API_URL = 'http://musiclink.webcomsult.de/v2/rest.php'

# MONGODB_SERVER = 'localhost'
# MONGODB_PORT = 27017
MONGODB_SERVER = 'linus.mongohq.com'
MONGODB_PORT = 10050
MONGODB_DB = 'musiccrawler'
MONGODB_COLLECTION = 'links'
MONGODB_UNIQ_KEY = 'url'
MONGODB_USER = 'dbuser'
MONGODB_PASSWORD = 'geheim'

HOSTS_FILE_PATH = 'hosts.json'
FEEDS_FILE_PATH = 'feeds.json'
