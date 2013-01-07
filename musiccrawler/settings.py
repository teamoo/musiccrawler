# Scrapy settings for musiccrawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#

BOT_NAME = 'musiccrawler'

SPIDER_MODULES = ['musiccrawler.spiders']
NEWSPIDER_MODULE = 'musiccrawler.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'musiccrawler (+http://www.yourdomain.com)'

MEMUSAGE_ENABLED = False
MEMUSAGE_NOTIFY_MAIL = ['thimo.brinkmann@googlemail.com']
MEMUSAGE_WARNING_MB = 500

ITEM_PIPELINES = [
    'musiccrawler.pipeline.CheckMusicDownloadLinkPipeline',
    'musiccrawler.pipeline.DuplicateURLsPipeline',
]
