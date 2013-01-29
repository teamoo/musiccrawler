# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class DownloadLinkItem(Item):
    # define the fields for your item here like:
    # name = Field()
    id = Field()
    url = Field()
    name = Field()
    size = Field()
    status = Field()
    source = Field()
    date_discovered = Field()
    date_published = Field()
    creator = Field()
    hoster = Field()
    password = Field()
    metainfo = Field()
