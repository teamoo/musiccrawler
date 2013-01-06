# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class DownloadLinkItem(Item):
    # define the fields for your item here like:
    # name = Field()
    id=Field()
    url=Field()
    name=Field()
    size=Field()
    status=Field()
    source=Field()
    date=Field()
    hoster=Field()
    password=Field()
    metainfo=Field()

class RssFeedItem(Item):
	title = Field()# the Title of the feed
	link = Field()# the URL to the web site(not the feed)
	summary = Field();# short description of feed
	entries = Field();# will contain the RSSEntrItems

class RssEntryItem(RssFeedItem):
	published = Field()