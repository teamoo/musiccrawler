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
    aid = Field()
    oid = Field()
    facebook = Field()
    youtube = Field()
    hypem = Field()
    soundcloud = Field()
    vk = Field()
    name_routing = Field()
    
class FacebookInformationItem(Item):
    shares = Field()
    
class SoundcloudInformationItem(Item):
    name = Field()
    date_created = Field()
    genre = Field()
    artwork_url = Field()
    comments = Field()
    downloads = Field()
    likes = Field()
    playbacks = Field()
    
class YoutubeInformationItem(Item):
    name = Field()
    date_published = Field()
    likes = Field()
    dislikes = Field()
    comments = Field()
    views = Field()
    favorites = Field()
    
class HypemInformationItem(Item):
    name = Field()
    date_published = Field()
    artwork_url = Field()
    posts = Field()
    likes = Field()
    
class VKInformationItem(Item):
    artist = Field()
    title = Field()
    duration = Field()
    genre = Field()
    audio_id = Field()
    owner_id = Field()