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
    youtube_name = Field()
    youtube_likes = Field()
    youtube_comments = Field()
    youtube_views = Field()
    youtube_dislikes = Field()
    youtube_favorites = Field()
    youtube_date_published = Field()
    hypem_likes = Field()
    hypem_posts = Field()
    hypem_date_published = Field()
    hypem_name = Field()
    hypem_artwork_url = Field()
    soundcloud_likes = Field()
    soundcloud_comments = Field()
    soundcloud_downloads = Field()
    soundcloud_playbacks = Field()
    soundcloud_genre = Field()
    soundcloud_name = Field()
    soundcloud_date_created = Field()
    soundcloud_artwork_url = Field()
    facebook_shares = Field()
    name_routing = Field()
facebook_shares = Field()