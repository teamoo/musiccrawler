# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JError
from scrapy.exceptions import DropItem
import json
import re
import traceback

class CheckMusicDownloadLinkPipeline(object):
    def __init__(self):
        gateway = JavaGateway(auto_convert=True)
        self.mdlb = gateway.entry_point.getMusicDownloadLinkBuilder()
        self.urlregex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
    
    def process_item(self, item, spider):
        try:
            if re.match(self.urlregex,item['url']):
                self.mdlb.init([item['url']],item['source'])
                jsonitem = json.loads(self.mdlb.buildMusicDownloadLinks())
                return jsonitem[0]
            else:
                raise DropItem("Link-URL is invalid:", item['url'], ", Item will be dropped.")
        except Py4JError:
            traceback.print_exc()
            
class DuplicateURLsPipeline(object):

    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        if item['url'] in self.ids_seen:
            raise DropItem("Duplicate Link-URL found: %s" % item['url'])
        else:
            self.urls_seen.add(item['urls'])
            return item

            