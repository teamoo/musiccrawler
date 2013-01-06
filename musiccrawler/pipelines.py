# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

from py4j.java_gateway import JavaGateway
import json

class MusiccrawlerPipeline(object):
    def __init__(self):
        gateway = JavaGateway(auto_convert=True)
        self.mdlb = gateway.entry_point.getMusicDownloadLinkBuilder()
        
    
    def process_item(self, item, spider):
        mdlb.init([item['url']],item['source'])
        jsonitem = json.loads(mdlb.buildMusicDownloadLinks())
        
        return jsonitem[0]
