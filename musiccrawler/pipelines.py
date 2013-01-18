# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JError
from scrapy.exceptions import DropItem
from scrapy import signals
from musiccrawler.exporters import SOAPWSExporter
from musiccrawler.exporters import RESTWSExporter
from musiccrawler.exporters import MongoDBExporter
import re
import json
import traceback

class CheckMusicDownloadLinkPipeline(object):
    urlregex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    def __init__(self):
        gateway = JavaGateway(auto_convert=True)
        gateway.jvm.py4j.GatewayServer.turnLoggingOff()
        self.mdlb = gateway.entry_point.getMusicDownloadLinkBuilder()
    
    def process_item(self, item, spider):
        try:
            if re.match(self.urlregex,item['url']):
                self.mdlb.init([item['url']],item['source'])
                jsonitem = json.loads(self.mdlb.buildMusicDownloadLinks())[0]
                jsonitem['password'] = item.get('password')
                jsonitem['metainfo'] = item.get('metainfo')
                return jsonitem
            else:
                raise DropItem("Link-URL is invalid:", item['url'], ", Item will be dropped.")
        except Py4JError:
            traceback.print_exc()
            
class DuplicateURLsPipeline(object):
    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        if item['url'] in self.urls_seen:
            raise DropItem("Duplicate Link-URL found: %s" % item['url'])
        else:
            self.urls_seen.add(item['url'])
            return item
        
class SOAPWSExportPipeline(object):
    def __init__(self):
        self.exporter = SOAPWSExporter();
        
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
    
class RESTExportPipeline(object):
    def __init__(self):
        self.exporter = RESTWSExporter();

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item
    
class MongoDBExportPipeline(object):
    def __init__(self):
        self.exporter = MongoDBExporter();

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item       