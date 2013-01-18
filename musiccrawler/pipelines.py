# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JError
from py4j.java_gateway import logging
from scrapy.exceptions import DropItem
from scrapy import signals
from datetime import datetime
from musiccrawler.exporters import SOAPWSExporter
from musiccrawler.exporters import RESTWSExporter
from musiccrawler.exporters import MongoDBExporter
from scrapy import log
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
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler())
        self.mdlb = gateway.entry_point.getMusicDownloadLinkBuilder()
    
    def process_item(self, item, spider):
        try:
            if re.match(self.urlregex,item['url']):
                self.mdlb.init(item['source'])
                log.msg(("Sending URL to Linkbuilder" + item['url']),level=log.DEBUG)
                jsonresult = self.mdlb.buildMusicDownloadLink(item['url'])
                log.msg(("Linkbuilder returned jsonstring:" + jsonresult),level=log.DEBUG)
                if not jsonresult is None:
                    jsonitem = json.loads(jsonresult)
                    jsonitem['password'] = item.get('password',"")
                    jsonitem['metainfo'] = item.get('metainfo',"")
                    jsonitem['date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if jsonitem is None:
                    raise DropItem("No item returned. Linkbuilder may be corrupt.")
                else:
                    log.msg(("Linkbuilder returned item:" + str(jsonitem)),level=log.DEBUG)
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
            log.msg(("Duplicate Item:" + str(item)),level=log.DEBUG)
            raise DropItem("Duplicate Link-URL found: %s" % item['url'])
        else:
            log.msg(("new Item:" + str(item)),level=log.DEBUG)
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