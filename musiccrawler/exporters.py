from scrapy.contrib.exporter import BaseItemExporter
from SOAPpy import WSDL
from siesta import API
import pymongo

import musiccrawler.settings

class SOAPWSExporter(BaseItemExporter):
    export_empty_fields = False
    
    def __init__(self):
        self.server = WSDL.Proxy(musiccrawler.settings.WSDL_FILE)
        self.server.soapproxy.config.dumpSOAPOut = 1     
        self.server.soapproxy.config.dumpSOAPIn = 1

    def export_item(self, item):
        self.server.addLink(item['url'],item['name'],item['size'],item['status'],item['source'],item['hoster'],item['password'],item['metainfo'])
        
    def start_exporting(self):
        BaseItemExporter.start_exporting(self)
        
    def finish_exporting(self):
        BaseItemExporter.finish_exporting(self)
            
class MongoDBExporter(BaseItemExporter):
    export_empty_fields = True

    def __init__(self):
        connection = pymongo.Connection(musiccrawler.settings.MONGODB_SERVER, musiccrawler.settings.MONGODB_PORT)
        self.db = connection[musiccrawler.settings.MONGODB_DB]
        self.collection = self.db[musiccrawler.settings.MONGODB_COLLECTION]
        if self.__get_uniq_key() is not None:
            self.collection.create_index(self.__get_uniq_key(), unique=True)

    def export_item(self, item):
        if self.__get_uniq_key() is None:
            self.collection.insert(dict(item))
        else:
            self.collection.update(
                            {self.__get_uniq_key(): item[self.__get_uniq_key()]},
                            dict(item),
                            upsert=True)  
        return item
        
    def start_exporting(self):
        BaseItemExporter.start_exporting(self)
        
    def finish_exporting(self):
        BaseItemExporter.finish_exporting(self)

    def __get_uniq_key(self):
        if not musiccrawler.settings.MONGODB_UNIQ_KEY or musiccrawler.settings.MONGODB_UNIQ_KEY == "":
            return None
        return musiccrawler.settings.MONGODB_UNIQ_KEY

class RESTWSExporter(BaseItemExporter):
    export_empty_fields = True
            
    def __init__(self):
        self.api = API(musiccrawler.settings.REST_API_URL)

    def export_item(self, item):
        self.api.link.post(item)
        
    def start_exporting(self):
        BaseItemExporter.start_exporting(self)
        
    def finish_exporting(self):
        BaseItemExporter.finish_exporting(self)