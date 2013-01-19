from scrapy.contrib.exporter import BaseItemExporter
from SOAPpy import WSDL
from siesta import API
from scrapy import log
import pymongo

import musiccrawler.settings

class SOAPWSExporter(BaseItemExporter):
    export_empty_fields = False
    
    def __init__(self):
        self.server = WSDL.Proxy(musiccrawler.settings.WSDL_FILE)

    def export_item(self, item):
        log.msg(("Sending item to SOAPWS:" + str(item)),level=log.DEBUG)
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
        log.msg("Authenticating to MongoDB",level=log.DEBUG)
        self.db.authenticate(musiccrawler.settings.MONGODB_USER,musiccrawler.settings.MONGODB_PASSWORD)
        self.collection = self.db[musiccrawler.settings.MONGODB_COLLECTION]
        if self.__get_uniq_key() is not None:
            self.collection.create_index(self.__get_uniq_key(), unique=True)

    def export_item(self, item):
        if self.__get_uniq_key() is None:
            log.msg(("Sending item to MongoDB:" + str(item) + " " + str(dict(item))),level=log.DEBUG)
            self.collection.insert(dict(item))
        else:
            log.msg(("Sending item to MongoDB:" + str(item) + " " + str(dict(item))),level=log.DEBUG)
            self.collection.update(
                            {self.__get_uniq_key(): item[self.__get_uniq_key()]},
                            dict(item),
                            upsert=True)
        
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