from scrapy.contrib.exporter import BaseItemExporter
from SOAPpy import WSDL

import musiccrawler.settings

class SOAPWSExporter(BaseItemExporter):
    export_empty_fields = True

    server = WSDL.Proxy(musiccrawler.settings.WSDL_FILE)

    def export_item(self, item):
        SOAPWSExporter.server.addLink(item)
        
    def start_exporting(self):
        BaseItemExporter.start_exporting(self)
        
    def finish_exporting(self):
        BaseItemExporter.finish_exporting(self)
        
    def serialize_field(self, field, name, value):
        return BaseItemExporter.serialize_field(self, field, name, value)    