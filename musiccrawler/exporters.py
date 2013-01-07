from scrapy.contrib.exporter import BaseItemExporter
from SOAPpy import WSDL

class SOAPWSExporter(BaseItemExporter):
    wsdlFile = 'http://musiclink.webcomsult.de/v2/index.php?wsdl'
    server = WSDL.Proxy(wsdlFile)

    def export_item(self, item):
        SOAPWSExporter.server.addLink(item)
        
    def start_exporting(self):
        BaseItemExporter.start_exporting(self)
        
    def finish_exporting(self):
        BaseItemExporter.finish_exporting(self)
        
    def serialize_field(self, field, name, value):
        return BaseItemExporter.serialize_field(self, field, name, value)    