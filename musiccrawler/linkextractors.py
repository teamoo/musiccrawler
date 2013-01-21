"""
Link extractor based on lxml.etree
"""

from lxml import etree
from lxml import html
from scrapy import log
from scrapy.link import Link
from scrapy.utils.python import unique as unique_list

class LxmlParserTreeLinkExtractor(object):
    def __init__(self, tag="a", unique=False):
        self.scan_tag = tag if callable(tag) else lambda t: t == tag
        #self.scan_attr = attr if callable(attr) else lambda a: a == attr
        #self.process_attr = process if callable(process) else lambda v: v
        self.unique = unique

        self.links = []

    def _extract_links(self, response_text, response_url):
        try:
            root = etree.fromstring(response_text.encode('utf-8'))
            #html.make_links_absolute(response_url)
        
            for e in root.iter():
                if self.scan_tag(e.tag):
                    link = Link(e.text, text=e.text)
                    self.links.append(link)
        except:
            log.msg("Could not parse XML-Tree, was probably HTML",level=log.DEBUG)
        finally:
            links = unique_list(self.links, key=lambda link: link.url) \
            if self.unique else self.links
            
            return links
    
#        def _extract_links(self, response_text, response_url):
#        html = lxml.html.fromstring(response_text)
#        html.make_links_absolute(response_url)
#        for e, a, l, p in html.iterlinks():
#            if self.scan_tag(e.tag):
#                if self.scan_attr(a):
#                    link = Link(self.process_attr(l), text=e.text)
#                    self.links.append(link)
#
#        links = unique_list(self.links, key=lambda link: link.url) \
#                if self.unique else self.links
#
#        return links

    def extract_links(self, response):
        return self._extract_links(response.body, response.url)