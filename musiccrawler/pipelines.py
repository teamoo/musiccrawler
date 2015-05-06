# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

from datetime import datetime
import dateutil.parser
import json
from operator import attrgetter
import pkg_resources
from pytz import timezone
import random
import re
import time
import traceback
from urllib2 import HTTPError
from musiccrawler.items import FacebookInformationItem, SoundcloudInformationItem, YoutubeInformationItem, HypemInformationItem, VKInformationItem

from apiclient.discovery import build
from apiclient.errors import HttpError
import facebook
import hypem
from py4j.java_gateway import JavaGateway, logging
from py4j.protocol import Py4JError
from scrapy import log, signals
from scrapy.exceptions import DropItem
import soundcloud

from musiccrawler.exporters import MongoDBExporter
import musiccrawler.settings


class GetMusicDownloadLinkStatisticsPipeline(object):
    def __init__(self):
        self.tz = timezone("Europe/Berlin")
        self.facebookGraphAPI = facebook.GraphAPI();
        self.soundcloudAPI = soundcloud.Client(client_id=musiccrawler.settings.SOUNDCLOUD_APP_ID);
        
        self.youtubeDataAPI = build(musiccrawler.settings.GOOGLE_API_SERVICE_NAME, musiccrawler.settings.GOOGLE_API_VERSION,
    developerKey=musiccrawler.settings.GOOGLE_API_KEY)

    def process_item(self, item, spider):
        videoids = []
        temp_fb_shares = 0;
        temp_sc_comment_count = 0;
        temp_sc_download_count = 0;
        temp_sc_favoritings_count = 0;
        temp_sc_playback_count = 0;
        temp_hm_loved_count = 0;
        temp_hm_posted_count = 0;
        temp_yt_comment_count = 0;
        temp_yt_favorite_count = 0;
        temp_yt_view_count = 0;
        temp_yt_like_count = 0;
        temp_yt_dislike_count = 0;
        
        facebook_info = FacebookInformationItem()
        soundcloud_info = SoundcloudInformationItem()
        youtube_info = YoutubeInformationItem();
        hypem_info = HypemInformationItem();
        vk_info = VKInformationItem();
        
        
        if item["hoster"] == "vk.com" and 'aid' in item and 'oid' in item:
            vk_info['audio_id'] = item['aid']
            vk_info['owner_id'] = item['oid']
        
        if item is None or item.get("name",None) is None:
            log.msg(("Received empty itemp (corrupted)"), level=log.DEBUG)
            raise DropItem("Dropped empty item (corrupted)")
        else:
            try:
                time.sleep(5)
                search_response_ids = self.youtubeDataAPI.search().list(
                                            q=item["name"],
                                            part="id",
                                            maxResults=musiccrawler.settings.STATISTICS_ITEM_BASE,
                                            type="video"
                                            ).execute()
    
                for search_result in search_response_ids.get("items", []):
                    if search_result["id"] is not None:
                        videoids.append(search_result["id"]["videoId"]);
                
                try:
                    search_response_videos = self.youtubeDataAPI.videos().list(
                                                id=",".join(videoids),
                                                part="statistics,snippet,id",
                                                maxResults=musiccrawler.settings.STATISTICS_ITEM_BASE
                                                ).execute()
                                                
                    if len(search_response_videos.get("items", [])) >= 1:
                        youtube_info["name"] = search_response_videos.get("items", [])[0]["snippet"]["title"];
                        youtube_info["date_published"] = dateutil.parser.parse(search_response_videos.get("items", [])[0]["snippet"]["publishedAt"]);
                                                
                    for search_result in search_response_videos.get("items", []):
                        if search_result["statistics"] is not None and search_result["snippet"] is not None:
                            temp_yt_comment_count += int(search_result["statistics"]["commentCount"])
                            temp_yt_view_count += int(search_result["statistics"]["viewCount"])
                            temp_yt_favorite_count += int(search_result["statistics"]["favoriteCount"])
                            temp_yt_dislike_count += int(search_result["statistics"]["dislikeCount"])
                            temp_yt_like_count += int(search_result["statistics"]["likeCount"])
        
                            if youtube_info["youtube_date_published"] < dateutil.parser.parse(search_result["snippet"]["publishedAt"]):
                                youtube_info["youtube_date_published"] = dateutil.parser.parse(search_result["snippet"]["publishedAt"]);
                
                except HttpError, e:
                    print e
            except HttpError, e:
                    print e

            try:
                time.sleep(5)
                searchresults = hypem.search(item["name"], 1)
                
                if searchresults is not None:
                    try:
                        if len(searchresults) >= 1:
                            hypem_info["name"] = searchresults[0].artist + " - " + searchresults[0].title;
                            hypem_info["date_published"] = self.tz.localize(datetime.fromtimestamp(searchresults[0].dateposted))
                            hypem_info["artwork_url"] = searchresults[0].thumb_url_medium;
            
                        for track in searchresults[:musiccrawler.settings.STATISTICS_ITEM_BASE]:
                            temp_hm_loved_count += track.loved_count;
                            temp_hm_posted_count += track.posted_count;
                            
                            if hypem_info["artwork_url"] is None:
                                hypem_info["artwork_url"] = track.thumb_url_medium;
                                
                            if hasattr(track, 'itunes_link'):
                                facebook_shares = self.facebookGraphAPI.get_object(track.itunes_link)
                                if facebook_shares.get('shares',None) is not None:
                                    temp_fb_shares += facebook_shares['shares'];
                                elif facebook_shares.get('likes',None) is not None:
                                    temp_fb_shares += facebook_shares['likes'];
                                    
                            if hypem_info["date_published"] < self.tz.localize(datetime.fromtimestamp(track.dateposted)):
                                hypem_info["date_published"] = self.tz.localize(datetime.fromtimestamp(track.dateposted));
                    except ValueError, e:
                        print e           
                    except HTTPError, e:
                            if e.code == 403:
                                print "Hypem Rate Throttling prevented Hypem API check, waiting for 15 to 60 seconds"
                                time.sleep(random.randint(15, 60))
                            else:
                                print e.reason
            except HTTPError, e:
                    if e.code == 403:
                        print "Hypem Rate Throttling prevented Hypem API check, waiting for 15 to 60 seconds"
                        time.sleep(random.randint(15, 60))
                    else:
                        print e.reason
            
            try:
                searchresults = sorted(self.soundcloudAPI.get('/tracks', q=item["name"], limit=musiccrawler.settings.STATISTICS_ITEM_BASE, filter='public'), key=attrgetter('created_at'), reverse=True);

                if len(searchresults) >= 1:
                    soundcloud_info["name"] = searchresults[0].title;
                    soundcloud_info["date_created"] = dateutil.parser.parse(searchresults[0].created_at);
                    item["name_routing"] = searchresults[0].permalink;
                    soundcloud_info["genre"] = searchresults[0].genre;
                    soundcloud_info["artwork_url"] = searchresults[0].artwork_url;
    
                for track in searchresults:
                    if hasattr(track,'permalink_url') and track.permalink_url is not None:
                        facebook_shares = self.facebookGraphAPI.get_object(track.permalink_url)
                        if facebook_shares.get('shares',None) is not None:
                            temp_fb_shares += facebook_shares['shares'];
                        elif facebook_shares.get('likes',None) is not None:
                            temp_fb_shares += facebook_shares['likes'];
                    
                    if hasattr(track,'video_url') and track.video_url is not None:
                        facebook_shares = self.facebookGraphAPI.get_object(track.video_url)
                        if facebook_shares.get('shares',None) is not None:
                            temp_fb_shares += facebook_shares['shares'];
                        elif facebook_shares.get('likes',None) is not None:
                                temp_fb_shares += facebook_shares['likes'];
                                
                    if soundcloud_info["artwork_url"] is None:
                        soundcloud_info["artwork_url"] = track.artwork_url;
                    if soundcloud_info["genre"] is None:
                        soundcloud_info["genre"] = track.genre; #VK Genres auch noch mitnehmen
                        
                    temp_sc_comment_count += track.comment_count;
                    temp_sc_download_count += track.download_count;
                    temp_sc_favoritings_count += track.favoritings_count;
                    temp_sc_playback_count += track.playback_count;
                    soundcloud_info["date_created"] = dateutil.parser.parse(track.created_at);
                    
                    if soundcloud_info["date_created"] < dateutil.parser.parse(track.created_at):
                        soundcloud_info["date_created"] = dateutil.parser.parse(track.created_at);
            except e:
                print e
            
            if temp_fb_shares > 0:
                facebook_info["shares"] = temp_fb_shares
                item['facebook'] = dict(facebook_info)
                
            if "name" in soundcloud_info:
                soundcloud_info["comments"] = temp_sc_comment_count;
                soundcloud_info["downloads"] = temp_sc_download_count;
                soundcloud_info["likes"] = temp_sc_favoritings_count;
                soundcloud_info["playbacks"] = temp_sc_playback_count;
                item['soundcloud'] = dict(soundcloud_info)
            if "name" in youtube_info:
                youtube_info["likes"] = temp_yt_like_count;
                youtube_info["comments"] = temp_yt_comment_count;
                youtube_info["views"] = temp_yt_view_count;
                youtube_info["dislikes"] = temp_yt_dislike_count;
                youtube_info["favorites"] = temp_yt_favorite_count;
                item['youtube'] = dict(youtube_info)
            if "name" in hypem_info:
                hypem_info["likes"] = temp_hm_loved_count;
                hypem_info["posts"] = temp_hm_posted_count;
                item['hypem'] = dict(hypem_info)
            
            if "aid" in vk_info and "oid" in vk_info:
                item["vk"] = dict(vk_info)
            
            log.msg(("Updated item statistics:" + str(item)), level=log.DEBUG)
            return item
            
class CheckMusicDownloadLinkPipeline(object):
    urlregex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
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
            if re.match(self.urlregex, item['url']):
                self.mdlb.init(item['source'])
                log.msg(("Sending URL to Linkbuilder: " + item['url']), level=log.DEBUG)
                jsonresult = self.mdlb.buildMusicDownloadLink(item['url'])
                if not jsonresult is None:
                    log.msg(("Linkbuilder returned jsonstring: " + jsonresult), level=log.DEBUG)
                    jsonitem = json.loads(jsonresult)
                    if jsonitem is None:
                        log.msg(("Linkbuilder returned corrupted information for link " + str(item['url'])) , level=log.WARNING)
                        item['status'] = 'unknown'
                        if not 'name' in item.keys():
                            item['name'] = item['url']
                        return item
                    else: 
                        item['status'] = jsonitem['status']
                        item['url'] = jsonitem['url']
                        if not '.vk.me' in item['url']:
                            item['hoster'] = jsonitem['hoster']
                            item['name'] = jsonitem['name']
                        else:
                            item['hoster'] = 'vk.com'
                        # item['password'] = jsonitem['password']
                        # item['metainfo'] = jsonitem['metainfo']
                        item['size'] = jsonitem['size']
                        log.msg(("Linkbuilder returned item:" + str(jsonitem)), level=log.DEBUG)
                        return item
                else:
                    log.msg(("Linkbuilder returned corrupted information for link " + str(item['url'])) , level=log.WARNING)
                    item['status'] = 'unknown'
                    if not 'name' in item.keys():
                        item['name'] = item['url']
                    return item
            else:
                raise DropItem("Link-URL is invalid: ", item['url'], ", Item will be dropped.")
        except Py4JError:
            traceback.print_exc()
            
class DuplicateURLsPipeline(object):
    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        if item is None:
            log.msg(("Received empty item (corrupted)"), level=log.DEBUG)
            raise DropItem("Dropped empty item (corrupted)")
        elif not 'url' in item.keys():
            log.msg(("Corrupted Item:" + str(item)), level=log.DEBUG)
            raise DropItem("Corrupted Link found: %s" % item)
        elif item['url'] in self.urls_seen:
            log.msg(("Duplicate Item:" + str(item)), level=log.DEBUG)
            raise DropItem("Duplicate Link-URL found: %s" % item['url'])
        else:
            log.msg(("new Item:" + str(item)), level=log.DEBUG)
            self.urls_seen.add(item['url'])
            return item

class CleanURLPipeline(object):
    def process_item(self, item, spider):
        url = str(item['url'])
        item['url'] = str(url.split('" ')[0].split('\n')[0]).rstrip().lstrip()
        
        return item
        
class BadFilesPipeline(object):
    def __init__(self):
        self.badlinks = pkg_resources.resource_string('musiccrawler.config', "badlinks.cfg").split(";")

    def process_item(self, item, spider):
        if str(item['url']).endswith(".jpg") or str(item['url']).endswith(".png") or str(item['url']).endswith(".gif"):
            log.msg(("Bad Item:" + str(item)), level=log.DEBUG)
            raise DropItem("Bad Link-URL found: %s" % item['url'])
        elif str(item['url']) in self.badlinks:
            raise DropItem("Bad Link-URL found: %s" % item['url'])
        elif "youtube.com/user" in str(item['url']):
            raise DropItem("Bad Link-URL found: %s" % item['url'])
        elif re.match(r"https?://soundcloud.com/\w*/$",str(item['url'])):
            raise DropItem("Bad Link-URL found: %s" % item['url'])
        elif "soundcloud.com" in str(item['url']) and "/download.mp3" in str(item['url']):
            item['url'] = str(item['url']).replace("/download.mp3","")
        else:
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
        if not item is None:
            self.exporter.export_item(item)
            return item       
