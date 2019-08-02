# -*- coding: utf-8 -*-
import scrapy
import json
import datetime
from datetime import timedelta, timezone
import numpy as np
from dateutil.tz import tzutc
from stockScrapyMongoDB.helpers import getCollectionCrawleredDaysArray
from urllib.parse import urlencode


name = 'bitmex_XBTUSD'


class bitmexSpider(scrapy.Spider):
    name = name
    
    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'MONGODB_COLLECTION': name,
        'MONGODB_ITEM_CACHE': 1,
    }
    
    def start_requests(self):
        url = "https://www.bitmex.com/api/v1" + "/trade/bucketed?"
        binSize = "1m"
        symbol = "XBTUSD"
        count = 500        
        

        lastCrawledTimestamp = getCollectionCrawleredDaysArray(name)
        if lastCrawledTimestamp: 
            startTime = lastCrawledTimestamp + timedelta(minutes=1)
        else: 
            startTime = datetime.datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=tzutc())        	
            
       
        startTimeArray = self.get_startTimeArray(startTime)
        for startTime in startTimeArray: 
            params = {'binSize': binSize, 
                       'symbol': symbol, 
                       'count' : count, 
                       'startTime': startTime}

            url = url + urlencode(params)
            yield scrapy.Request(url) 

    def parse(self, response):
        js = json.loads(response.text)
        for item in js: 
            yield item

    def get_startTimeArray(self, initialStartTime): 
        startTimeArray = []
        now = datetime.datetime.now(timezone.utc)
        t = initialStartTime
        while True:
            if (t - now).total_seconds() < 0:
                startTimeArray.append(t)
                t = t + timedelta(minutes=500)
            else: 
                break    
        return startTimeArray

