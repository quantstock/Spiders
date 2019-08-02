# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import json
import datetime
from datetime import timedelta, timezone
import numpy as np
from dateutil.tz import tzutc
from stockScrapyMongoDB.helpers import connect_mongo
from urllib.parse import urlencode
import requests
import pymongo

name = 'bitmexActive'

class bitmexSpider(scrapy.Spider):
    name = name
    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'MONGODB_COLLECTION': "bitmex_1m",
        'MONGODB_ITEM_CACHE': 1,
    }
    
    def start_requests(self):
        url = "https://www.bitmex.com/api/v1" + "/trade/bucketed?"
        binSize = "1m"
        count = 500        

        reqResult = requests.get("https://www.bitmex.com/api/v1/instrument/active")
        items = reqResult.json()
        for i,item in enumerate(items): 
            symbol = item["symbol"]
            absStartTime = pd.to_datetime(item["listing"])
            try: 
                lastCrawledTimestamp = self.get_lastCrawledTimestamp(symbol=symbol)
                startTime = lastCrawledTimestamp + timedelta(minutes=1)
            except IndexError: 
                startTime = absStartTime

            if item["settle"] is None: 
                endTime = datetime.datetime.now()
            else: 
                endTime = pd.to_datetime(item["settle"])
            startTimeArray = self.get_startTimeArray(startTime)
            for startTime in startTimeArray:
                params = {'binSize': binSize, 
                           'symbol': symbol, 
                           'count' : count, 
                           'startTime': startTime,
                           'endTime': endTime}
                yield scrapy.Request(url + urlencode(params))
            print("symbol %s upto date"%symbol)
            print("%d of samples left"%(len(items)-i))

    def parse(self, response):
        js = json.loads(response.text)
        for item in js: 
            item["stockId"] =item.pop("symbol")
            item["timestamp"] = pd.to_datetime(item["timestamp"])
            yield item

    def get_startTimeArray(self, initialStartTime): 
        startTimeArray = []
        now = datetime.datetime.now(timezone.utc).replace(tzinfo=None)
        t = initialStartTime.replace(tzinfo=None)
        
        while True:
            if (t - now).total_seconds() < 0:
                startTimeArray.append(t)
                t = t + timedelta(minutes=500)
            else: 
                break    

        return startTimeArray

    def get_lastCrawledTimestamp(self, symbol): 
        db = connect_mongo()
        dic = list(db["bitmex_1m"].find({"stockId":symbol}).sort("timestamp",pymongo.DESCENDING).limit(1))[0]["timestamp"]
        return dic
