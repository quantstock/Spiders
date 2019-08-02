# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import json
import datetime
from datetime import timedelta, timezone
import numpy as np
from dateutil.tz import tzutc
from urllib.parse import urlencode
from stockScrapyMongoDB.helpers import connect_mongo

name = 'bitmexRealized'


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
        symbol_endTime = [
                  ['XBTH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['XBTU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['XBTZ18', datetime.datetime(2018, 12, 28, 8, 0)],
                  ['XRPH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['XRPU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['XRPZ18', datetime.datetime(2018, 12, 28, 8, 0)],
                  ['ADAH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['ADAU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['ADAZ18', datetime.datetime(2018, 12, 28, 8, 0)],
                  ['BCHH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['BCHU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['BCHZ18', datetime.datetime(2018, 12, 28, 8, 0)],
                  ['EOSH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['EOSU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['EOSZ18', datetime.datetime(2018, 12, 28, 8, 0)],
                  ['ETHH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['ETHU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['ETHZ18', datetime.datetime(2018, 12, 28, 8, 0)],
                  ['LTCH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['LTCU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['LTCZ18', datetime.datetime(2018, 12, 28, 8, 0)],
                  ['TRXH19', datetime.datetime(2019, 3, 29, 8, 0)],
                  ['TRXU18', datetime.datetime(2018, 9, 28, 8, 0)],
                  ['TRXZ18', datetime.datetime(2018, 12, 28, 8, 0)]]
        
        for s in symbol_endTime: 
            symbol = s[0]
            try: 
            	endTime = self.get_crawledEndTime(symbol) - timedelta(minutes=1)
            except IndexError: 
            	endTime = s[1] 

            endTimeArray = self.get_endTimeArray(endTime)
            for endTime in endTimeArray: 
                params = {'binSize': binSize, 
                           'symbol': symbol, 
                           'count' : count, 
                           'reverse': True,
                           'endTime': endTime}
                yield scrapy.Request(url + urlencode(params)) 

    def parse(self, response):
        js = json.loads(response.text)
        for item in js: 
            item["stockId"] =item.pop("symbol")
            item["timestamp"] = pd.to_datetime(item["timestamp"])
            yield item

    def get_endTimeArray(self, initialEndTime): 
        endTimeArray = []
        startTime = (initialEndTime - timedelta(days=194)).replace(tzinfo=None)
        t = initialEndTime.replace(tzinfo=None)
        
        while True:
            if (t - startTime).total_seconds() > 0:
                endTimeArray.append(t)
                t = t - timedelta(minutes=500)
            else: 
                break    
        return endTimeArray

    def get_crawledEndTime(self, symbol): 
        db = connect_mongo()
        crawledEndTime = list(db["bitmex_1m"].find({"stockId":symbol}, {"timestamp":1}).limit(5))[0]['timestamp']
        return crawledEndTime
