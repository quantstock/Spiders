import scrapy
import pandas as pd
import numpy as np
import ast
import json
import datetime
import re
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf, getCollectionCrawleredDaysArray

class dayTradingSpider(scrapy.Spider):
    name = "dailyDayTrading" 
    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        startTime = datetime.datetime(2014, 1, 6)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyDayTrading")).values
        wantDaysArray = getRecordTradingDaysdf(startTime).values
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]
        
        for date in wantDaysArray: 
            url = "http://www.tse.com.tw/exchangeReport/TWTB4U?response=json&date=%s"%date
            yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        try: 
            dataDic = json.loads(response.text)
            if dataDic["stat"] == "OK": 
                dayTradesdf = self.getDayTradesdf(dataDic)
                itemDic = dayTradesdf.to_dict("records")
                for item in itemDic:
                    yield item
            else: 
                pass
        except: 
            pass

    def getDayTradesdf(self, dataDic): 
        df = pd.DataFrame(dataDic["data"])
        df["timestamp"] = datetime.datetime.strptime(dataDic["date"], "%Y%m%d")
        df.columns = ['stockId',
                      '證券名稱',
                      '暫停現股賣出後現款買進當沖註記',
                      '當日沖銷交易成交股數',
                      '當日沖銷交易買進成交金額',
                      '當日沖銷交易賣出成交金額',
                      'timestamp']
        return df

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))
