import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf, getCollectionCrawleredDaysArray

class creditTradingSpider(scrapy.Spider):
    name = "dailyCreditTrading" 
    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):    
        startTime = datetime.datetime(2008, 1, 1)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyCreditTrading")).values
        wantDaysArray = getRecordTradingDaysdf(startTime).values
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]
        
        for date in wantDaysArray: 
            url = "http://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=%s&selectType=ALL"%date
            yield scrapy.Request(url, callback = self.parse , errback = lambda x: self.download_errback(x, url)) 

    def parse(self, response):
        try: 
            dataDic = json.loads(response.text)
            if dataDic["stat"] == "OK": 
                creditTradesdf = self.getCreditTradesdf(dataDic)
                itemDic = creditTradesdf.to_dict("records")
                for item in itemDic:
                    yield item
            else: 
                pass
        except: 
            pass

    def getCreditTradesdf(self, dataDic): 
        df = pd.DataFrame(dataDic["data"])
        df["timestamp"] = datetime.datetime.strptime(dataDic["date"], "%Y%m%d")
        df.columns = ['stockId',
                      '股票名稱',
                      '融資買進',
                      '融資賣出',
                      '融資現金償還',
                      '融資前日餘額',
                      '融資今日餘額',
                      '融資限額',
                      '融券買進',
                      '融券賣出',
                      '融券現券償還',
                      '融券前日餘額',
                      '融券今日餘額',
                      '融券限額',
                      '資券互抵',
                      '註記',
                      'timestamp']
        return df
    
    def download_errback(self, e, url):
        yield scrapy.Request(url, callback=self.parse, errback=lambda x: self.download_errback(x, url))        
