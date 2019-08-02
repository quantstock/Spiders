import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf, getLatestDayFromStockLending, getCollectionCrawleredDaysArray


class StockLendingSpider(scrapy.Spider):
    name = "dailyStockLending" 
    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        startTime = datetime.datetime(2005, 7, 1)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyStockLending")).values
        wantDaysArray = getRecordTradingDaysdf(startTime).values
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]
        
        for date in wantDaysArray: 
            url = "http://www.tse.com.tw/exchangeReport/TWT93U?response=json&date=%s"%date
            yield scrapy.Request(url, callback = self.parse , errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        dataDic = json.loads(response.text)
        try: 
            if dataDic["stat"] == "OK": 
                stockLendsdf = self.getStockLendsdf(dataDic)
                itemDic = stockLendsdf.to_dict("records")
                for item in itemDic:
                    yield item
            else: 
                pass
        except KeyError: 
            pass

    def getStockLendsdf(self, dataDic): 
        df = pd.DataFrame(dataDic["data"])
        df["timestamp"] = datetime.datetime.strptime(dataDic["date"], "%Y%m%d")
        try:               
            df.columns = ['stockId',
                          '股票名稱',
                          '融券前日餘額',
                          '融券賣出',
                          '融券買進',
                          '融券現券',
                          '融券今日餘額',
                          '融券限額',
                          '借券賣出前日餘額',
                          '借券賣出當日賣出',
                          '借券賣出當日還券',
                          '借券賣出當日調整',
                          '借券賣出當日餘額',
                          '借券賣出今日可限額',
                          '備註',
                          'timestamp']
        except: 
            df.columns = ['stockId',
                          '股票名稱',
                          '融券前日餘額',
                          '融券賣出',
                          '融券買進',
                          '融券現券',
                          '融券今日餘額',
                          '融券限額',
                          '借券賣出前日餘額',
                          '借券賣出',
                          '借券賣出庫存異動',
                          '借券賣出今日餘額',
                          '借券賣出可使用額度',
                          '備註',
                          'timestamp']        	
        return df

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        
  
