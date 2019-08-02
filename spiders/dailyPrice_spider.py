import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np 
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf, getCollectionCrawleredDaysArray


class priceSpider(scrapy.Spider):
    name = "dailyPrice"   
    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        startTime = datetime.datetime(2004, 2, 11)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyPrice")).values
        wantDaysArray = getRecordTradingDaysdf(startTime).values
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]

        for date in wantDaysArray: 
            url = "http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s&type=ALLBUT0999"%date
            yield scrapy.Request(url, callback = self.parse , errback = lambda x: self.download_errback(x, url))        

    def parse(self, response):
        dataDic = json.loads(response.text)
        try: 
            if dataDic["stat"] == "OK": 
                pricedf = self.getPricedf(dataDic)
                itemDic = pricedf.to_dict("records")
                for item in itemDic:
                    yield item
            else: 
                pass
        except KeyError: 
            pass

    def getPricedf(self, dataDic): 
        for key in list(dataDic.keys()):
            if "fields" in key: 
                if "本益比" in dataDic[key]: 
                    datakey = "data" + key.split("fields")[1]        
                    df = pd.DataFrame(dataDic[datakey], columns=dataDic[key])
                    df["timestamp"] = datetime.datetime.strptime(dataDic["date"], "%Y%m%d")
                    df = df.drop(columns= ['漲跌(+/-)'])
                    df = df.rename(columns = {'證券代號':'stockId'})
                    return df

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        


