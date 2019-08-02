import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np 
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf, getCollectionCrawleredDaysArray


class benchmarkSpider(scrapy.Spider):
    name = "dailyBenchmarks"   

    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        startTime = datetime.datetime(2009, 1, 1)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyBenchmarks")).values
        wantDaysArray = getRecordTradingDaysdf(startTime).values
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]

        for date in wantDaysArray: 
            url = "http://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=%s"%date
            yield scrapy.Request(url, meta = {"timestamp": date}, callback = self.parse , errback = lambda x: self.download_errback(x, url))        

    def parse(self, response):
        m = response.meta 
        dataDic = json.loads(response.text)
        try: 
            if dataDic["stat"] == "OK": 
                dfs = []
                for types in ["1", "2"]: 
                    df = pd.DataFrame(dataDic["data1"])
                    df.columns = dataDic["fields1"]
                    df = df.drop(columns = "漲跌(+/-)").rename(columns = {"漲跌百分比(%)": "漲跌百分比", "指數":"stockId", "報酬指數":"stockId"})
                    dfs.append(df)
                df = pd.concat(dfs)
                df["timestamp"] = datetime.datetime.strptime(dataDic["date"], "%Y%m%d")
                for item in df.to_dict("records"):
                    yield item
            else: 
                pass
        except KeyError: 
            pass

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        


