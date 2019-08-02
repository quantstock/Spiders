import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np 
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf, getCollectionCrawleredDaysArray


class priceSpider(scrapy.Spider):
    name = "dailyOddLots"   

    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        startTime = datetime.datetime(2004, 12, 24)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyOddLots")).values
        wantDaysArray = getRecordTradingDaysdf(startTime).values
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]

        for date in wantDaysArray: 
            url = "http://www.twse.com.tw/exchangeReport/TWT53U?response=json&date=%s&type=ALLBUT0999"%date
            yield scrapy.Request(url, meta = {"timestamp": date}, callback = self.parse , errback = lambda x: self.download_errback(x, url))        

    def parse(self, response):
        m = response.meta 
        dataDic = json.loads(response.text)
        try: 
            if dataDic["stat"] == "OK": 
                df = pd.DataFrame(dataDic["data"], columns=dataDic["fields"])
                df["timestamp"] = datetime.datetime.strptime(m["timestamp"], "%Y%m%d")
                df = df.rename(columns = {'證券代號':'stockId'})
                for item in df.to_dict("records"):
                    yield item
            else: 
                pass
        except KeyError: 
            pass

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        


