import scrapy
import pandas as pd
import json
import datetime
import numpy as np
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf, getLatestDayFromFundTrading, getCollectionCrawleredDaysArray

class fundTradingSpider(scrapy.Spider):
    name = "dailyFundTrading"        
    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        startTime = datetime.datetime(2012, 5, 2)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyFundTrading")).values
        wantDaysArray = getRecordTradingDaysdf(startTime).values
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]

        for date in wantDaysArray: 
            url = "http://www.twse.com.tw/fund/T86?response=json&date=%s&selectType=ALL"%date
            yield scrapy.Request(url, callback = self.parse , errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        try: 
            dataDic = json.loads(response.text)
            if dataDic["stat"] == "OK":
                fundTradingdf = self.getFundTradingdf(dataDic)
                itemDic = fundTradingdf.to_dict("records")
                for item in itemDic:
                    yield item
            else: 
                pass
        except: 
            pass

    def getFundTradingdf(self, dataDic): 
        df = pd.DataFrame(dataDic["data"])
        df.columns = dataDic["fields"]
        df.columns= [c.replace('（', '').replace('）', '').replace('(', '').replace(')', '') for c in df.columns.values]
        df["timestamp"] = datetime.datetime.strptime(dataDic["date"], "%Y%m%d")
        df = df.rename(columns = {'證券代號':'stockId'})
        return df

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback=self.parse, errback=lambda x: self.download_errback(x, url))    
