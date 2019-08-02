import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np
from stockScrapyMongoDB.helpers import connect_mongo, getStockIdArray


class StockLendingSpider(scrapy.Spider):
    name = "advancedBrokerPoints" 
    custom_settings = {
    'DOWNLOAD_DELAY': 0,
    'CONCURRENT_REQUESTS': 16,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        stockIdArray = getStockIdArray()

        for stockId in stockIdArray: 
            url = "https://histock.tw/stock/chip/chartdata.aspx?no=%s&m=dailyk,close,volume,mean5,mean10,mean20,mean60,mean120,mean240,mean5volume,mean20volume,broker1,broker3,broker5,broker10,chip1,chip3,chip5,chip10,focus1,focus3,focus5,focus10"%stockId
            yield scrapy.Request(url, meta={"stockId":stockId}, callback = self.parse , errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        meta = response.meta
        try: 
            dataDic = json.loads(response.text)
            advancedBrokersdf = self.getAdvancedBrokersdf(dataDic, meta["stockId"])
            itemDic = advancedBrokersdf.to_dict("records")
            for item in itemDic:
                yield item
            else: 
                pass
        except:                 
            pass

    def getAdvancedBrokersdf(self, dataDic, stockId): 
        dfs = []
        for key in list(dataDic.keys()):
            df = pd.DataFrame(ast.literal_eval(dataDic[key]))
            if key == "chip1": 
                df.columns = ["timestamp", "主力籌碼張數買賣超"]
            elif key == "broker1":
                df.columns = ["timestamp", "券商買賣家數"]
            elif key == "focus1":
                df.columns = ["timestamp", "籌碼集中度"]                         
            else: 
                continue
            df["timestamp"] = df["timestamp"].apply(lambda x : pd.Timestamp.utcfromtimestamp(int(x/1000.0)))
            df = df.set_index("timestamp")
            dfs.append(df)
        df = pd.concat(dfs, axis=1, sort=True)
        df["stockId"] = stockId
        df["timestamp"] = df.index

        newDays = df["timestamp"].values
        oldDays = self.getCrawleredDays(stockId)
        mask = np.in1d(newDays, oldDays, invert=True)
        newDays = newDays[mask]
        df = df.loc[df["timestamp"].isin(newDays)]
        return df

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        
  
    def getCrawleredDays(self, stockId):
        db = connect_mongo()
        return pd.to_datetime(list(db['advancedBrokerPoints'].find({"stockId": stockId}).distinct("timestamp"))).values
