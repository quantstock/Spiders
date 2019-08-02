import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np
from stockScrapyMongoDB.helpers import getStockIdArray, connect_mongo


class StockLendingSpider(scrapy.Spider):
    name = "advancedChipsPercentage" 
    custom_settings = {
    'DOWNLOAD_DELAY': 0,
    'CONCURRENT_REQUESTS': 16,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        stockIdArray = getStockIdArray()

        for stockId in stockIdArray: 
            url = "https://histock.tw/stock/large.aspx?no=%s"%stockId
            yield scrapy.Request(url, meta={"stockId":stockId}, callback = self.parse , errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        try: 
            meta = response.meta
            types = ["籌碼集中度", "外資籌碼", "大戶籌碼", "董監持股"]
            dfs = []
            for subtype in types: 
                data = re.search('name: \'%s\',\r\n.*,\r\n'%subtype  ,response.text)
                df = pd.DataFrame(ast.literal_eval(data.group(0).split(",\r\n")[1].split("data: ")[1]))
                df.columns = ["timestamp", "%s百分比"%subtype]       
                df["timestamp"] = df["timestamp"].apply(lambda x : pd.Timestamp.utcfromtimestamp(int(x/1000.0)))
                df= df.set_index("timestamp")
                dfs.append(df)
            df = pd.concat(dfs, axis=1, sort=True)
            df["stockId"] = meta["stockId"]
            df["timestamp"] = df.index
    
            newDays = pd.to_datetime(df["timestamp"].values)
            oldDays = self.getCrawleredDays(meta["stockId"])
            mask = np.in1d(newDays, oldDays, invert=True)
            newDays = newDays[mask]
            df = df.loc[df["timestamp"].isin(newDays)]
    
            for item in df.to_dict("records"):
                yield item      
        except (ValueError, AttributeError) as e: 
            pass          

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        
    
    def getCrawleredDays(self, stockId):
        db = connect_mongo()
        return pd.to_datetime(list(db['advancedChipsPercentage'].find({"stockId": stockId}).distinct("timestamp"))).values
