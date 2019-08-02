import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np
import time
import requests
from io import StringIO
from stockScrapyMongoDB.helpers import getStockIdArray,connect_mongo

class brokerPointsSpider(scrapy.Spider):
    name = 'dailyWorldIndices'

    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 10,
        'MONGODB_COLLECTION': name,
        'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        url = "https://finance.yahoo.com/world-indices/"
        r = requests.get(url)
        df = pd.read_html(StringIO(r.text))[0]
        symbolArray = df["Symbol"].values
        nameArray = df["Name"].values        

        for symbol, name in zip(symbolArray, nameArray): 
            url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=0&period2=1549258857&interval=1d&events=history&crumb=hP2rOschxO0"%symbol
            yield scrapy.Request(url, meta={"name":name, "symbol":symbol}, method='POST')  

    def parse(self, response):
        meta = response.meta
        df = pd.read_csv(StringIO(response.text))
        df = df.rename(columns={"Date":"timestamp"})
        df["timestamp"] = df["timestamp"].apply(lambda x: pd.to_datetime(x))
        df["stockId"] = meta["symbol"]
        df["symbolName"] = meta["name"]
        newDays = pd.to_datetime(df["timestamp"].values)
        oldDays = self.getCrawleredDays(meta["symbol"])
        mask = np.in1d(newDays, oldDays, invert=True)
        newDays = newDays[mask]
        df = df.loc[df["timestamp"].isin(newDays)]

        for item in df.to_dict("records"): 
            yield item

    def getCrawleredDays(self, symbol):
        db = connect_mongo()
        return pd.to_datetime(list(db['dailyWorldIndices'].find({"stockId": symbol}).distinct("timestamp"))).values

        
