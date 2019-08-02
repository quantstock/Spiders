# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import pandas as pd
import scrapy
from htmltable_df.extractor import Extractor
from stockScrapyMongoDB.helpers import getStockIdArray, getLatestDayFromStockHold
import time
import numpy as np 
from io import StringIO

class stockHoldSpider(scrapy.Spider):
    name = 'weeklyStockHold_pyramid'

    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 16,
        'MONGODB_COLLECTION': name,
        'COOKIES_ENABLED': False
    }

    def start_requests(self):
        # startTime = getLatestDayFromStockHold()

        stockIdArray = getStockIdArray()
        # url = 'https://www.tdcc.com.tw/smWeb/QryStockAjax.do'

        for stockId in stockIdArray:
            url = "http://norway.twsthr.info/StockHolders.aspx?stock=%s"%stockId
            yield scrapy.Request(url, meta={'stockId': stockId}, callback = self.parse , errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        m = response.meta
        dfs = pd.read_html(StringIO(response.text))
        try:  
            df = dfs[10]
            df.columns = [np.nan,
                np.nan,
                'timestamp',
                '集保總張數',
                '總股東人數',
                '每人平均張數',
                '大於400張大股東持有張數',
                '大於400張大股東持有百分比',
                '大於400張大股東人數',
                '人數400至600張',
                '人數600至800張',
                '人數800至1000張',
                '大於1000張人數',
                '大於1000張大股東持有百分比',
                '收盤價',
                np.nan]
            df = df.loc[df["timestamp"].dropna().index].dropna(axis=1).drop([0])
            df["timestamp"] = df["timestamp"].apply(lambda x: pd.to_datetime(x))
            df.insert(0, "stockId", m["stockId"])
            for item in df.to_dict("records"):
                yield item
        except: 
            pass

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        
            
