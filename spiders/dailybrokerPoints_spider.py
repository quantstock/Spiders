import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np
import time
from stockScrapyMongoDB.helpers import getStockIdArray, getRecordTradingDaysdf, getLatestDayFromBrokerPoints, getCollectionCrawleredDaysArray

class brokerPointsSpider(scrapy.Spider):
    name = 'dailyBrokerPoints'

    custom_settings = {
        'DOWNLOAD_DELAY': 0,
        'CONCURRENT_REQUESTS': 10,
        'MONGODB_COLLECTION': name,
        'COOKIES_ENABLED': False
    }
    
    def start_requests(self):
        stockIdArray = getStockIdArray()

        startTime = datetime.datetime(2016, 1, 1)
        aldyDaysArray = pd.to_datetime(getCollectionCrawleredDaysArray(collection="dailyBrokerPoints"))
        wantDaysArray = pd.to_datetime(getRecordTradingDaysdf(startTime).values)
        mask = np.in1d(wantDaysArray, aldyDaysArray, invert=True)
        wantDaysArray = pd.to_datetime(wantDaysArray[mask]) 
        wantDaysArray = [datetime.datetime.strftime(d, "%Y%m%d") for d in wantDaysArray]

        for date in wantDaysArray: 
            for stockId in stockIdArray: 
                url = "https://www.wantgoo.com/stock/astock/agentstat_ajax?StockNo=%s&Types=2&StartDate=%s&EndDate=%s&Rows=9999&Agent="%(stockId, date, date)
                yield scrapy.Request(url)  

    def parse(self, response):
        stockId = re.search('StockNo=(.+?)&', response.url).group(1)
        date = re.search('StartDate=(.+?)&', response.url).group(1)
        df = pd.DataFrame(ast.literal_eval(json.loads(response.text)["returnValues"]))
        df["timestamp"] = datetime.datetime.strptime(date, "%Y%m%d")
        df["stockId"] = stockId
        itemDic = df.to_dict("records")
        for item in itemDic: 
            yield item

