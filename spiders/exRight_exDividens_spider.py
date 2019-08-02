import scrapy
import pandas as pd
import ast
import json
import datetime
import re
import numpy as np
from pymongo import MongoClient
from stockScrapyMongoDB.helpers import getCollectionCrawleredDaysArray


col_name = "exRight_exDividens"
class exSpider(scrapy.Spider):
    name = col_name
    custom_settings = {
    'DOWNLOAD_DELAY': 5,
    'CONCURRENT_REQUESTS': 1,
    'MONGODB_COLLECTION': name,
    'COOKIES_ENABLED': False
    }

    def start_requests(self):
        # 取得startTime 
        try: 
            startTime = pd.to_datetime(getCollectionCrawleredDaysArray(collection=col_name)).to_list()[-1]
            startTime_str = startTime.strftime("%Y%m%d")
        except: 
            startTime_str = datetime.datetime(2004, 2, 11).strftime("%Y%m%d").strftime("%Y%m%d")
            

        # 取得endTime 
        endTime_Str = (datetime.datetime.today() + datetime.timedelta(days=10)).strftime("%Y%m%d")
  
        url = "http://www.twse.com.tw/exchangeReport/TWT49U?response=json&strDate={}&endDate={}".format(startTime_str, endTime_Str)
        yield scrapy.Request(url, callback = self.parse , errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        dataDic = json.loads(response.text)
        try:
            if dataDic["stat"] == "OK":
                Exdf = self.getEXdf(dataDic)
                Exdf = self.get_not_exist_in_db_df(Exdf)
                itemDic = Exdf.to_dict("records")
                for item in itemDic:
                    yield item
            else:
                pass
        except KeyError:
            pass

    def getEXdf(self, dataDic):
        df = pd.DataFrame(dataDic["data"], columns=dataDic["fields"])
        df = df.rename(columns={"資料日期":"timestamp", 
                                "股票代號":"stockId",
                                "權值+息值": "權值加息值", 
                                "權/息": "權或息",
                                "最近一次申報資料 季別/日期": "最近一次申報資料與季別和日期",
                                "最近一次申報每股 (單位)淨值": "最近一次申報每股單位淨值",
                                "最近一次申報每股 (單位)盈餘": "最近一次申報每股單位盈餘"})
        # def parse_link(x): 
        #     reduced_x = x.split("../../../")[1].split("' target")[0]
        #     return "http://www.twse.com.tw/zh/" + reduced_x
        
        df["詳細資料"] = df["詳細資料"].apply(lambda x: "http://www.twse.com.tw/zh/" + 
                                                      x.split("../../../")[1].split("' target")[0])

        def parse_season(x):
            try: 
                link = x.split("<a href='")[1].split("' target")[0]    
                season = x.split("target=_blank>")[1].split("</a>")[0]
                return (link, season)
            except IndexError: 
                return np.nan
        df["最近一次申報資料與季別和日期"] = df["最近一次申報資料與季別和日期"].apply(parse_season)

        def parse_timestamp(x): 
            y = int(x.split("年")[0]) + 1911
            m = int(x.split("年")[1].split("月")[0])
            d = int(x.split("年")[1].split("月")[1].split("日")[0])
            if 93 > int(x.split("年")[0]) or int(x.split("年")[0]) > 110: 
                return np.nan
            else: 
                return datetime.datetime(y, m, d)
        df["timestamp"] = df["timestamp"].apply(parse_timestamp)

        df = df[~df["timestamp"].isna()]# 將timestamp中有nan的刪除

        return df
    
    def get_not_exist_in_db_df(self, df): 
        mongo_uri = 'mongodb://stockUser:stockUserPwd@localhost:27017/stock_data' # local mongodb address
        dbName = "stock_data" # database name
        db = MongoClient(mongo_uri)[dbName]

        t = df["timestamp"].to_list()
        s = df["stockId"].to_list()
        count = 0
        index = []
        for i, pack in enumerate(zip(s, t)): 
            if not list(db["col_name"].find({"timestamp":pack[1], "stockId": pack[0]})): 
                index.append(i)   

        filtered_df = df.iloc[index]        
        return filtered_df

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))



