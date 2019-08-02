import scrapy
import pandas as pd
import json
import datetime
import numpy as np
from simplejson import JSONDecodeError
from stockScrapyMongoDB.helpers import getRecordTradingDaysdf

class tradingDaysSpider(scrapy.Spider):

    name = "tradingDays"        

    custom_settings = {
        'DOWNLOAD_DELAY': 10,
        'CONCURRENT_REQUESTS': 4,
        'MONGODB_COLLECTION': name,
        'MONGODB_ITEM_CACHE': 1,
    }
    def start_requests(self):
        try: 
            oldDays = getRecordTradingDaysdf().values
            date = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
            url = "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=%s&stockNo=2330"%date
            yield scrapy.Request(url)

        except AttributeError: 
            # build the db from scratch
            dates = []
            for year in range(1995, 2020, 1): 
                for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]: 
                    dates.append("%d%s01"%(year, month))
            dates.append(datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d"))
            for date in dates: 
                url = "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=%s&stockNo=2330"%date
                yield scrapy.Request(url)  


    def parse(self, response):
        js =  json.loads(response.text)
        try: 
            if js["stat"] == "OK":
                df = pd.DataFrame(js["data"], columns=js["fields"])
                df = df.rename(columns={"日期": "timestamp"})
                df["timestamp"] = df["timestamp"].apply(lambda x: datetime.datetime.strptime((str(int(x.split("/")[0])+1911)+"/"+x.split("/")[1]+"/"+x.split("/")[2]), "%Y/%m/%d"))
                # df = df[["timestamp"]]
                # df = pd.DataFrame(js["data"])[0].apply(lambda x: datetime.datetime.strptime((str(int(x.split("/")[0])+1911)+"/"+x.split("/")[1]+"/"+x.split("/")[2]), "%Y/%m/%d"))
                try: 
                    newTradingDaysArray = df["timestamp"].values
                    oldTradingDaysArray = getRecordTradingDaysdf().values
                    df = self.getMergedTradingDaysdf(newTradingDaysArray, oldTradingDaysArray)
                    dic = df.to_dict("records")            
                except AttributeError:
                    dic = df[["timestamp"]].to_dict("records")
                if dic: 
                    for item in dic: 
                        yield item   
                else: 
                    pass        
            else: 
                print('someting wrong with', response.url)
        except JSONDecodeError: 
            pass

    def getMergedTradingDaysdf(self,newTradingDaysArray, oldTradingDaysArray): 
        c = np.in1d(newTradingDaysArray, oldTradingDaysArray)
        df = pd.DataFrame(newTradingDaysArray[~c], columns =["timestamp"])
        return df


