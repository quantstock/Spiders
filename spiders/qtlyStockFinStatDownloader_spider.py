# -*- coding: utf-8 -*-
import scrapy
import itertools
import sys
import time
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime
import os

from stockScrapyMongoDB.helpers import getStockIdArray, connect_mongo

class qtlyStockFinStat_Spider(scrapy.Spider):
    name = "qtlyStockFinStat_downloader"
    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 16,
        'MONGODB_COLLECTION': name,
        }

    def start_requests(self):
        self.path = "/home/wenping/stock_data/financial_statement/"
        # self.path = "/home/wenping/sean/stockScrapyMongoDB/financial_statement/"
        yearNseasonDictList = self.getYearNseasonDictList()
        if yearNseasonDictList:
            # stockIdArray = getStockIdArray(ETF=False)
            stockIdArray = ['2330']
            ranges = [stockIdArray, yearNseasonDictList]
            for stockId, yearNseason in itertools.product(*ranges):
                year = yearNseason["year"]
                season = yearNseason["season"]
                url = 'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID=%s&SYEAR=%s&SSEASON=%s&REPORT_ID=C'%(stockId, year, season)
                yield scrapy.Request(url, callback = self.parse , errback = lambda x: self.download_errback(x, url), meta={'stockId': stockId, 'year': year, 'season': season})
        else:
            pass

    def parse(self, response):
        meta = response.meta
        filename = meta["stockId"] + ".html"
        dirPath = self.path + meta["year"] + meta["season"]
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        with open(dirPath + "/" + filename, 'wb') as html_file:
            time.sleep(10)
            html_file.write(response.body)
            print("stockId: %s crawlered" %meta["stockId"])

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))

    def getYearNseasonDictList(self):
        startYear, startSeason = self.getLatestTimeFromFinStat()
        endYear, endSeason = self.now2LatestAvailableSeason()
        yearNseasonDicList = []
        if startYear != endYear:
            # 處理startYear
            for i in range(startSeason, 5):
                yearNseasonDicList.append({"year": str(startYear), "season": str(i)})
            # 處理第二年到endYear前一年
            yearArray = range(startYear+1, endYear-1)
            seasonArray = range(1,5)
            for year, season in itertools.product(yearArray, seasonArray):
                yearNseasonDicList.append({"year": str(year), "season": str(season)})
            #處理endYear
            for s in range(1, endSeason+1):
                yearNseasonDicList.append({"year": str(endYear), "season": str(s)})
        else:
            # 要求年份與現在同年
            for s in range(startSeason+1, endSeason+1):
                # 不包含startSeason
                yearNseasonDicList.append({"year": str(endYear), "season": str(s)})
        return yearNseasonDicList

    def now2LatestAvailableSeason(self):
        date = datetime.now()
        year = date.year
        tarr = np.array([
            datetime(year, 3, 31),
            datetime(year, 5, 15),
            datetime(year, 8, 14),
            datetime(year, 11, 14)])
        tarr = date - tarr
        tarr = [dif.total_seconds() for dif in tarr]
        if tarr[0]< 0:
            # not reaching the season 4 of last year, but season 3 of last year is avala
            year, season = date.year - 1, 3
        elif tarr[0] > 0 and tarr[1] < 0:
            # last year season 4 available
            year, season = date.year - 1, 4
        elif tarr[1] > 0 and tarr[2] < 0:
            # this season 1 available
            year, season = date.year, 1
        elif tarr[2] > 0 and tarr[3] < 0:
            # season 2 available
            year, season = date.year, 2
        elif tarr[3] > 0:
            # season 3 available
            year, season = date.year, 3
        return year, season

    def getLatestTimeFromFinStat(self):
        yearNseasonList = os.listdir(self.path)
        yearNseasonDicList = []
        for yearNseason in yearNseasonList:
            try:
                year = int(yearNseason[:-1])
                season = int(yearNseason[-1:])
                yearNseasonDicList.append({"year":year, "season":season})
            except:
                continue
        df = pd.DataFrame(yearNseasonDicList)
        year = df["year"].max()
        season = df.loc[df["year"] == year]["season"].max()
        return year, season
