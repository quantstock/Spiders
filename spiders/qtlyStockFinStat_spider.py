# -*- coding: utf-8 -*-
for i in range(1):
    import scrapy
    from scrapy.crawler import CrawlerProcess
    import itertools
    import sys
    import time
    import pandas as pd
    import numpy as np
    from io import StringIO
    from datetime import datetime
    from stockScrapyMongoDB.helpers import getStockIdArray, getLatestTimeFromFinStat, connect_mongo
    import os
    import json
    import re

Name = "qtlyStockFinancialStatement"

class qtlyStockFinStat_Spider(scrapy.Spider):
    name = Name
    # _BalanceSheet
    # _CashFlows
    # _IncomeStatement

    custom_settings = {
        'DOWNLOAD_DELAY': 0,
        'CONCURRENT_REQUESTS': 64,
        'MONGODB_COLLECTION': name,
        'MONGODB_ITEM_CACHE': 1000,
        'MONGODB_INDEX': [("year", -1), ("season", 1), ("stockId", 1), ("timestamp", 1), ("表格形式", 1)]
        }

    def start_requests(self):
        self.collectionName = Name
        self.path = "/home/wenping/stock_data/financial_statement/"
        yearNseasonList = os.listdir(self.path)
        dictList = []
        for yearNseason in yearNseasonList:
            year = yearNseason[:-1]
            season = yearNseason[-1:]
            try:
                rawStockIdList = os.listdir(self.path+yearNseason+"/")
                stockIdList = []
                for stockId in rawStockIdList:
                    stockIdList.append(stockId.split(".html")[0])
                dictList.append({"year": year, "season": season, "stockIds": stockIdList})
            except NotADirectoryError:
                continue
        self.toRestoreUrlsDicList = self.getToRestoreUrls(dictList)

        print("urls to restore:",len(self.toRestoreUrlsDicList))
        for dic in self.toRestoreUrlsDicList:
            yield scrapy.Request(dic["url"], callback = self.parse , errback = lambda x: self.download_errback(x, url), meta={'stockId': dic["stockId"], 'year': dic["year"], 'season': dic["season"]})
        # yield scrapy.Request(self.url, callback = self.parse , errback = lambda x: self.download_errback(x, url), meta={'stockId': dic["stockId"], 'year': dic["year"], 'season': dic["season"]})

    def getToRestoreUrls(self,dictList):
        db = connect_mongo()
        toRestoreUrlsDicList = []
        for dic in dictList:
            year = dic["year"]
            season = dic["season"]
            fileStockIdArray = np.array(dic["stockIds"])
            dbStockIdArray = np.array(list(db[self.collectionName].distinct("stockId", {"year": year, "season": season})))
            mask = np.in1d(fileStockIdArray, dbStockIdArray, invert=True)
            for stockId in fileStockIdArray[mask]:
                toRestoreUrlsDicList.append(
                	{"url": "file://"+self.path+"%s%s/%s.html"%(year, season, stockId),
                    "year": year,
                    "season": season,
                    "stockId": stockId})
        return toRestoreUrlsDicList


    def parse(self, response):
        meta = response.meta
        if int(meta["year"]) < 2019:
            dfs = pd.read_html(response.url)
        else:
            dfs = self.read_html2019(response.url)

        if len(response.text) < 10000:
            pass
        else:
            # dfs = pd.read_html(StringIO(response.text)
            for sheetType in ["BalanceSheet", "IncomeStatement", "CashFlows"]:
                dic = self.getSheetsDict(dfs, meta["year"], meta["season"], meta["stockId"], sheetType)
                for item in dic:
                    yield item

    def remove_english(self, s):
        result = re.sub(r'[a-zA-Z()]', "", s)
        return result

    def patch2019(self, df):
        df = df.copy()
        dfname = df.columns.levels[0][0]

        df = df.iloc[:,1:].rename(columns={'會計項目Accounting Title':'會計項目'})


        refined_name = df[(dfname,'會計項目')].str.split(" ").str[0].str.replace("　", "").apply(self.remove_english)

        subdf = df[dfname].copy()
        subdf['會計項目'] = refined_name
        df[dfname] = subdf

        newCol = []
        for col in df.columns.levels[1]:
            s = col.split('日2')
            if len(s) > 1:
                newCol.append(s[0]+'日')
            else:
                newCol.append(col)
        newCol = pd.Index(newCol)

        df.columns = pd.MultiIndex(levels=[newCol, df.columns.levels[0]],codes=[df.columns.codes[1], df.columns.codes[0]])

        def neg(s):

            if isinstance(s, float):
                return s

            if str(s) == 'nan':
                return np.nan

            s = s.replace(",", "")
            if s[0] == '(':
                return -float(s[1:-1])
            else:
                return float(s)

        df.iloc[:,1:] = df.iloc[:,1:].applymap(neg)
        return df

    def read_html2019(self, file):
        dfs = pd.read_html(file)
        return [pd.DataFrame(), self.patch2019(dfs[0]), self.patch2019(dfs[1]), self.patch2019(dfs[2])]

    def getSheetsDict(self, dfs, year, season, stockId, sheetType):
        if sheetType == "BalanceSheet":
            i = 1
        elif sheetType == "IncomeStatement":
            i = 2
        elif sheetType == "CashFlows":
            i = 3
        else:
            print("sheetType name is wrong, shutting down")
            sys.exit()
        try:
            df = dfs[i]
        except IndexError:
            pass
        df.index=df["會計項目"].values[:,].T[0]
        df = df.drop(["會計項目"], axis=1)
        df = df.apply(pd.to_numeric, errors='force')
        df = df[~df.index.duplicated(keep='first')]
        df = df.dropna().T

        df["會計項目"] = np.array([list(s) for s in df.index.values])[:, 0]
        df["stockId"] = stockId
        df["year"] = year
        df["season"] = season
        df["表格形式"] = sheetType

        df.index = np.array([list(s) for s in df.index.values])[:, 0]
        dicList = list(json.loads(df.T.to_json()).values())
        # add up datetime
        for dic in dicList:
            dic["timestamp"] = self.afterIFRS(int(year), int(season))
        return dicList

    def afterIFRS(self, year, season):
        season2date = [datetime(year, 5, 15),
                       datetime(year, 8, 14),
                       datetime(year, 11, 14),
                       datetime(year+1, 3, 31)]

        return pd.to_datetime(season2date[season-1].date())


    def getYearNseasonDictArray(self):
        startYear, startSeason = getLatestTimeFromFinStat(collectionName=self.collectionName)
        endYear, endSeason = self.now2LatestAvailableSeason()
        yearNseasonArray = []
        if startYear != endYear:
            # 處理startYear
            for i in range(startSeason, 5):
                yearNseasonArray.append({"year": str(startYear), "season": str(i)})
            # 處理第二年到endYear前一年
            yearArray = range(startYear+1, endYear-1)
            seasonArray = range(1,5)
            for year, season in itertools.product(yearArray, seasonArray):
                yearNseasonArray.append({"year": str(year), "season": str(season)})
            #處理endYear
            for s in range(1, endSeason+1):
                yearNseasonArray.append({"year": str(endYear), "season": str(s)})
        else:
            # 要求年份與現在同年
            for s in range(startSeason, endSeason+1):
                yearNseasonArray.append({"year": str(endYear), "season": str(s)})
        return yearNseasonArray

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

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))


# qtly = qtlyStockFinStat_Spider()
# process = CrawlerProcess({
#     'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
# })
# process.crawl(qtly)
# process.start()
# process.start()
# process.start()
# process.stop()
# qtly.itemRecord
# print(qtly.pdf)
# print(qtly.resp)
# qtly.resp.text
# qtly.itemRecord
