import pandas as pd
from pymongo import MongoClient
import datetime
import numpy as np
import stockScrapyMongoDB.settings as settings

def connect_mongo():
    """ A util for making a connection to mongo """
    mongo_uri = settings.MONGODB_URI
    db = settings.MONGODB_DATABASE
    conn = MongoClient(mongo_uri)
    return conn[db]

def getdfFromMongoDB(collection, cri=None, proj=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """
    # Connect to MongoDB
    db = connect_mongo()
    # Make a query to the specific DB and Collection
    cursor = db[collection].find(cri, proj)
    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))
    # Delete the _id
    # Delete the _id
    try: 
        if no_id:
            del df['_id']
    except KeyError:
        pass
    return df

def getRecordTradingDaysdf(startTime=None):
	# output: df index of datetime type
    try: 
        df = getdfFromMongoDB(collection="tradingDays")
        df["timestamp"] = df["timestamp"].apply(lambda x: pd.to_datetime(x))
        df = df.set_index("timestamp")
        if startTime is None: 
            df = df.index
        else: 
            df = df[startTime:].index
        return df
    except:
        return None

def getStockIdArray(ETF=True):
    # try: 
    if ETF: 
        cri = {"$or": [{"dtype": {'$eq': '股票'}}, {"dtype": {'$eq': "ETF"}}]}
    else: 
        cri = {"dtype": {'$eq': '股票'}}
    df = getdfFromMongoDB(collection = "stockId", cri=cri, proj={"code":1})
    stockIdArray = df.sort_values(by="code")["code"].values
    return stockIdArray
    # except: 
    #     return None

def getStockIdDictList(ETF=True):
    try: 
        if ETF: 
            cri = {"$or": [{"dtype": {'$eq': '股票'}}, {"dtype": {'$eq': "ETF"}}]}
        else: 
            cri = {"dtype": {'$eq': '股票'}}
        df = getdfFromMongoDB(collection = "stockId", cri=cri, proj={'code': 1, 'name': 1, '市場別': 1})
        df = df.sort_values(by="code")
        df = df.rename(columns = {'code':'stockId'})
        stockIdDictList = df.to_dict("records")        
        return stockIdDictList
    except: 
        return None

def getLatestDayFromCreditTrading():
    try: 
        df = getdfFromMongoDB(collection="dailyCreditTrading", cri=None, proj={"timestamp":1})
        return pd.to_datetime(df.iloc[-1].values[0])
    except: 
        return datetime.datetime(2008, 1, 1)

def getLatestDayFromStockLending():
    try: 
        df = getdfFromMongoDB(collection="dailyStockLending", cri=None, proj={"timestamp":1})
        return pd.to_datetime(df.iloc[-1].values[0])
    except: 
        return datetime.datetime(2005, 7, 1)   

def getLatestDayFromFundTrading():
    try: 
        df = getdfFromMongoDB(collection="dailyFundTrading", cri=None, proj={"timestamp":1})
        return pd.to_datetime(df.iloc[-1].values[0])
    except: 
        return datetime.datetime(2012, 5, 2)

def getLatestDayFromDailyPrice():
    try: 
        df = getdfFromMongoDB(collection="dailyPrice", cri=None, proj={"timestamp":1})
        return pd.to_datetime(df.iloc[-1].values[0])
    except: 
        return datetime.datetime(2004, 2, 11)

def getLatestDayFromDayTrading():
    try: 
        df = getdfFromMongoDB(collection="dailyTrading", cri=None, proj={"timestamp":1})
        return pd.to_datetime(df.iloc[-1].values[0])
    except: 
        return datetime.datetime(2014, 1, 6)

def getLatestMonthFromStockRev():
    try: 
        df = getdfFromMongoDB(collection="monthlyStockRev", cri=None, proj={"timestamp":1})
        return pd.to_datetime(df.iloc[-1].values[0])
    except: 
        return datetime.datetime(2010, 1, 1)

def getLatestDayFromStockHold():
    try: 
        # df = getdfFromMongoDB(collection="weeklyStockHold", cri=None, proj={"timestamp":1, "stockId":1})
        # df = df.sort_values(by="timestamp")
        # df = df.set_index("timestamp")
        # s_c = df.iloc[df.index==df.index[-1]].drop_duplicates()["stockId"].values
        db = connect_mongo()
        dates = list(db["weeklyStockHold"].distinct("timestamp"))    
        s_c = np.array(list(db["weeklyStockHold"].find({"timestamp":dates[0]}, {"stockId":1}).distinct("stockId")))
    except: 
        return datetime.datetime(2017,10, 5)

    s = getStockIdArray()
    c = np.in1d(s,s_c)
    try: 
        if 1 - c.sum()/len(s) < 0.01: # all crawlered
            time = dates[-1]
        else: 
            time = dates[-2]
    except:
        time = datetime.datetime(2017,10, 5)     
    return time

# def getLatestDayFromBrokerPoints():
    try: 
        df = getdfFromMongoDB(collection="dailyBrokerPoints", cri=None, proj={"timestamp":1, "stockId":1})
        df = df.sort_values(by="timestamp")
        df = df.set_index("timestamp")
        s_c = df.iloc[df.index==df.index[-1]].drop_duplicates()["stockId"].values
    except: 
        return datetime.datetime(2016,1,1)
    s = getStockIdArray()
    c = np.in1d(s,s_c)
    if 1 - c.sum()/len(s) < 0.1: 
        # all crawlered
        time = datetime.datetime.strftime(pd.to_datetime(df.index[-1]), "%Y%m%d")
    else: 
        try:
            index = df.index.drop_duplicates()
            time = datetime.datetime.strftime(pd.to_datetime(index[-2]), "%Y%m%d")
        except IndexError: 
            time = datetime.datetime(2016,1,1)
    return time

def getLatestDayFromBrokerPoints():
    db = connect_mongo()
    try: 
        date = list(db["dailyBrokerPoints"].find().sort('_id', -1).limit(1))[0]["timestamp"]
    except: 
    	date = datetime.datetime(2016,1,1)
    return date


def getLatestTimeFromFinStat(collectionName): 
    try: 
        df = getdfFromMongoDB(collection=collectionName, cri=None, proj={"timestamp":1, "stockId":1})
        df = df.sort_values(by="timestamp")
        df = df.set_index("timestamp")
        s_c = df.iloc[df.index==df.index[-1]].drop_duplicates()["stockId"].values
    except: 
        year,season = 2013, 1
        return year,season
    def date2season(datetime): 
        if datetime.month == 5: 
            season = 1
        elif datetime.month == 8:
            season = 2
        elif datetime.month == 11:
            season = 3
        elif datetime.month == 3: 
            season = 4
        else: 
            print("something strange...")

        if season == 4: 
            year = datetime.year - 1
        else: 
            year = datetime.year 

        return year, season
    
    s = getStockIdArray()
    c = np.in1d(s,s_c)
    if 1 - c.sum()/len(s) < 0.1: 
        # all crawlered
        year, season = date2season(pd.to_datetime(df.index[-1])) # 這是資料庫中對應到的年份與季
        # 轉換成要求爬蟲開爬的年與季
        if season != 4: 
            season += 1  
        else: 
            season = 1
            year += 1
    else: 
        try:
            year, season = date2season(pd.to_datetime(df.index[-1]))
        except IndexError: 
            year,season = 2013, 1
    return year,season 

def getProxyDict():
    try: 
        df = getdfFromMongoDB(collection="proxy", cri=None, proj={"scheme":1, "proxy":1, "port":1})
        return df.to_dict("records")
    except: 
        return None

def getCollectionCrawleredDaysArray(collection):
	# output list 
    try: 
        # df = getdfFromMongoDB(collection=collection, proj={"timestamp":1})
        db = connect_mongo()
        daysArray = np.array(list(db[collection].distinct("timestamp")))
    except: 
        daysArray = np.zeros([1])
    if daysArray.size == 0: 
    	daysArray = np.zeros([1])
    return daysArray


# def dropProxyCollection(): 
    # db = connect_mongo(db=db)
    # cursor = db["proxy"].drop()

    

# if __name__ == '__main__':
#     print(getStockIdArray(ETF=False))









