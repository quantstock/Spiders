# -*- coding: utf-8 -*-

import pymongo

class MongoPipeline(object):
    def __init__(self, collection_name, mongodb_uri, mongodb_dbName, mongodb_index):
        self.collection_name = collection_name
        self.mongodb_uri = mongodb_uri
        self.mongodb_dbName = mongodb_dbName
        self.mongodb_index = mongodb_index

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            collection_name = crawler.settings.get('MONGODB_COLLECTION'), #根據不同爬蟲，將資料放進不同的collection中
            mongodb_uri = crawler.settings.get('MONGODB_URI'),
            mongodb_dbName = crawler.settings.get('MONGODB_DATABASE'),
            mongodb_index = crawler.settings.get('MONGODB_INDEX')
        )

    def open_spider(self, spider):
        # 爬蟲開始後，開啟資料庫連接    
        self.client = pymongo.MongoClient(self.mongodb_uri)
        self.db = self.client[self.mongodb_dbName]
        self.createIndex()
        if self.collection_name == "proxy": 
            self.db[self.collection_name].drop()


    def close_spider(self, spider):
        # 爬蟲結束後，關閉資料庫連接    
        self.client.close()

    def process_item(self, item, spider):
        # 將每筆item插入資料庫
        self.db[self.collection_name].insert(dict(item))
        return item

    def createIndex(self):
    	if self.collection_name not in self.db.collection_names(): 
            # collection not existed, need to create index
            # self.db[self.collection_name].create_index([("timestamp",pymongo.ASCENDING),("stockId", pymongo.ASCENDING)])
            self.db[self.collection_name].create_index(self.mongodb_index)