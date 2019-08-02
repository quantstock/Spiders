# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import pandas as pd
import scrapy
from htmltable_df.extractor import Extractor
from stockScrapyMongoDB.helpers import getStockIdArray, getLatestDayFromStockHold, getCollectionCrawleredDaysArray
import time

class stockHoldSpider(scrapy.Spider):
    name = 'weeklyStockHold'

    custom_settings = {
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 16,
        'MONGODB_COLLECTION': name,
        'COOKIES_ENABLED': False
    }

    def start_requests(self):
        #startTime = getLatestDayFromStockHold()
        startTime = getCollectionCrawleredDaysArray("weeklyStockHold")[-1]
        stockIdArray = getStockIdArray()
        url = 'https://www.tdcc.com.tw/smWeb/QryStockAjax.do'

        for date in pd.date_range(startTime, datetime.now(), freq='W-FRI'):
            scaDate = '{}{:02d}{:02d}'.format(date.year, date.month, date.day)
            timestamp = date

            for stockId in stockIdArray:
                payload = {
                    'scaDates': scaDate,
                    'scaDate': scaDate,
                    'SqlMethod': 'StockNo',
                    'StockNo': stockId,
                    'radioStockNo': stockId,
                    'StockName': '',
                    'REQ_OPR': 'SELECT',
                    'clkStockNo': stockId,
                    'clkStockName': ''
                }
                yield scrapy.FormRequest(url, formdata=payload, meta={'stockId': stockId, "timestamp": timestamp},
                                         dont_filter=True)
        #print(pd.date_range(startTime + timedelta(days=1), datetime.now(), freq='W-FRI'))

    def parse(self, response):
        m = response.meta

        data = Extractor(response.text, 'table.mt:eq(1)').df(1)
        del data['持股/單位數分級']
        data.loc[15, '序'] = '合計'
        data.columns = ['持股分級', '人數', '股數', '股數佔集保庫存數百分比']

        data.insert(0, 'stockId', m['stockId'])
        data.insert(0, 'timestamp', m["timestamp"])

        for item in data.to_dict('row'):
            yield item
            
