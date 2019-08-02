# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import re
import pandas as pd
import scrapy
from htmltable_df.extractor import Extractor
from pyquery import PyQuery
from stockScrapyMongoDB.helpers import getLatestMonthFromStockRev
import time
from dateutil.relativedelta import relativedelta

stock_type = {'國內上市': 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_{}_{}_0.html'}
              # '國外上市': 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_{}_{}_1.html'
              # '國內上櫃': 'http://mops.twse.com.tw/nas/t21/otc/t21sc03_{}_{}_0.html',
              # '國外上櫃': 'http://mops.twse.com.tw/nas/t21/otc/t21sc03_{}_{}_1.html',
              # '國內興櫃': 'http://mops.twse.com.tw/nas/t21/rotc/t21sc03_{}_{}_0.html',
              # '國外興櫃': 'http://mops.twse.com.tw/nas/t21/rotc/t21sc03_{}_{}_1.html',
              # '國內公發公司': 'http://mops.twse.com.tw/nas/t21/pub/t21sc03_{}_{}_0.html',
              # '國外公發公司': 'http://mops.twse.com.tw/nas/t21/pub/t21sc03_{}_{}_1.html'
              


class monthlyStockRevSpider(scrapy.Spider):
    name = "monthlyStockRev"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 16,
        'MONGODB_COLLECTION': name,
        'MONGODB_INDEX': [("YY", -1), ("MM", 1), ("stockId", 1), ("timestamp", 1)]
        }

    def start_requests(self):
        startTime = getLatestMonthFromStockRev()
        startYY = startTime.year 
        startMM = startTime.month

        for date in pd.date_range(startTime, datetime.now(), freq='M'):
            for key, val in stock_type.items():
                YY = date.year - 1911
                MM = date.month
                url = val.format(YY, MM)
                recordeDate = date + relativedelta(months=1)
                timestamp = datetime(recordeDate.year, recordeDate.month, 10)
                yield scrapy.Request(url, 
                	encoding = 'big5',
                	callback = self.parse,
                	meta={'STOCK_TYPE': key, 'YY': str(YY), 'MM': str(MM), "timestamp": timestamp},
                	errback = lambda x: self.download_errback(x, url))

    def parse(self, response):
        meta = response.meta
        try: 
            text = response.text.encode('ISO-8859-1').decode('big5', 'replace')
        except UnicodeEncodeError:
            text = response.text
        
        for table in PyQuery(text)('table[bgcolor="#FFFFFF"]').items():
            treq0 = table.parent().parent().parent()('tr:eq(0)')
            INDUSTRY_TYPE = re.search('產業別：(\w+)', treq0("th:contains('產業別')").text()).group(1)

                
            extractor = Extractor(table)
            data = extractor.df()[:-1]
            data = data.applymap(lambda x: x.strip().replace(',', ''))
            data.columns = [s.replace("(%)", "百分比") for s in data.columns.values]
        
            data.insert(0, 'MM', meta.get('MM'))
            data.insert(0, 'YY', meta.get('YY'))
            data.insert(0, 'timestamp', meta.get('timestamp'))
            data.insert(0, 'INDUSTRY_TYPE', INDUSTRY_TYPE)
            data.insert(0, 'STOCK_TYPE', meta.get('STOCK_TYPE'))
        
            for r in data.to_dict('row'):
                yield r

    def download_errback(self, e, url):
        yield scrapy.Request(url, callback = self.parse, errback = lambda x: self.download_errback(x, url))        
