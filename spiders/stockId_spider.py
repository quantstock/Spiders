import scrapy
from pyquery import PyQuery

TWSE_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
TPEX_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=4'

columns = ['dtype', 'code', 'name', '國際證券辨識號碼', '上市日', '市場別', '產業別', 'CFI']


class StockIdSpider(scrapy.Spider):
    name = 'stockId'
    start_urls = [TWSE_URL, TPEX_URL]

    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 1,
        'MONGODB_COLLECTION': name,
        'MONGODB_ITEM_CACHE': 1000,
        'MONGODB_DROP': True
    }

    def parse(self, response):
        for tr in PyQuery(response.text)('.h4 tr:eq(0)').next_all().items():
            if tr('b'):
                dtype = tr.text()
            else:
                row = [td.text() for td in tr('td').items()]
                code, name = row[0].split('\u3000')
                yield dict(zip(columns, [dtype, code, name, *row[1: -1]]))