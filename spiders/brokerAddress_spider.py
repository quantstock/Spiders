import scrapy
import pandas as pd
from scrapy import FormRequest
from pyquery import PyQuery

brokerList_url = 'http://www.twse.com.tw/brokerService/brokerList?response=html&lang=zh'
branchList_url = 'http://www.twse.com.tw/brokerService/branchList.html'


class BrokerAddressSpider(scrapy.Spider):
    name = 'brokerAddress'
    start_urls = [brokerList_url, branchList_url]

    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 1,
        'MONGODB_COLLECTION': name,
        'MONGODB_ITEM_CACHE': 1,
        'MONGODB_UNIQ_KEY': [("證券商代號", 1)],
    }

    def parse(self, response):
        data = pd.read_html(response.text)
        data = data[0]
        del data['分公司']
        if response.url == brokerList_url:
            data['公司'] = '總公司'
        else:
            data['公司'] = '分公司'

        url = 'https://moisagis.moi.gov.tw/moiap/gis2010/content/user/matchservice/singleMatch.cfm'

        for info in data.to_dict('records'):
            yield FormRequest(url, meta={'info': info},
                              formdata={
                                  'address': info['地址'],
                                  'matchRange': '0',
                                  'fuzzyNum': '02',
                                  'roadEQstreet': 'true',
                                  'subnumEQnum': 'true',
                                  'isLockTown': 'false',
                                  'isLockVillage': 'false',
                                  'ex_coor': 'EPSG:4326',
                                  'U02DataYear': '2015',
                                  'output_xml': '1'
                              },
                              callback=self.parse_info, dont_filter=True)

    def parse_info(self, response):
        item = response.meta['info']
        ths = [th.text() for th in PyQuery(response.text)('tr').eq(0)('th').items()]
        tds = [td.text() for td in PyQuery(response.text)('.bwhite').eq(0)('td').items()]
        item.update(dict(zip(ths, tds)))
        yield item

