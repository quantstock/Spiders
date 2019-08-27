for i in range(1):
    from bs4 import BeautifulSoup
    import requests, re
    from tqdm import tqdm
    # import re, uniout
    import pandas as pd
    import scrapy as sp
    from scrapy.crawler import CrawlerProcess
    # from stockScrapyMongoDB.spiders.qtlyStockFinStat_spider import qtlyStockFinStat_Spider
    # from stockScrapyMongoDB.pipelines import MongoPipeline

    # from requests_testadapter import Resp

# a=pd.read_pickle('/Users/George/Documents/Finance_Management/remoteWenping/finlab/data/financial_statement/pack20191.pickle')
# a['balance_sheet']
# class LocalFileAdapter(requests.adapters.HTTPAdapter):
#     def build_response_from_file(self, request):
#         file_path = request.url[7:]
#         with open(file_path, 'rb') as file:
#             buff = bytearray(os.path.getsize(file_path))
#             file.readinto(buff)
#             resp = Resp(buff)
#             r = self.build_response(request, resp)
#
#             return r
#
#     def send(self, request, stream=False, timeout=None,
#              verify=True, cert=None, proxies=None):
#
#         return self.build_response_from_file(request)

# requests_session = requests.session()
# requests_session.mount('file://', LocalFileAdapter())
# requests_session.get('file://<some_local_path>')
# a = qtlyStockFinStat_Spider()
# reqList = list(a.start_requests())
# len(reqList)
# len(reqList)
# reqList[0]
# # 使用 GET 方式下載普通網頁
# r = requests.get('http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID=1101&SYEAR=2018&SSEASON=4&REPORT_ID=C')
# scrapy.download('http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID=1101&SYEAR=2018&SSEASON=4&REPORT_ID=C')
# res = a.Request('http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID=1101&SYEAR=2018&SSEASON=4&REPORT_ID=C')
#
#
# def funcRes(res):
#     meta = res.meta
#     filename = meta["stockId"] + ".html"
#     print(filename)
# scrapy.Request('http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID=1101&SYEAR=2018&SSEASON=4&REPORT_ID=C'
# , errback = lambda x: self.download_errback(x, url), callback=funcRes, meta={'stockId': "1101", 'year': 2018, 'season': 4})
# response = requests_session.get('file:///home/wenping/sean/stockScrapyMongoDB/financial_statement/20191/1104.html')
# response.encoding = 'utf-8'
def remove_english(s):
    result = re.sub(r'[a-zA-Z()]', "", s)
    return result
table20191 = pd.read_html('file:///home/wenping/sean/stockScrapyMongoDB/financial_statement/20191/1101.html')
df = table20191[0].copy()
dfname = df.columns.levels[0][0]
dfname
df = df.iloc[:,1:].rename(columns={'會計項目Accounting Title':'會計項目'})
df[(dfname,'會計項目')]
refined_name = df[(dfname,'會計項目')].str.split(" ").str[0].str.replace("　", "").apply(remove_english)
refined_name
subdf = df[dfname].copy()
subdf
subdf['會計項目'] = refined_name
df[dfname] = subdf
list(df.columns.levels[0])[0]
list(df.columns.levels[0])[0].split('日2')[0]+'日'
df.columns.codes[0]
newCol = []
for col in df.columns.levels[0]:
    s = col.split('日2')
    if len(s) > 1:
        newCol.append(s[0]+'日')
    else:
        newCol.append(col)
newCol = pd.Index(newCol)
# df.columns.levels[0][0]=list(df.columns.levels[0])[0].split('日2')[0]+'日'
df.columns = pd.MultiIndex(levels=[df.columns.levels[1], newCol],codes=[df.columns.codes[1], df.columns.codes[0]])
df.head()
BS20191 = table20191[0]
dfname = BS20191.columns.levels[0][0]
BS20191 = BS20191.iloc[:,1:].rename(columns={'會計項目Accounting Title':'會計項目'})
BS20191 =
# table = pd.read_html('file:///home/wenping/sean/stockScrapyMongoDB/financial_statement/20184/1101.html')
table20181 = pd.read_html('file:///home/wenping/sean/stockScrapyMongoDB/financial_statement/20181/1101.html')
len(table20191)
dfs = [pd.DataFrame()]+ table20191
len(dfs)
Balance_Sheet=table[0]
Balance_Sheet.head()
Balance_Sheet.columns = ['代號', '會計項目', '2019年3月31日2019/3/31', '2018年12月31日2018/12/31', '2018年3月31日2018/3/31']
# print(response.text)
# title = re.findall('class="zh">(.*?)</span>',response.text,re.S)
# print(title[19])
Name = "qtlyStockFinancialStatement"

Req = sp.Request('file://home/wenping/sean/stockScrapyMongoDB/financial_statement/20184/1101.html')
list(sp.Spider.start_requests(Req))
spd = qtlyStockFinStat_Spider()
item = spd.parse(Req)
list(item)
# mpl = MongoPipeline()
# mpl.process_item(item, spd)

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
process.crawl(spd)
process.stop()
