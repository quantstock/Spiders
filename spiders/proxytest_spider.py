import scrapy

class ProxyExampleSpider(scrapy.Spider):
    name = "testProxies"
    # start_urls = ['https://httpbin.org/ip']
    def start_requests(self):
        for i in range(10):
            yield scrapy.Request('https://httpbin.org/ip', dont_filter=True)

    def parse(self, response):
        print(response.text)