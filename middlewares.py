# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
from collections import defaultdict
import json
import random
from stockScrapyMongoDB.helpers import getProxyDict


class RandomProxyMiddleware(HttpProxyMiddleware):

    def __init__(self, auth_encoding="latin-1"):
        self.auth_encoding = auth_encoding
        self.proxies = defaultdict(list)

        proxyDict = getProxyDict()
        for proxy in proxyDict:
            scheme = proxy["scheme"]
            url = proxy["proxy"]
            self.proxies[scheme].append(self._get_proxy(url, scheme))

    @classmethod
    def from_crawler(cls, crawler):
        auth_encoding = crawler.settings.get("HTTPPROXY_AUTH_ENCODING", "latin-1")
        return cls(auth_encoding)

    def _set_proxy(self, request, scheme):
        creds, proxy = random.choice(self.proxies[scheme])
        request.meta["proxy"] = proxy
        if creds:
            request.headers["Proxy-Authorization"] = b"Basic" + creds


