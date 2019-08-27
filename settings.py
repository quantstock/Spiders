# -*- coding: utf-8 -*-

# Scrapy settings for stockScrapyMongoDB project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'stockScrapyMongoDB'

SPIDER_MODULES = ['stockScrapyMongoDB.spiders']
NEWSPIDER_MODULE = 'stockScrapyMongoDB.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'stockScrapyMongoDB (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'stockScrapyMongoDB.middlewares.StockFormalSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'stockScrapyMongoDB.middlewares.StockFormalDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'stockScrapyMongoDB.pipelines.StockFormalPipeline': 300,
#}

MONGODB_URSER = "stockUser"
MONGODB_PASSWORD = "stockUserPwd"

# local
#<<<<<<< HEAD
MONGODB_URI = 'mongodb://%s:%s@localhost:27017/stock_data'%(MONGODB_URSER, MONGODB_PASSWORD)

# online


#=======
#MONGODB_URI = 'mongodb://%s:%s@slocalhost:27017/stock_data'%(MONGODB_URSER, MONGODB_PASSWORD)

# online

#MONGODB_URI = 'mongodb://localhost:27017/stock_data'
#>>>>>>> e95f65a5581be277e7c02fd276b32c0952418997
# MONGODB_URI = "mongodb://%s:%s@stockcluster-shard-00-00-knvb0.gcp.mongodb.net:27017,stockcluster-shard-00-01-knvb0.gcp.mongodb.net:27017,stockcluster-shard-00-02-knvb0.gcp.mongodb.net:27017/test?ssl=true&replicaSet=StockCluster-shard-0&authSource=admin&retryWrites=true"%(MONGODB_URSER, MONGODB_PASSWORD)

MONGODB_DATABASE = 'stock_data'

MONGODB_INDEX =  [("stockId", 1), ("timestamp", 1)]

ITEM_PIPELINES = {
   'stockScrapyMongoDB.pipelines.MongoPipeline': 300,
}

DOWNLOADER_MIDDLEWARES = {
   'stockScrapyMongoDB.middlewares.RandomProxyMiddleware': None,
   'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400
}

# MONGODB_API_secret_key ="335145ae-46f9-444e-a5c0-e92c98abd9cd"


# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

LOG_LEVEL = 'INFO'

RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 403, 404, 408]

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
