"""Scrapy settings for the MaxPreps crawler.

Tuned to be polite: MaxPreps is a large commercial site (CBS / PlayOnSports) that
rate-limits aggressive crawlers. AutoThrottle + a modest concurrency keep us under
the radar; retries with backoff handle the occasional 403/429. For a true
nationwide run you will likely still want rotating proxies (see README).
"""

BOT_NAME = "maxpreps_scraper"
SPIDER_MODULES = ["maxpreps_scraper.spiders"]
NEWSPIDER_MODULE = "maxpreps_scraper.spiders"

# --- identity & politeness ------------------------------------------------- #
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
ROBOTSTXT_OBEY = True

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# --- concurrency / throttling ---------------------------------------------- #
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 0.75
DOWNLOAD_TIMEOUT = 30

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 30.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 3.0
# AUTOTHROTTLE_DEBUG = True

# --- retries (incl. soft blocks) ------------------------------------------- #
RETRY_ENABLED = True
RETRY_TIMES = 4
RETRY_HTTP_CODES = [403, 429, 500, 502, 503, 504, 408, 522, 524]

# --- caching (handy while developing; disable for a fresh full crawl) ------- #
HTTPCACHE_ENABLED = False
HTTPCACHE_EXPIRATION_SECS = 86400
HTTPCACHE_DIR = "httpcache"
HTTPCACHE_IGNORE_HTTP_CODES = [403, 429, 500, 502, 503, 504]

# --- output ---------------------------------------------------------------- #
OUTPUT_DIR = "output"
SQLITE_FILE = "maxpreps.db"
ITEM_PIPELINES = {
    "maxpreps_scraper.pipelines.MultiFormatPipeline": 300,
}
FEED_EXPORT_ENCODING = "utf-8"

# --- misc ------------------------------------------------------------------ #
LOG_LEVEL = "INFO"
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
DUPEFILTER_DEBUG = False
