import re
from urllib.parse import urlparse
import scrapy
from scrapy import Request
from scrapy_playwright.page import PageMethod
from scrapy.http import HtmlResponse, TextResponse
import os
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


ROBUST_RE = re.compile(
    r"""
    \b(?:                 # word boundary and non-capturing group
        (?:precautionary|mandatory)\s+boil[- ]water\s+(?:advisory|notice|order|alert|advisories|notices|orders|alerts)
      | boil[- ]water\s+(?:advisory|notice|order|alert|advisories|notices|orders|alerts)
      | boil[- ]water     # bare phrase (less strict)
    )\b
    """,
    re.IGNORECASE | re.VERBOSE
)

DENY_EXTS = [
    '.pdf', '.png', '.jpg', '.jpeg', 'gif', '.svg', '.webp', '.ico', '.css', '.js', '.woff', '.woff2', '.ttf',
    '.eot', '.mp4', '.mp3', '.zip', '.rar', '.7z'
]

def is_probably_html(url):
    path = urlparse(url).path.lower()
    _, ext = os.path.splitext(path)
    return (ext == "") or (ext not in DENY_EXTS)

def same_reg_domain(a, b):
    ha = (urlparse(a).hostname or "").split(".")
    hb = (urlparse(b).hostname or "").split('.')
    return len(ha) >= 2 and len(hb) >= 2 and ha[-2:] == hb[-2:]

class BoilAdvisorySpider(scrapy.Spider):
    name = 'boil_advisory_spider'

    custom_settings ={
    # --- Politeness / scope ---
    "ROBOTSTXT_OBEY": True,
    "AUTOTHROTTLE_ENABLED": True,
    "AUTOTHROTTLE_START_DELAY": 1,
    "AUTOTHROTTLE_MAX_DELAY": 10.0,
    "DEPTH_LIMIT": 100,
    "CONCURRENT_REQUESTS": 32,
    "FEEDS": {
        'boil_links.csv': {
            "format": "csv",
            "overwrite": False,
            "encoding": "utf-8",
            "item_export_kwargs": {"include_headers_line": True},
        }
    },
    'DEFAULT_REQUEST_HEADERS': {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    },
    'USER_AGENT': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
    ),
    'PLAYWRIGHT_BROWSER_TYPE' : 'chromium',
    'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT' : 30000,
    'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT_PAGE' : 30000,

    'DOWNLOAD_HANDLERS': {
        'http': "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler'
    },

    'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
    
    'PLAYWRIGHT_BROWSER_TYPE': 'chromium',

    'PLAYWRIGHT_LAUNCH_OPTIONS': {
        'headless': True
    }
    

    }
    def __init__(self, start_url = None, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.start_url = start_url
        self.seed_domain = urlparse(start_url).hostname
        self.seen_urls = set()
        seed_host = (urlparse(start_url).hostname or "").lower().strip(".")
        self.allowed_domains = [seed_host]

    def start_requests(self):
        yield scrapy.Request(
            self.start_url,
            meta = {'playwright': True},
            dont_filter= True
        )

        # for url in self.start_url:
        #     yield Request(
        #         url,
        #         meta = {
        #             'playwright': True,
        #         },
        #         dont_filter = True,
        #         callback = self.parse
        #     )
    
    def parse(self, response):

        if not isinstance(response, (HtmlResponse, TextResponse)):
            self.logger.debug(f"Skipping non-text response: {response.url}")

        title = (response.css('title::text').get() or "").strip()
        url = response.url
        self.seen_urls.add(url)
        body_text = " ".join(response.css('p::text').getall())

        match = ROBUST_RE.search(title) or ROBUST_RE.search(url) or ROBUST_RE.search(body_text)
        if match:
            yield {
                'seed_url': self.start_url,
                'url': url,
                'title': title.lower(),
            }
        
        seed_host = urlparse(self.start_url).hostname or ""
        seed_reg = seed_host.split(".")[-2:] if seed_host else []

        for href in response.css('a::attr(href)').getall():
            href = href.strip()

            if not href or href.startswith(("mailto:", 'tel', '#')):
                continue
        
            target = response.urljoin(href)
            host = (urlparse(target).hostname or "")
            host_reg = host.split('.')[-2:]
            if seed_reg and host_reg == seed_reg:
                if target not in self.seen_urls:
                    self.seen_urls.add(target)
                    if is_probably_html(url):
                        yield scrapy.Request(target,meta = {'playwright': True}, callback = self.parse)

    