import re
from urllib.parse import urlparse
import scrapy


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
    "AUTOTHROTTLE_START_DELAY": 0.5,
    "AUTOTHROTTLE_MAX_DELAY": 5.0,
    "DEPTH_LIMIT": 4,
    "CONCURRENT_REQUESTS": 8,

    "FEEDS": {
        'boil_links.csv': {
            "format": "csv",
            "overwrite": False,
            "encoding": "utf-8",
            "item_export_kwargs": {"include_headers_line": True},
        }
    },

    }
    def __init__(self, start_url, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.start_url = start_url
        self.seed_domain = urlparse(start_url).hostname

    def start_requests(self):
        yield scrapy.Request(self.start_url, callback = self.parse)
    
    def parse(self, response):
        title = (response.css('title::text').get() or "").strip()
        url = response.url
        body_text = " ".join(response.css('p::text').getall())

        match = ROBUST_RE.search(title) or ROBUST_RE.search(url) or ROBUST_RE.search(body_text)
        if match:
            yield {
                'seed_url': self.start_url,
                'url': url,
                'title': title.lower()
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
                yield scrapy.Request(target, callback = self.parse)

    