import extruct
from lxml import html
from bs4 import BeautifulSoup
import re
import dateparser
import feedparser
import requests
from waybackpy import WaybackMachineCDXServerAPI
from PyPDF2 import PdfReader

def normalize_date(date_str):
    dt = dateparser.parse(date_str)
    if dt:
        return dt.isoformat()
    return None

def extract_structured_data(html_content, url = None):
    tree = html.fromstring(html_content)
    data = extruct.extract(html_content, base_url = url, syntaxes = ['json-ld', 'microdata', 'rdfa'])
    dates = []
    for syntax in ['json-ld', 'microdata', 'rdfa']:
        for item in data.get(syntax, []):
            for key in ['datePublished', 'dateCreated', 'dateModified']:
                if key in item:
                    norm = normalize_date(item[key])
                    if norm:
                        dates.append({'source': f"structured_data:{syntax}:{key}", 'value': norm, 'confidence': 0.95})
    return dates

def extract_meta_tags(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    meta_dates = []
    meta_names = [
        'article:published_time', 'og:published_time', 'date', 'pubdate', 'dc.date', 'dc.date.issued', 'datePublished', 'dateCreated'
    ]
    for meta in soup.find_all('meta'):
        for attr in ['name', 'property', 'itemprop', 'http-equiv']:
            if meta.get(attr) in meta_names:
                content = meta.get('content')
                if content:
                    norm = normalize_date(content)
                    if norm:
                        meta_dates.append({'source': f"meta:{meta.get(attr)}", 'value': norm, 'confidence': 0.95})
    
    for time_tag in soup.find_all('time'):
        datetime_val = time_tag.get('datetime')
        if datetime_val:
            norm = normalize_date(datetime_val)
            if norm:
                meta_dates.append({'source': 'html:time', 'value': norm, 'confidence': 0.8})
    return meta_dates

def extract_url_date(url):
    patterns = [
        r'/(\d{4})/(\d{1,2})/(\d{1,2})/',
        r'/(\d{4})-(\d{1,2})-(\d{1,2})/',
        r'/(\d{4})(\d{2})(\d{2})/'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            date_str = '-'.join(match.groups())
            norm = normalize_date(date_str)
            if norm:
                return [{'source': 'url', 'value': norm, 'confidence': 0.7}]
    return []

def extract_feed_dates(feed_url, target_url):
    feed = feedparser.parse(feed_url)
    for entry in feed.entries:
        if entry.get('link') == target_url:
            for key in ['published', 'updated', 'pubDate']:
                if key in entry:
                    norm = normalize_date(entry[key])
                    if norm:
                        return [{'source': f"feed{key}", 'value': norm, 'confidence': 0.7}]
    return []

def extract_http_last_modified(url):
    try:
        resp = requests.head(url,allow_redirects = True, timeout = 10)
        last_modified = resp.headers.get('Last-Modified')
        if last_modified:
            norm = normalize_date(last_modified)
            if norm:
                return [{'source': 'http:last-modified', 'value': norm, 'confidence': 0.4}]
    except Exception:
        pass
    return []

def extract_web_archive_dates(url, user_agent = 'Mozilla/5.0'):
    try:
        cdx =   WaybackMachineCDXServerAPI(url, user_agent)
        oldest = cdx.oldest()
        if oldest:
            norm = oldest.datetime_timestamp.isoformat()
            return [{'source': 'wayback', 'value': norm, 'confidence': 0.7}]
    except Exception:
        pass
    return []

def extract_pdf_metadata(pdf_path):
    reader = PdfReader(pdf_path)
    meta = reader.metadata
    dates = []
    for key in ['/CreationDate', '/ModDate']:
        if key in meta:
            raw = meta[key].replace('D:', '').replace("'", "")
            norm = normalize_date(raw)
            if norm:
                dates.append({'source': f"pdf{key}", 'value': norm, 'confidence': 0.7})
    return dates

def aggregate_dates(evidence):
    date_groups = {}
    for item in evidence:
        date_key = item['value'][:10]

        date_groups.setdefault(date_key, []).append(item)

    best_date, best_score = None, 0.0
    for date_key, items in date_groups.items():
        score = sum(i['confidence'] for i in items)
        if score > best_score:
            best_date = items[0]['value']
            best_score = score
    
    max_score = sum(i['confidence'] for i in evidence)
    confidence = best_score / max_score if max_score else 0
    return best_date , confidence

def estimate_publication_date(url, html_content = None, pdf_path = None):
    evidence = []
    if pdf_path:
        evidence.extend(extract_pdf_metadata(pdf_path))
    else:
        if not html_content:
            resp = requests.get(url, timeout = 10)
            html_content = resp.text
        evidence.extend(extract_structured_data(html_content, url))
        evidence.extend(extract_meta_tags(html_content))
        evidence.extend(extract_url_date(url))
        evidence.extend(extract_http_last_modified(url))
        evidence.extend(extract_web_archive_dates(url))
    
    best_date, confidence = aggregate_dates(evidence)

    return {
        'url': url,
        'estimated_publication_date': best_date,
        'confidence_score': round(confidence,2),
        'evidence': evidence
    }