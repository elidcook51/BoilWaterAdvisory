import requests
from bs4 import BeautifulSoup
import json
import trafilatura
import re
from readability import Document
import html2text
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time


def fetch_html(url):
    header = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    }
    try:
        response = requests.get(url, headers = header, timeout = 10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return fetch_with_headless_browser(url)

def fetch_with_headless_browser(url, wait_time = 10):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options = options)
        driver.get(url)
        driver.set_page_load_timeout(wait_time)
        html = driver.page_source
        driver.quit()
        print(f"Headless browser success for {url}")
        return html
    except Exception as e:
        print(f"Headless browser failed for {url}: {e}")
        return None

def extract_from_jsonld(html: str) -> dict | None:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all("script", type="application/ld+json"):
        # Some sites pack multiple JSON objects/arrays in one script tag
        try:
            data = json.loads(tag.string or "{}")
        except json.JSONDecodeError:
            continue

        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            t = obj.get("@type", "")
            if isinstance(t, list):
                is_article = any(x in ("NewsArticle", "Article", "ReportageNewsArticle") for x in t)
            else:
                is_article = t in ("NewsArticle", "Article", "ReportageNewsArticle")

            if is_article:
                out = {
                    "title": obj.get("headline") or obj.get("name"),
                    "author": None,
                    "date": obj.get("datePublished") or obj.get("dateCreated"),
                    "description": obj.get("description"),
                    "articleBody": obj.get("articleBody"),
                    "publisher": (obj.get("publisher") or {}).get("name")
                                 if isinstance(obj.get("publisher"), dict) else obj.get("publisher"),
                    "url": obj.get("url"),
                }
                # Normalize author(s)
                author = obj.get("author")
                if isinstance(author, list):
                    out["author"] = ", ".join(
                        [a.get("name") if isinstance(a, dict) else str(a) for a in author]
                    )
                elif isinstance(author, dict):
                    out["author"] = author.get("name")
                elif author:
                    out["author"] = str(author)

                # If articleBody is present, this is ideal.
                if out.get("articleBody"):
                    return out
                # If articleBody missing, keep metadata; we'll fill body via other extractors
                return out
    return None

def extract_main_text(html: str) -> str | None:
    # 3a) Trafilatura (very strong on news)
    downloaded = trafilatura.extract(html, output_format="txt", include_images=False, include_tables=False,
                                     favor_recall=False, with_metadata=False)
    if downloaded and len(downloaded.strip()) > 200:  # heuristic
        return downloaded.strip()

    # 3b) Readability
    try:
        doc = Document(html)
        article_html = doc.summary(html_partial=True)
        soup = BeautifulSoup(article_html, "lxml")
        for tag in soup(["script", "style", "noscript", "form", "footer", "nav", "aside"]):
            tag.decompose()
        text = soup.get_text(separator= '\n', strip = True)
        text = re.sub(r"\n{2,}", "\n\n", text).strip()
        if len(text) > 200:
            return text
    except Exception:
        pass

    # 3c) Very simple density fallback
    soup = BeautifulSoup(html, "lxml")
    for t in soup(["script", "style", "noscript", "form"]):
        t.decompose()

    best_node, best_score = None, 0
    for node in soup.find_all(["article", "section", "div", "main"]):
        text = node.get_text(separator= '\n', strip = True)
        link_count = len(node.find_all("a")) + 1
        score = len(text) / link_count  # crude text/link density
        if score > best_score and len(text) > 200:
            best_score, best_node = score, node
    if best_node:
        text = best_node.get_text(separator= '\n', strip = True)
        text = re.sub(r"\n{2,}", "\n\n", text)
        return text.strip()
    return None

def html_to_markdown(html: str) -> str:
    h = html2text.HTML2Text()
    h.ignore_links = False  # keep URLs if helpful
    h.ignore_images = True
    h.body_width = 0  # no wrapping
    md = h.handle(html)
    return "\n".join(line.rstrip() for line in md.splitlines()).strip()


def extract_article(url: str) -> dict:
    html = fetch_html(url)

    if not html:
        print(f"Skipping URL due to empty or failed fetch: {url}")
        return {
            "title": None,
            "author": None,
            "date": None,
            "publisher": None,
            "url": url,
            "description": None,
            'text': None,
        }

    
    meta = extract_from_jsonld(html) or {}
    body = meta.get("articleBody")

    if not body:
        body = extract_main_text(html)

    # If we still somehow have only HTML (from readability), ensure plaintext:
    if body and ("<" in body and ">" in body and "</" in body):
        body = BeautifulSoup(body, "lxml").get_text(separator = '\n', strip = True)

    # Fallback: convert whole HTML to markdown if everything else fails
    if not body:
        body = html_to_markdown(html)

    # Collapse whitespace
    body = "\n".join(chunk.strip() for chunk in body.splitlines() if chunk.strip())

    return {
        "title": meta.get("title"),
        "author": meta.get("author"),
        "date": meta.get("date"),
        "publisher": meta.get("publisher"),
        "url": meta.get("url") or url,
        "description": meta.get("description"),
        "text": body,
    }



