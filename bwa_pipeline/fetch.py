"""Download article text for candidate URLs.

Reads links.csv, fetches each URL with an identified user-agent, checks
robots.txt first, and extracts the article text with trafilatura
(readability-lxml as fallback). Pages too short to be real articles are
marked Loaded=False. When the live fetch fails, the Wayback Machine's
archived copy is tried before giving up.

No Common Crawl index-lookup path here: CC-NEWS has no usable URL index
(the community index was never verified), so searching it means streaming
whole monthly WARC files -- far too heavy for a per-article fallback.
Wayback CDX covers the same recovery job at a fraction of the cost.
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
import pandas as pd
import trafilatura
from bs4 import BeautifulSoup
from readability import Document

try:
    import config
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bwa_pipeline"))
    import config

TEXT_COLUMNS = ["Link", "Loaded", "Text", "publish_date", "source"]
WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"

_robots_cache = {}


def _allowed(url):
    """True unless the site's robots.txt disallows our user-agent."""
    domain = urlparse(url).netloc
    if domain not in _robots_cache:
        parser = RobotFileParser()
        try:
            resp = httpx.get(urljoin(url, "/robots.txt"), timeout=5,
                             headers={"User-Agent": config.USER_AGENT})
            parser.parse(resp.text.splitlines())
        except Exception:
            # No robots.txt (or unreachable): fail open.
            parser = None
        _robots_cache[domain] = parser
    parser = _robots_cache[domain]
    if parser is None:
        return True
    try:
        return parser.can_fetch(config.USER_AGENT, url)
    except Exception:
        return True


def _extract_text(html, url):
    """Best-effort article text: trafilatura first, readability second."""
    text = ""
    try:
        text = trafilatura.extract(html, url=url) or ""
    except Exception:
        text = ""
    if len(text) >= config.MIN_TEXT_CHARS:
        return text.strip()
    try:
        summary_html = Document(html).summary()
        fallback = BeautifulSoup(summary_html, "html.parser").get_text(" ")
        fallback = re.sub(r"\s+", " ", fallback).strip()
        if len(fallback) > len(text):
            text = fallback
    except Exception:
        pass
    return text.strip()


def _fetch_live(url):
    """One URL with retries. Returns (html, note) or (None, note)."""
    headers = {"User-Agent": config.USER_AGENT}
    waits = [0] + [2 ** i for i in range(config.FETCH_RETRIES)]
    with httpx.Client(timeout=config.FETCH_TIMEOUT, follow_redirects=True,
                      headers=headers) as client:
        for i, wait in enumerate(waits):
            if wait:
                time.sleep(wait)
            try:
                resp = client.get(url)
            except httpx.HTTPError as e:
                if i < len(waits) - 1:
                    continue
                return None, f"request failed: {e}"
            if resp.status_code == 429 and i < len(waits) - 1:
                continue
            if resp.status_code >= 500 and i < len(waits) - 1:
                continue
            if resp.status_code != 200:
                return None, f"HTTP {resp.status_code}"
            content_type = resp.headers.get("content-type", "")
            if "html" not in content_type and content_type:
                return None, f"not HTML ({content_type})"
            return resp.text, "live"
    return None, "request failed"


def _fetch_wayback(url):
    """Archived copy via the Wayback CDX API. Returns (html, note)."""
    try:
        resp = httpx.get(WAYBACK_CDX, params={
            "url": url, "output": "json",
            "filter": "statuscode:200", "limit": 1,
        }, timeout=20, follow_redirects=True)
        data = resp.json()
    except Exception as e:
        return None, f"wayback lookup failed: {e}"
    # First row is the header; rows after it are snapshots.
    if not isinstance(data, list) or len(data) < 2:
        return None, "no wayback snapshot"
    row = data[1]
    timestamp, original = row[1], row[2]
    snapshot = f"https://web.archive.org/web/{timestamp}id_/{original}"
    try:
        resp = httpx.get(snapshot, timeout=config.FETCH_TIMEOUT,
                         follow_redirects=True,
                         headers={"User-Agent": config.USER_AGENT})
    except httpx.HTTPError as e:
        return None, f"wayback fetch failed: {e}"
    if resp.status_code != 200:
        return None, f"wayback HTTP {resp.status_code}"
    return resp.text, "wayback"


def fetch_text(url):
    """Fetch and extract one article.

    Returns (loaded, text, note). loaded is False when nothing usable
    came back; note says why.
    """
    if not _allowed(url):
        return False, "", "blocked by robots.txt"
    html, note = _fetch_live(url)
    if html:
        text = _extract_text(html, url)
        if len(text) >= config.MIN_TEXT_CHARS:
            return True, text, "live"
        note = f"live text too short ({len(text)} chars)"
    print(f"  live fetch failed for {url} ({note}); trying Wayback")
    html, wb_note = _fetch_wayback(url)
    if html:
        text = _extract_text(html, url)
        if len(text) >= config.MIN_TEXT_CHARS:
            return True, text, "wayback"
        return False, "", f"wayback text too short ({len(text)} chars)"
    return False, "", wb_note


def fetch_all(links_csv, out_csv, limit=None):
    """Fetch every URL in links.csv, resuming past URLs already in out_csv."""
    links = pd.read_csv(links_csv, dtype=str).fillna("")
    done = set()
    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        done = set(pd.read_csv(out, dtype=str).fillna("")["Link"])

    new_file = not out.exists() or out.stat().st_size == 0
    count = ok = 0
    with open(out, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEXT_COLUMNS)
        if new_file:
            writer.writeheader()
        for _, link_row in links.iterrows():
            url = (link_row.get("url") or "").strip()
            if not url or url in done:
                continue
            if limit is not None and count >= limit:
                break
            count += 1
            loaded, text, note = fetch_text(url)
            writer.writerow({
                "Link": url,
                "Loaded": loaded,
                "Text": text if loaded else "",
                "publish_date": link_row.get("publish_date", ""),
                "source": link_row.get("source", ""),
            })
            f.flush()
            if loaded:
                ok += 1
                print(f"  [{count}] ok ({note}): {url[:80]}")
            else:
                print(f"  [{count}] FAILED ({note}): {url[:80]}")
    print(f"fetched {count} new links, {ok} with usable text")
    return count, ok


def main():
    parser = argparse.ArgumentParser(
        description="Fetch article text for discovered links.")
    parser.add_argument("--links", default=str(config.DATA_DIR / "links.csv"))
    parser.add_argument("--out", default=str(config.DATA_DIR / "texts.csv"))
    parser.add_argument("--limit", type=int, default=None,
                        help="cap the number of links fetched (smoke tests)")
    args = parser.parse_args()
    fetch_all(args.links, args.out, limit=args.limit)


if __name__ == "__main__":
    main()
