"""Download article text for candidate URLs.

Reads links.csv, fetches each URL with an identified user-agent, checks
robots.txt first, and extracts the article text with trafilatura
(readability-lxml as fallback). Pages too short to be real articles are
marked Loaded=False. When the live fetch fails, the Wayback Machine's
archived copy is tried before giving up.

Pass --workers N to fetch N URLs concurrently (threads; the work is
network-bound, so threads are the right tool). Output order matches the
input order, and re-runs still resume past URLs already in the output
file, so a parallel run is interchangeable with a sequential one.

No Common Crawl index-lookup path here: CC-NEWS has no usable URL index
(the community index was never verified), so searching it means streaming
whole monthly WARC files -- far too heavy for a per-article fallback.
Wayback CDX covers the same recovery job at a fraction of the cost.
"""

import argparse
import csv
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
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
_robots_lock = threading.Lock()


def _allowed(url):
    """True unless the site's robots.txt disallows our user-agent."""
    domain = urlparse(url).netloc
    with _robots_lock:
        parser = _robots_cache.get(domain, False)
    if parser is False:
        parser = RobotFileParser()
        try:
            resp = httpx.get(urljoin(url, "/robots.txt"), timeout=5,
                             headers={"User-Agent": config.USER_AGENT})
            parser.parse(resp.text.splitlines())
        except Exception:
            # No robots.txt (or unreachable): fail open.
            parser = None
        with _robots_lock:
            _robots_cache.setdefault(domain, parser)
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


def _fetch_live(url, client=None):
    """One URL with retries. Returns (html, note) or (None, note).

    client: an httpx.Client to reuse (shared across threads; httpx
    clients are thread-safe). If None, a throwaway client is created,
    as before.
    """
    headers = {"User-Agent": config.USER_AGENT}
    waits = [0] + [2 ** i for i in range(config.FETCH_RETRIES)]
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=config.FETCH_TIMEOUT,
                              follow_redirects=True, headers=headers)
    try:
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
                # The site is explicitly asking us to slow down: honour
                # its Retry-After (capped at 30s) instead of hammering
                # again after 1s, which just earns another 429.
                retry_after = resp.headers.get("retry-after")
                if retry_after:
                    try:
                        time.sleep(min(float(retry_after), 30))
                    except ValueError:
                        pass
                continue
            if resp.status_code >= 500 and i < len(waits) - 1:
                continue
            if resp.status_code != 200:
                return None, f"HTTP {resp.status_code}"
            content_type = resp.headers.get("content-type", "")
            if "html" not in content_type and content_type:
                return None, f"not HTML ({content_type})"
            return resp.text, "live"
    finally:
        if own_client:
            client.close()
        else:
            # Fresh state per URL, as with the old per-URL client:
            # cookies a site set (Cloudflare, trackers) must not ride
            # along into the next site's request. Only real httpx
            # clients carry a cookie jar (test stubs do not).
            if isinstance(client, httpx.Client):
                client.cookies.clear()
    return None, "request failed"


def _fetch_wayback(url):
    """Archived copy via the Wayback CDX API. Returns (html, note).

    The CDX lookup is retried (3 attempts, 2s/4s backoff): a single
    slow lookup used to be a permanent FAILED for the URL, and under
    parallel load several lookups queue on archive.org at once, so a
    transient 20s timeout is common enough to need a second chance.
    Only the lookup is retried -- a real "no snapshot" answer is final.
    """
    data = None
    for attempt, wait in enumerate([0, 2, 4]):
        if wait:
            time.sleep(wait)
        try:
            resp = httpx.get(WAYBACK_CDX, params={
                "url": url, "output": "json",
                "filter": "statuscode:200", "limit": 1,
            }, timeout=20, follow_redirects=True)
            data = resp.json()
            break
        except Exception as e:
            if attempt == 2:
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


class _HostLimiter:
    """Cap concurrent requests per host, plus a separate cap for Wayback.

    With 50 workers and no per-host cap, one small newspaper could get 50
    simultaneous hits. With this, each host sees at most `per_host`
    concurrent requests (robots check + live fetch), while other hosts
    proceed in parallel, and web.archive.org gets its own small cap
    because every failed live fetch falls back to it. A worker never
    holds two slots at once, so there is no deadlock risk.

    Concurrency caps alone do not limit request RATE: two-at-a-time with
    fast responses still hits one host 10 times in a few seconds, and a
    75-worker burst hits 75 hosts at once from one IP -- both trip
    rate limiters. So the limiter also smooths request starts: at most
    `max_rate` requests per second overall, and at least `host_delay`
    seconds between two requests to the same host. Either is 0 to turn
    that floor off. Slots are reserved under one lock, so no two threads
    can reserve the same start time.
    """
    def __init__(self, per_host=2, wayback=3, host_delay=1.0, max_rate=10):
        if per_host < 1:
            raise ValueError("per_host must be >= 1")
        if wayback < 1:
            raise ValueError("wayback must be >= 1")
        self.per_host = per_host
        self.host_delay = host_delay
        self.global_interval = 1.0 / max_rate if max_rate else 0.0
        self._lock = threading.Lock()
        self._semaphores = {}
        self._wayback_sem = threading.Semaphore(wayback)
        self._next_global = 0.0
        self._next_host = {}

    def wait_turn(self, host):
        """Sleep until this request's reserved start time, if any.

        Both clocks are reserved from the request's actual start time,
        not from reservation time: with a deep global queue, reserving
        from `now` let two same-host requests start the global
        interval (0.1s) apart instead of `host_delay` (1.0s) apart.
        """
        with self._lock:
            now = time.monotonic()
            start = now
            if self.global_interval > 0:
                start = max(start, self._next_global)
                self._next_global = start + self.global_interval
            if self.host_delay > 0:
                start = max(start, self._next_host.get(host, 0.0))
                self._next_host[host] = start + self.host_delay
            wait = start - now
        if wait > 0:
            time.sleep(wait)

    def host_slot(self, host):
        with self._lock:
            sem = self._semaphores.get(host)
            if sem is None:
                sem = threading.Semaphore(self.per_host)
                self._semaphores[host] = sem
            return sem

    def wayback_slot(self):
        return self._wayback_sem


def fetch_text(url, client=None, limiter=None):
    """Fetch and extract one article.

    Returns (loaded, text, note). loaded is False when nothing usable
    came back; note says why. client, if given, is a shared httpx.Client
    reused across calls (and threads). limiter, if given, is a
    _HostLimiter capping concurrent requests per host (and to Wayback).
    Text extraction runs outside the host slot, so a slow page parse
    does not hold up the next request to the same site.
    """
    host = urlparse(url).netloc
    if limiter:
        limiter.wait_turn(host)
    host_sem = limiter.host_slot(host) if limiter else nullcontext()
    with host_sem:
        if not _allowed(url):
            return False, "", "blocked by robots.txt"
        html, note = _fetch_live(url, client=client)
    if html:
        text = _extract_text(html, url)
        if len(text) >= config.MIN_TEXT_CHARS:
            return True, text, "live"
        note = f"live text too short ({len(text)} chars)"
    print(f"  live fetch failed for {url} ({note}); trying Wayback")
    wb_sem = limiter.wayback_slot() if limiter else nullcontext()
    with wb_sem:
        html, wb_note = _fetch_wayback(url)
    if html:
        text = _extract_text(html, url)
        if len(text) >= config.MIN_TEXT_CHARS:
            return True, text, "wayback"
        return False, "", f"wayback text too short ({len(text)} chars)"
    return False, "", wb_note


def _fetch_one(args):
    """Worker for the thread pool: fetch one URL, never raise."""
    url, client, limiter = args
    try:
        loaded, text, note = fetch_text(url, client=client, limiter=limiter)
    except Exception as e:
        return url, False, "", f"worker failed: {e}"
    return url, loaded, text if loaded else "", note


def fetch_all(links_csv, out_csv, limit=None, workers=1, per_host=2,
              wayback_max=3, host_delay=1.0, max_rate=10):
    """Fetch every URL in links.csv, resuming past URLs already in out_csv.

    workers=1 is the original sequential loop. With workers=N, N URLs are
    fetched concurrently and rows are written in input order, so the output
    file is identical to a sequential run (modulo timing). per_host caps
    concurrent requests to any one website (default 2); wayback_max caps
    concurrent requests to web.archive.org (default 3); host_delay is the
    minimum gap in seconds between two requests to the same website
    (default 1.0); max_rate caps total requests per second across all
    websites (default 10, 0 for no cap). The rate floors only matter
    with workers > 1.
    """
    links = pd.read_csv(links_csv, dtype=str).fillna("")
    done = set()
    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        done = set(pd.read_csv(out, dtype=str).fillna("")["Link"])

    pending = []
    for _, link_row in links.iterrows():
        url = (link_row.get("url") or "").strip()
        if not url or url in done:
            continue
        if limit is not None and len(pending) >= limit:
            break
        pending.append((url, link_row.get("publish_date", ""),
                        link_row.get("source", "")))

    new_file = not out.exists() or out.stat().st_size == 0
    count = ok = 0
    with httpx.Client(timeout=config.FETCH_TIMEOUT, follow_redirects=True,
                      headers={"User-Agent": config.USER_AGENT}) as client:
        with open(out, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TEXT_COLUMNS)
            if new_file:
                writer.writeheader()

            def write_row(url, publish_date, source, loaded, text, note):
                nonlocal count, ok
                count += 1
                writer.writerow({
                    "Link": url,
                    "Loaded": loaded,
                    "Text": text if loaded else "",
                    "publish_date": publish_date,
                    "source": source,
                })
                f.flush()
                if loaded:
                    ok += 1
                    print(f"  [{count}] ok ({note}): {url[:80]}")
                else:
                    print(f"  [{count}] FAILED ({note}): {url[:80]}")

            limiter = (_HostLimiter(per_host=per_host,
                                   wayback=wayback_max,
                                   host_delay=host_delay,
                                   max_rate=max_rate)
                       if workers > 1 else None)
            if workers <= 1:
                for url, publish_date, source in pending:
                    loaded, text, note = fetch_text(url, client=client)
                    write_row(url, publish_date, source, loaded, text, note)
            else:
                jobs = [(url, client, limiter) for url, _, _ in pending]
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    for (url, loaded, text, note), (_, publish_date,
                                                    source) in zip(
                            pool.map(_fetch_one, jobs), pending):
                        write_row(url, publish_date, source,
                                  loaded, text, note)
    print(f"fetched {count} new links, {ok} with usable text")
    return count, ok


def main():
    parser = argparse.ArgumentParser(
        description="Fetch article text for discovered links.")
    parser.add_argument("--links", default=str(config.DATA_DIR / "links.csv"))
    parser.add_argument("--out", default=str(config.DATA_DIR / "texts.csv"))
    parser.add_argument("--limit", type=int, default=None,
                        help="cap the number of links fetched (smoke tests)")
    parser.add_argument("--workers", type=int, default=1,
                        help="fetch N URLs concurrently (default 1, "
                             "sequential; 8-16 is a good range for a big "
                             "harvest -- output order is unchanged)")
    parser.add_argument("--per-host", type=int, default=2,
                        help="max concurrent requests to one website "
                             "(default 2; only matters with --workers > 1)")
    parser.add_argument("--wayback-max", type=int, default=3,
                        help="max concurrent requests to web.archive.org "
                             "(default 3; only matters with --workers > 1)")
    parser.add_argument("--per-host-delay", type=float, default=1.0,
                        help="minimum seconds between two requests to the "
                             "same website (default 1.0; 0 for none; only "
                             "matters with --workers > 1)")
    parser.add_argument("--max-rate", type=float, default=10,
                        help="max requests per second across all websites "
                             "(default 10; 0 for no cap; only matters with "
                             "--workers > 1)")
    args = parser.parse_args()
    if args.per_host < 1:
        parser.error("--per-host must be >= 1")
    if args.wayback_max < 1:
        parser.error("--wayback-max must be >= 1")
    fetch_all(args.links, args.out, limit=args.limit, workers=args.workers,
              per_host=args.per_host, wayback_max=args.wayback_max,
              host_delay=args.per_host_delay, max_rate=args.max_rate)


if __name__ == "__main__":
    main()
