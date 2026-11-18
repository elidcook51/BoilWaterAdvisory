"""Collect candidate boil-water-advisory article URLs from free sources.

Sources: GDELT DOC API, Google News RSS, GNews, NewsData.io, Media Cloud,
and the Mississippi health department's current-notices page. Each source
has its own function; discover_all() runs the requested ones, dedupes by
canonical URL, and writes links.csv.

Sources that need an API key are skipped with a printed note when the key
is missing -- a missing key never crashes the run.
"""

import argparse
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin

import feedparser
import httpx
import pandas as pd

try:
    import config
    from common import canonicalize_url, parse_date
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bwa_pipeline"))
    import config
    from common import canonicalize_url, parse_date

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_QUERY = ('("boil water advisory" OR "boil water notice" OR "boil water order"'
               ' OR "boil your water") sourcecountry:us sourcelang:english')
GNEWS_URL = "https://gnews.io/api/v4/search"
NEWSDATA_URL = "https://newsdata.io/api/1/news"
MEDIACLOUD_URL = "https://search.mediacloud.org/api/search"
MSDH_LISTING = "https://msdh.ms.gov/safe-food-water-and-housing/water-supply/water-safety-notices"

LINK_COLUMNS = ["url", "source", "source_name", "publish_date",
                "capture_date", "title", "query"]

# GDELT rate-limits aggressively (shared IPs have seen 429s even with
# 20-75s spacing). Be patient, back off hard, and run GDELT harvests from
# your own machine/IP rather than a shared VM when you can.
GDELT_PAUSE = 10
GDELT_BACKOFFS = (30, 60, 120)

FAILED_COLUMNS = ["source", "window_start", "window_end", "reason",
                  "attempted_at"]


def _today():
    return date.today().isoformat()


def _row(url, source, source_name="", publish_date="", capture_date=None,
         title="", query=""):
    return {
        "url": url or "",
        "source": source,
        "source_name": source_name or "",
        "publish_date": publish_date or "",
        "capture_date": capture_date or _today(),
        "title": title or "",
        "query": query or "",
    }


def _retry_after_seconds(resp, cap=300):
    """Seconds from a Retry-After header, or None if absent/unusable.

    Only integer-second values are used; an HTTP-date value falls back
    to the scheduled backoff.
    """
    value = resp.headers.get("retry-after")
    if value is None:
        return None
    try:
        return max(0, min(int(value), cap))
    except ValueError:
        return None


def _get(url, params=None, timeout=30, backoffs=(2, 4)):
    """GET with retries on 429/5xx. Returns the response, or None.

    On a 429 the server's Retry-After header wins over the scheduled
    backoff (capped at 300s); the backoff tuple is the fallback.
    """
    waits = [0] + list(backoffs)
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for i in range(len(waits)):
            if waits[i]:
                time.sleep(waits[i])
            try:
                resp = client.get(url, params=params)
            except httpx.HTTPError as e:
                if i < len(waits) - 1:
                    print(f"  request failed ({e}); retrying")
                    continue
                print(f"  request failed ({e}); giving up")
                return None
            if resp.status_code == 429 and i < len(waits) - 1:
                retry_after = _retry_after_seconds(resp)
                if retry_after is not None:
                    waits[i + 1] = retry_after
                    print(f"  got 429; server asked for {retry_after}s; backing off")
                else:
                    print("  got 429; backing off")
                continue
            if resp.status_code >= 500 and i < len(waits) - 1:
                continue
            return resp
    return None


def _articles_from_gdelt(data):
    """Pull article dicts out of a GDELT DOC response, whatever its shape."""
    if not isinstance(data, dict):
        return []
    articles = data.get("articles") or []
    for entry in data.get("timeline") or []:
        if isinstance(entry, dict):
            articles = articles + (entry.get("toparticles") or entry.get("articles") or [])
    return [a for a in articles if isinstance(a, dict)]


def _gdelt_row(article, query):
    # GDELT gives no publisher date, only seendate (when GDELT captured
    # the article, usually the same day or the next). Use it as the
    # publish-date fallback so the extractor can resolve relative dates
    # ("yesterday", "last Tuesday") instead of leaving them null, and
    # keep it as the capture date too.
    seendate = parse_date(article.get("seendate"))
    seendate_iso = seendate.isoformat() if seendate else ""
    return _row(article.get("url"), "gdelt_doc",
                source_name=article.get("domain") or "",
                publish_date=seendate_iso,
                capture_date=seendate_iso,
                title=article.get("title") or "",
                query=query)


def _read_failed_windows(failed_csv):
    """Rows of failed_windows.csv as dicts, or [] if missing/unreadable."""
    path = Path(failed_csv)
    if not path.exists():
        return []
    try:
        return pd.read_csv(path, dtype=str).fillna("").to_dict("records")
    except Exception:
        return []


def _record_failed_window(failed_csv, source, window_start, window_end, reason):
    """Log a failed window, or just refresh attempted_at if already listed.

    A window appears at most once, keyed by (source, window_start,
    window_end), so re-running a failed week never duplicates the row.
    """
    path = Path(failed_csv)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = _read_failed_windows(path)
    key = (source, str(window_start), str(window_end))
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for r in rows:
        if (r.get("source"), r.get("window_start"), r.get("window_end")) == key:
            r["attempted_at"] = now
            r["reason"] = reason
            break
    else:
        rows.append({"source": source, "window_start": str(window_start),
                     "window_end": str(window_end), "reason": reason,
                     "attempted_at": now})
    pd.DataFrame(rows, columns=FAILED_COLUMNS).fillna("").to_csv(path, index=False)


def _clear_failed_window(failed_csv, source, window_start, window_end):
    """Drop one window from failed_windows.csv after a successful retry."""
    path = Path(failed_csv)
    if not path.exists():
        return
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
        keep = ~((df["source"] == source)
                 & (df["window_start"] == str(window_start))
                 & (df["window_end"] == str(window_end)))
        df[keep].to_csv(path, index=False)
    except Exception:
        return


def _gdelt_week(window_start, window_end, failed_csv):
    """Fetch one timelinevolinfo week. Returns (rows, failure_reason).

    failure_reason is None on success, even when the week genuinely has
    0 articles -- a real empty answer is not a failure. A failed week is
    logged FAILED and recorded in failed_windows.csv; it is never
    reported as 0 articles.
    """
    fmt = lambda d: d.strftime("%Y%m%d") + "000000"
    resp = _get(GDELT_DOC_URL, params={
        "query": GDELT_QUERY, "mode": "timelinevolinfo",
        "STARTDATETIME": fmt(window_start),
        "ENDDATETIME": fmt(window_end),
    }, backoffs=GDELT_BACKOFFS)
    if resp is None:
        reason = "request failed after retries"
    elif resp.status_code == 429 or resp.status_code >= 500:
        reason = f"HTTP {resp.status_code} after retries"
    else:
        try:
            data = resp.json()
        except ValueError:
            reason = "response was not valid JSON"
        else:
            found = [_gdelt_row(a, GDELT_QUERY) for a in _articles_from_gdelt(data)]
            print(f"  gdelt {window_start}..{window_end}: {len(found)} articles")
            return found, None
    print(f"  gdelt {window_start}..{window_end}: FAILED ({reason})")
    if failed_csv is not None:
        _record_failed_window(failed_csv, "gdelt_doc", window_start,
                              window_end, reason)
    return [], reason


def discover_gdelt_doc(since=None, until=None, failed_csv=None):
    """GDELT DOC API. Recent via artlist; historical via timelinevolinfo.

    timelinevolinfo answers go back to Jan 2017, but long responses get
    truncated mid-JSON around 40KB, so history is walked in weekly windows.
    Weeks that fail are recorded in failed_windows.csv (next to the
    links.csv, or config.DATA_DIR when called directly) instead of
    being silently reported as 0 articles.
    """
    if failed_csv is None:
        failed_csv = config.DATA_DIR / "failed_windows.csv"
    rows = []
    if since is None:
        resp = _get(GDELT_DOC_URL, params={
            "query": GDELT_QUERY, "mode": "artlist", "format": "json",
            "maxrecords": 250, "timespan": "24h", "sort": "datedesc",
        }, backoffs=GDELT_BACKOFFS)
        today = date.today()
        if resp is None:
            print("  gdelt artlist request failed; skipping")
            _record_failed_window(failed_csv, "gdelt_doc", today, today,
                                  "artlist request failed")
            return rows
        try:
            data = resp.json()
        except ValueError:
            print("  gdelt artlist returned non-JSON; skipping")
            _record_failed_window(failed_csv, "gdelt_doc", today, today,
                                  "artlist returned non-JSON")
            return rows
        if not data:
            print("  gdelt artlist returned an empty body; skipping")
            _record_failed_window(failed_csv, "gdelt_doc", today, today,
                                  "artlist returned an empty body")
            return rows
        rows = [_gdelt_row(a, GDELT_QUERY) for a in _articles_from_gdelt(data)]
        print(f"  gdelt artlist: {len(rows)} articles")
        return rows

    end = parse_date(until) or date.today()
    start = parse_date(since)
    window_start = start
    while window_start <= end:
        window_end = min(window_start + timedelta(days=6), end)
        found, _ = _gdelt_week(window_start, window_end, failed_csv)
        rows += found
        window_start = window_end + timedelta(days=1)
        if window_start <= end:
            time.sleep(GDELT_PAUSE)
    return rows


def retry_failed_windows(out_csv, failed_csv):
    """Re-run the GDELT windows listed in failed_windows.csv.

    Recovered rows are appended to out_csv (deduped by URL against
    existing rows). A window is removed from the failed list once it
    returns a real answer, even if that answer is 0 articles. A window
    that fails again stays listed, with attempted_at refreshed.
    """
    failed = [r for r in _read_failed_windows(failed_csv)
              if r.get("source") == "gdelt_doc"]
    if not failed:
        print(f"no failed gdelt_doc windows in {failed_csv}")
        return 0
    out = Path(out_csv)
    existing = set()
    if out.exists():
        try:
            existing = {canonicalize_url(u) for u in
                        pd.read_csv(out, usecols=["url"], dtype=str)["url"]
                        if isinstance(u, str)}
        except Exception:
            existing = set()
    recovered = []
    for entry in failed:
        ws = parse_date(entry.get("window_start"))
        we = parse_date(entry.get("window_end"))
        if ws is None or we is None:
            print(f"  skipping malformed failed-window row: {entry}")
            continue
        found, reason = _gdelt_week(ws, we, failed_csv)
        if reason is None:
            _clear_failed_window(failed_csv, "gdelt_doc", ws, we)
            for r in found:
                key = canonicalize_url(r["url"])
                if key and key not in existing:
                    existing.add(key)
                    r["url"] = key
                    recovered.append(r)
        time.sleep(GDELT_PAUSE)
    if recovered:
        out.parent.mkdir(parents=True, exist_ok=True)
        header = not out.exists() or out.stat().st_size == 0
        pd.DataFrame(recovered, columns=LINK_COLUMNS).to_csv(
            out, mode="a", header=header, index=False)
    print(f"retried {len(failed)} failed windows, "
          f"recovered {len(recovered)} new links")
    return len(recovered)


def _resolve_google_news_url(url):
    """Turn a news.google.com/rss/articles/CBMi... link into a real URL.

    These redirects do NOT resolve with a plain GET -- they need the gnews
    package (which uses Playwright) or the batchexecute trick. A plain GET
    just hangs, so it is only attempted for non-CBMi links. If decoding
    fails, the redirect URL is kept and marked rather than dropped.
    Returns (url, resolved_bool).
    """
    if "news.google.com" not in url:
        return url, True
    if "/rss/articles/CBM" in url:
        try:
            from gnews.utils.utils import resolve_url
            resolved = resolve_url(url)
            if resolved and "news.google.com" not in resolved:
                return resolved, True
        except Exception:
            pass
        return url, False
    try:
        resp = _get(url, timeout=10)
        if resp is not None and "news.google.com" not in str(resp.url):
            return str(resp.url), True
    except Exception:
        pass
    return url, False


def discover_google_news_rss(queries=None):
    """Google News RSS search feeds. Current coverage only, no archive."""
    rows = []
    for query in queries or config.DISCOVERY_QUERIES:
        resp = _get("https://news.google.com/rss/search", params={
            "q": query, "hl": "en-US", "gl": "US", "ceid": "US:en",
        })
        if resp is None:
            print(f"  google news rss ({query}): request failed; skipping")
            continue
        feed = feedparser.parse(resp.text)
        n = 0
        for entry in feed.entries:
            title = entry.get("title") or ""
            src = entry.get("source")
            if isinstance(src, dict):
                source_name = src.get("title") or src.get("href") or ""
            elif isinstance(src, str):
                source_name = src
            else:
                source_name = ""
            if not source_name and " - " in title:
                source_name = title.rsplit(" - ", 1)[-1]
            published = parse_date(entry.get("published"))
            url, resolved = _resolve_google_news_url(entry.get("link") or "")
            if not url:
                continue
            if not resolved:
                title = (title + " [unresolved Google redirect]").strip()
            rows.append(_row(url, "google_news_rss", source_name=source_name,
                             publish_date=published.isoformat() if published else "",
                             title=title, query=query))
            n += 1
        print(f"  google news rss ({query}): {n} entries")
    return rows


def discover_gnews(since=None, until=None):
    """GNews API. Free tier: 100 req/day, 30-day window, ~12h delay."""
    if not config.GNEWS_API_KEY:
        print("  gnews: no GNEWS_API_KEY set; skipping")
        return []
    rows = []
    for query in config.DISCOVERY_QUERIES:
        params = {"q": query, "token": config.GNEWS_API_KEY,
                  "lang": "en", "country": "us", "max": 10}
        # Date params are `from`/`to`, ISO 8601 with a timezone.
        if since:
            params["from"] = f"{since}T00:00:00Z"
        if until:
            params["to"] = f"{until}T23:59:59Z"
        resp = _get(GNEWS_URL, params=params)
        if resp is None:
            print(f"  gnews ({query}): request failed; skipping")
            continue
        try:
            articles = resp.json().get("articles") or []
        except ValueError:
            articles = []
        for a in articles:
            published = parse_date(a.get("publishedAt"))
            source = a.get("source") or {}
            rows.append(_row(a.get("url"), "gnews",
                             source_name=source.get("name") or "",
                             publish_date=published.isoformat() if published else "",
                             title=a.get("title") or "", query=query))
        time.sleep(1)
    print(f"  gnews: {len(rows)} articles")
    return rows


def discover_newsdata():
    """NewsData.io. Free tier covers the past 48 hours ONLY -- this is a
    daily cross-check source, not a backfill source. (Paid archive:
    Basic $199.99/mo = 6 months; Pro $349.99/mo = 2 years; Corporate = 10.)
    """
    if not config.NEWSDATA_API_KEY:
        print("  newsdata: no NEWSDATA_API_KEY set; skipping")
        return []
    rows = []
    for query in config.DISCOVERY_QUERIES:
        resp = _get(NEWSDATA_URL, params={
            "apikey": config.NEWSDATA_API_KEY, "q": query,
            "language": "en", "country": "us",
        })
        if resp is None:
            print(f"  newsdata ({query}): request failed; skipping")
            continue
        try:
            results = resp.json().get("results") or []
        except ValueError:
            results = []
        for a in results:
            published = parse_date(a.get("pubDate"))
            rows.append(_row(a.get("link"), "newsdata",
                             source_name=a.get("source_name") or "",
                             publish_date=published.isoformat() if published else "",
                             title=a.get("title") or "", query=query))
        time.sleep(1)
    print(f"  newsdata: {len(rows)} articles")
    return rows


def discover_mediacloud(queries=None):
    """Media Cloud search API. Metadata + URLs only (no full text, no
    snippet) -- a discovery layer, not a text source. Quota: 1,000 calls
    and 20,000 stories per 7 days on a research key.
    """
    if not config.MEDIACLOUD_API_KEY:
        print("  mediacloud: no MEDIACLOUD_API_KEY set; skipping")
        return []
    rows = []
    for query in queries or config.DISCOVERY_QUERIES:
        resp = _get(MEDIACLOUD_URL, params={
            "q": query, "api_key": config.MEDIACLOUD_API_KEY,
        })
        if resp is None:
            print(f"  mediacloud ({query}): request failed; skipping")
            continue
        try:
            docs = resp.json().get("response", {}).get("docs") or []
        except ValueError:
            docs = []
        for d in docs:
            published = parse_date(d.get("publish_date"))
            rows.append(_row(d.get("url"), "mediacloud",
                             source_name=d.get("media_name") or "",
                             publish_date=published.isoformat() if published else "",
                             title=d.get("title") or "", query=query))
        time.sleep(1)
    print(f"  mediacloud: {len(rows)} stories")
    return rows


def discover_gov():
    """Mississippi health department's current boil-water-notices page.

    Small supplement: one state, current notices only, no archive. The
    listing links to one page per notice; each detail page carries the
    system name, county, and effective date. Anything that fails here is
    skipped -- this source must never break a run. Note: as of Nov 2026
    the listing is a JS-rendered app, so a plain HTML fetch usually finds
    no notice links and this source returns nothing. That is expected;
    fixing it would need a Playwright render or finding the site's data
    API, which is not worth it for one state.
    """
    import re
    from bs4 import BeautifulSoup

    try:
        resp = _get(MSDH_LISTING, timeout=20)
        if resp is None:
            print("  gov (msdh): listing page unreachable; skipping")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Old-style notice pages, or new-style detail pages under the
            # same section as the listing (but not the listing itself).
            if re.search(r"/page/23,\d+,148\.html", href):
                links.append(a)
            elif "water-safety" in href and href.rstrip("/").lower() != \
                    MSDH_LISTING.rstrip("/").lower() and "/water-safety-notices/" in href:
                links.append(a)
    except Exception as e:
        print(f"  gov (msdh): failed to parse listing ({e}); skipping")
        return []

    if not links:
        print("  gov (msdh): no notice links in the listing page "
              "(JS-rendered); skipping")
        return []

    rows = []
    for a in links[:50]:
        url = urljoin(MSDH_LISTING, a["href"])
        title = (a.get_text() or "").strip()
        publish_date = ""
        try:
            detail = _get(url, timeout=20)
            if detail is not None:
                m = re.search(r"Effective Date:\s*(\d{1,2}/\d{1,2}/\d{4})",
                              detail.text)
                if m:
                    d = parse_date(m.group(1))
                    publish_date = d.isoformat() if d else ""
        except Exception:
            pass
        rows.append(_row(url, "gov", source_name="MSDH",
                         publish_date=publish_date, title=title, query=""))
    print(f"  gov (msdh): {len(rows)} notices")
    return rows


SOURCES = {
    "gdelt_doc": discover_gdelt_doc,
    "google_news_rss": discover_google_news_rss,
    "gnews": discover_gnews,
    "newsdata": discover_newsdata,
    "mediacloud": discover_mediacloud,
    "gov": discover_gov,
}


def discover_all(out_csv, since=None, until=None, sources=None):
    """Run the requested sources, dedupe by canonical URL, write links.csv."""
    wanted = sources or list(SOURCES)
    rows = []
    for name in wanted:
        fn = SOURCES.get(name)
        if fn is None:
            print(f"  unknown source '{name}'; skipping")
            continue
        print(f"discovering from {name}...")
        try:
            if name == "gdelt_doc":
                rows += fn(since, until,
                           Path(out_csv).parent / "failed_windows.csv")
            elif name == "gnews":
                rows += fn(since, until)
            else:
                rows += fn()
        except Exception as e:
            print(f"  {name} failed ({e}); continuing with the rest")

    seen = set()
    unique = []
    for r in rows:
        key = canonicalize_url(r["url"])
        if not key or key in seen:
            continue
        seen.add(key)
        r["url"] = key
        unique.append(r)
    print(f"collected {len(rows)} links, {len(unique)} unique after dedupe")

    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(unique, columns=LINK_COLUMNS)
    df.to_csv(out, index=False)
    print(f"wrote {out} ({len(df)} rows)")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Discover boil-water-advisory article URLs.")
    parser.add_argument("--out", default=str(config.DATA_DIR / "links.csv"))
    parser.add_argument("--since", default=None,
                        help="start date YYYY-MM-DD (gdelt/gnews history)")
    parser.add_argument("--until", default=None, help="end date YYYY-MM-DD")
    parser.add_argument("--sources", default=None,
                        help="comma-separated subset of: " + ",".join(SOURCES))
    parser.add_argument("--limit", type=int, default=None,
                        help="cap the number of links written (smoke tests)")
    parser.add_argument("--retry-failed", nargs="?", const="", default=None,
                        metavar="FAILED_CSV",
                        help="retry only the GDELT windows in failed_windows.csv "
                             "(default: failed_windows.csv next to --out)")
    args = parser.parse_args()

    if args.retry_failed is not None:
        failed_path = (Path(args.retry_failed) if args.retry_failed
                       else Path(args.out).parent / "failed_windows.csv")
        retry_failed_windows(args.out, failed_path)
        return

    sources = [s.strip() for s in args.sources.split(",")] if args.sources else None
    df = discover_all(args.out, since=args.since, until=args.until,
                      sources=sources)
    if args.limit is not None and len(df) > args.limit:
        df = df.head(args.limit)
        df.to_csv(args.out, index=False)
        print(f"limited to {args.limit} links")


if __name__ == "__main__":
    main()
