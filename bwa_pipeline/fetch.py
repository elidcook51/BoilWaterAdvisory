"""Download article text for candidate URLs (v4: live-only).

Reads links.csv, fetches each URL with an identified user-agent, checks
robots.txt first, and extracts the article text with trafilatura
(readability-lxml as fallback). Pages too short to be real articles are
marked Loaded=False.

v4 is the live pass ONLY. A live failure is recorded in SQLite as
wayback-retry (deferred) and the run moves on: this module never calls
the Wayback Machine, never waits on it, and never knows it exists. The
separate archive.py command works over the deferred rows later, from
any machine, with its own circuit breaker. That decoupling is the fix
for the v3 stall (21 rows in 107 min, 2026-11-19): a blocked CDX can no
longer serialize the live run, because the live worker cannot call it.

Pass --workers N to fetch N URLs concurrently (threads; the work is
network-bound, so threads are the right tool -- asyncio measured no
better). Default 20 workers, the measured knee. Results are recorded
in SQLite (URL-keyed) as they complete, out of order, and texts.csv is
rendered from the database in input order at the end, so the CSV
contract is kept without in-order chunk writes amplifying a stall.
Re-running resumes past finished URLs by URL key; nothing is
re-fetched. A worker never dies on one URL: per-URL failures become
rows, and a bad SQLite write is logged, not fatal.

v4 kept from v3: two-tier HTTP (httpx, then curl_cffi
impersonate="chrome" for hosts that 403/429 httpx), a per-host
AIMD/AutoThrottle controller with upward-only jitter, host parking
(blocked hosts skip the live attempt and go straight to the deferred
queue), challenge-page detection, a shared httpx client with
connection pooling, and a JSON run manifest next to the output file.
No proxy tier: parked hosts go to the deferred queue, never through a
proxy.

v4 removed vs v3: the inline Wayback fallback (moved to archive.py),
the per-URL live retry loop (one live attempt; retries belong to a
separate --retry-failed pass), chunked in-order writes (replaced by
SQLite-then-export), and the texts.csv-scan resume fallback (the state
DB is the resume index; at backfill scale the scan is O(gigabytes)).
Read timeout is 8s (down from 15s): timeouts consumed ~55% of
worker-seconds at 15s, and cutting to 8s is worth ~20-25% throughput.
"""

import argparse
import csv
import datetime
import email.utils
import json
import os
import random
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

try:
    from state import RunState
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from state import RunState

TEXT_COLUMNS = ["Link", "Loaded", "Text", "publish_date", "source"]

# Per-phase timeouts for live fetches (v4 per-URL budget). Honest
# worst case per URL, excluding time queued behind a busy host: one
# live attempt (5s connect + 8s read) + at most one curl_cffi attempt
# at the same timeouts + up to 4.5s jittered backoff -- about 35s.
# Anything over budget is deferred to the archive queue or a
# --retry-failed pass instead of retried in place.
_LIVE_TIMEOUT = httpx.Timeout(
    connect=getattr(config, "FETCH_CONNECT_TIMEOUT", 5),
    read=getattr(config, "FETCH_READ_TIMEOUT", 8),
    write=getattr(config, "FETCH_WRITE_TIMEOUT", 10),
    pool=getattr(config, "FETCH_POOL_TIMEOUT", 5))

_robots_cache = {}
_robots_lock = threading.Lock()

# Browsers send a full header set with every navigation; a bare
# User-Agent alone is what bot filters key on (one site 429'd the bot
# UA alone and answered 200 once these headers were added, with the
# bot UA kept). These defaults are sent on every request the fetcher
# makes (live, robots.txt, Wayback). Point BROWSER_HEADERS_FILE at a
# JSON file of your own browser's headers to use those instead -- a
# User-Agent in the file replaces the bot UA too.
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

_headers_cache = None

# Markers of a bot-challenge / interstitial page served with HTTP 200
# (Cloudflare and friends). A 200 carrying one of these is a blocked
# outcome, not a loaded article -- it must never reach texts.csv.
CHALLENGE_MARKERS = (
    "just a moment",
    "checking your browser",
    "cf-chl-",
    "ray id",
    "attention required",
    "cf-turnstile",
    "verify you are human",
    "please verify you are",
)


def _is_challenge(html):
    """True when a 200 body looks like a challenge page, not an article."""
    if not html:
        return False
    lowered = html.lower()
    return any(marker in lowered for marker in CHALLENGE_MARKERS)


def _jitter_up(seconds):
    """Upward-only jitter: base * uniform(1.0, 1.5).

    Textbook backoff uses full jitter (uniform(0, base)), but the v2.7
    timing contracts are lower bounds -- same-host gaps, the global
    rate, Retry-After sleeps -- so jitter here may only stretch a wait,
    never shrink it below the floor it is enforcing.
    """
    if not seconds:
        return 0.0
    return seconds * random.uniform(1.0, 1.5)


# Stop flag for prompt Ctrl+C exit. Set by fetch_all's
# KeyboardInterrupt handler; every pace / backoff sleep in a worker
# thread waits on it instead of time.sleep, so a worker wakes the
# moment the run is interrupted instead of sleeping out a 60s
# host-wide pause while holding its slot. (SIGINT itself only ever
# lands in the main thread, so worker threads cannot rely on
# KeyboardInterrupt to wake them.)
_STOP = threading.Event()


class _StopRequested(Exception):
    """Raised in a worker thread when the run was interrupted mid-sleep."""


def _sleep(seconds):
    """Interruptible sleep: wait on _STOP, raising if the run stopped.

    Returns silently once the full interval has elapsed with no stop.
    Identical to time.sleep when the run is not stopping.
    """
    if not seconds:
        return
    if _STOP.wait(seconds):
        raise _StopRequested("run interrupted")


def _parse_retry_after(value):
    """Retry-After in seconds, from the seconds form or an HTTP-date.

    Returns None when the header is absent or unparseable. A date in
    the past is 0 (retry now). It feeds only the host-wide pause in
    _HostLimiter: no worker ever sleeps on it per URL, because the
    live fetch does not retry and does not sleep on Retry-After.
    """
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    return max(0.0, (when - now).total_seconds())


def _snippet(body, length=200):
    """First ~200 chars of a body, collapsed to one line, for logs."""
    try:
        return re.sub(r"\s+", " ", str(body or "")[:length]).strip()
    except Exception:
        return ""


class _RunContext:
    """Per-run mutable state shared by the fetch helpers.

    fetch_text's pinned call shape (url, client, limiter) leaves no
    room for a config parameter, so run-wide settings (curl tier flag,
    robots cache path, parked hosts, outcome stats) travel on this
    module-global context, replaced fresh by each fetch_all call.
    Direct calls to fetch_text/_fetch_live outside fetch_all use the
    default context (curl tier on, no disk caches, nothing parked).
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.curl_cffi = True
        self.state = None    # RunState, set by fetch_all; None for
                             # direct fetch_text/_fetch_live calls,
                             # where there is no DB to persist to
        self.robots_cache_path = None
        self.parked = {}       # host -> reason
        self.host_stats = {}   # host -> outcome counters + latencies
        self.counters = {"curl_cffi_ok": 0}


_CTX = _RunContext()


def _host_stat(host):
    """The _CTX stats record for a host, creating it if needed."""
    stat = _CTX.host_stats.get(host)
    if stat is None:
        stat = {"requests": 0, "ok": 0, "rate_limited": 0, "blocked": 0,
                "terminal": 0, "timeout": 0, "challenge": 0,
                "cdx_errors": 0, "latencies": [], "consec_blocked": 0}
        _CTX.host_stats[host] = stat
    return stat


def _record_outcome(host, outcome, latency=None, limiter=None,
                    retry_after=None):
    """Record one classified outcome for a host, in _CTX and the limiter.

    outcome is one of ok / rate-limited / blocked / terminal /
    challenge / timeout. Parking is decided here, from the host's own
    history: a challenge page always parks, a 403 parks when the host
    has succeeded before (or 403s three times running with no success).
    """
    if not host:
        return
    with _CTX.lock:
        stat = _host_stat(host)
        stat["requests"] += 1
        key = {"ok": "ok", "rate-limited": "rate_limited",
               "blocked": "blocked", "terminal": "terminal",
               "timeout": "timeout", "challenge": "challenge"}.get(outcome)
        if key:
            stat[key] += 1
        if latency is not None:
            stat["latencies"].append(latency)
            if len(stat["latencies"]) > 500:
                stat["latencies"] = stat["latencies"][-500:]
        if outcome == "ok":
            stat["consec_blocked"] = 0
        elif outcome == "blocked":
            stat["consec_blocked"] += 1
        reason = None
        if outcome == "challenge":
            reason = "challenge page"
        elif outcome == "blocked" and stat["ok"] > 0:
            reason = "403 after earlier success"
        elif outcome == "blocked" and stat["consec_blocked"] >= 3:
            reason = "persistent 403"
        if reason and host not in _CTX.parked:
            _CTX.parked[host] = reason
            print(f"  parking host {host} ({reason}); Wayback-only from now on")
            # Persist the parking immediately, not only at end-of-run:
            # a SIGKILL mid-run used to lose newly parked hosts.
            if _CTX.state is not None:
                try:
                    _CTX.state.park_host(host, reason)
                except Exception as e:
                    print(f"  warning: could not persist parking for "
                          f"{host}: {e}")
    if limiter is not None:
        limiter.record_outcome(host, outcome, latency, retry_after)


def _is_parked(host):
    return host in _CTX.parked


def _request_headers():
    """Header dict for fetcher requests (cached after the first call)."""
    global _headers_cache
    if _headers_cache is not None:
        return _headers_cache
    headers = dict(DEFAULT_HEADERS)
    headers["User-Agent"] = config.USER_AGENT
    path = getattr(config, "BROWSER_HEADERS_FILE", "") or ""
    if path:
        try:
            with open(path, encoding="utf-8") as f:
                custom = json.load(f)
            headers.update({str(k): str(v) for k, v in custom.items()})
        except Exception as e:
            print(f"  warning: could not read BROWSER_HEADERS_FILE: {e}")
    _headers_cache = headers
    return headers


def _parse_crawl_delay(text, ua):
    """Crawl-delay for our user-agent (or *), or None.

    Groups are separated by User-agent lines that follow other
    directives; a delay only applies when its group names us or *.
    Best-effort: robots.txt has no strict grammar for this field.
    """
    token = (ua or "").split("/")[0].split()[0].lower()
    best = None
    group_uas = []
    seen_directive = False
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if key == "user-agent":
            if seen_directive:
                group_uas = []
                seen_directive = False
            group_uas.append(value.lower())
        elif key == "crawl-delay":
            if any(g == "*" or (g and (token.startswith(g)
                                       or g.startswith(token)))
                   for g in group_uas):
                try:
                    delay = float(value)
                except ValueError:
                    continue
                if delay > 0:
                    best = delay if best is None else max(best, delay)
        else:
            seen_directive = True
    return best


def _robots_fetch(url):
    """Fetch one robots.txt body, using the 24h disk cache when set.

    Returns (body or None, status or None). The disk cache is best
    effort: a stale or unreadable cache file is just ignored.
    """
    domain = urlparse(url).netloc
    robots_url = urljoin(url, "/robots.txt")
    ttl = getattr(config, "ROBOTS_CACHE_TTL_HOURS", 24) * 3600
    now = time.time()
    entries = {}
    if _CTX.robots_cache_path:
        try:
            with open(_CTX.robots_cache_path, encoding="utf-8") as f:
                entries = json.load(f)
            if not isinstance(entries, dict):
                entries = {}
        except (OSError, ValueError):
            entries = {}
    entry = entries.get(domain)
    if entry and now - entry.get("fetched_at", 0) < ttl:
        return entry.get("body"), entry.get("status")
    body, status = None, None
    try:
        resp = httpx.get(robots_url, timeout=5, headers=_request_headers(),
                         trust_env=False)
        status = resp.status_code
        body = resp.text if status == 200 else None
    except Exception:
        body, status = None, None
    if _CTX.robots_cache_path:
        entries[domain] = {"fetched_at": now, "status": status, "body": body}
        try:
            tmp = str(_CTX.robots_cache_path) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2, sort_keys=True)
            os.replace(tmp, _CTX.robots_cache_path)
        except OSError:
            pass
    return body, status


def _allowed(url, limiter=None):
    """True unless the site's robots.txt disallows our user-agent.

    A 5xx on robots.txt means disallow (RFC 9309), not fail-open; any
    other failure (timeout, connection error, 404) still fails open.
    A published Crawl-delay becomes the host's delay floor.
    """
    domain = urlparse(url).netloc
    with _robots_lock:
        parser = _robots_cache.get(domain, False)
    if parser is False:
        body, status = _robots_fetch(url)
        if status is not None and 500 <= status < 600:
            parser = RobotFileParser()
            parser.parse(["User-agent: *", "Disallow: /"])
        elif body is None:
            # No robots.txt (or unreachable): fail open.
            parser = None
        else:
            parser = RobotFileParser()
            parser.parse(body.splitlines())
            delay = _parse_crawl_delay(body, config.USER_AGENT)
            if delay is not None and limiter is not None:
                limiter.set_crawl_delay(domain, delay)
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


_curl_cffi = None   # None = not tried yet; False = not installed
_curl_warned = False


def _curl_client():
    """The curl_cffi requests module, or None when it is unavailable.

    Warns once when the package is missing so an httpx-only run does
    not fail silently on the tier-2 path.
    """
    global _curl_cffi, _curl_warned
    if _curl_cffi is None:
        try:
            from curl_cffi import requests as curl_requests
            _curl_cffi = curl_requests
        except ImportError:
            _curl_cffi = False
            if not _curl_warned:
                _curl_warned = True
                print("  warning: curl_cffi is not installed; continuing "
                      "httpx-only (pip install curl_cffi for the "
                      "Chrome-TLS second attempt)")
    return _curl_cffi or None


def _curl_may_try(client):
    """True when a curl_cffi second attempt is allowed for this call.

    Test stubs are not httpx clients, so they never trigger a real
    network fetch through the second tier.
    """
    return bool(_CTX.curl_cffi) and (client is None
                                     or isinstance(client, httpx.Client))


def _fetch_live_curl(url, limiter=None, host=None):
    """One live attempt with curl_cffi impersonate="chrome".

    Returns (html, note) like _fetch_live. Called only after httpx
    403/429'd or served a challenge page, and only when curl_cffi is
    installed. The impersonated Chrome profile supplies its own
    User-Agent: sending the bot UA with a Chrome TLS fingerprint is
    the contradiction detectors read. A real browser UA from
    BROWSER_HEADERS_FILE stays -- it is consistent with Chrome TLS.
    """
    host = host or urlparse(url).netloc
    curl_requests = _curl_client()
    if curl_requests is None:
        return None, "curl_cffi unavailable"
    headers = _request_headers()
    if not getattr(config, "BROWSER_HEADERS_FILE", ""):
        headers = {k: v for k, v in headers.items()
                   if k.lower() != "user-agent"}
    t0 = time.monotonic()
    try:
        resp = curl_requests.get(
            url, impersonate="chrome", headers=headers,
            timeout=(_LIVE_TIMEOUT.connect, _LIVE_TIMEOUT.read),
            allow_redirects=True)
    except Exception as e:
        latency = time.monotonic() - t0
        _record_outcome(host, "timeout", latency, limiter)
        return None, f"curl_cffi request failed: {e}"
    latency = time.monotonic() - t0
    if resp.status_code == 429:
        _record_outcome(host, "rate-limited", latency, limiter,
                        retry_after=_parse_retry_after(
                            resp.headers.get("retry-after")))
        return None, "HTTP 429 (curl_cffi)"
    if resp.status_code == 403:
        _record_outcome(host, "blocked", latency, limiter)
        return None, "HTTP 403 (curl_cffi)"
    if resp.status_code != 200:
        _record_outcome(host, "terminal", latency, limiter)
        return None, f"HTTP {resp.status_code} (curl_cffi)"
    content_type = resp.headers.get("content-type", "")
    if "html" not in content_type and content_type:
        return None, f"not HTML ({content_type})"
    if _is_challenge(resp.text):
        _record_outcome(host, "challenge", latency, limiter)
        return None, "challenge page (blocked)"
    _record_outcome(host, "ok", latency, limiter)
    with _CTX.lock:
        _CTX.counters["curl_cffi_ok"] += 1
    return resp.text, "live (curl_cffi)"


def _fetch_live(url, client=None, limiter=None, host=None):
    """One live attempt for one URL. Returns (html, note) or (None, note).

    client: an httpx.Client to reuse (shared across threads; httpx
    clients are thread-safe). If None, a throwaway client is created,
    as before. limiter, if given, receives every classified outcome
    so the per-host AIMD controller can adapt. A host that 403/429s
    httpx, or serves a challenge page, gets one curl_cffi attempt
    before the URL is given up. There is exactly one attempt here,
    by construction: no retry loop, no Retry-After sleep, no config
    flag that could restore them. A 429 still pauses the whole host
    (the limiter honours the site's Retry-After via _record_outcome),
    but the URL itself is deferred, not retried.
    """
    host = host or urlparse(url).netloc
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=_LIVE_TIMEOUT,
                              follow_redirects=True,
                              headers=_request_headers(),
                              trust_env=False)
    try:
        t0 = time.monotonic()
        try:
            resp = client.get(url)
        except httpx.HTTPError as e:
            latency = time.monotonic() - t0
            _record_outcome(host, "timeout", latency, limiter)
            return None, f"request failed: {e}"
        latency = time.monotonic() - t0
        if resp.status_code != 200:
            outcome = ("blocked" if resp.status_code == 403
                       else "rate-limited" if resp.status_code == 429
                       else "terminal")
            _record_outcome(
                host, outcome, latency, limiter,
                retry_after=_parse_retry_after(
                    resp.headers.get("retry-after"))
                if resp.status_code == 429 else None)
            if (resp.status_code in (403, 429)
                    and _curl_may_try(client)
                    and _curl_client() is not None):
                return _fetch_live_curl(url, limiter=limiter, host=host)
            return None, f"HTTP {resp.status_code}"
        content_type = resp.headers.get("content-type", "")
        if "html" not in content_type and content_type:
            return None, f"not HTML ({content_type})"
        if _is_challenge(resp.text):
            _record_outcome(host, "challenge", latency, limiter)
            if _curl_may_try(client) and _curl_client() is not None:
                return _fetch_live_curl(url, limiter=limiter, host=host)
            return None, "challenge page (blocked)"
        _record_outcome(host, "ok", latency, limiter)
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


def classify_note(note):
    """Map a fetch note to a failure-taxonomy category for the manifest.

    Match order matters: the specific "wayback HTTP ...",
    "wayback fetch failed", and "curl_cffi" notes must be checked
    before the generic "HTTP ...", "request failed", and fallthrough
    branches, or they land in "other" / "request-failed" and the
    curl-failed bucket is unreachable.
    """
    note = note or ""
    if note == "live":
        return "ok-live"
    if note == "wayback":
        return "ok-wayback"
    if note.startswith("wayback-retry"):
        return "wayback-retry"
    if "challenge page" in note:
        return "challenge"
    if note.startswith("wayback HTTP 429"):
        return "rate-limited"
    if note.startswith("wayback HTTP 5"):
        return "failed-5xx"
    if note.startswith("wayback HTTP "):
        return "failed-other-http"
    if note.startswith("wayback fetch failed"):
        if "Timeout" in note or "timed out" in note:
            return "timeout"
        return "request-failed"
    if note.startswith("HTTP 404"):
        return "failed-404"
    if note.startswith("HTTP 403"):
        return "failed-403"
    if note.startswith("HTTP 429"):
        return "rate-limited"
    if note.startswith("HTTP 5"):
        return "failed-5xx"
    if note.startswith("HTTP "):
        return "failed-other-http"
    if "blocked by robots" in note:
        return "robots-blocked"
    if note == "no wayback snapshot":
        return "no-snapshot"
    if "text too short" in note:
        return "text-too-short"
    if "wayback lookup failed" in note or "CDX" in note:
        return "cdx-error"
    if "curl_cffi" in note:
        return "curl-failed"
    if "request failed" in note:
        if "Timeout" in note or "timed out" in note:
            return "timeout"
        return "request-failed"
    if "parked host" in note:
        return "parked"
    return "other"

class _HostLimiter:
    """Cap concurrent requests per host and adapt each host's delay and
    concurrency from its outcomes (AIMD + AutoThrottle hybrid).

    With 50 workers and no per-host cap, one small newspaper could get 50
    simultaneous hits. With this, each host sees at most `per_host`
    concurrent requests (robots check + live fetch), while other hosts
    proceed in parallel. A worker never holds two slots at once, so
    there is no deadlock risk. (v4: the old separate Wayback cap is
    gone -- the live pass never touches archive.org, and archive.py
    has its own limiter and breaker.)

    Concurrency caps alone do not limit request RATE: two-at-a-time with
    fast responses still hits one host 10 times in a few seconds, and a
    75-worker burst hits 75 hosts at once from one IP -- both trip
    rate limiters. So the limiter also smooths request starts: at most
    `max_rate` requests per second overall, and at least `host_delay`
    seconds between two requests to the same host. Either is 0 to turn
    that floor off. Slots are reserved under one lock, so no two threads
    can reserve the same start time.

    Adaptation, per host: delay starts at `host_delay`, never drops
    below max(host_delay, Crawl-delay) -- with the default host_delay
    of 1.0s that floor is max(1.0s, Crawl-delay) -- and never exceeds
    60s. Target concurrency starts at 1 and rises to 2 only after a
    clean history (3 straight 200s). On each 200, delay averages with
    latency / target concurrency (Scrapy AutoThrottle); non-200
    latencies never decrease the delay. On 429/503 the delay doubles,
    concurrency drops to 1, and the whole host pauses until the
    Retry-After (seconds or HTTP-date) has passed -- all workers for
    that host wait, not just the one that got the 429.

    The pace wait happens while holding the host slot (see fetch_text):
    a thread that reserved an early start but then queued behind a
    slow fetch would otherwise start bunched right behind it, under
    the delay. Both clocks are reserved from the request's actual
    start time, not from reservation time: with a deep global queue,
    reserving from `now` let two same-host requests start the global
    interval (0.1s) apart instead of `host_delay` (1.0s) apart.
    """
    def __init__(self, per_host=2, wayback=3, host_delay=1.0, max_rate=10):
        if per_host < 1:
            raise ValueError("per_host must be >= 1")
        if wayback < 1:
            raise ValueError("wayback must be >= 1")
        # wayback is accepted for call compatibility with v3 and
        # validated, but ignored: v4 has no Wayback slot in the live
        # limiter (see the class docstring).
        self.per_host = per_host
        self.host_delay = host_delay
        self.global_interval = 1.0 / max_rate if max_rate else 0.0
        self._lock = threading.Lock()
        self._semaphores = {}
        self._sem_target = {}
        self._next_global = 0.0
        self._next_host = {}
        self._state = {}

    def _host_state(self, host):
        """Per-host AIMD state, creating it (delay = host_delay) if new."""
        state = self._state.get(host)
        if state is None:
            state = {"delay": self.host_delay, "floor": self.host_delay,
                     "concurrency": 1, "consec_ok": 0, "ewma": None,
                     "not_before": 0.0}
            self._state[host] = state
        return state

    def wait_turn(self, host):
        """Sleep until this request's reserved start time, if any.

        The host and global intervals are stretched by upward-only
        jitter (see _jitter_up): the metronome pattern is itself a bot
        signal, but the tested minimum gaps must still hold.
        """
        with self._lock:
            now = time.monotonic()
            start = now
            if self.global_interval > 0:
                start = max(start, self._next_global)
                self._next_global = start + self.global_interval * _jitter_up(1.0)
            state = self._host_state(host)
            if state["delay"] > 0:
                start = max(start, self._next_host.get(host, 0.0))
                start = max(start, state["not_before"])
                self._next_host[host] = start + state["delay"] * _jitter_up(1.0)
            elif state["not_before"] > now:
                # Host delay disabled, but a 429 still pauses the host.
                start = max(start, state["not_before"])
            wait = start - now
        if wait > 0:
            _sleep(wait)

    def host_slot(self, host):
        """Semaphore capping concurrent requests to one host.

        Sized to the host's current AIMD target concurrency (1, or 2
        for clean-history hosts), capped by per_host. The semaphore is
        replaced when the target changes; threads already holding the
        old one finish under it, so the cap converges without a
        resize race.
        """
        with self._lock:
            state = self._host_state(host)
            target = min(self.per_host, state["concurrency"])
            sem = self._semaphores.get(host)
            if sem is None or self._sem_target.get(host) != target:
                sem = threading.Semaphore(target)
                self._semaphores[host] = sem
                self._sem_target[host] = target
            return sem

    def record_outcome(self, host, outcome, latency=None, retry_after=None):
        """Feed one classified outcome into the host's AIMD state."""
        with self._lock:
            state = self._host_state(host)
            if outcome == "ok":
                state["consec_ok"] += 1
                if latency is not None:
                    if state["ewma"] is None:
                        state["ewma"] = latency
                    else:
                        state["ewma"] = 0.7 * state["ewma"] + 0.3 * latency
                    target = max(state["concurrency"], 1)
                    new_delay = (state["delay"] + latency / target) / 2.0
                    state["delay"] = min(60.0, max(state["floor"], new_delay))
                if (state["consec_ok"] >= 3
                        and state["concurrency"] < min(2, self.per_host)):
                    state["concurrency"] = min(2, self.per_host)
            elif outcome == "rate-limited":
                state["consec_ok"] = 0
                state["concurrency"] = 1
                state["delay"] = min(60.0, max(state["floor"],
                                               state["delay"] * 2.0))
                pause = (retry_after if retry_after is not None
                         else state["delay"])
                state["not_before"] = max(
                    state["not_before"],
                    time.monotonic() + min(pause, 60.0))
            elif outcome == "timeout":
                state["consec_ok"] = 0
                state["delay"] = min(60.0, max(state["floor"],
                                               state["delay"] * 1.5))
            elif outcome in ("blocked", "challenge"):
                state["consec_ok"] = 0
            # terminal: no change -- the host answered, just finally.

    def note_rate_limited(self, host, retry_after=None):
        """A 429/5xx for a host (e.g. a CDX lookup): cool it down."""
        self.record_outcome(host, "rate-limited", None, retry_after)

    def set_crawl_delay(self, host, seconds):
        """A published Crawl-delay becomes the host's delay floor.

        It applies even when the configured host_delay is 0: the site
        asked for it, and violating it is a block trigger, not just
        rudeness.
        """
        with self._lock:
            state = self._host_state(host)
            state["floor"] = max(state["floor"], float(seconds))
            state["delay"] = max(state["delay"], state["floor"])

    def get_delay(self, host):
        with self._lock:
            return self._host_state(host)["delay"]

    def get_concurrency(self, host):
        with self._lock:
            return self._host_state(host)["concurrency"]

    def cooldown_hosts(self):
        """Hosts currently paused by a host-wide not_before."""
        now = time.monotonic()
        with self._lock:
            return sorted(h for h, s in self._state.items()
                          if s["not_before"] > now)

    def host_delay_state(self):
        """{host: {delay, concurrency}} for persisting across runs."""
        with self._lock:
            return {h: {"delay": s["delay"],
                        "concurrency": s["concurrency"]}
                    for h, s in self._state.items()}

    def restore_host(self, host, delay, concurrency):
        """Restore a host's learned delay/concurrency from the state DB."""
        with self._lock:
            state = self._host_state(host)
            state["delay"] = max(state["floor"], float(delay))
            state["concurrency"] = max(1, min(int(concurrency),
                                              self.per_host))


def fetch_text(url, client=None, limiter=None):
    """Fetch and extract one article, live only.

    Returns (loaded, text, note). loaded is False when nothing usable
    came back; note says why. client, if given, is a shared httpx.Client
    reused across calls (and threads). limiter, if given, is a
    _HostLimiter capping concurrent requests per host. Text extraction
    runs outside the host slot, so a slow page parse does not hold up
    the next request to the same site. A parked host skips the live
    fetch entirely: the caller records the row as wayback-retry
    (deferred to the archive command), it is never loaded here.
    """
    host = urlparse(url).netloc
    host_sem = limiter.host_slot(host) if limiter else nullcontext()
    with host_sem:
        # The pace wait happens while holding the host slot, not before
        # it: a thread that reserved an early start but then queued
        # behind a slow fetch would otherwise start bunched right
        # behind it, under the delay (see _HostLimiter).
        if limiter:
            limiter.wait_turn(host)
        if not _allowed(url, limiter=limiter):
            return False, "", "blocked by robots.txt"
        if _is_parked(host):
            return False, "", "parked host (wayback only)"
        html, note = _fetch_live(url, client=client, limiter=limiter,
                                 host=host)
    if html:
        text = _extract_text(html, url)
        if len(text) >= config.MIN_TEXT_CHARS:
            return True, text, "live"
        return False, "", f"live text too short ({len(text)} chars)"
    return False, "", note


def _fetch_one(args):
    """Worker for the thread pool: fetch one URL, never raise."""
    url, client, limiter = args
    try:
        loaded, text, note = fetch_text(url, client=client, limiter=limiter)
    except Exception as e:
        return url, False, "", f"worker failed: {e}"
    return url, loaded, text if loaded else "", note


def _interleave(rows):
    """Round-robin a chunk's rows by host, for fair scheduling.

    Returns [(original_index, row), ...] in submission order; callers
    restore input order before writing, so the file still matches a
    sequential run row-for-row. One hot host cannot hog a chunk's
    workers while a long tail of small sites waits behind it.
    """
    by_host = {}
    order = []
    for idx, row in enumerate(rows):
        host = urlparse(row[0]).netloc
        if host not in by_host:
            by_host[host] = []
            order.append(host)
        by_host[host].append((idx, row))
    out = []
    slot = 0
    while len(out) < len(rows):
        for host in order:
            group = by_host[host]
            if slot < len(group):
                out.append(group[slot])
        slot += 1
    return out


def _load_parked(parked_path, state):
    """Parked hosts from the state DB and parked_hosts.json, into _CTX."""
    merged = dict(state.parked_hosts())
    try:
        with open(parked_path, encoding="utf-8") as f:
            saved = json.load(f)
        hosts = saved.get("hosts", {}) if isinstance(saved, dict) else {}
        for host, info in hosts.items():
            reason = (info.get("reason", "") if isinstance(info, dict)
                      else str(info))
            merged.setdefault(host, reason)
    except (OSError, ValueError):
        pass
    for host, reason in merged.items():
        _CTX.parked[host] = reason
    if merged:
        shown = ", ".join(sorted(merged)[:5])
        extra = "..." if len(merged) > 5 else ""
        print(f"  {len(merged)} parked host(s) from earlier runs "
              f"(Wayback-only): {shown}{extra}")


def _save_parked(parked_path):
    """Write the run's parked hosts (parked_hosts.json, beside the out file)."""
    with _CTX.lock:
        parked = dict(_CTX.parked)
    if not parked:
        return
    payload = {"hosts": {host: {"reason": reason,
                                "parked_at": time.strftime(
                                    "%Y-%m-%dT%H:%M:%S", time.localtime())}
                         for host, reason in sorted(parked.items())}}
    tmp = str(parked_path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, parked_path)


def _persist_hosts(state, limiter):
    """Write the run's per-host stats and learned delays to the state DB.

    Counters accumulate across runs: the DB holds each host's totals,
    not just the latest run's -- this run's counters are added to the
    stored ones, and latencies keep the most recent 500 samples.
    """
    delays = limiter.host_delay_state() if limiter else {}
    with _CTX.lock:
        stats = {h: dict(v) for h, v in _CTX.host_stats.items()}
        parked = dict(_CTX.parked)
    for host, stat in stats.items():
        delay = delays.get(host, {})
        existing = state.get_host(host) or {}
        latencies = (list(existing.get("latencies") or [])
                     + list(stat["latencies"]))
        state.update_host(
            host, parked=host in parked, park_reason=parked.get(host, ""),
            delay=delay.get("delay", 1.0),
            concurrency=delay.get("concurrency", 1),
            requests=existing.get("requests", 0) + stat["requests"],
            ok=existing.get("ok", 0) + stat["ok"],
            rate_limited=existing.get("rate_limited", 0)
            + stat["rate_limited"],
            blocked=existing.get("blocked", 0) + stat["blocked"],
            terminal=existing.get("terminal", 0) + stat["terminal"],
            timeout=existing.get("timeout", 0) + stat["timeout"],
            challenge=existing.get("challenge", 0) + stat["challenge"],
            cdx_errors=existing.get("cdx_errors", 0) + stat["cdx_errors"],
            latencies=latencies[-500:])
    for host, delay in delays.items():
        if host not in stats:
            state.update_host(host, delay=delay["delay"],
                              concurrency=delay["concurrency"])


def _percentile(values, pct):
    """Nearest-rank percentile of a latency sample, or None."""
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(pct / 100.0 * len(ordered))))
    return round(ordered[idx], 3)


def _write_manifest(manifest_path, params, count, ok, taxonomy, started,
                    limiter):
    """Run manifest JSON next to the out file: params, per-host stats,
    failure taxonomy (CDX errors counted separately), parked hosts."""
    elapsed = time.monotonic() - started
    with _CTX.lock:
        stats = {h: dict(v) for h, v in _CTX.host_stats.items()}
        parked = dict(_CTX.parked)
        counters = dict(_CTX.counters)
    delays = limiter.host_delay_state() if limiter else {}
    hosts = {}
    total_requests = 0
    for host, stat in sorted(stats.items()):
        total_requests += stat["requests"]
        delay = delays.get(host, {})
        hosts[host] = {
            "requests": stat["requests"],
            "ok": stat["ok"],
            "rate_limited": stat["rate_limited"],
            "blocked": stat["blocked"],
            "terminal": stat["terminal"],
            "timeout": stat["timeout"],
            "challenge": stat["challenge"],
            "cdx_errors": stat["cdx_errors"],
            "latency_p50": _percentile(stat["latencies"], 50),
            "latency_p95": _percentile(stat["latencies"], 95),
            "final_delay": round(delay.get("delay", 1.0), 3),
            "concurrency": delay.get("concurrency", 1),
            "parked": host in parked,
        }
    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()),
        "params": params,
        "totals": {"attempted": count, "loaded": ok, "failed": count - ok,
                   "elapsed_secs": round(elapsed, 1),
                   "host_requests": total_requests,
                   "effective_req_per_sec":
                       round(total_requests / elapsed, 2) if elapsed else None},
        "taxonomy": dict(sorted(taxonomy.items())),
        # The live pass makes no CDX calls, so this is always 0; the
        # archive command reports its own CDX errors in its manifest.
        "cdx_errors": 0,
        "curl_cffi_ok": counters.get("curl_cffi_ok", 0),
        "parked_hosts": dict(sorted(parked.items())),
        "wayback_only_hosts": sorted(parked),
        "hosts": hosts,
    }
    tmp = str(manifest_path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, manifest_path)
    return manifest


def export_texts_csv(state, link_rows, out_csv):
    """Render texts.csv from the state DB, in input order.

    Every input row appears exactly once: loaded rows carry their
    extracted text, everything else carries Loaded=False and empty
    text. Downstream stages key on the URL, so the row set is what
    matters; the order matches a sequential run. Called at the end of
    a run and after an interrupt, so a partial run still exports
    everything it completed.
    """
    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out.with_name(out.stem + "_export.tmp")
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TEXT_COLUMNS)
        writer.writeheader()
        for url, publish_date, source in link_rows:
            record = state.get_record(url)
            loaded = bool(record and record["status"] == "loaded"
                          and record["text"])
            writer.writerow({
                "Link": url,
                "Loaded": loaded,
                "Text": record["text"] if loaded else "",
                "publish_date": publish_date,
                "source": source,
            })
    os.replace(tmp_path, out)


def fetch_all(links_csv, out_csv, limit=None, workers=None, per_host=2,
              wayback_max=3, host_delay=1.0, max_rate=20,
              heartbeat_secs=30, curl_cffi=True, chunk_size=2000,
              retry_failed=False, read_timeout=None):
    """Fetch every URL in links.csv, live only, resuming by URL key.

    workers defaults to config.DEFAULT_WORKERS (20, the measured knee);
    per_host caps concurrent requests to any one website (default 2);
    host_delay is the minimum gap in seconds between two requests to
    the same website (default 1.0); max_rate caps total requests per
    second across all websites (default 20, 0 for no cap). The rate
    floors only matter with workers > 1. heartbeat_secs prints a
    progress line every N seconds while fetching (default 30, 0 for
    none), so a long run on slow sites shows it is still working
    instead of looking stuck. read_timeout overrides the live read
    timeout in seconds (default 8).

    Each completed URL is recorded in the SQLite state DB next to the
    out file (<stem>_state.db) the moment it resolves -- loaded, or
    deferred to the archive queue as wayback-retry -- and texts.csv is
    rendered from the DB in input order when the run ends (or is
    interrupted). There is no chunked in-order write: one slow URL
    cannot stall any other row's recording. A re-run skips every URL
    already in the DB; with --retry-failed, only rows recorded FAILED
    (or wayback-retry / archive-deferred) are re-attempted, and rows
    for URLs no longer in links.csv are dropped by the export.

    wayback_max and chunk_size are accepted for call compatibility
    with v3 and ignored: v4 has no Wayback traffic in the live pass
    and no chunked writes.
    """
    global _CTX, _LIVE_TIMEOUT
    _CTX = _RunContext()
    _STOP.clear()
    _CTX.curl_cffi = bool(curl_cffi)
    if workers is None:
        workers = getattr(config, "DEFAULT_WORKERS", 20)
    if workers < 1:
        raise ValueError("workers must be >= 1")
    if read_timeout is not None:
        _LIVE_TIMEOUT = httpx.Timeout(
            connect=getattr(config, "FETCH_CONNECT_TIMEOUT", 5),
            read=float(read_timeout),
            write=getattr(config, "FETCH_WRITE_TIMEOUT", 10),
            pool=getattr(config, "FETCH_POOL_TIMEOUT", 5))

    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    state_path = out.with_name(out.stem + config.STATE_DB_SUFFIX)
    manifest_path = out.with_name(out.stem + config.MANIFEST_SUFFIX)
    parked_path = out.parent / config.PARKED_HOSTS_FILE
    _CTX.robots_cache_path = out.parent / config.ROBOTS_CACHE_FILE

    state = RunState(state_path)
    _CTX.state = state
    _load_parked(parked_path, state)

    links = pd.read_csv(links_csv, dtype=str).fillna("")
    link_rows = []
    for _, link_row in links.iterrows():
        url = (link_row.get("url") or "").strip()
        if url:
            link_rows.append((url, link_row.get("publish_date", ""),
                              link_row.get("source", "")))

    done = state.done_urls()
    failed = set(state.retryable_urls())
    if retry_failed:
        pending = [row for row in link_rows if row[0] in failed]
    else:
        pending = [row for row in link_rows if row[0] not in done]
    if limit is not None:
        pending = pending[:limit]

    limiter = (_HostLimiter(per_host=per_host, wayback=wayback_max,
                           host_delay=host_delay, max_rate=max_rate)
               if workers > 1 else None)
    if limiter is not None:
        for record in state.host_stats():
            limiter.restore_host(record["host"], record["delay"],
                                 record["concurrency"])

    limits = httpx.Limits(
        max_connections=max(1, workers + 10),
        max_keepalive_connections=max(1, workers),
        keepalive_expiry=25)
    count = ok = 0
    taxonomy = {}
    started = time.monotonic()
    stop_hb = threading.Event()

    def _tally(loaded, note):
        nonlocal count, ok
        count += 1
        if loaded:
            ok += 1
        taxonomy[classify_note(note)] = taxonomy.get(classify_note(note), 0) + 1

    def _heartbeat():
        while not stop_hb.wait(heartbeat_secs):
            extra = ""
            parked = sorted(_CTX.parked)
            if parked:
                shown = ", ".join(parked[:3])
                extra += (f"; parked: {len(parked)} ({shown}"
                          f"{'...' if len(parked) > 3 else ''})")
            print(f"  ... {count}/{len(pending)} done, "
                  f"{time.monotonic() - started:.0f}s elapsed{extra}")

    if heartbeat_secs:
        threading.Thread(target=_heartbeat, daemon=True).start()
    params = {"links_csv": str(links_csv), "out_csv": str(out_csv),
              "limit": limit, "workers": workers, "per_host": per_host,
              "host_delay": host_delay, "max_rate": max_rate,
              "read_timeout": _LIVE_TIMEOUT.read,
              "curl_cffi": bool(curl_cffi), "retry_failed": retry_failed}
    manifest = None

    def record_result(url, loaded, text, note):
        """Record one resolved URL in the DB, as it completes."""
        status = ("loaded" if loaded else
                  "failed" if note.startswith("worker failed")
                  else "wayback-retry")
        try:
            state.record_url(url, status, note, text if loaded else "")
        except Exception as e:
            # A bad DB write must never kill the run (this is the
            # failure shape that stalled the v3-era queue: a worker
            # dying on a dependency problem). Log it and move on;
            # the URL is re-attempted on the next run.
            print(f"  warning: could not record {url}: {e}")
            return
        _tally(loaded, note)
        if loaded:
            print(f"  [{count}] ok ({note}): {url[:80]}")
        else:
            print(f"  [{count}] deferred ({note}): {url[:80]}")

    try:
        with httpx.Client(timeout=_LIVE_TIMEOUT, follow_redirects=True,
                          headers=_request_headers(), trust_env=False,
                          limits=limits) as client:
            pool = (ThreadPoolExecutor(max_workers=workers)
                    if workers > 1 else None)
            try:
                if pool is None:
                    for url, _, _ in pending:
                        loaded, text, note = fetch_text(url, client=client)
                        record_result(url, loaded, text, note)
                else:
                    ordered = _interleave(pending)
                    jobs = [(row[0], client, limiter) for _, row in ordered]
                    for url, loaded, text, note in pool.map(_fetch_one, jobs):
                        record_result(url, loaded, text, note)
            except KeyboardInterrupt:
                # Wake every worker out of its pace / backoff sleep at
                # once, then stop promptly. Completed rows are already
                # in the DB; the export below saves them.
                _STOP.set()
                if pool is not None:
                    pool.shutdown(wait=False, cancel_futures=True)
                print("  interrupted -- partial results saved in "
                      f"{out_csv}")
                raise
            else:
                if pool is not None:
                    pool.shutdown(wait=True)
    finally:
        stop_hb.set()
        try:
            _persist_hosts(state, limiter)
        except Exception as e:
            print(f"  warning: could not persist host stats: {e}")
        try:
            _save_parked(parked_path)
        except Exception as e:
            print(f"  warning: could not save parked hosts: {e}")
        try:
            export_texts_csv(state, link_rows, out)
        except Exception as e:
            print(f"  warning: could not export {out_csv}: {e}")
        try:
            manifest = _write_manifest(manifest_path, params, count, ok,
                                       taxonomy, started, limiter)
        except Exception as e:
            print(f"  warning: could not write manifest: {e}")
        state.close()
    print(f"fetched {count} new links, {ok} with usable text")
    if manifest is not None:
        print(f"  manifest: {manifest_path} "
              f"({len(manifest['hosts'])} hosts, "
              f"{len(manifest['parked_hosts'])} parked, "
              f"{manifest['cdx_errors']} CDX errors)")
    return count, ok


def main():
    parser = argparse.ArgumentParser(
        description="Fetch article text for discovered links (live only).")
    parser.add_argument("--links", default=str(config.DATA_DIR / "links.csv"))
    parser.add_argument("--out", default=str(config.DATA_DIR / "texts.csv"))
    parser.add_argument("--limit", type=int, default=None,
                        help="cap the number of links fetched (smoke tests)")
    parser.add_argument("--workers", type=int, default=None,
                        help="fetch N URLs concurrently (default 20, the "
                             "measured knee; 1 is sequential)")
    parser.add_argument("--per-host", type=int, default=2,
                        help="max concurrent requests to one website "
                             "(default 2; only matters with --workers > 1)")
    parser.add_argument("--per-host-delay", type=float, default=1.0,
                        help="minimum seconds between two requests to the "
                             "same website (default 1.0; 0 for none; only "
                             "matters with --workers > 1)")
    parser.add_argument("--max-rate", type=float, default=20,
                        help="max requests per second across all websites "
                             "(default 20; 0 for no cap; only matters with "
                             "--workers > 1)")
    parser.add_argument("--read-timeout", type=float, default=None,
                        help="live read timeout in seconds (default 8; "
                             "timeouts consumed ~55%% of worker-seconds "
                             "at 15s)")
    parser.add_argument("--curl-cffi", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="try curl_cffi impersonate=chrome as a second "
                             "attempt after httpx 403/429 (default on; "
                             "--no-curl-cffi disables it)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="re-attempt only rows previously recorded "
                             "FAILED (or wayback-retry / archive-deferred) "
                             "instead of resuming past them")
    args = parser.parse_args()
    if args.workers is not None and args.workers < 1:
        parser.error("--workers must be >= 1")
    if args.per_host < 1:
        parser.error("--per-host must be >= 1")
    fetch_all(args.links, args.out, limit=args.limit, workers=args.workers,
              per_host=args.per_host, host_delay=args.per_host_delay,
              max_rate=args.max_rate, curl_cffi=args.curl_cffi,
              retry_failed=args.retry_failed,
              read_timeout=args.read_timeout)


if __name__ == "__main__":
    main()
