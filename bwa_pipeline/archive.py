"""Archive pass: recover deferred URLs from the Wayback Machine (v4 Stage 2).

This is a SEPARATE command from the live pass. It works over the rows
the live pass deferred (status wayback-retry in the state DB) and only
those rows. It never shares a limiter, a worker pool, or a client with
the live pass, so a dead archive.org cannot slow down, stall, or
serialize anything else: the live run is already finished and banked
by the time this runs, possibly days later, possibly from a different
machine or egress.

    python archive.py --links data/links.csv --out data/texts.csv
    python archive.py --links data/links.csv --out data/texts.csv \
        --retry-deferred

Per URL: one snapshot-availability lookup (CDX by default, or the
wayback/available endpoint with --archive-api available), then one
snapshot fetch. A lookup that answers -- even with "no snapshot" --
proves the dependency is alive and counts as a success for the
breaker. A lookup that fails (429 / 5xx / timeout / non-JSON) counts
toward tripping it.

Circuit breaker (the core of this command): the breaker trips after
10 consecutive lookup/snapshot failures, or an error rate above 50%
over the last 50 lookups, whichever comes first. While open, ZERO
archive requests are sent: every remaining queued URL is left
deferred and the command exits 0 within seconds, with a manifest that
says the breaker is open. After a 10-minute cooldown the next run
makes a single half-open probe lookup: success closes the breaker and
the queue resumes; failure re-opens it with a doubled cooldown (cap
2 hours) and the run exits again. Breaker state is persisted in the
state DB, so it survives across runs. There is no sleep for
Retry-After anywhere in a worker, and no not_before/pause path for
archive.org at all: a host that 429s on essentially every lookup
needs a breaker, not a delay.

The --archive-api available option uses the wayback/available
endpoint instead of CDX. From the sandbox it answered 17/17 HTTP 200
with 0x 429 while CDX choked minutes apart -- but that is a sandbox
result, and its behavior from the UVA IP is UNMEASURED. Do not rely
on it until it has been tested from the run machine's IP (200 URLs,
sequential at ~1/s, then concurrency 2-3, logging status/latency).
Default is cdx.
"""

import argparse
import csv
import json
import re
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pandas as pd

try:
    import config
    import fetch
    from state import RunState
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import config
    import fetch
    from state import RunState

CDX_BASE = "https://web.archive.org/cdx/search/cdx"
ARCHIVE_BASE = "https://web.archive.org"
AVAILABLE_BASE = "https://archive.org/wayback/available"

BREAKER_META_KEY = "archive_breaker"


def _cdx_timeout():
    return getattr(config, "CDX_TIMEOUT", 10)


def _snapshot_timeout():
    return getattr(config, "SNAPSHOT_TIMEOUT", 15)


class CircuitBreaker:
    """Fail-fast breaker for one dependency (archive.org).

    CLOSED -> OPEN -> HALF-OPEN -> CLOSED. Trips after
    max_consecutive consecutive failures, or an error rate above
    max_error_rate over the last window lookups (once the window is
    full). While OPEN, allow() returns False and no request is sent.
    After the cooldown, allow() admits exactly one half-open probe:
    its outcome closes the breaker (success, cooldown reset) or
    re-opens it with a doubled cooldown (failure, capped). State is
    plain data (to_dict/from_dict) so it can persist in the state DB.
    """

    def __init__(self, max_consecutive=10, window=50, max_error_rate=0.5,
                 cooldown=600.0, max_cooldown=7200.0):
        self.max_consecutive = max_consecutive
        self.window = window
        self.max_error_rate = max_error_rate
        self.cooldown = float(cooldown)
        self.max_cooldown = float(max_cooldown)
        self.base_cooldown = float(cooldown)
        self.state = "closed"
        self.consecutive = 0
        self.recent = deque(maxlen=window)
        self.opened_at = None
        self.last_error = ""
        self._probe_in_flight = False
        self._lock = threading.Lock()

    def to_dict(self):
        with self._lock:
            return {"state": self.state, "consecutive": self.consecutive,
                    "recent": list(self.recent),
                    "opened_at": self.opened_at,
                    "cooldown": self.cooldown,
                    "last_error": self.last_error}

    @classmethod
    def from_dict(cls, data, **kwargs):
        breaker = cls(**kwargs)
        if not isinstance(data, dict):
            return breaker
        with breaker._lock:
            breaker.state = data.get("state", "closed")
            # A half-open probe that never completed (the process died
            # mid-probe) comes back as open: the probe is gone, so no
            # request may be admitted until the cooldown elapses.
            if breaker.state == "half-open":
                breaker.state = "open"
            breaker.consecutive = int(data.get("consecutive", 0))
            breaker.recent = deque(
                [bool(v) for v in data.get("recent", [])],
                maxlen=breaker.window)
            breaker.opened_at = data.get("opened_at")
            breaker.cooldown = float(
                data.get("cooldown", breaker.base_cooldown))
            breaker.last_error = data.get("last_error", "")
        return breaker

    def cooling_down(self):
        """True when the breaker is open and its cooldown has not elapsed
        (so no request may be sent and no probe is due yet)."""
        with self._lock:
            return (self.state == "open" and self.opened_at is not None
                    and time.time() - self.opened_at < self.cooldown)

    def allow(self):
        """True when a request may be sent (closed, or the half-open probe)."""
        with self._lock:
            if self.state == "closed":
                return True
            if self.state == "open":
                if (self.opened_at is not None
                        and time.time() - self.opened_at >= self.cooldown
                        and not self._probe_in_flight):
                    self.state = "half-open"
                    self._probe_in_flight = True
                    return True
                return False
            # half-open: only the one probe is admitted.
            return False

    def _trip_locked(self, error=""):
        self.state = "open"
        self.opened_at = time.time()
        self._probe_in_flight = False
        if error:
            self.last_error = error

    def _check_window_locked(self):
        """Trip on an error rate above max_error_rate over the last
        window lookups, once the window is full."""
        if len(self.recent) >= self.window:
            errors = sum(1 for ok in self.recent if not ok)
            if errors / len(self.recent) > self.max_error_rate:
                self._trip_locked()

    def record_success(self):
        """A lookup/snapshot that answered (even "no snapshot")."""
        with self._lock:
            self.consecutive = 0
            self.recent.append(True)
            if self.state == "half-open":
                self.state = "closed"
                self.cooldown = self.base_cooldown
                self._probe_in_flight = False
            elif self.state == "closed":
                self._check_window_locked()

    def record_failure(self, error=""):
        """A failed lookup/snapshot (429 / 5xx / timeout / non-JSON)."""
        with self._lock:
            self.consecutive += 1
            self.recent.append(False)
            if error:
                self.last_error = error
            if self.state == "half-open":
                # Probe failed: re-open with a doubled cooldown.
                self.cooldown = min(self.cooldown * 2, self.max_cooldown)
                self._trip_locked()
                return
            if self.consecutive >= self.max_consecutive:
                self._trip_locked()
                return
            self._check_window_locked()


class _RateLimiter:
    """Token bucket: at most `rate` acquisitions per second (0: no cap)."""

    def __init__(self, rate):
        self.interval = 1.0 / rate if rate else 0.0
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def acquire(self):
        if self.interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_slot)
            self._next_slot = start + self.interval
            wait = start - now
        if wait > 0:
            time.sleep(wait)


def _snapshot_url(timestamp, original, archive_base=ARCHIVE_BASE):
    return f"{archive_base}/web/{timestamp}id_/{original}"


def _cdx_lookup(client, url, breaker, limiter, cdx_base, timeout):
    """One CDX lookup. Returns (target, status).

    target is (timestamp, original) on success, else None. status is
    "ok", "no_snapshot" (CDX answered that nothing exists -- a success
    for the breaker), "breaker_open" (no request was sent), or a
    "cdx_..." failure string. One retry on a transient failure
    (timeout / 5xx / non-JSON), and only while the breaker is still
    closed. A 429 is never retried in place: it counts toward
    tripping the breaker and the lookup returns at once, so a
    rate-limiting endpoint is not hit twice for one URL.
    """
    if not breaker.allow():
        return None, "breaker_open"
    limiter.acquire()
    for attempt in range(2):
        try:
            resp = client.get(cdx_base, params={
                "url": url, "output": "json",
                "filter": "statuscode:200", "limit": 1,
            }, timeout=timeout, follow_redirects=True)
        except httpx.HTTPError as e:
            breaker.record_failure(f"CDX lookup failed: {e}")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, f"cdx_error: {e}"
        if resp.status_code == 429:
            # Never retried in place: a 429 counts toward the breaker
            # and the lookup returns at once.
            breaker.record_failure(
                f"CDX HTTP 429 {fetch._snippet(resp.text)}")
            return None, "cdx_http_429"
        if resp.status_code >= 500:
            breaker.record_failure(
                f"CDX HTTP {resp.status_code} "
                f"{fetch._snippet(resp.text)}")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, f"cdx_http_{resp.status_code}"
        if "json" not in resp.headers.get("content-type", ""):
            breaker.record_failure(
                f"CDX non-JSON ({resp.headers.get('content-type', '')})")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, "cdx_non_json"
        try:
            data = resp.json()
            if isinstance(data, dict):
                raise ValueError(f"CDX error object: {str(data)[:200]}")
        except Exception as e:
            breaker.record_failure(f"CDX invalid JSON: {e}")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, f"cdx_bad_json: {e}"
        breaker.record_success()
        if not isinstance(data, list) or len(data) < 2:
            return None, "no_snapshot"
        return (data[1][1], data[1][2]), "ok"
    return None, "cdx_error"


def _available_lookup(client, url, publish_date, breaker, limiter,
                      available_base, archive_base, timeout):
    """One wayback/available lookup. Same return shape as _cdx_lookup:
    (snapshot URL, "ok") on success, else (None, status).

    timestamp is the known publish date (closest snapshot to it is
    arguably better than the earliest capture, but a "closest"
    snapshot can predate publication or be a later revision -- the
    timestamp choice is the publish date, explicitly). A server-side
    timeout wrapped inside an HTTP 200 is a retryable failure, not a
    success and not a "no snapshot".
    """
    if not breaker.allow():
        return None, "breaker_open"
    limiter.acquire()
    params = {"url": url}
    if publish_date:
        compact = str(publish_date).replace("-", "")
        if len(compact) >= 8 and compact[:8].isdigit():
            params["timestamp"] = compact[:8]
    for attempt in range(2):
        try:
            resp = client.get(available_base, params=params,
                              timeout=timeout, follow_redirects=True)
        except httpx.HTTPError as e:
            breaker.record_failure(f"available lookup failed: {e}")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, f"available_error: {e}"
        if resp.status_code == 429:
            # Never retried in place (same rule as the CDX lookup).
            breaker.record_failure("available HTTP 429")
            return None, "available_http_429"
        if resp.status_code >= 500:
            breaker.record_failure(
                f"available HTTP {resp.status_code}")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, f"available_http_{resp.status_code}"
        try:
            data = resp.json()
        except Exception as e:
            breaker.record_failure(f"available invalid JSON: {e}")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, f"available_bad_json: {e}"
        if isinstance(data, dict) and data.get("error"):
            breaker.record_failure(
                f"available server error: {str(data['error'])[:200]}")
            if attempt == 0 and breaker.state == "closed":
                continue
            return None, f"available_server_error: {data['error']}"
        closest = (data.get("archived_snapshots", {}) or {}).get("closest")
        breaker.record_success()
        if not closest or not closest.get("url"):
            return None, "no_snapshot"
        snap_url = closest["url"]
        # The available endpoint returns the bannered snapshot URL;
        # rewrite it to the raw-original form, as the CDX path does.
        snap_url = re.sub(r"/web/(\d+)/", r"/web/\1id_/", snap_url, count=1)
        if not snap_url.startswith("http"):
            snap_url = archive_base + snap_url
        return snap_url, "ok"
    return None, "available_error"


def _fetch_snapshot(client, snapshot, breaker, limiter, timeout):
    """One snapshot fetch. Returns (text, note); text is "" on failure.

    A 429 / 5xx / timeout counts toward the breaker (same dependency,
    same breaker). A 404 or other terminal status is a per-URL
    failure and does not count.
    """
    if not breaker.allow():
        return "", "breaker_open"
    limiter.acquire()
    try:
        resp = client.get(snapshot, timeout=timeout, follow_redirects=True)
    except httpx.HTTPError as e:
        breaker.record_failure(f"snapshot fetch failed: {e}")
        return "", f"snapshot_error: {e}"
    if resp.status_code == 429:
        breaker.record_failure("snapshot HTTP 429")
        return "", "snapshot_http_429"
    if resp.status_code >= 500:
        breaker.record_failure(f"snapshot HTTP {resp.status_code}")
        return "", f"snapshot_http_{resp.status_code}"
    if resp.status_code != 200:
        return "", f"snapshot_http_{resp.status_code}"
    if fetch._is_challenge(resp.text):
        return "", "challenge page (blocked)"
    breaker.record_success()
    text = fetch._extract_text(resp.text, snapshot)
    if len(text) < config.MIN_TEXT_CHARS:
        return "", f"snapshot text too short ({len(text)} chars)"
    return text, "wayback"


def _archive_one(url, publish_date, client, breaker, limiter, api,
                 cdx_base, available_base, archive_base,
                 cdx_timeout, snapshot_timeout):
    """Archive one deferred URL. Never raises: every failure becomes a
    deferred row, so one bad URL cannot kill the run."""
    try:
        if api == "available":
            snapshot_url, status = _available_lookup(
                client, url, publish_date, breaker, limiter,
                available_base, archive_base, cdx_timeout)
            if status == "no_snapshot":
                return url, "no-snapshot", "no wayback snapshot", ""
            if status != "ok":
                return url, "archive-deferred", status, ""
        else:
            target, status = _cdx_lookup(client, url, breaker, limiter,
                                         cdx_base, cdx_timeout)
            if status == "no_snapshot":
                return url, "no-snapshot", "no wayback snapshot", ""
            if status != "ok":
                return url, "archive-deferred", status, ""
            timestamp, original = target
            snapshot_url = _snapshot_url(timestamp, original, archive_base)
        text, note = _fetch_snapshot(client, snapshot_url, breaker, limiter,
                                     snapshot_timeout)
        if text:
            return url, "loaded", note, text
        if note == "breaker_open":
            return url, "archive-deferred", "breaker open", ""
        return url, "archive-deferred", note, ""
    except Exception as e:
        return url, "archive-deferred", f"worker failed: {e}", ""


def archive_all(links_csv, out_csv, limit=None, workers=None,
                archive_rate=None, archive_api=None, cdx_timeout=None,
                snapshot_timeout=None, retry_deferred=False,
                heartbeat_secs=30, breaker_cooldown=None,
                cdx_base=CDX_BASE, available_base=AVAILABLE_BASE,
                archive_base=ARCHIVE_BASE):
    """Run the archive pass over the live pass's deferred rows.

    Returns (attempted, loaded). Rows are recorded in the same state
    DB as the live pass, and texts.csv is re-rendered from the DB in
    input order at the end, so archive recoveries land in the same
    file the gate reads. When the breaker is open at the start (and
    its cooldown has not elapsed), no archive request is sent at all:
    the queue is left deferred and the command exits 0.
    """
    if workers is None:
        workers = getattr(config, "ARCHIVE_WORKERS", 4)
    if archive_rate is None:
        archive_rate = getattr(config, "ARCHIVE_RATE", 2)
    if archive_api is None:
        archive_api = getattr(config, "ARCHIVE_API", "cdx")
    if archive_api not in ("cdx", "available"):
        raise ValueError('--archive-api must be "cdx" or "available"')
    if cdx_timeout is None:
        cdx_timeout = _cdx_timeout()
    if snapshot_timeout is None:
        snapshot_timeout = _snapshot_timeout()
    if breaker_cooldown is None:
        breaker_cooldown = getattr(config, "BREAKER_COOLDOWN_SECS", 600)

    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    state_path = out.with_name(out.stem + config.STATE_DB_SUFFIX)
    manifest_path = out.with_name(out.stem + "_archive_manifest.json")
    state = RunState(state_path)

    links = pd.read_csv(links_csv, dtype=str).fillna("")
    link_rows = []
    for _, link_row in links.iterrows():
        url = (link_row.get("url") or "").strip()
        if url:
            link_rows.append((url, link_row.get("publish_date", ""),
                              link_row.get("source", "")))
    by_url = {url: publish_date for url, publish_date, _ in link_rows}

    queued = state.wayback_queue(retry_deferred=retry_deferred)
    pending = [(url, by_url.get(url, "")) for url in queued if url in by_url]
    if limit is not None:
        pending = pending[:limit]

    raw_breaker = state.get_meta(BREAKER_META_KEY)
    breaker = CircuitBreaker.from_dict(
        json.loads(raw_breaker) if raw_breaker else None,
        max_consecutive=getattr(config, "BREAKER_MAX_CONSECUTIVE", 10),
        window=getattr(config, "BREAKER_WINDOW", 50),
        max_error_rate=getattr(config, "BREAKER_MAX_ERROR_RATE", 0.5),
        cooldown=breaker_cooldown,
        max_cooldown=getattr(config, "BREAKER_MAX_COOLDOWN_SECS", 7200))
    # A stored cooldown longer than this run's setting survives; a
    # shorter stored cooldown is kept too (the breaker, not the flag,
    # owns the backoff once it has tripped).
    limiter = _RateLimiter(archive_rate)

    count = loaded = 0
    deferred = 0
    no_snapshot = 0
    taxonomy = {}
    started = time.monotonic()
    stop_hb = threading.Event()

    def _heartbeat():
        while not stop_hb.wait(heartbeat_secs):
            print(f"  ... archive {count}/{len(pending)} done, "
                  f"{time.monotonic() - started:.0f}s elapsed, "
                  f"breaker={breaker.state}")

    if heartbeat_secs:
        threading.Thread(target=_heartbeat, daemon=True).start()

    manifest = None
    try:
        if pending and breaker.cooling_down():
            # Breaker open and still cooling down: zero archive
            # requests, everything stays deferred, exit 0 quickly.
            # (cooling_down() only inspects state -- it does not admit
            # the half-open probe the way allow() does, so the probe
            # is still available when the cooldown elapses.)
            print(f"  archive breaker OPEN (cooldown "
                  f"{breaker.cooldown:.1f}s, last error: "
                  f"{breaker.last_error}); deferring {len(pending)} "
                  f"rows without sending any archive request")
            deferred = len(pending)
            count = len(pending)
        else:
            limits = httpx.Limits(
                max_connections=max(1, workers + 10),
                max_keepalive_connections=max(1, workers),
                keepalive_expiry=25)
            with httpx.Client(timeout=httpx.Timeout(
                                  30.0, connect=5.0),
                              follow_redirects=True,
                              headers=fetch._request_headers(),
                              trust_env=False,
                              limits=limits) as client:
                def _one(item):
                    url, publish_date = item
                    return _archive_one(
                        url, publish_date, client, breaker, limiter,
                        archive_api, cdx_base, available_base,
                        archive_base, cdx_timeout, snapshot_timeout)

                if workers > 1:
                    pool = ThreadPoolExecutor(max_workers=workers)
                    try:
                        results = pool.map(_one, pending)
                        for url, status, note, text in results:
                            count += 1
                            if status == "loaded":
                                loaded += 1
                            elif status == "no-snapshot":
                                no_snapshot += 1
                            else:
                                deferred += 1
                            taxonomy[note] = taxonomy.get(note, 0) + 1
                            try:
                                state.record_url(url, status, note,
                                                 text if status == "loaded"
                                                 else "")
                            except Exception as e:
                                print(f"  warning: could not record {url}: {e}")
                    finally:
                        pool.shutdown(wait=True)
                else:
                    for item in pending:
                        url, status, note, text = _one(item)
                        count += 1
                        if status == "loaded":
                            loaded += 1
                        elif status == "no-snapshot":
                            no_snapshot += 1
                        else:
                            deferred += 1
                        taxonomy[note] = taxonomy.get(note, 0) + 1
                        try:
                            state.record_url(url, status, note,
                                             text if status == "loaded"
                                             else "")
                        except Exception as e:
                            print(f"  warning: could not record {url}: {e}")
    finally:
        stop_hb.set()
        try:
            state.set_meta(BREAKER_META_KEY,
                           json.dumps(breaker.to_dict()))
        except Exception as e:
            print(f"  warning: could not persist breaker state: {e}")
        try:
            fetch.export_texts_csv(state, link_rows, out)
        except Exception as e:
            print(f"  warning: could not export {out_csv}: {e}")
        manifest = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S",
                                          time.localtime()),
            "params": {"links_csv": str(links_csv),
                       "out_csv": str(out_csv), "limit": limit,
                       "workers": workers, "archive_rate": archive_rate,
                       "archive_api": archive_api,
                       "cdx_timeout": cdx_timeout,
                       "snapshot_timeout": snapshot_timeout,
                       "retry_deferred": retry_deferred},
            "totals": {"attempted": count, "loaded": loaded,
                       "no_snapshot": no_snapshot, "deferred": deferred,
                       "elapsed_secs": round(time.monotonic() - started, 1)},
            "breaker": breaker.to_dict(),
            "taxonomy": dict(sorted(taxonomy.items())),
        }
        try:
            tmp = str(manifest_path) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2, sort_keys=True)
            Path(tmp).replace(manifest_path)
        except Exception as e:
            print(f"  warning: could not write manifest: {e}")
        state.close()
    print(f"archive: {count} attempted, {loaded} loaded, "
          f"{no_snapshot} no snapshot, {deferred} deferred "
          f"(breaker={breaker.state})")
    return count, loaded


def main():
    parser = argparse.ArgumentParser(
        description="Archive pass: recover deferred URLs from the "
                    "Wayback Machine.")
    parser.add_argument("--links", default=str(config.DATA_DIR / "links.csv"))
    parser.add_argument("--out", default=str(config.DATA_DIR / "texts.csv"))
    parser.add_argument("--limit", type=int, default=None,
                        help="cap the number of deferred URLs attempted")
    parser.add_argument("--archive-workers", type=int, default=None,
                        help="concurrent archive lookups/fetches "
                             "(default 4)")
    parser.add_argument("--archive-rate", type=float, default=None,
                        help="max archive.org requests/second "
                             "(default 2; 0 for no cap)")
    parser.add_argument("--archive-api", choices=["cdx", "available"],
                        default=None,
                        help="snapshot-availability API (default cdx; "
                             "available is untested from the UVA IP -- "
                             "see the module docstring)")
    parser.add_argument("--cdx-timeout", type=float, default=None,
                        help="seconds per CDX/available lookup (default 10)")
    parser.add_argument("--snapshot-timeout", type=float, default=None,
                        help="seconds per snapshot fetch (default 15)")
    parser.add_argument("--retry-deferred", action="store_true",
                        help="also re-attempt rows deferred by an earlier "
                             "archive pass (archive-deferred), not just "
                             "live-deferred rows")
    parser.add_argument("--breaker-cooldown", type=float, default=None,
                        help="seconds the breaker stays open after "
                             "tripping (default 600; doubles per failed "
                             "probe, cap 2h)")
    parser.add_argument("--cdx-base", default=CDX_BASE,
                        help="CDX endpoint (test override)")
    parser.add_argument("--available-base", default=AVAILABLE_BASE,
                        help="wayback/available endpoint (test override)")
    parser.add_argument("--archive-base", default=ARCHIVE_BASE,
                        help="archive.org base for snapshot URLs "
                             "(test override)")
    args = parser.parse_args()
    if args.archive_workers is not None and args.archive_workers < 1:
        parser.error("--archive-workers must be >= 1")
    archive_all(args.links, args.out, limit=args.limit,
                workers=args.archive_workers,
                archive_rate=args.archive_rate,
                archive_api=args.archive_api,
                cdx_timeout=args.cdx_timeout,
                snapshot_timeout=args.snapshot_timeout,
                retry_deferred=args.retry_deferred,
                breaker_cooldown=args.breaker_cooldown,
                cdx_base=args.cdx_base,
                available_base=args.available_base,
                archive_base=args.archive_base)


if __name__ == "__main__":
    main()
