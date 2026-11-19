"""SQLite run state for the fetcher (v3).

One small database next to the output CSV, holding per-URL outcomes and
per-host throttle/block state so a re-run resumes in O(1) instead of
re-scanning a multi-gigabyte texts.csv. The database is the resume
index: if it exists, fetch_all trusts it and never reads texts.csv for
resume purposes. If it does not exist (e.g. a v2.9 output), fetch_all
falls back to scanning texts.csv once, exactly as before.

If you delete texts.csv by hand, delete the matching *_state.db too,
or the next run will still believe every URL is done.
"""

import json
import sqlite3
import threading
import time

# Statuses recorded per URL. "loaded" and "failed" are terminal for a
# normal run; "wayback-retry" means the Wayback CDX lookup failed in a
# retryable way (rate-limited / non-JSON / 5xx) and the URL is a
# candidate for a later --retry-failed pass, which re-attempts it along
# with the "failed" rows.
DONE_STATUSES = ("loaded", "failed", "wayback-retry")
RETRYABLE_STATUSES = ("failed", "wayback-retry")


class RunState:
    """Per-run SQLite store: URL outcomes + per-host stats."""

    def __init__(self, path):
        self.path = str(path)
        # RLock, not Lock: update_host reads the current record via
        # get_host while holding the lock, and a plain Lock would
        # deadlock on that re-entrant acquire.
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS urls ("
            " url TEXT PRIMARY KEY,"
            " status TEXT NOT NULL,"
            " note TEXT NOT NULL DEFAULT '',"
            " fetched_at TEXT NOT NULL)")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS hosts ("
            " host TEXT PRIMARY KEY,"
            " parked INTEGER NOT NULL DEFAULT 0,"
            " park_reason TEXT NOT NULL DEFAULT '',"
            " delay REAL NOT NULL DEFAULT 1.0,"
            " concurrency INTEGER NOT NULL DEFAULT 1,"
            " requests INTEGER NOT NULL DEFAULT 0,"
            " ok INTEGER NOT NULL DEFAULT 0,"
            " rate_limited INTEGER NOT NULL DEFAULT 0,"
            " blocked INTEGER NOT NULL DEFAULT 0,"
            " terminal INTEGER NOT NULL DEFAULT 0,"
            " timeout INTEGER NOT NULL DEFAULT 0,"
            " challenge INTEGER NOT NULL DEFAULT 0,"
            " cdx_errors INTEGER NOT NULL DEFAULT 0,"
            " latencies TEXT NOT NULL DEFAULT '[]',"
            " updated_at TEXT NOT NULL)")
        self._conn.commit()

    def _now(self):
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

    def is_done(self, url):
        """True when the URL has any recorded outcome."""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM urls WHERE url = ?", (url,)).fetchone()
        return row is not None

    def get_record(self, url):
        """The recorded {status, note, fetched_at} for a URL, or None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT status, note, fetched_at FROM urls WHERE url = ?",
                (url,)).fetchone()
        if row is None:
            return None
        return {"status": row[0], "note": row[1], "fetched_at": row[2]}

    def record_url(self, url, status, note=""):
        """Upsert one URL outcome (called as each row is written)."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO urls (url, status, note, fetched_at)"
                " VALUES (?, ?, ?, ?)"
                " ON CONFLICT(url) DO UPDATE SET"
                " status = excluded.status, note = excluded.note,"
                " fetched_at = excluded.fetched_at",
                (url, status, note or "", self._now()))
            self._conn.commit()

    def done_urls(self):
        """All URLs with a recorded outcome."""
        with self._lock:
            rows = self._conn.execute("SELECT url FROM urls").fetchall()
        return {r[0] for r in rows}

    def retryable_urls(self):
        """URLs whose outcome was failed or wayback-retry, in first-seen order."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT url FROM urls WHERE status IN ('failed',"
                " 'wayback-retry') ORDER BY rowid").fetchall()
        return [r[0] for r in rows]

    def get_host(self, host):
        """Per-host record as a dict, or None when never seen."""
        with self._lock:
            row = self._conn.execute(
                "SELECT parked, park_reason, delay, concurrency, requests,"
                " ok, rate_limited, blocked, terminal, timeout, challenge,"
                " cdx_errors, latencies, updated_at FROM hosts"
                " WHERE host = ?", (host,)).fetchone()
        if row is None:
            return None
        try:
            latencies = json.loads(row[12] or "[]")
        except ValueError:
            latencies = []
        return {"host": host, "parked": bool(row[0]), "park_reason": row[1],
                "delay": row[2], "concurrency": row[3], "requests": row[4],
                "ok": row[5], "rate_limited": row[6], "blocked": row[7],
                "terminal": row[8], "timeout": row[9], "challenge": row[10],
                "cdx_errors": row[11], "latencies": latencies,
                "updated_at": row[13]}

    def update_host(self, host, **fields):
        """Upsert per-host fields (unknown keys are ignored).

        Counters are stored as given: callers pass cumulative totals
        (fetch._persist_hosts adds each run's counters to the stored
        ones), not just the latest run's counts.
        """
        allowed = {"parked", "park_reason", "delay", "concurrency",
                   "requests", "ok", "rate_limited", "blocked", "terminal",
                   "timeout", "challenge", "cdx_errors", "latencies"}
        values = {k: v for k, v in fields.items() if k in allowed}
        if "latencies" in values and not isinstance(values["latencies"], str):
            values["latencies"] = json.dumps(list(values["latencies"]))
        if "parked" in values:
            values["parked"] = 1 if values["parked"] else 0
        with self._lock:
            current = self.get_host(host)
            merged = dict(current) if current else {}
            merged.update(values)
            merged.setdefault("parked", 0)
            merged.setdefault("park_reason", "")
            merged.setdefault("delay", 1.0)
            merged.setdefault("concurrency", 1)
            for key in ("requests", "ok", "rate_limited", "blocked",
                        "terminal", "timeout", "challenge", "cdx_errors"):
                merged.setdefault(key, 0)
            merged.setdefault("latencies", "[]")
            if isinstance(merged["latencies"], list):
                merged["latencies"] = json.dumps(merged["latencies"])
            self._conn.execute(
                "INSERT INTO hosts (host, parked, park_reason, delay,"
                " concurrency, requests, ok, rate_limited, blocked,"
                " terminal, timeout, challenge, cdx_errors, latencies,"
                " updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                " ?, ?, ?) ON CONFLICT(host) DO UPDATE SET"
                " parked = excluded.parked,"
                " park_reason = excluded.park_reason,"
                " delay = excluded.delay,"
                " concurrency = excluded.concurrency,"
                " requests = excluded.requests, ok = excluded.ok,"
                " rate_limited = excluded.rate_limited,"
                " blocked = excluded.blocked,"
                " terminal = excluded.terminal,"
                " timeout = excluded.timeout,"
                " challenge = excluded.challenge,"
                " cdx_errors = excluded.cdx_errors,"
                " latencies = excluded.latencies,"
                " updated_at = excluded.updated_at",
                (host, merged["parked"], merged["park_reason"],
                 merged["delay"], merged["concurrency"], merged["requests"],
                 merged["ok"], merged["rate_limited"], merged["blocked"],
                 merged["terminal"], merged["timeout"], merged["challenge"],
                 merged["cdx_errors"], merged["latencies"], self._now()))
            self._conn.commit()

    def park_host(self, host, reason=""):
        """Mark a host parked (Wayback-only from now on)."""
        self.update_host(host, parked=True, park_reason=reason)

    def is_parked(self, host):
        record = self.get_host(host)
        return bool(record and record["parked"])

    def parked_hosts(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT host, park_reason FROM hosts WHERE parked = 1"
                " ORDER BY host").fetchall()
        return {host: reason for host, reason in rows}

    def host_stats(self):
        """All per-host records, as a list of dicts."""
        with self._lock:
            rows = self._conn.execute("SELECT host FROM hosts").fetchall()
        return [self.get_host(r[0]) for r in rows]

    def close(self):
        with self._lock:
            self._conn.commit()
            self._conn.close()
