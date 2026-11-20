"""Settings for the boil water advisory pipeline.

Put API keys in a .env file at the repo head (never commit it):
    OPEN_AI_API_KEY=...        # required, for the LLM extraction step
    GNEWS_API_KEY=...          # optional, gnews.io (free: 100 requests/day)
    NEWSDATA_API_KEY=...       # optional, newsdata.io (free tier)
    MEDIACLOUD_API_KEY=...     # optional, mediacloud.org (free research key)
    BQ_KEY_FILE=...            # optional, path to a Google Cloud
                               # service-account JSON key (BigQuery User
                               # role) for the GDELT BigQuery harvester
    BROWSER_HEADERS_FILE=...   # optional, path to a JSON file with your
                               # own browser's request headers (see
                               # fetch.py); without it, a generic
                               # browser-like header set is sent

Paths are relative to the bwa_pipeline/ folder unless you override them
on the command line.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

LLM_MODEL = "gpt-4.1-nano"
OPEN_AI_API_KEY = os.getenv("OPEN_AI_API_KEY")

GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY")
MEDIACLOUD_API_KEY = os.getenv("MEDIACLOUD_API_KEY")
BQ_KEY_FILE = os.getenv("BQ_KEY_FILE")
BROWSER_HEADERS_FILE = os.getenv("BROWSER_HEADERS_FILE")

# Fetching: identify the bot, drop junk pages.
USER_AGENT = "BoilWaterAdvisoryBot/1.0 (academic research project)"
FETCH_TIMEOUT = 20
# v4 makes exactly one live attempt per URL, by construction: there
# is no retry loop in fetch.py and no setting that could restore one.
# A second live attempt on a 404/timeout has ~no chance and costs a
# full latency; retries belong to a separate --retry-failed pass, not
# the hot path. The curl_cffi second attempt after an httpx 403/429
# still applies.
MIN_TEXT_CHARS = 300

# v4 fetcher defaults.
MAX_RATE = 20              # global requests/second cap (0 for no cap)
CURL_CFFI = True           # try curl_cffi impersonate="chrome" as a
                           # second attempt after an httpx 403/429
DEFAULT_WORKERS = 20       # thread-pool size for the live pass (the
                           # measured knee; re-probe on the run machine)
FETCH_CONNECT_TIMEOUT = 5  # per-phase timeouts for live fetches (s)
FETCH_READ_TIMEOUT = 8     # v4: 8s, down from 15s -- timeouts consumed
                           # ~55% of worker-seconds at 15s, and cutting
                           # to 8s is worth ~20-25% throughput
FETCH_WRITE_TIMEOUT = 10
FETCH_POOL_TIMEOUT = 5
ROBOTS_CACHE_TTL_HOURS = 24
STATE_DB_SUFFIX = "_state.db"       # appended to the out-file stem
MANIFEST_SUFFIX = "_manifest.json"  # appended to the out-file stem
PARKED_HOSTS_FILE = "parked_hosts.json"  # beside the out file
ROBOTS_CACHE_FILE = "robots_cache.json"  # beside the state DB

# Archive command (archive.py) defaults. The archive pass is a
# separate command over the rows the live pass deferred; it never
# shares a limiter with the live pass.
CDX_TIMEOUT = 10           # seconds per Wayback CDX attempt
SNAPSHOT_TIMEOUT = 15      # seconds for a Wayback snapshot fetch
ARCHIVE_WORKERS = 4        # concurrent archive lookups/fetches
ARCHIVE_RATE = 2           # max archive.org requests/second (0: no cap)
ARCHIVE_API = "cdx"        # snapshot-availability API: "cdx" or
                           # "available" (wayback/available endpoint)
# Circuit breaker (archive only): trip after this many consecutive
# CDX/snapshot failures, or an error rate above BREAKER_MAX_ERROR_RATE
# over the last BREAKER_WINDOW lookups. While open, zero archive
# requests are sent; the remaining queue is deferred and the command
# exits 0. Cooldown doubles after each failed half-open probe, capped.
BREAKER_MAX_CONSECUTIVE = 10
BREAKER_WINDOW = 50
BREAKER_MAX_ERROR_RATE = 0.5
BREAKER_COOLDOWN_SECS = 600     # 10 minutes
BREAKER_MAX_COOLDOWN_SECS = 7200  # 2 hours

# Discovery queries. The same list feeds every search-based source so
# results are comparable across them.
DISCOVERY_QUERIES = [
    '"boil water advisory"',
    '"boil water notice"',
    '"boil-water advisory"',
    '"precautionary boil water"',
    '"boil water order"',
    '"lifted" "boil water"',
    '"do not drink" water',
]

# Dedupe tuning (see dedupe.py).
DEDUP_CONFIDENCE = 0.6
DEDUP_DATE_WINDOW_DAYS = 21
