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

# Fetching: identify the bot, retry briefly, drop junk pages.
USER_AGENT = "BoilWaterAdvisoryBot/1.0 (academic research project)"
FETCH_TIMEOUT = 20
# Live attempts per URL = FETCH_RETRIES + 1 = 3 (v3 per-URL budget).
FETCH_RETRIES = 2
MIN_TEXT_CHARS = 300

# v3 fetcher defaults.
MAX_RATE = 20              # global requests/second cap (0 for no cap)
CURL_CFFI = True           # try curl_cffi impersonate="chrome" as a
                           # second attempt after an httpx 403/429
CHUNK_SIZE = 2000          # URLs submitted per chunk; rows are written
                           # in input order within each chunk, so one
                           # slow URL stalls only its chunk
FETCH_CONNECT_TIMEOUT = 5  # per-phase timeouts for live fetches (s)
FETCH_READ_TIMEOUT = 15
FETCH_WRITE_TIMEOUT = 10
FETCH_POOL_TIMEOUT = 5
CDX_TIMEOUT = 10           # seconds per Wayback CDX attempt (2 attempts)
SNAPSHOT_TIMEOUT = 15      # seconds for a Wayback snapshot fetch
ROBOTS_CACHE_TTL_HOURS = 24
STATE_DB_SUFFIX = "_state.db"       # appended to the out-file stem
MANIFEST_SUFFIX = "_manifest.json"  # appended to the out-file stem
PARKED_HOSTS_FILE = "parked_hosts.json"  # beside the out file
ROBOTS_CACHE_FILE = "robots_cache.json"  # beside the state DB

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
