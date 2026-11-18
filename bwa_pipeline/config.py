"""Settings for the boil water advisory pipeline.

Put API keys in a .env file at the repo head (never commit it):
    OPEN_AI_API_KEY=...        # required, for the LLM extraction step
    GNEWS_API_KEY=...          # optional, gnews.io (free: 100 requests/day)
    NEWSDATA_API_KEY=...       # optional, newsdata.io (free tier)
    MEDIACLOUD_API_KEY=...     # optional, mediacloud.org (free research key)

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

# Fetching: identify the bot, retry briefly, drop junk pages.
USER_AGENT = "BoilWaterAdvisoryBot/1.0 (academic research project)"
FETCH_TIMEOUT = 20
FETCH_RETRIES = 3
MIN_TEXT_CHARS = 300

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
