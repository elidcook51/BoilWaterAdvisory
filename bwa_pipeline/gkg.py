"""Harvest boil water advisory article links from GDELT's bulk GKG files.

The DOC API only covers January 2017 onward and rate-limits hard, so for
2015-2016 history (and as a backstop for any week the API fails on) this
module goes straight to the source: the daily GKG v2 files listed at
http://data.gdeltproject.org/gdeltv2/masterfilelist.txt (coverage starts
18 Feb 2015, 96 files per day, one per 15 minutes).

GKG rows have no article text and no title, so the filter works on
pre-computed metadata only: themes, locations, and the article URL. A row
matches when it looks like US water news AND its URL slug says water,
or when the URL itself carries a "boil-water" slug. Everything matched
still goes through fetch + LLM extraction downstream, which rejects the
non-advisories, so the filter favors recall -- but the loose theme-only
variant was measured at ~6,000-9,000 matches a day, which is unusable,
so the URL check is what keeps the volume sane (~300-450/day).

Each day's files are downloaded to a temp file, processed, and deleted;
nothing accumulates on disk. A failed file is logged and the day is left
unmarked, so a later --resume run retries it instead of silently
recording zero matches.
"""

import argparse
import csv
import re
import sys
import tempfile
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
import config
from common import canonicalize_url

try:
    import httpx
except ImportError:
    httpx = None

MASTER_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
LINK_COLUMNS = ["url", "source", "source_name", "publish_date",
                "capture_date", "title", "query"]
SOURCE = "gdelt_gkg"

# GKG v2 layout, verified live on 2015-03-04 and 2024-03-04 files
# (27 tab-separated fields in both years, no header row, no title field):
#   [1] DATE, YYYYMMDDHHMMSS       [3] SourceCommonName
#   [4] DocumentIdentifier (URL)   [7] V1Themes   [8] V2Themes
#   [9] V1Locations                [10] V2Locations
IDX_DATE = 1
IDX_SOURCE_NAME = 3
IDX_URL = 4
IDX_V1_THEMES = 7
IDX_V2_THEMES = 8
IDX_V1_LOCATIONS = 9
IDX_V2_LOCATIONS = 10

# Theme tokens carrying WATER or SANITATION, minus the two animal themes
# that share the substring (waterfowl, water buffalo).
ANIMAL_THEMES = {"TAX_WORLDBIRDS_WATERFOWL", "TAX_WORLDMAMMALS_WATER_BUFFALO"}
# Words in the URL slug that suggest the story is actually about water.
# "advisory" and "notice" were tried and dropped: they only ever added
# weather/traffic/earthquake notices, never a water story that "water",
# "drink", or "boil" had not already caught.
URL_WORDS = ("water", "drink", "boil")

# The tight filter below was measured on two full real days:
# 427 matches on 2015-03-04, 336 on 2024-03-04. Theme-only was
# ~6,000-9,000/day (themes are assigned very loosely, e.g. a DHS
# funding bill carries water themes), so the URL check is load-bearing.
# Precision is still low -- maybe one in ten matches is a real
# advisory -- but the LLM stage filters those out at ~$0.0003 each,
# so ~$50 of extraction per harvested year.
GKG_FILE_RE = re.compile(r"(\d{14})\.gkg\.csv\.zip$")


def _themes(fields):
    themes = set()
    for idx in (IDX_V1_THEMES, IDX_V2_THEMES):
        for token in fields[idx].split(";"):
            name = token.split(",")[0].strip()
            if name:
                themes.add(name)
    return themes


def _is_us(fields):
    locations = fields[IDX_V1_LOCATIONS] + ";" + fields[IDX_V2_LOCATIONS]
    return "#US#" in locations


def _has_water_theme(fields):
    for theme in _themes(fields):
        if theme in ANIMAL_THEMES:
            continue
        if "WATER" in theme or "SANITATION" in theme:
            return theme
    return None


def match_row(fields):
    """Decide whether a GKG row is a candidate advisory link.

    Returns (True, query_tag) or (False, ""). The tag records what
    matched, so the links file says why each URL was harvested.
    """
    if len(fields) <= IDX_V2_LOCATIONS:
        return False, ""
    url = fields[IDX_URL].strip()
    if not url or not url.lower().startswith("http"):
        return False, ""
    slug = url.lower()
    if "boil-water" in slug or "boilwater" in slug or "boil_water" in slug:
        return True, "gkg:url-boil-water"
    theme = _has_water_theme(fields)
    if theme and _is_us(fields):
        if any(word in slug for word in URL_WORDS):
            return True, "gkg:" + theme
    return False, ""


def _row_to_link(fields, query_tag):
    raw_date = fields[IDX_DATE].strip()
    publish_date = ""
    if len(raw_date) >= 8 and raw_date[:8].isdigit():
        publish_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
    return {
        "url": fields[IDX_URL].strip(),
        "source": SOURCE,
        "source_name": fields[IDX_SOURCE_NAME].strip(),
        "publish_date": publish_date,
        "capture_date": date.today().isoformat(),
        "title": "",
        "query": query_tag,
    }


def _iter_master_list_lines(master_list_path):
    """Yield raw lines of the master file list, local file or download."""
    if master_list_path:
        with open(master_list_path, encoding="utf-8", errors="replace") as f:
            yield from f
        return
    print("fetching master file list (~130 MB, one-time per run)...", flush=True)
    if httpx is not None:
        with httpx.stream("GET", MASTER_LIST_URL, timeout=600,
                          follow_redirects=True) as resp:
            resp.raise_for_status()
            yield from resp.iter_lines()
    else:
        import urllib.request
        with urllib.request.urlopen(MASTER_LIST_URL, timeout=600) as resp:
            for raw in resp:
                yield raw.decode("utf-8", errors="replace")


def list_gkg_files(since, until, master_list_path=None):
    """Map each date in [since, until] to its sorted GKG file URLs."""
    by_day = {}
    for line in _iter_master_list_lines(master_list_path):
        parts = line.split()
        if len(parts) < 3:
            continue
        url = parts[-1]
        m = GKG_FILE_RE.search(url)
        if not m:
            continue
        stamp = m.group(1)
        day = stamp[:4] + "-" + stamp[4:6] + "-" + stamp[6:8]
        if since.isoformat() <= day <= until.isoformat():
            by_day.setdefault(day, []).append(url)
    return {day: sorted(urls) for day, urls in sorted(by_day.items())}


def _download_httpx(url, dest, timeout):
    with httpx.stream("GET", url, timeout=timeout,
                      follow_redirects=True) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)


def _download_urllib(url, dest, timeout):
    import urllib.request
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        with open(dest, "wb") as f:
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)


def _download(url, dest, timeout=180):
    """Download url to dest path. Raises on failure after 2 retries.

    Prefers httpx, but falls back to urllib per attempt: some sandboxes
    ship a NO_PROXY list that httpx cannot even parse, which would
    otherwise fail every download before a byte is sent.
    """
    last_error = None
    for attempt in range(3):
        for fetch in (_download_httpx, _download_urllib):
            if fetch is _download_httpx and httpx is None:
                continue
            try:
                fetch(url, dest, timeout)
                return
            except Exception as e:
                last_error = e
        if attempt < 2:
            time.sleep(5)
    raise last_error


def process_file(url, seen, limit):
    """Download one GKG zip and return (rows_scanned, matched_links)."""
    rows = 0
    links = []
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()
    try:
        _download(url, tmp.name)
        with zipfile.ZipFile(tmp.name) as zf:
            member = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(member) as raw:
                for line in raw:
                    fields = line.decode("utf-8", errors="replace")
                    fields = fields.rstrip("\n").rstrip("\r").split("\t")
                    if len(fields) <= IDX_V2_LOCATIONS:
                        continue
                    rows += 1
                    matched, tag = match_row(fields)
                    if not matched:
                        continue
                    link = _row_to_link(fields, tag)
                    key = canonicalize_url(link["url"])
                    if key in seen:
                        continue
                    seen.add(key)
                    links.append(link)
                    if limit is not None and len(links) >= limit:
                        break
    finally:
        Path(tmp.name).unlink(missing_ok=True)
    return rows, links


def _load_done_days(done_path):
    if not done_path.exists():
        return set()
    return {line.strip() for line in done_path.read_text().splitlines()
            if line.strip()}


def _load_seen_urls(out_path):
    seen = set()
    if out_path.exists() and out_path.stat().st_size > 0:
        with open(out_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("url"):
                    seen.add(canonicalize_url(row["url"]))
    return seen


def harvest(since, until, out_path, limit=None, resume=False,
            master_list_path=None):
    """Run the harvest over [since, until]. Returns (matched, failed_days)."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    done_path = Path(str(out_path) + ".done-days")
    done_days = _load_done_days(done_path) if resume else set()
    seen = _load_seen_urls(out_path)
    total_matched = 0
    failed_days = []
    wrote_header = (not out_path.exists()) or out_path.stat().st_size == 0

    files_by_day = list_gkg_files(since, until, master_list_path)
    if not files_by_day:
        print("no GKG files found in that date range "
              "(coverage starts 2015-02-18)", flush=True)
        return 0, []
    day = since
    while day <= until:
        day_str = day.isoformat()
        urls = files_by_day.get(day_str, [])
        if resume and day_str in done_days:
            print(f"gkg {day_str}: already done, skipping", flush=True)
            day += timedelta(days=1)
            continue
        if not urls:
            print(f"gkg {day_str}: no GKG files listed", flush=True)
            day += timedelta(days=1)
            continue
        day_rows = 0
        day_links = []
        failed_files = []
        files_done = 0
        limit_hit = False
        for url in urls:
            if limit is not None and total_matched + len(day_links) >= limit:
                limit_hit = True
                break
            try:
                rows, links = process_file(
                    url, seen,
                    None if limit is None
                    else limit - total_matched - len(day_links))
            except Exception as e:
                print(f"  {urlparse(url).path.split('/')[-1]}: "
                      f"download failed ({e})", flush=True)
                failed_files.append(url)
                files_done += 1
                continue
            files_done += 1
            day_rows += rows
            day_links.extend(links)
        if day_links:
            with open(out_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=LINK_COLUMNS)
                if wrote_header:
                    writer.writeheader()
                    wrote_header = False
                writer.writerows(day_links)
            total_matched += len(day_links)
        if failed_files:
            print(f"gkg {day_str}: FAILED "
                  f"({len(failed_files)} of {files_done} files failed; "
                  f"{len(day_links)} matches kept)", flush=True)
            failed_days.append(day_str)
        else:
            print(f"gkg {day_str}: {files_done} files, "
                  f"{day_rows} rows scanned, {len(day_links)} matches", flush=True)
            if not limit_hit:
                with open(done_path, "a", encoding="utf-8") as f:
                    f.write(day_str + "\n")
        if limit_hit:
            print(f"limit of {limit} matched URLs reached; stopping", flush=True)
            break
        day += timedelta(days=1)
    print(f"\nmatched {total_matched} URLs in total", flush=True)
    if failed_days:
        print("failed days (not marked done, retry with --resume): "
              + ", ".join(failed_days), flush=True)
    return total_matched, failed_days


def main():
    parser = argparse.ArgumentParser(
        description="Harvest boil water advisory links from GDELT bulk "
                    "GKG files (2015-02-18 onward).")
    parser.add_argument("--since", default=None,
                        help="start date YYYY-MM-DD (default: 7 days ago)")
    parser.add_argument("--until", default=None,
                        help="end date YYYY-MM-DD (default: today)")
    parser.add_argument("--out", default=str(config.DATA_DIR / "links_gkg.csv"),
                        help="output links.csv (appended to; resume-safe)")
    parser.add_argument("--limit", type=int, default=None,
                        help="stop after this many matched URLs (smoke test)")
    parser.add_argument("--resume", action="store_true",
                        help="skip days already recorded as done")
    parser.add_argument("--master-list", default=None,
                        help="use a local copy of masterfilelist.txt "
                             "instead of downloading it")
    args = parser.parse_args()
    today = date.today()
    since = (date.fromisoformat(args.since) if args.since
             else today - timedelta(days=7))
    until = date.fromisoformat(args.until) if args.until else today
    if since > until:
        parser.error("--since must not be after --until")
    harvest(since, until, Path(args.out), limit=args.limit,
            resume=args.resume, master_list_path=args.master_list)


if __name__ == "__main__":
    main()
