"""Discover boil-water-advisory article URLs through GDELT in BigQuery.

Why this exists: the GDELT DOC API (see discover.py) rate-limits hard
enough to be unusable from some IPs, and it only covers Jan 2017 onward.
GDELT also publishes its full archive as the public BigQuery dataset
gdelt-bq, so this module queries the GKG table directly with SQL instead.

Table and schema, verified 2026-11-18 against:
- GDELT's "Announcing Partitioned GDELT BigQuery Tables" post
  (blog.gdeltproject.org) and the py-gdelt data-sources page: the
  partitioned GKG table is `gdelt-bq.gdeltv2.gkg_partitioned` (note the
  underscore; plain `gkg` is NOT partitioned and a full scan of it is
  ~3.6TB). It is partitioned by ingestion time (_PARTITIONTIME, daily).
- The GDELT GKG 2.0 codebook field list: DATE is an INT64
  YYYYMMDDHHMMSS, DocumentIdentifier is the article URL,
  SourceCommonName the publisher domain, V2Themes the semicolon-delimited
  theme list, V2Locations the location list.
- The GDELT theme category list
  (data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_CategoryList.xlsx,
  fetched 2026-11-18): the water themes it documents are WATER_SECURITY
  ("clean water"), ENV_WATERWAYS, and SANITATION. The list is known to be
  incomplete (real GKG data carries ~50k distinct themes), so the filter
  matches on substrings, which catches undocumented water themes too.

What the filter can and cannot do: GKG is metadata, not full text, so
there is no exact-phrase search like the DOC API's "boil water advisory".
Rows match when V2Themes contains WATER, DRINKING, SANITATION, or
CONTAMINAT. That is deliberately recall-first: the downstream fetch and
LLM extraction stages reject the non-advisories. By default rows are also
restricted to US-located articles (V2Locations mentions "United States");
pass --no-us-only to drop that filter. GKG carries no title, so the title
column is left empty.

COST — read this before running. BigQuery bills by bytes scanned, not by
rows matched, and the free tier is 1 TB/month. The query selects only the
columns it needs and prunes partitions to the chunk being harvested, but a
month still scans roughly 30-60 GB (estimated from GDELT's own example of
15 GB for 15 days of a 3-column query). A full 10-year backfill is
therefore roughly 4-7 TB scanned, which exceeds the free tier if done in
one billing month. Two rules follow:
- ALWAYS run --dry-run first. It reports the estimated bytes per chunk
  without executing anything, so you see the bill before spending it.
- Then validate with one small chunk: --chunk-days 1 --limit 20 on a
  single day. Note --limit caps rows written, NOT bytes scanned; a
  --limit 20 run over a month still scans the whole month.
Each executed chunk also enforces a maximum_bytes_billed cap (default
250 GB) and prints bytes processed plus a running total.

Auth: a Google Cloud service-account key file (BigQuery User role) whose
path is in .env as BQ_KEY_FILE. The key file lives on your machine; this
module was built and tested without one (see the report), so the first
live run is yours: dry-run, then one day, then a month.
"""

import argparse
import csv
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from common import canonicalize_url, parse_date

TABLE = "gdelt-bq.gdeltv2.gkg_partitioned"

# GKG v2 coverage starts 19 Feb 2015; chunks before that are skipped
# without querying (they would scan partitions and return nothing).
GKG_START = date(2015, 2, 18)

# Substrings matched against V2Themes (LIKE '%...%'). WATER covers
# WATER_SECURITY, ENV_WATERWAYS, and any undocumented water theme;
# DRINKING covers DRINKING_WATER-style themes; SANITATION is a documented
# theme; CONTAMINAT is speculative (no documented contamination theme, but
# harmless if one exists in the wild). Same columns are scanned whatever
# the patterns, so adding a pattern costs nothing in BigQuery bytes.
THEME_PATTERNS = ["WATER", "DRINKING", "SANITATION", "CONTAMINAT"]

LINKS_COLUMNS = ["url", "source", "source_name", "publish_date",
                 "capture_date", "title", "query"]

WARN_BYTES = 500_000_000_000  # warn once the running total passes 500 GB
DEFAULT_MAX_BYTES_BILLED = 250_000_000_000  # per-chunk cap, 250 GB


def _today():
    return date.today().isoformat()


def _date_int(d):
    """A date as a GKG DATE integer at 00:00:00, e.g. 20240601000000."""
    return int(d.strftime("%Y%m%d") + "000000")


def _parse_gkg_date(value):
    """GKG DATE (YYYYMMDDHHMMSS int) -> YYYY-MM-DD string, or None."""
    if value is None:
        return None
    text = str(value).strip()
    if len(text) != 14 or not text.isdigit():
        return None
    return parse_date(f"{text[:4]}-{text[4:6]}-{text[6:8]}")


def _matched_pattern(themes):
    """First THEME_PATTERNS substring found in the themes string, or None."""
    themes = themes or ""
    for pattern in THEME_PATTERNS:
        if pattern in themes:
            return pattern
    return None


def build_sql(chunk_start, chunk_end, us_only=True):
    """SQL for one chunk, plus the partition window it prunes to.

    The _PARTITIONTIME window is padded a day on each side because it is
    ingestion time, not article time: an article published late in the
    chunk can be ingested the next day. The DATE range keeps the result
    to the chunk itself, so the padding costs a little scan, not bad rows.
    All interpolated values come from parse_date-validated dates.
    """
    part_start = chunk_start - timedelta(days=1)
    part_end = chunk_end + timedelta(days=2)
    theme_clause = " OR ".join(
        f"V2Themes LIKE '%{p}%'" for p in THEME_PATTERNS)
    us_clause = "\n  AND V2Locations LIKE '%United States%'" if us_only else ""
    sql = f"""SELECT DATE, DocumentIdentifier, SourceCommonName, V2Themes
FROM `{TABLE}`
WHERE _PARTITIONTIME >= TIMESTAMP('{part_start.isoformat()}')
  AND _PARTITIONTIME < TIMESTAMP('{part_end.isoformat()}')
  AND DATE >= {_date_int(chunk_start)}
  AND DATE < {_date_int(chunk_end + timedelta(days=1))}
  AND SourceCollectionIdentifier = 1{us_clause}
  AND ({theme_clause})"""
    return sql


def build_client():
    """BigQuery client from the service-account key in config.BQ_KEY_FILE."""
    key_file = (config.BQ_KEY_FILE or "").strip()
    if not key_file:
        sys.exit("BQ_KEY_FILE is not set. Add it to a .env file at the repo "
                 "head:\n    BQ_KEY_FILE=C:\\path\\to\\service-account.json")
    key_path = Path(key_file)
    if not key_path.exists():
        sys.exit(f"BQ_KEY_FILE points at a file that does not exist: "
                 f"{key_file}")
    try:
        from google.cloud import bigquery
    except ImportError:
        sys.exit("google-cloud-bigquery is not installed. Run:\n"
                 "    pip install -r bwa_pipeline/requirements.txt")
    return bigquery.Client.from_service_account_json(str(key_path))


def _read_done(done_path):
    if not done_path.exists():
        return set()
    try:
        return {line.strip() for line in done_path.read_text().splitlines()
                if line.strip()}
    except OSError:
        return set()


def _mark_done(done_path, chunk_start):
    with open(done_path, "a", encoding="utf-8") as f:
        f.write(chunk_start.isoformat() + "\n")


def _existing_urls(out_path):
    if not out_path.exists():
        return set()
    try:
        with open(out_path, newline="", encoding="utf-8") as f:
            return {canonicalize_url(r.get("url", ""))
                    for r in csv.DictReader(f)}
    except OSError:
        return set()


def _chunks(since, until, chunk_days):
    chunk_start = since
    while chunk_start <= until:
        chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), until)
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


def _rows_to_links(rows, seen):
    """Turn BigQuery rows into links.csv dicts, skipping bad rows."""
    links = []
    for row in rows:
        url = (row["DocumentIdentifier"] or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            continue
        publish_date = _parse_gkg_date(row.get("DATE"))
        if publish_date is None:
            continue
        pattern = _matched_pattern(row.get("V2Themes"))
        if pattern is None:
            continue
        canon = canonicalize_url(url)
        if not canon or canon in seen:
            continue
        seen.add(canon)
        links.append({
            "url": canon,
            "source": "gdelt_bq",
            "source_name": row.get("SourceCommonName") or "",
            "publish_date": publish_date.isoformat(),
            "capture_date": _today(),
            "title": "",
            "query": f"bq:{pattern}",
        })
    return links


def _write_links(out_path, links):
    header = (not out_path.exists()) or out_path.stat().st_size == 0
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LINKS_COLUMNS)
        if header:
            writer.writeheader()
        writer.writerows(links)


def discover_bq(out, since=None, until=None, limit=None, dry_run=False,
                 chunk_days=31, us_only=True,
                 max_bytes_billed=DEFAULT_MAX_BYTES_BILLED):
    """Query GKG month by month (or chunk_days at a time) into links.csv.

    Returns (links_written, bytes_processed, failed_chunks). In dry-run
    mode nothing is executed or written; bytes_processed is the estimate.
    """
    from google.cloud import bigquery

    end = parse_date(until) or date.today()
    start = parse_date(since) or (end - timedelta(days=30))
    out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    done_path = Path(str(out_path) + ".done")
    done = _read_done(done_path)
    seen = _existing_urls(out_path)

    client = build_client()
    written = 0
    bytes_total = 0
    warned = False
    failed = []
    for chunk_start, chunk_end in _chunks(start, end, chunk_days):
        label = f"{chunk_start}..{chunk_end}"
        if chunk_end < GKG_START:
            print(f"  bq {label}: before GKG coverage (starts 2015-02-18); skipped")
            continue
        if chunk_start.isoformat() in done:
            print(f"  bq {label}: already done; skipped")
            continue
        sql = build_sql(chunk_start, chunk_end, us_only=us_only)
        if dry_run:
            job_config = bigquery.QueryJobConfig(
                dry_run=True, use_query_cache=False)
            job = client.query(sql, job_config=job_config)
            estimate = job.total_bytes_processed or 0
            bytes_total += estimate
            print(f"  bq {label}: dry-run estimate "
                  f"{estimate / 1e9:.1f} GB")
            if estimate > max_bytes_billed:
                print(f"    over the {max_bytes_billed / 1e9:.0f} GB "
                      f"per-chunk cap; raise --max-bytes-billed to run it")
            continue
        job_config = bigquery.QueryJobConfig(
            use_query_cache=False, maximum_bytes_billed=max_bytes_billed)
        try:
            job = client.query(sql, job_config=job_config)
            rows = list(job.result())
        except Exception as e:
            print(f"  bq {label}: FAILED ({e})")
            failed.append((label, str(e)))
            continue
        processed = job.total_bytes_processed or 0
        bytes_total += processed
        if bytes_total > WARN_BYTES and not warned:
            print(f"  WARNING: running total passed "
                  f"{WARN_BYTES / 1e9:.0f} GB scanned "
                  f"({bytes_total / 1e9:.1f} GB so far)")
            warned = True
        links = _rows_to_links(rows, seen)
        truncated = False
        if limit is not None:
            remaining = max(0, limit - written)
            if len(links) > remaining:
                links = links[:remaining]
                truncated = True
        if links:
            _write_links(out_path, links)
            written += len(links)
        if not truncated:
            _mark_done(done_path, chunk_start)
        else:
            print(f"  bq {label}: limit cut this chunk short; "
                  f"not marked done, re-run to finish it")
        print(f"  bq {label}: {len(links)} links, "
              f"{processed / 1e9:.1f} GB processed "
              f"(running total {bytes_total / 1e9:.1f} GB)")
        if limit is not None and written >= limit:
            print(f"  limit of {limit} links reached; stopping")
            break
    if failed:
        print("failed chunks (not marked done; re-run to retry):")
        for label, reason in failed:
            print(f"  {label}: {reason}")
    return written, bytes_total, failed


def main():
    parser = argparse.ArgumentParser(
        description="Discover boil-water-advisory article URLs from "
                    "GDELT's GKG table in BigQuery.")
    parser.add_argument("--out", default=str(config.DATA_DIR / "links_bq.csv"))
    parser.add_argument("--since", default=None,
                        help="start date YYYY-MM-DD (default: 30 days ago)")
    parser.add_argument("--until", default=None, help="end date YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=None,
                        help="cap the number of links written (smoke tests; "
                             "does NOT reduce bytes scanned)")
    parser.add_argument("--dry-run", action="store_true",
                        help="report the estimated bytes per chunk without "
                             "executing anything")
    parser.add_argument("--chunk-days", type=int, default=31,
                        help="days per query chunk (default 31; use 1 for a "
                             "cheap first live test)")
    parser.add_argument("--max-bytes-billed", type=int,
                        default=DEFAULT_MAX_BYTES_BILLED,
                        help="per-chunk bytes cap; a chunk estimated above "
                             "this is not run (default 250 GB)")
    parser.add_argument("--no-us-only", action="store_true",
                        help="drop the US-locations filter (global water "
                             "news; much noisier)")
    args = parser.parse_args()

    written, bytes_total, failed = discover_bq(
        args.out, since=args.since, until=args.until, limit=args.limit,
        dry_run=args.dry_run, chunk_days=args.chunk_days,
        us_only=not args.no_us_only,
        max_bytes_billed=args.max_bytes_billed)
    if args.dry_run:
        print(f"dry-run total estimate: {bytes_total / 1e9:.1f} GB")
    else:
        print(f"wrote {written} links to {args.out} "
              f"({bytes_total / 1e9:.1f} GB scanned)")


if __name__ == "__main__":
    main()
