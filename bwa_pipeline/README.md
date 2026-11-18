# Boil Water Advisory Pipeline

Builds a historical database (2015+) of US boil water advisories from news
articles and government pages. Each article is fetched, run through an LLM
to extract structured fields (dates, location, utility, cause), and then
deduplicated so multiple articles about the same advisory collapse to one row.

## Setup

Requires Python 3.10+.

```bash
pip install -r requirements.txt
```

Copy `.env.example` to `.env` (or create `.env` at the repo head) and fill
in your keys. Never commit `.env`.

| Key | Required? | Used for |
| --- | --- | --- |
| `OPEN_AI_API_KEY` | **Yes** | LLM extraction (`extract.py`) and dedupe adjudication (`dedupe.py`) |
| `GNEWS_API_KEY` | No | gnews.io discovery (free: 100 requests/day) |
| `NEWSDATA_API_KEY` | No | newsdata.io discovery (free tier) |
| `MEDIACLOUD_API_KEY` | No | Media Cloud discovery (free research key) |
| `BQ_KEY_FILE` | No | BigQuery harvester (`bq.py`): path to a Google Cloud service-account JSON key with the BigQuery User role |

If an optional key is missing, its discovery source is skipped and the
pipeline carries on with the remaining sources. The only hard requirement
is `OPEN_AI_API_KEY`: without it, `extract.py` and `dedupe.py` exit with a
message telling you to set it.

## Running one stage at a time

Every stage is its own script and can be run directly. The data folder
defaults to `bwa_pipeline/data`; pass `--data-dir` to `run.py` (or the
equivalent flags on the scripts) to put it somewhere else.

```bash
python discover.py --out data/links.csv --since 2025-11-01 --until 2026-11-17
python fetch.py --links data/links.csv --out data/texts.csv
python extract.py --texts data/texts.csv --out data/structured.csv
python dedupe.py --structured data/structured.csv --texts data/texts.csv \
    --out data/deduped.csv --review data/dedup_review.csv
```

Or through the runner, which calls the same scripts for you:

```bash
python run.py discover
python run.py fetch
python run.py extract
python run.py dedupe
```

Add `--limit N` to any of these for a smoke test (for example,
`python run.py fetch --limit 10` fetches at most 10 articles). Note that
`--limit` is not used by `dedupe`.

## Running the full pipeline

```bash
python run.py full
```

This runs discover, fetch, extract, dedupe in order and stops on the first
stage that fails. By default it discovers articles from the last 12 months.
Use `--since` / `--until` to set the window:

```bash
python run.py full --since 2024-01-01 --until 2024-12-31
```

For a historical backfill, `--backfill START END` discovers year by year
and merges the yearly link files before fetching:

```bash
python run.py full --backfill 2017 2024
```

That command only uses the GDELT DOC API, which covers January 2017
onward and only works while the API is reachable (see "When the GDELT API
rate-limits you" below). For 2015–2016, or for any year while the API is
rate-limited, harvest with `gkg` or `bq` first and then run the remaining
stages:

```bash
python run.py gkg --backfill 2015 2016   # or: python run.py bq --backfill 2015 2024
python run.py fetch
python run.py extract
python run.py dedupe
```

## When the GDELT API rate-limits you

GDELT's free DOC API throttles aggressively, and the limit appears to be
per IP, not per request rate: in November 2026 a single isolated request
for one week in 2024 returned 429 on every retry from a home IP. If that
happens to you, the API path is unusable until the limit lifts — but no
weeks are silently lost. Failed weeks are printed as `FAILED` and recorded
in `data/failed_windows.csv` (one row per week, with the reason and the
time of the attempt). Retry just those weeks later with:

```bash
python run.py discover --retry-failed
```

A week that succeeds on retry — even one that genuinely returns 0
articles — is removed from the file. The DOC API also only covers January
2017 onward. For anything earlier, or for any week the API will not serve,
use one of the two harvesters below instead. Both write the same
`links.csv` rows, so `fetch`, `extract`, and `dedupe` work unchanged.

## Harvester 1: GDELT bulk GKG files (`gkg.py`)

Downloads GDELT's raw GKG bulk files (one 15-minute file per slot, 96 per
day) from `data.gdeltproject.org`, filters records by water/sanitation
themes plus a US location and a water-related word in the article URL, and
keeps the article URLs. Coverage starts **18 February 2015** (nothing
exists before that in GKG v2). No key, no signup — but it downloads a lot:
about 1 GB of zipped files per day, roughly 350 GB for a full year, at
around 7 minutes per day on a fast connection. Measured on real days:
~430 matches/day in 2015, ~340/day in 2024, of which roughly 1 in 10
looks like a real advisory on inspection — the downstream LLM is the real
classifier, so budget extraction calls accordingly (up to ~156,000 matched
URLs per year (the 2015 rate; recent years run lower, roughly 70,000–125,000),
~$21–47 of LLM calls at nano rates). Run it as a background
job, one year at a time:

```bash
python run.py gkg --backfill 2015 2016
```

That writes `data/links_gkg.csv` and merges it into `data/links.csv`.
Re-running with `--resume` (the default through `run.py`) skips days
already harvested. To save re-downloading the ~130 MB master file list on
every invocation, pass a saved copy with
`python gkg.py --master-list /path/to/masterfilelist.txt`. A day whose
files fail to download is reported as FAILED, not counted as zero, and is
not marked done.

## Harvester 2: GDELT in BigQuery (`bq.py`)

Queries GDELT's public BigQuery table `gdelt-bq.gdeltv2.gkg_partitioned`
(the date-partitioned GKG table) for the same theme-filtered article URLs,
without downloading anything. Needs `BQ_KEY_FILE` in `.env` (a Google
Cloud service-account JSON key with the BigQuery User role; the public
dataset itself is free to read). It queries month by month, selecting only
the four columns it needs, US articles only by default, with a per-chunk
bytes cap. **Always dry-run first** — it prints the estimated bytes per
month before anything executes:

```bash
python run.py bq --dry-run --since 2024-06-01 --until 2024-06-30
python run.py bq --since 2024-06-03 --until 2024-06-03 --limit 20
```

The first command costs nothing and shows the real per-month scan size
(expect roughly 30–60 GB/month by GDELT's 2016 example — treat that as
a rough guide and confirm with the dry-run; if the estimate is wildly
above it, stop — the partition pruning is not behaving). The second runs one cheap day and
proves the key, the SQL, and the output format. A full 10-year backfill
scans roughly 4–7 TB, which is over BigQuery's 1 TB/month free tier if
done inside one billing month — spread it across months, or expect about
$6.25 per TB beyond the free tier (the BigQuery on-demand rate in 2026). Use `--chunk-days 1` for the cheapest
first live test.

```bash
python run.py bq --backfill 2015 2024
```

That writes `data/links_bq.csv` and merges it into `data/links.csv`.
Note `--limit` caps the rows written, not the bytes scanned.

## Output files

All outputs land in the data folder (`bwa_pipeline/data` by default).

| File | Produced by | Contents |
| --- | --- | --- |
| `links.csv` | discover / gkg / bq (merged) | One row per candidate article: `url`, `source`, `source_name`, `publish_date`, `capture_date`, `title`, `query` |
| `links_gkg.csv` | gkg | GKG harvester output before merging |
| `links_bq.csv` | bq | BigQuery harvester output before merging |
| `failed_windows.csv` | discover | GDELT weeks the API refused, for `--retry-failed` |
| `texts.csv` | fetch | One row per link: `Link`, `Loaded`, `Text`, `publish_date`, `source` |
| `structured.csv` | extract | One row per advisory, flattened LLM output plus `Source URL` |
| `deduped.csv` | dedupe | One row per advisory after merging duplicates, plus `source_urls`, `cluster_size`, `dedup_confidence`, `needs_review` |
| `dedup_review.csv` | dedupe | Candidate duplicate pairs the pipeline would not auto-merge, for human review |

Re-runs are safe: `fetch` skips URLs already in `texts.csv` and `extract`
skips URLs already in `structured.csv`, so an interrupted run picks up
where it left off. `discover` rewrites `links.csv` fresh on every run
(deduped by URL within that run), so re-running it does not accumulate
duplicates either. `gkg` and `bq` append to their own files and merge
into `links.csv` without duplicating URLs already there.

## How it works

**Discover (`discover.py`).** Queries several free sources for articles
matching boil-water-advisory search terms and writes the candidate URLs to
`links.csv`, with each source's publish date and capture date kept in
separate columns. GDELT is the backbone (its DOC API: `artlist` for the
last few months, the `timelinevolinfo` endpoint walked week by week back
to 2017); Google News RSS, GNews, NewsData.io, and Media Cloud add coverage
and cross-checks. State health-department notice pages are a small
supplement, not a source: only Mississippi publishes a statewide list, and
it is current-only — and as of late 2026 its listing page is JS-rendered,
so that source usually returns nothing. Sources whose API key is missing
are skipped with a warning rather than failing the run. For pre-2017
coverage, or when the DOC API rate-limits, use the GKG bulk harvester
(`gkg.py`) or the BigQuery harvester (`bq.py`) above instead of, or
alongside, this stage.

One known limitation: Google News RSS returns encoded redirect links
(`news.google.com/rss/articles/CBMi...`) that cannot be decoded without a
browser. A plain GET of one returns a splash page with no publisher URL in
it, and the `batchexecute` endpoint needs a session token that Google no
longer exposes in its pages, so the pure-Python decode was tried and does
not work. Those rows are kept in `links.csv` and marked "[unresolved
Google redirect]", but `fetch` cannot retrieve them — Google News also
blocks all bots in its robots.txt. The GNews API is the free source that
returns real, fetchable URLs; Google News RSS is still useful for its
titles, source names, and publish dates.

**Fetch (`fetch.py`).** Downloads each link with a polite crawl (checks
robots.txt first, identified user agent, retries), extracts the article
text with trafilatura (readability as a fallback), and records whether the
fetch succeeded in the `Loaded` column. Short or empty extractions are
marked not loaded instead of being sent to the LLM. If a live page is dead
or blocked, the stage falls back to an archived copy from the Wayback
Machine (CDX lookup) where one exists.

**Extract (`extract.py`).** Sends each loaded article to the LLM once,
with the article's publish date passed in as metadata. That publish date
is what makes the extraction work: the prompt resolves relative phrases
("yesterday", "last Tuesday", "effective immediately") against it, and
every date the model returns must come with a verbatim quote from the
article as evidence. The key fix over the earlier version of this code is
that `publish_date` is actually wired through from `texts.csv` into every
LLM call (previously the extractor was called without it, so relative
dates came back null). The publish date is what `extract` uses (it reads
it from `texts.csv`); the capture date is kept alongside it in `links.csv`.
For GDELT rows there is no publisher date, so the publish date is GDELT's
`seendate` (when GDELT captured the article, usually the same day or the
next) — approximate, but good enough for resolving relative dates. The
same approximation applies to GKG and BigQuery rows, whose date is when
GDELT saw the article.

**Dedupe (`dedupe.py`).** Duplicate articles about the same advisory get
extracted as separate rows, so this stage merges them. Cheap deterministic
blocking first (same state, compatible location, dates within 21 days;
year is a hard boundary), then pairwise LLM adjudication on the two
article texts, then union-find clustering and a merge that keeps the
earliest start date, latest end date, and both source URLs. Pairs the LLM
is unsure about, and any pair involving a record with no usable dates, go
to `dedup_review.csv` instead of being merged automatically.
