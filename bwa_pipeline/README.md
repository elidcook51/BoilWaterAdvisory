# Boil Water Advisory Pipeline

Builds a historical database (2013+) of US boil water advisories from news
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
python run.py full --backfill 2015 2024
```

## Output files

All outputs land in the data folder (`bwa_pipeline/data` by default).

| File | Produced by | Contents |
| --- | --- | --- |
| `links.csv` | discover | One row per candidate article: `url`, `source`, `source_name`, `publish_date`, `capture_date`, `title`, `query` |
| `texts.csv` | fetch | One row per link: `Link`, `Loaded`, `Text`, `publish_date`, `source` |
| `structured.csv` | extract | One row per advisory, flattened LLM output plus `Source URL` |
| `deduped.csv` | dedupe | One row per advisory after merging duplicates, plus `source_urls`, `cluster_size`, `dedup_confidence`, `needs_review` |
| `dedup_review.csv` | dedupe | Candidate duplicate pairs the pipeline would not auto-merge, for human review |

Re-runs are safe: `fetch` skips URLs already in `texts.csv` and `extract`
skips URLs already in `structured.csv`, so an interrupted run picks up
where it left off. `discover` rewrites `links.csv` fresh on every run
(deduped by URL within that run), so re-running it does not accumulate
duplicates either.

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
are skipped with a warning rather than failing the run. Raw GDELT GKG bulk
files (the route back to 2015) are not implemented in this package yet;
that is the obvious next extension if you need pre-2017 coverage.

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
next) — approximate, but good enough for resolving relative dates.

**Dedupe (`dedupe.py`).** Duplicate articles about the same advisory get
extracted as separate rows, so this stage merges them. Cheap deterministic
blocking first (same state, compatible location, dates within 21 days;
year is a hard boundary), then pairwise LLM adjudication on the two
article texts, then union-find clustering and a merge that keeps the
earliest start date, latest end date, and both source URLs. Pairs the LLM
is unsure about, and any pair involving a record with no usable dates, go
to `dedup_review.csv` instead of being merged automatically.
