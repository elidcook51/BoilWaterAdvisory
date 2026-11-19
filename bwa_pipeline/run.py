"""Full-runner for the boil water advisory pipeline.

Run one stage on its own or the whole pipeline:
    python run.py discover
    python run.py fetch
    python run.py extract
    python run.py dedupe
    python run.py full

Harvesters that bypass the rate-limited GDELT DOC API (their rows are
merged into links.csv, so the rest of the pipeline is unchanged):
    python run.py gkg      # GDELT bulk GKG files, 2015-02-18 onward
    python run.py bq       # GDELT GKG table in BigQuery (needs BQ_KEY_FILE)

Each stage is its own script, run as a subprocess, so it can also be run
directly (for example: python extract.py --texts data/texts.csv
--out data/structured.csv). The `full` command just chains the stages and
stops on the first failure.
"""

import argparse
import csv
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
try:
    import config
except ImportError:
    sys.path.insert(0, str(_HERE.parent / "bwa_pipeline"))
    import config


def _default_since():
    return (date.today() - timedelta(days=365)).isoformat()


def _default_until():
    return date.today().isoformat()


def run_stage(script, *args):
    cmd = [sys.executable, str(_HERE / script)] + [str(a) for a in args]
    print("$", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def _merge_links(files, out_path):
    seen = set()
    rows = []
    for path in files:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                url = (row.get("url") or "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                rows.append(row)
    fieldnames = ["url", "source", "source_name", "publish_date",
                  "capture_date", "title", "query"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Merged {len(rows)} links into {out_path}")


def _merge_into_links(data_dir, new_file):
    """Merge one harvester's output file into the shared links.csv."""
    links = data_dir / "links.csv"
    inputs = [p for p in (links, new_file) if p.exists()]
    if not inputs:
        print(f"Nothing to merge into {links}")
        return
    _merge_links(inputs, links)


def discover(data_dir, since, until, backfill, limit, sources=None,
             retry_failed=False):
    data_dir.mkdir(parents=True, exist_ok=True)
    links = data_dir / "links.csv"
    if retry_failed:
        run_stage("discover.py", "--out", links, "--retry-failed")
        return
    extra = ["--limit", limit] if limit else []
    if sources:
        extra += ["--sources", sources]
    if backfill:
        start_year, end_year = backfill
        year_files = []
        for year in range(start_year, end_year + 1):
            year_out = data_dir / f"links_{year}.csv"
            run_stage("discover.py", "--out", year_out,
                      "--since", f"{year}-01-01", "--until", f"{year}-12-31",
                      *extra)
            year_files.append(year_out)
        _merge_links(year_files, links)
    else:
        run_stage("discover.py", "--out", links,
                  "--since", since, "--until", until, *extra)


def gkg(data_dir, since, until, backfill, limit):
    """Harvest links from GDELT bulk GKG files, merge into links.csv."""
    data_dir.mkdir(parents=True, exist_ok=True)
    out = data_dir / "links_gkg.csv"
    if backfill:
        start_year, end_year = backfill
        for year in range(start_year, end_year + 1):
            args = ["--out", out, "--since", f"{year}-01-01",
                    "--until", f"{year}-12-31", "--resume"]
            if limit:
                args += ["--limit", limit]
            run_stage("gkg.py", *args)
    else:
        args = ["--out", out, "--since", since, "--until", until, "--resume"]
        if limit:
            args += ["--limit", limit]
        run_stage("gkg.py", *args)
    _merge_into_links(data_dir, out)


def bq(data_dir, since, until, backfill, limit, dry_run=False, slim=False):
    """Harvest links from GDELT's GKG table in BigQuery, merge into links.csv."""
    data_dir.mkdir(parents=True, exist_ok=True)
    out = data_dir / "links_bq.csv"
    if backfill:
        start_year, end_year = backfill
        for year in range(start_year, end_year + 1):
            args = ["--out", out, "--since", f"{year}-01-01",
                    "--until", f"{year}-12-31"]
            if limit:
                args += ["--limit", limit]
            if dry_run:
                args += ["--dry-run"]
            if slim:
                args += ["--slim"]
            run_stage("bq.py", *args)
    else:
        args = ["--out", out, "--since", since, "--until", until]
        if limit:
            args += ["--limit", limit]
        if dry_run:
            args += ["--dry-run"]
        if slim:
            args += ["--slim"]
        run_stage("bq.py", *args)
    if not dry_run:
        _merge_into_links(data_dir, out)


def fetch(data_dir, limit, workers=1, per_host=2, wayback_max=3):
    args = ["--links", data_dir / "links.csv", "--out", data_dir / "texts.csv"]
    if limit:
        args += ["--limit", limit]
    if workers and workers != 1:
        args += ["--workers", str(workers)]
    if per_host and per_host != 2:
        args += ["--per-host", str(per_host)]
    if wayback_max and wayback_max != 3:
        args += ["--wayback-max", str(wayback_max)]
    run_stage("fetch.py", *args)


def extract(data_dir, limit):
    args = ["--texts", data_dir / "texts.csv", "--out", data_dir / "structured.csv"]
    if limit:
        args += ["--limit", limit]
    run_stage("extract.py", *args)


def dedupe(data_dir):
    run_stage("dedupe.py",
              "--structured", data_dir / "structured.csv",
              "--texts", data_dir / "texts.csv",
              "--out", data_dir / "deduped.csv",
              "--review", data_dir / "dedup_review.csv")


def full(data_dir, since, until, backfill, limit, sources=None,
         retry_failed=False, workers=1, per_host=2, wayback_max=3):
    stages = [
        ("discover", lambda: discover(data_dir, since, until, backfill, limit,
                                      sources, retry_failed)),
        ("fetch", lambda: fetch(data_dir, limit, workers, per_host,
                                wayback_max)),
        ("extract", lambda: extract(data_dir, limit)),
        ("dedupe", lambda: dedupe(data_dir)),
    ]
    for name, fn in stages:
        print(f"=== {name} ===", flush=True)
        try:
            fn()
        except subprocess.CalledProcessError as e:
            print(f"Stage '{name}' failed (exit {e.returncode}); stopping.")
            sys.exit(1)
    print("Full run finished.")


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-dir", default=str(config.DATA_DIR),
                        help="folder for the pipeline CSVs (default: %(default)s)")
    common.add_argument("--since", default=_default_since(),
                        help="discovery window start, YYYY-MM-DD (default: 12 months ago)")
    common.add_argument("--until", default=_default_until(),
                        help="discovery window end, YYYY-MM-DD (default: today)")
    common.add_argument("--backfill", nargs=2, type=int, metavar=("START", "END"),
                        help="historical run: discover year by year, "
                             "e.g. --backfill 2015 2024")
    common.add_argument("--limit", type=int, default=None,
                        help="smoke-test cap on rows per stage (not used by dedupe)")
    common.add_argument("--sources", default=None,
                        help="comma-separated discovery sources for the discover "
                             "stage (gdelt_doc,google_news_rss,gnews,newsdata,"
                             "mediacloud,gov); default is all of them")
    common.add_argument("--retry-failed", action="store_true",
                        help="discover stage only: retry the GDELT weeks listed "
                             "in failed_windows.csv instead of discovering")
    common.add_argument("--dry-run", action="store_true",
                        help="bq stage only: report estimated BigQuery bytes "
                             "without executing anything")
    common.add_argument("--workers", type=int, default=1,
                        help="fetch stage only: fetch N URLs concurrently "
                             "(default 1, sequential; 8-16 is a good range)")
    common.add_argument("--per-host", type=int, default=2,
                        help="fetch stage only: max concurrent requests to "
                             "one website (default 2; only with --workers)")
    common.add_argument("--wayback-max", type=int, default=3,
                        help="fetch stage only: max concurrent requests to "
                             "web.archive.org (default 3; only with "
                             "--workers)")
    common.add_argument("--slim", action="store_true",
                        help="bq stage only: read only DATE + URL from BigQuery "
                             "and filter on the URL slug alone (far fewer bytes "
                             "scanned; noisier links, screened later by the "
                             "extract content gate)")

    parser = argparse.ArgumentParser(
        description="Run the boil water advisory pipeline, one stage or all of it.",
        parents=[common])
    sub = parser.add_subparsers(dest="stage", required=True)
    sub.add_parser("discover", parents=[common],
                   help="collect candidate article links")
    sub.add_parser("gkg", parents=[common],
                   help="harvest links from GDELT bulk GKG files (2015+)")
    sub.add_parser("bq", parents=[common],
                   help="harvest links from GDELT GKG in BigQuery")
    sub.add_parser("fetch", parents=[common],
                   help="download article text for each link")
    sub.add_parser("extract", parents=[common],
                   help="run the LLM extraction over article texts")
    sub.add_parser("dedupe", parents=[common],
                   help="merge duplicate advisories")
    sub.add_parser("full", parents=[common],
                   help="run discover, fetch, extract, dedupe in order")
    args = parser.parse_args()

    if args.backfill and args.backfill[0] > args.backfill[1]:
        parser.error("--backfill START must be <= END")

    data_dir = Path(args.data_dir)
    stages = {
        "discover": lambda: discover(data_dir, args.since, args.until,
                                     args.backfill, args.limit, args.sources,
                                     args.retry_failed),
        "gkg": lambda: gkg(data_dir, args.since, args.until,
                           args.backfill, args.limit),
        "bq": lambda: bq(data_dir, args.since, args.until,
                         args.backfill, args.limit, args.dry_run, args.slim),
        "fetch": lambda: fetch(data_dir, args.limit, args.workers,
                               args.per_host, args.wayback_max),
        "extract": lambda: extract(data_dir, args.limit),
        "dedupe": lambda: dedupe(data_dir),
        "full": lambda: full(data_dir, args.since, args.until,
                             args.backfill, args.limit, args.sources,
                             args.retry_failed, args.workers,
                             args.per_host, args.wayback_max),
    }
    stages[args.stage]()


if __name__ == "__main__":
    main()
