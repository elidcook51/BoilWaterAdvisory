"""Full-runner for the boil water advisory pipeline.

Run one stage on its own or the whole pipeline:
    python run.py discover
    python run.py fetch
    python run.py extract
    python run.py dedupe
    python run.py full

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
    if rows:
        fieldnames = list(rows[0].keys())
    else:
        fieldnames = ["url", "source", "source_name", "publish_date",
                      "capture_date", "title", "query"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Merged {len(rows)} links into {out_path}")


def discover(data_dir, since, until, backfill, limit, sources=None):
    data_dir.mkdir(parents=True, exist_ok=True)
    links = data_dir / "links.csv"
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


def fetch(data_dir, limit):
    args = ["--links", data_dir / "links.csv", "--out", data_dir / "texts.csv"]
    if limit:
        args += ["--limit", limit]
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


def full(data_dir, since, until, backfill, limit, sources=None):
    stages = [
        ("discover", lambda: discover(data_dir, since, until, backfill, limit,
                                      sources)),
        ("fetch", lambda: fetch(data_dir, limit)),
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

    parser = argparse.ArgumentParser(
        description="Run the boil water advisory pipeline, one stage or all of it.",
        parents=[common])
    sub = parser.add_subparsers(dest="stage", required=True)
    sub.add_parser("discover", parents=[common],
                   help="collect candidate article links")
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
                                     args.backfill, args.limit, args.sources),
        "fetch": lambda: fetch(data_dir, args.limit),
        "extract": lambda: extract(data_dir, args.limit),
        "dedupe": lambda: dedupe(data_dir),
        "full": lambda: full(data_dir, args.since, args.until,
                             args.backfill, args.limit, args.sources),
    }
    stages[args.stage]()


if __name__ == "__main__":
    main()
