"""Prune a links.csv before fetching: drop URL-only junk and duplicate copies.

S1 stack, measured on the 2024-06-03 one-day slim set (1,623 URLs, 140
valuable by the gate: passed + review + duplicates). Rules, in order:

  1. Drop bq:slug-drink rows. The slim query tags each URL with the first
     slug word it contains (boil, then water, then drink), so a slug-drink
     URL has "drink" but no "water" or "boil" in it. On the one-day set
     that was 339 URLs (20.9%) and the only two "valuable" ones among them
     were a Tylenol-and-alcohol advice page and a THC-drinks roundup --
     zero real advisories lost.
  2. Drop TLDs .uk / .au / .ie / .nz. UK, Australian, Irish, and New
     Zealand papers; the database is US advisories. On the one-day set
     245 URLs, 0 valuable lost. .ca is deliberately kept: a Toronto
     outlet carried a real Atlanta AP copy that day.
  3. Drop path words "maxine" and "cocktail". Maxine Waters stories and
     drink recipes; mechanism sound (a person's name, a drink), not a
     learned quirk. On the one-day set 42 + 51 URLs, 0 valuable lost.
  4. Drop domains containing a water-town name: bridgewater, bridgwater,
     clearwater, waterford, stillwater, waterbury, watertown, whitewater,
     blackwater, coldwater, sweetwater. Papers/towns named for water,
     matching on the host alone. (The Bridgwater Mercury domain is
     spelled "bridgwater", no "e"; both spellings are listed so the rule
     matches the real host as well as the town name.)
  5. Drop whole-word "waters" in the path (word-boundary match, NOT a
     substring). A substring match loses two real "Atlanta watershed"
     stories; the word-boundary form keeps them. On the one-day set
     81 URLs, 0 valuable lost.
  6. Keep the first N copies of each normalized URL slug (default N=2).
     Syndicated stories appear on dozens of domains with the same slug,
     and the gate already drops the extra copies after fetching. Doing it
     here saves the fetch. Normalization strips /YYYY/MM/DD/ date
     segments and long article IDs, lowercases, and collapses
     non-alphanumerics. Keep-2 (default) costs ~5 points of cut versus
     keep-1 but covers the case where the first copy fails to load while
     a later copy would have loaded.

Measured on the one-day set (default keep-2, all six rules):
  1,623 -> 972 kept (40.1% cut), 107 of 140 valuable URLs kept,
  1 passed URL lost (the Tylenol advice page -- not a real advisory),
  0 additional real-advisory loss beyond the baseline prune. With
  --copies 1: 918 kept (43.4% cut).

Deliberately NOT in this stack (see the v4 research report, agent C):
the aggressive S2 TLD drop (foreign-TLD outlets do syndicate US
advisories -- the .ca Toronto case proves it), any host blocklist,
theme/tone filtering (not measurable from the files), and the
first-5,000-char prefix gate (needs a prototype against raw HTML).

Usage:
  python prune.py links.csv --out links_pruned.csv
  python prune.py links.csv --out links_pruned.csv --copies 1
  python prune.py links.csv --dry-run        (report only, no file)
  python prune.py links.csv --out links_pruned.csv --keep-drink

Only the URL (and the query column, if present) is read. Row order and
all columns are preserved. Never run this on a links file a fetch is
currently reading; prune a copy, or the next harvest's file.
"""

import argparse
import csv
import re
import sys
from collections import Counter
from urllib.parse import urlparse

# Foreign TLDs dropped by S1 rule 2 (last label of the host).
DROP_TLDS = frozenset({"uk", "au", "ie", "nz"})

# Path words dropped by S1 rule 3 (substring of the URL path only).
DROP_PATH_WORDS = ("maxine", "cocktail")

# Water-town names dropped by S1 rule 4 (substring of the host).
# "bridgwater" is the real spelling of the Bridgwater Mercury host;
# "bridgewater" is the town-name spelling. Both are listed.
WATER_TOWNS = (
    "bridgewater", "bridgwater", "clearwater", "waterford", "stillwater",
    "waterbury", "watertown", "whitewater", "blackwater", "coldwater",
    "sweetwater",
)

# Whole-word "waters" in the path (S1 rule 5). Word-boundary, not a
# substring: "watershed" must survive.
WATERS_WORD = re.compile(r"\bwaters\b")


def slug_word(url):
    """Which slug word a URL contains, most specific first (as in bq.py)."""
    low = (url or "").lower()
    for word in ("boil", "water", "drink"):
        if word in low:
            return word
    return None


def normalize_slug(url):
    """Normalized URL path, for grouping syndicated copies of one story.

    Identical to the shipped prune_links.py normalizer, so its measured
    numbers still apply: strips /YYYY/MM/DD/ date segments and long
    article IDs, lowercases, collapses non-alphanumerics.
    """
    path = urlparse(url or "").path.lower()
    path = re.sub(r"/\d{4}/\d{2}/\d{2}/", "/", path)
    path = re.sub(r"article_[0-9a-f-]{20,}", "article", path)
    return re.sub(r"[^a-z0-9]+", "-", path).strip("-")


def host_of(url):
    """Lowercased host without any port, or "" when unparseable."""
    try:
        return urlparse(url or "").netloc.lower().split(":")[0]
    except Exception:
        return ""


def tld_of(url):
    """Last label of the host (uk for www.example.co.uk), or ""."""
    host = host_of(url)
    if "." not in host:
        return ""
    return host.rsplit(".", 1)[-1]


def drop_reason(row):
    """S1 rule that drops this row, or None when it survives rules 1-5.

    Returns one of "slug-drink", "tld", "path-word", "water-town",
    "waters-word", or None. Slug dedup (rule 6) is applied separately
    by prune(), after these rules, in input order.
    """
    url = (row.get("url") or "").strip()
    if not url:
        return None
    query = (row.get("query") or "").strip()
    word = (query[len("bq:slug-"):] if query.startswith("bq:slug-")
            else slug_word(url))
    if word == "drink":
        return "slug-drink"
    if tld_of(url) in DROP_TLDS:
        return "tld"
    path = urlparse(url.lower()).path
    if any(w in path for w in DROP_PATH_WORDS):
        return "path-word"
    if any(t in host_of(url) for t in WATER_TOWNS):
        return "water-town"
    if WATERS_WORD.search(path):
        return "waters-word"
    return None


def prune(rows, copies=2, drop_drink=True):
    """Apply the S1 stack to parsed link rows, in input order.

    Returns (kept, reasons) where reasons maps each drop reason to its
    count. Rows with no URL are kept (nothing to judge them by).
    """
    kept = []
    reasons = Counter()
    seen_slugs = Counter()
    for row in rows:
        url = (row.get("url") or "").strip()
        if not url:
            kept.append(row)
            continue
        reason = drop_reason(row)
        if not drop_drink and reason == "slug-drink":
            reason = None  # --keep-drink disables rule 1 only
        if reason is not None:
            reasons[reason] += 1
            continue
        slug = normalize_slug(url)
        seen_slugs[slug] += 1
        if slug and seen_slugs[slug] > copies:
            reasons["extra slug copy"] += 1
            continue
        kept.append(row)
    return kept, reasons


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Prune a links.csv before fetching (S1 URL-only rules).")
    parser.add_argument("links", help="input links.csv")
    parser.add_argument("--out", help="output pruned links.csv")
    parser.add_argument("--copies", type=int, default=2,
                        help="copies kept per normalized slug (default 2)")
    parser.add_argument("--keep-drink", action="store_true",
                        help="do not drop bq:slug-drink rows (rule 1 only)")
    parser.add_argument("--dry-run", action="store_true",
                        help="report only, write no file")
    args = parser.parse_args(argv)
    if args.copies < 1:
        parser.error("--copies must be at least 1")
    if not args.dry_run and not args.out:
        parser.error("--out is required unless --dry-run is given")

    with open(args.links, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    if not fieldnames or "url" not in fieldnames:
        print("prune: input has no 'url' column; nothing done.",
              file=sys.stderr)
        return 1

    kept, reasons = prune(rows, copies=args.copies,
                          drop_drink=not args.keep_drink)
    total = len(rows)
    cut = total - len(kept)
    print(f"prune: read {total} rows from {args.links}")
    for reason, count in reasons.most_common():
        print(f"  dropped {count} as {reason}")
    print(f"  kept {len(kept)} rows ({100.0 * cut / total:.1f}% cut)"
          if total else "  kept 0 rows")
    if args.dry_run:
        print("  dry run: no file written")
        return 0
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)
    print(f"  wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
