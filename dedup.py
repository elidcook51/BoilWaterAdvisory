"""
dedup.py -- Semantic dedup for the Boil Water Advisory structured database.

PROBLEM: duplicate news articles about the SAME advisory get extracted as separate
rows (e.g. an "issued" article and a "lifted" article, or the same story carried
by two outlets). Link-level dedup (cleaninggdelt.py) cannot catch these because
the URLs differ.

APPROACH (two stages, exactly as designed):
  1. BLOCKING (cheap, deterministic): group records by (state, year). Within a
     block, propose candidate pairs only if the locations are compatible AND the
     known dates are within DATE_WINDOW_DAYS of each other. Year is a HARD
     boundary -- a 2019 and a 2024 advisory in the same town never merge.
     Records with no usable dates are only paired on very strong location match
     and are always flagged for human review.
  2. LLM ADJUDICATION (expensive, accurate): each candidate pair's article texts
     (+ extracted fields) go back to the LLM, which decides same/different
     advisory and, if same, returns a merged canonical record. Pairs are joined
     into clusters with union-find; clusters collapse to one row.

MERGE RULES (also enforced inside the LLM prompt; the Python fallback mirrors them):
  - start_date: earliest non-null. end_date: latest non-null ("still active"/null
    loses to a stated lift date). backup_date: first non-null.
  - location: most specific non-null value per field; state by majority vote.
  - people_affected: max stated. reason: longest non-null. advisory_type /
    source_type: majority vote excluding "unknown".
  - source_urls: union of member URLs (pipe-separated in the output CSV).

USAGE:
    python dedup.py --structured "Virginia Run.csv" --texts "Virginia News Text.csv" \\
        --out "Virginia Deduped.csv" --review "dedup_review.csv"

    --dry-run : only run blocking and print candidate-pair counts (no LLM calls,
                no charges). Use this first to sanity-check the blocking.
    --max-pairs N : cap LLM adjudications (default: no cap).
    --model : OpenAI model for adjudication (default: gpt-4.1-mini).
    --threshold : min LLM confidence to auto-merge (default 0.6). Below this,
                pairs go to the review CSV instead of merging.

INPUT COLUMNS expected in --structured (v1 or v2 schema, flattened):
    start_date, end_date, backup_date, location_state, location_county,
    location_locality, location_utility_affected, location_PWS_ID,
    advisory_type, reason, people_affected, source_type, Source URL
  (--texts columns: Link, Text -- joined on Source URL == Link.)

OUTPUT --out: one row per advisory cluster, same columns as input plus:
    source_urls, cluster_size, dedup_confidence, needs_review
OUTPUT --review: candidate pairs the pipeline would NOT auto-merge
    (low LLM confidence or LLM call failed), for human eyes.
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

import pandas as pd
try:
    from dotenv import load_dotenv
except ImportError:  # optional; only needed if you use a .env file
    def load_dotenv(*a, **k):
        return False
try:
    from rapidfuzz import fuzz
except ImportError:  # fallback so the script runs without rapidfuzz
    import difflib
    class _Fuzz:
        @staticmethod
        def token_set_ratio(a, b):
            ta, tb = sorted(a.split()), sorted(b.split())
            return int(100 * difflib.SequenceMatcher(None, ta, tb).ratio())
    fuzz = _Fuzz()

load_dotenv()

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
DATE_WINDOW_DAYS = 21       # max gap between known dates for two records to pair
LOCATION_FUZZ = 80          # token_set_ratio for "compatible" locations
LOCATION_FUZZ_STRONG = 90   # token_set_ratio for dateless records ("strong" match)
TEXT_TRUNCATE = 6000        # chars of article text sent to the LLM per article
DEFAULT_MODEL = "gpt-4.1-mini"
DEFAULT_THRESHOLD = 0.6

GENERIC_LOCATION_TOKENS = {
    "county", "city", "town", "village", "borough", "parish",
    "water", "utility", "utilities", "authority", "district", "department",
    "system", "systems", "supply", "works", "service", "services", "company",
    "municipal", "regional", "rural", "public",
}


def norm_text(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def norm_location_key(s):
    """Location string minus generic tokens, for fuzzy comparison."""
    toks = [t for t in norm_text(s).split() if t not in GENERIC_LOCATION_TOKENS]
    return " ".join(toks)


def parse_date(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    if not s or s.lower() in {"null", "none", "nan", "nat"}:
        return None
    try:
        return pd.to_datetime(s, errors="raise").date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Record normalization
# ---------------------------------------------------------------------------
def to_record(row):
    """Flattened CSV row -> normalized record dict (keeps raw row for output)."""
    g = lambda c: row.get(c)
    rec = {
        "idx": int(row.name),
        "state": norm_text(g("location_state")),
        "county": norm_text(g("location_county")),
        "county_key": norm_location_key(g("location_county")),
        "locality": norm_text(g("location_locality")),
        "locality_key": norm_location_key(g("location_locality")),
        "utility": norm_text(g("location_utility_affected")),
        "utility_key": norm_location_key(g("location_utility_affected")),
        "pwsid": norm_text(g("location_PWS_ID")),
        "start": parse_date(g("start_date")),
        "end": parse_date(g("end_date")),
        "backup": parse_date(g("backup_date")),
        "url": (str(g("Source URL")).strip()
                if g("Source URL") is not None and str(g("Source URL")).strip().lower() != "nan"
                else ""),
        "raw": row.to_dict(),
    }
    dates = [d for d in (rec["start"], rec["end"], rec["backup"]) if d]
    rec["year"] = dates[0].year if dates else None
    rec["loc_blob"] = " ".join(k for k in
                               (rec["county_key"], rec["locality_key"], rec["utility_key"]) if k)
    return rec


def known_dates(rec):
    return [d for d in (rec["start"], rec["end"], rec["backup"]) if d]


# ---------------------------------------------------------------------------
# Blocking: cheap candidate-pair generation
# ---------------------------------------------------------------------------
def location_compatible(a, b):
    # Exact PWS ID match is decisive.
    if a["pwsid"] and b["pwsid"] and a["pwsid"] == b["pwsid"]:
        return True
    # Same county (both known) is strong evidence.
    if a["county_key"] and b["county_key"] and a["county_key"] == b["county_key"]:
        return True
    # Otherwise fuzzy match on the combined location blob.
    if a["loc_blob"] and b["loc_blob"]:
        return fuzz.token_set_ratio(a["loc_blob"], b["loc_blob"]) >= LOCATION_FUZZ
    return False


def location_strong(a, b):
    """Stricter bar used when one/both records have no dates."""
    if a["pwsid"] and b["pwsid"] and a["pwsid"] == b["pwsid"]:
        return True
    if (a["county_key"] and b["county_key"]
            and a["county_key"] == b["county_key"]
            and a["loc_blob"] and b["loc_blob"]
            and fuzz.token_set_ratio(a["loc_blob"], b["loc_blob"]) >= LOCATION_FUZZ_STRONG):
        return True
    return False


def dates_compatible(a, b):
    da, db = known_dates(a), known_dates(b)
    if da and db:
        return any(abs((x - y).days) <= DATE_WINDOW_DAYS for x in da for y in db)
    # Dateless on either side: defer entirely to the strong location bar,
    # and mark the pair for review later.
    return location_strong(a, b)


def is_candidate(a, b):
    if a["idx"] == b["idx"]:
        return False
    return location_compatible(a, b) and dates_compatible(a, b)


def generate_candidates(records):
    """Block by state; year is a hard boundary (dateless records match any year)."""
    by_state = defaultdict(list)
    for r in records:
        by_state[r["state"] or "unknown"].append(r)
    pairs = []
    for state, members in by_state.items():
        # Safety sub-block by county for pathological states.
        if len(members) > 2000:
            sub = defaultdict(list)
            for r in members:
                sub[r["county_key"] or "unknown"].append(r)
            groups = list(sub.values())
        else:
            groups = [members]
        for grp in groups:
            for i in range(len(grp)):
                for j in range(i + 1, len(grp)):
                    a, b = grp[i], grp[j]
                    # Hard year boundary: two KNOWN different years never pair.
                    # A dateless record (year=None) may pair with any year.
                    if a["year"] and b["year"] and a["year"] != b["year"]:
                        continue
                    if is_candidate(a, b):
                        dateless = not known_dates(a) or not known_dates(b)
                        pairs.append((a, b, dateless))
    return pairs

# ---------------------------------------------------------------------------
# LLM adjudication
# ---------------------------------------------------------------------------
ADJUDICATION_INSTRUCTIONS = """You decide whether two database entries describe the SAME real-world boil water advisory.
Each entry was extracted from a news article by an LLM; fields may be missing or slightly wrong.
Return ONLY valid JSON: {"same_advisory": true|false, "confidence": 0.0-1.0,
"reason": "<=40 words", "merged": {object}|null}.

DECISION RULES:
- SAME if: same water system / utility and same affected area, with overlapping or
  adjacent date windows. Advisories typically last days to weeks; the same advisory
  reported twice usually has start dates within ~3 weeks of each other.
- An "advisory issued" article and an "advisory lifted" article about the same
  system days apart describe the SAME advisory.
- DIFFERENT years ALWAYS means different advisories, no matter how similar the location.
- Different towns/utilities, or clearly different causes months apart, mean different advisories.
- Missing fields are not evidence against sameness; contradictory fields (different
  named utility, dates months apart) are.

If same_advisory is true, "merged" must be the best-estimate canonical record:
- start_date: earliest non-null start_date. end_date: latest non-null end_date
  (a stated lift date beats "still active"/null). backup_date: first non-null.
- location fields: the most specific non-null value for each of state, county,
  locality, utility_affected, PWS_ID.
- advisory_type: prefer a non-"unknown" value. reason: the most informative non-null.
- people_affected: the largest stated number, or null.
- source_urls: [url_a, url_b].
All dates YYYY-MM-DD or null. If same_advisory is false, "merged" must be null.
"""


def _fields_for_prompt(rec):
    raw = rec["raw"]
    return {
        "start_date": raw.get("start_date"),
        "end_date": raw.get("end_date"),
        "backup_date": raw.get("backup_date"),
        "state": raw.get("location_state"),
        "county": raw.get("location_county"),
        "locality": raw.get("location_locality"),
        "utility_affected": raw.get("location_utility_affected"),
        "PWS_ID": raw.get("location_PWS_ID"),
        "advisory_type": raw.get("advisory_type"),
        "reason": raw.get("reason"),
        "people_affected": raw.get("people_affected"),
        "source_type": raw.get("source_type"),
    }


def adjudicate_pair(client, model, a, b, texts):
    """Ask the LLM whether records a and b are the same advisory.

    Returns (verdict_dict, error). verdict_dict has same_advisory, confidence,
    reason, merged. On LLM failure returns (None, error_message).
    """
    ta = (texts.get(a["url"]) or "")[:TEXT_TRUNCATE]
    tb = (texts.get(b["url"]) or "")[:TEXT_TRUNCATE]
    user_block = (
        "ENTRY A:\n"
        f"Source URL: {a['url'] or '(unknown)'}\n"
        f"Extracted fields: {json.dumps(_fields_for_prompt(a), default=str)}\n"
        f"Article A text (truncated):\n{ta or '(no article text available -- decide from fields and URLs only)'}\n\n"
        "ENTRY B:\n"
        f"Source URL: {b['url'] or '(unknown)'}\n"
        f"Extracted fields: {json.dumps(_fields_for_prompt(b), default=str)}\n"
        f"Article B text (truncated):\n{tb or '(no article text available -- decide from fields and URLs only)'}\n\n"
        "Are A and B the same boil water advisory? Return ONLY the JSON."
    )
    try:
        response = client.responses.create(
            model=model,
            instructions=ADJUDICATION_INSTRUCTIONS,
            input=user_block,
        )
        text = (response.output_text or "").strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        m = re.search(r"\{.*\}", text, flags=re.S)
        if not m:
            return None, "LLM did not return JSON"
        verdict = json.loads(m.group(0))
        verdict["same_advisory"] = bool(verdict.get("same_advisory"))
        verdict["confidence"] = float(verdict.get("confidence", 0.0) or 0.0)
        verdict.setdefault("reason", "")
        return verdict, None
    except Exception as e:  # network, auth, rate limits, bad JSON...
        return None, f"{type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Union-find clustering + deterministic merge fallback
# ---------------------------------------------------------------------------
class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _longest_non_null(rows, col):
    best = None
    for r in rows:
        v = r.get(col)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        v = str(v).strip()
        if v and (best is None or len(v) > len(best)):
            best = v
    return best


def _majority(rows, col, exclude={"unknown", ""}):
    vals = [str(r.get(col)).strip() for r in rows
            if r.get(col) is not None and str(r.get(col)).strip().lower() not in exclude
            and str(r.get(col)).strip().lower() != "nan"]
    return Counter(vals).most_common(1)[0][0] if vals else None


def deterministic_merge(rows):
    """Rule-based canonical record when the LLM merge is unavailable."""
    starts = [parse_date(r.get("start_date")) for r in rows]
    ends = [parse_date(r.get("end_date")) for r in rows]
    starts = [d for d in starts if d]
    ends = [d for d in ends if d]
    nums = []
    for r in rows:
        try:
            v = r.get("people_affected")
            if v is not None and str(v).strip().lower() not in {"", "nan", "null", "none"}:
                nums.append(float(v))
        except (ValueError, TypeError):
            pass
    merged = {
        "start_date": min(starts).isoformat() if starts else None,
        "end_date": max(ends).isoformat() if ends else None,
        "backup_date": next((parse_date(r.get("backup_date")).isoformat()
                             for r in rows if parse_date(r.get("backup_date"))), None),
        "location_state": _majority(rows, "location_state"),
        "location_county": _longest_non_null(rows, "location_county"),
        "location_locality": _longest_non_null(rows, "location_locality"),
        "location_utility_affected": _longest_non_null(rows, "location_utility_affected"),
        "location_PWS_ID": _longest_non_null(rows, "location_PWS_ID"),
        "advisory_type": _majority(rows, "advisory_type"),
        "reason": _longest_non_null(rows, "reason"),
        "people_affected": int(max(nums)) if nums else None,
        "source_type": _majority(rows, "source_type"),
    }
    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Semantic dedup for BWA structured CSV.")
    ap.add_argument("--structured", required=True, help="Structured advisories CSV (flattened LLM output).")
    ap.add_argument("--texts", default=None, help="Raw article texts CSV with columns Link, Text.")
    ap.add_argument("--out", required=True, help="Deduped output CSV path.")
    ap.add_argument("--review", required=True, help="Human-review CSV path for unmerged pairs.")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    ap.add_argument("--max-pairs", type=int, default=0, help="Cap LLM adjudications (0 = no cap).")
    ap.add_argument("--dry-run", action="store_true", help="Blocking only; no LLM calls.")
    args = ap.parse_args()

    df = pd.read_csv(args.structured)
    records = [to_record(row) for _, row in df.iterrows()]
    print(f"Loaded {len(records)} records from {args.structured}")

    texts = {}
    if args.texts:
        tdf = pd.read_csv(args.texts)
        for _, r in tdf.iterrows():
            link = str(r.get("Link", "")).strip()
            if link and link.lower() != "nan":
                texts[link] = str(r.get("Text", ""))
        print(f"Loaded article texts for {len(texts)} URLs from {args.texts}")

    pairs = generate_candidates(records)
    n_dateless = sum(1 for _, _, d in pairs if d)
    print(f"Blocking proposed {len(pairs)} candidate pairs ({n_dateless} involve a dateless record).")
    if args.dry_run:
        print("Dry run: stopping before LLM adjudication.")
        return 0

    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPEN_AI_API_KEY"))

    uf = UnionFind()
    merges = {}          # root -> LLM merged dict (best confidence wins)
    review_rows = []
    cap = args.max_pairs or len(pairs)
    for n, (a, b, dateless) in enumerate(pairs[:cap], 1):
        verdict, err = adjudicate_pair(client, args.model, a, b, texts)
        tag = f"pair {n}/{min(cap, len(pairs))} (rows {a['idx']},{b['idx']})"
        if err:
            print(f"  {tag}: LLM FAILED ({err}) -> review")
            review_rows.append({"row_a": a["idx"], "row_b": b["idx"],
                                "url_a": a["url"], "url_b": b["url"],
                                "reason": f"LLM error: {err}", "dateless_pair": dateless})
            continue
        conf = verdict["confidence"]
        print(f"  {tag}: same={verdict['same_advisory']} conf={conf:.2f} -- {verdict['reason'][:80]}")
        if verdict["same_advisory"] and conf >= args.threshold and not dateless:
            uf.union(a["idx"], b["idx"])
            root = uf.find(a["idx"])
            prev = merges.get(root)
            if verdict.get("merged") and (prev is None or conf > prev["_conf"]):
                m = dict(verdict["merged"])
                m["_conf"] = conf
                merges[root] = m
        else:
            why = ("dateless pair held for review" if dateless and verdict["same_advisory"]
                   else f"conf={conf:.2f} below threshold" if verdict["same_advisory"]
                   else "LLM says different advisories")
            if verdict["same_advisory"]:  # only review plausible merges, not clear negatives
                review_rows.append({"row_a": a["idx"], "row_b": b["idx"],
                                    "url_a": a["url"], "url_b": b["url"],
                                    "reason": f"{verdict['reason']} [{why}]",
                                    "dateless_pair": dateless})

    # Collapse clusters.
    clusters = defaultdict(list)
    for r in records:
        clusters[uf.find(r["idx"])].append(r)

    out_rows = []
    for root, members in clusters.items():
        rows = [m["raw"] for m in members]
        if len(members) == 1:
            row = dict(rows[0])
            row["source_urls"] = members[0]["url"]
            row["cluster_size"] = 1
            row["dedup_confidence"] = ""
            row["needs_review"] = False
        else:
            llm_m = merges.get(root)
            if llm_m:
                conf = llm_m.pop("_conf", "")
                merged = {
                    "start_date": llm_m.get("start_date"),
                    "end_date": llm_m.get("end_date"),
                    "backup_date": llm_m.get("backup_date"),
                    "location_state": (llm_m.get("location") or {}).get("state") if isinstance(llm_m.get("location"), dict) else llm_m.get("location_state"),
                    "location_county": (llm_m.get("location") or {}).get("county") if isinstance(llm_m.get("location"), dict) else llm_m.get("location_county"),
                    "location_locality": (llm_m.get("location") or {}).get("locality") if isinstance(llm_m.get("location"), dict) else llm_m.get("location_locality"),
                    "location_utility_affected": (llm_m.get("location") or {}).get("utility_affected") if isinstance(llm_m.get("location"), dict) else llm_m.get("location_utility_affected"),
                    "location_PWS_ID": (llm_m.get("location") or {}).get("PWS_ID") if isinstance(llm_m.get("location"), dict) else llm_m.get("location_PWS_ID"),
                    "advisory_type": llm_m.get("advisory_type"),
                    "reason": llm_m.get("reason"),
                    "people_affected": llm_m.get("people_affected"),
                    "source_type": llm_m.get("source_type"),
                }
            else:
                merged, conf = deterministic_merge(rows), "rule-based"
                # Rule-based merges always want a human glance.
            row = dict(rows[0])
            row.update({k: (v if v is not None else "") for k, v in merged.items()})
            row["source_urls"] = "|".join(u for u in
                                          dict.fromkeys(m["url"] for m in members) if u)
            row["cluster_size"] = len(members)
            row["dedup_confidence"] = conf
            row["needs_review"] = not bool(llm_m)
        out_rows.append(row)

    out_df = pd.DataFrame(out_rows).sort_index() if out_rows else pd.DataFrame()
    # Keep original column order, then the new dedup columns.
    new_cols = ["source_urls", "cluster_size", "dedup_confidence", "needs_review"]
    for c in new_cols:
        if c not in out_df.columns:
            out_df[c] = ""
    out_df.to_csv(args.out, index=False)
    pd.DataFrame(review_rows).to_csv(args.review, index=False)
    n_merged = sum(1 for r in out_rows if r.get("cluster_size", 1) > 1)
    print(f"\nDone: {len(out_rows)} advisory clusters from {len(records)} rows "
          f"({n_merged} merged clusters, {len(review_rows)} pairs need review).")
    print(f"Wrote {args.out} and {args.review}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
