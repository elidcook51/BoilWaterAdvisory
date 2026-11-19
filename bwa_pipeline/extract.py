"""LLM extraction: turn fetched article texts into structured advisory rows.

Reads texts.csv (Link, Loaded, Text, publish_date, source), sends each
loaded article to the LLM once, and appends one flattened row per article
to structured.csv. Re-runs are safe: URLs already in the output file are
skipped, so a crashed or interrupted run picks up where it left off.

Before any LLM call, a cheap weighted content gate screens the fetched
text (see score_content for the table): exact advisory phrases score
highest, generic boil+water wording scores less, and known junk
(recipes, water parks, drink-driving) scores negative. Score >= 4
passes to the LLM; 2-3 goes to a review log for hand-tuning; below 2 is
rejected. Articles that fail the gate are logged to
extract_rejected.csv and never reach the LLM, which is what makes noisy
discovery (e.g. BigQuery --slim, URL-slug only) affordable: the gate
kills water parks, drink-driving and waterfront junk for free, and the
LLM only sees plausible advisories. Pass --no-gate to disable it. The
gate only buys an LLM call, it never decides an advisory exists.

Quality checks run before scoring: texts under 300 chars are rejected;
texts over ~50,000 chars are flagged for review (usually a failed
extraction that swallowed a whole front page); a line repeated >= 5
times means the extraction failed; Cloudflare/paywall text is BLOCKED,
not gate-rejected, so retry decisions can tell it apart. Language is
checked with langdetect only on pass/review texts (it costs 10-50 ms
each): confident non-English is dropped, English and low-confidence
are kept, and a missing langdetect install skips the check with a
warning. Near-duplicates are caught before the LLM with a 64-bit
SimHash (Hamming distance <= 3, banded lookup); fingerprints live in a
simhash[_suffix].json sidecar next to the texts file, never in
texts.csv. The duplicate threshold is deliberately conservative: two
different advisories in one town can share boilerplate, and a false
merge loses an advisory. So only an exact fingerprint match
(distance 0, a true syndicated copy) is dropped as a duplicate;
a near match at distance 1-3 goes to the review log for a hand
check instead, where a human can tell a reprint from a different
advisory in the same town.

--gate-only runs the gate with no LLM calls at all (and no API key
needed): it reads texts.csv, writes the URLs that pass to
gate_passed[_suffix].csv, the ones that fail to
gate_rejected[_suffix].csv, the 2-3 review band to
gate_review[_suffix].csv, blocked pages to gate_blocked[_suffix].csv
and near-duplicates to gate_duplicates[_suffix].csv next to the output
(the suffix comes from the texts filename, e.g. texts_full.csv ->
gate_passed_full.csv), and writes no structured.csv. Use it to validate
the gate on a harvested set before spending anything on extraction.
Gate log columns: gate_passed has url, publish_date, score, signals;
gate_rejected has url, reason, score, signals; gate_review has url,
publish_date, score, signals, reason; gate_blocked has url, reason,
score; gate_duplicates has url, reason, score, duplicate_of. A normal
run writes the same files as extract_rejected.csv, extract_review.csv,
extract_blocked.csv and extract_duplicates.csv.
"""

import argparse
import csv
import hashlib
import json
import re
import sys
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd
from openai import OpenAI

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
try:
    import config
except ImportError:
    sys.path.insert(0, str(_HERE.parent / "bwa_pipeline"))
    import config

BOIL_WATER_STRUCTURE_V2 = {
    'type': 'object',
    'properties': {
        'publish_date': {'type': ['string', 'null']},
        'start_date': {'type': ['string', 'null']},
        'end_date': {'type': ['string', 'null']},
        'backup_date': {'type': ['string', 'null']},
        'backup_date_note': {'type': ['string', 'null']},
        'date_evidence': {
            'type': 'object',
            'properties': {
                'start_date': {'type': ['string', 'null']},
                'end_date': {'type': ['string', 'null']},
                'backup_date': {'type': ['string', 'null']},
            },
            'required': ['start_date', 'end_date', 'backup_date'],
        },
        'date_notes': {'type': ['string', 'null']},
        'multiple_advisories': {'type': 'boolean'},
        'location': {
            'type': 'object',
            'properties': {
                'state': {'type': ['string', 'null']},
                'county': {'type': ['string', 'null']},
                'locality': {'type': ['string', 'null']},
                'utility_affected': {'type': ['string', 'null']},
                'PWS_ID': {'type': ['string', 'null']},
            },
            'required': ['state', 'county', 'locality', 'utility_affected', 'PWS_ID'],
        },
        'advisory_type': {
            'type': 'string',
            'enum': ['emergency', 'planned', 'unknown'],
        },
        'reason': {'type': ['string', 'null']},
        'people_affected': {'type': ['number', 'null']},
        'source_type': {
            'type': 'string',
            'enum': ['government website', 'utility website', 'government Facebook',
                     'utility Facebook', 'media', 'other', 'unknown'],
        },
    },
    'required': [
        'publish_date', 'start_date', 'end_date', 'backup_date', 'backup_date_note',
        'date_evidence', 'date_notes', 'multiple_advisories', 'location',
        'advisory_type', 'reason', 'people_affected', 'source_type',
    ],
}

EXTRACTION_INSTRUCTIONS = """You extract structured data about ONE boil water advisory from a news article.
Return ONLY valid JSON matching the schema exactly. No prose, no code fences, no comments.
If a field is not stated in the article, use null. Never invent dates, places, or numbers.

You are given ARTICLE METADATA (publish date and source URL) separately from the article text.
- Echo the provided publish_date back in the publish_date field. Do NOT re-derive it from the text.
  If no publish date was provided, use null.
- Use publish_date ONLY to resolve relative date expressions in the text:
  "yesterday" = publish_date minus 1 day; "last Tuesday" = the most recent Tuesday
  strictly before publish_date; "effective immediately" / "today" = publish_date.
  If publish_date is null, leave relative dates as null rather than guessing.

DATE RULES (read carefully -- the old prompt failed here):
- start_date: the date the advisory TOOK EFFECT. Prefer an explicitly stated effective
  date ("effective March 3", "beginning Monday"). If the article only gives the
  announcement/issue date, use that date and say so in date_notes.
- end_date: the date the advisory was LIFTED or ended ("lifted on Friday",
  "rescinded March 10"). If the article says the advisory is still active
  ("until further notice", "remains in effect", present tense with no end date),
  use null. NEVER use the publish_date as the end_date.
- backup_date: ONLY a date explicitly tied to THIS advisory's timing that is NEITHER
  the start NOR the end. Allowed: a water-retest/results date, a public meeting date
  about the advisory, or the date of a PREVIOUS related advisory (previous-advisory
  dates go here, never in start_date). If no such date is stated, use null.
  backup_date is NEVER the publish date.
- backup_date_note: at most 12 words saying what backup_date refers to
  (e.g. "water retest date", "date of previous advisory"), or null.
- date_evidence: for EACH of start_date, end_date, backup_date that is not null,
  a verbatim quote of at most 25 words from the article supporting that date.
  If you cannot quote supporting text, the date must be null.
- date_notes: caveats, e.g. "start_date is the announcement date; effective date
  not stated". Null if none.
- All dates must be YYYY-MM-DD.

OTHER RULES:
- advisory_type: "emergency" for advisories responding to a sudden event (pipe burst,
  pressure loss, contamination found); "planned" for scheduled work (construction,
  maintenance); "unknown" otherwise.
- location: fill state/county/locality/utility_affected/PWS_ID only with what the
  article states. Do not expand abbreviations into guesses; do not infer the county
  from the city unless the article names it.
- people_affected: a number only (e.g. 2000 for "about 2,000 residents"). Null if
  not stated ("hundreds" -> null, note it in date_notes is wrong place -- just null).
- source_type: classify the article's publisher.
- If the article is NOT about a boil water advisory, return null for every field
  (multiple_advisories=false) but still follow the schema exactly.
- If the article describes MORE THAN ONE distinct advisory (different towns and/or
  different dates), extract the FIRST/main one and set multiple_advisories=true.

EXAMPLE:
Article publish date: 2025-03-05. Source: local news site.
Article text: "The City of Springfield issued a precautionary boil water advisory
for the north side on Tuesday after a water main break on Elm Street. About 2,000
residents are affected. Officials say the advisory will remain in effect until
further notice. Water samples will be retested on Friday."

Correct output:
{"publish_date": "2025-03-05", "start_date": "2025-03-04", "end_date": null,
"backup_date": "2025-03-07", "backup_date_note": "water retest date",
"date_evidence": {"start_date": "issued a precautionary boil water advisory for the north side on Tuesday",
"end_date": null, "backup_date": "Water samples will be retested on Friday"},
"date_notes": null, "multiple_advisories": false,
"location": {"state": null, "county": null, "locality": "Springfield",
"utility_affected": null, "PWS_ID": null},
"advisory_type": "emergency", "reason": "water main break on Elm Street",
"people_affected": 2000, "source_type": "media"}

Note: "Tuesday" resolved against publish_date 2025-03-05 (a Wednesday) -> 2025-03-04.
"until further notice" -> end_date null, NOT the publish date. The retest Friday ->
backup_date with a note. State left null because the article never names it.
"""


def flatten_dict(d, parent_key="", sep="_"):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        else:
            items[new_key] = v
    return items


def _empty_columns():
    """Column names for a header-only structured.csv (empty run)."""
    record = {k: None for k in BOIL_WATER_STRUCTURE_V2["required"]}
    record["date_evidence"] = {k: None for k in
                               ("start_date", "end_date", "backup_date")}
    record["location"] = {k: None for k in
                          ("state", "county", "locality", "utility_affected",
                           "PWS_ID")}
    return list(flatten_dict(record).keys()) + ["Source URL"]


def extract_advisory(client, article_text, publish_date=None, source_url=None,
                        model="gpt-4.1-mini"):
    """Extract one advisory record from article text.

    client: an OpenAI client (as in llm_segment.py).
    article_text: full extracted article text.
    publish_date: "YYYY-MM-DD" from dateChecker (or None).
    source_url: article URL (or None).
    Returns: dict matching BOIL_WATER_STRUCTURE_V2.
    Raises: ValueError if the model output cannot be parsed as JSON.
    """
    user_block = (
        f"Article publish date (metadata): {publish_date}\n"
        f"Article source URL (metadata): {source_url}\n\n"
        f"Article text:\n{article_text}\n\n"
        "Extract the advisory as per the instructions and schema."
    )
    response = client.responses.create(
        model=model,
        instructions=EXTRACTION_INSTRUCTIONS,
        input=user_block,
    )
    text = response.output_text or ""
    # Defensive: strip code fences if the model adds them anyway.
    text = re.sub(r"^```(?:json)?\s*", "", text.strip())
    text = re.sub(r"\s*```$", "", text)
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("LLM did not return JSON.")
    data = json.loads(m.group(0))
    # Light validation: all required top-level keys present.
    missing = [k for k in BOIL_WATER_STRUCTURE_V2["required"] if k not in data]
    if missing:
        raise ValueError(f"LLM JSON missing keys: {missing}")
    return data


# Content-gate scoring (v3; weights are starting points to tune on the
# gate-test files, see research section 6).
PASS_THRESHOLD = 4
REVIEW_THRESHOLD = 2
MAX_TEXT_CHARS = 50000
_REPEATED_LINE_THRESHOLD = 5
_LANG_CONFIDENCE = 0.90
_SIMHASH_MAX_DISTANCE = 3

_STRONG_PHRASES_5 = ("boil water advisory", "boil-water advisory",
                     "precautionary boil water")
_STRONG_PHRASES_4 = ("boil water notice", "boil water order",
                     "boil advisory")
_RECIPE_PHRASES = ("bring water to a boil", "boil the water",
                    "bring it to a boil", "bring to a boil", "to a boil",
                    "boiling water", "water is boiling", "water boiler")
_JUNK_PHRASES = ("water park", "bottled water", "waterfront",
                 "drink-driving")
_WATER_CONTEXT_WORDS = ("advisory", "notice", "warning", "lifted",
                        "rescinded")
# Challenge-specific phrases only: a real article may mention Cloudflare
# the company, so the bare word is not a marker.
_CLOUDFLARE_MARKERS = ("just a moment", "checking your browser",
                       "cf-chl-", "ray id", "verify you are human",
                       "performance & security by cloudflare")
_PAYWALL_PHRASES = ("subscribe to continue", "subscribe to read",
                    "to continue reading", "sign up to continue",
                    "this article is for subscribers",
                    "subscription required")
_PAYWALL_LONG_TEXT = 2000


def _has_word(text, word):
    """True when word occurs as a whole word, not part of a longer one."""
    return re.search(r"\b" + re.escape(word) + r"\b", text) is not None


def _same_sentence(low, word_a, word_b):
    """True when both words occur in one sentence of the lowered text.

    Word-boundary matching: "boiling" and "boiler" do not count as
    "boil", so a recipe ("when boiling") or an appliance manual
    ("water boiler") does not earn the boil+water signal.
    """
    for sentence in re.split(r"[.!?\n]+", low):
        if _has_word(sentence, word_a) and _has_word(sentence, word_b):
            return True
    return False


def score_content(text, url=None, title=None):
    """Score fetched text for how much it reads like a water advisory.

    Returns (score, band, matched_signals), where band is "pass"
    (score >= 4), "review" (2-3) or "reject" (below 2), and
    matched_signals is a list like ["phrase:boil water advisory:+5"].
    url feeds the slug signal ("boil" in the URL path); title feeds the
    title signal. texts.csv has no title column, so callers pass
    title=None unless the input file carries one.

    Scoring table (research section 6):
      +5  "boil water advisory" / "boil-water advisory" /
          "precautionary boil water" (first match only)
      +4  "boil water notice" / "boil water order" / "boil advisory"
          (first match only)
      +4  "do not drink" + "water"
      +2  "boil" + "water" in the same sentence (whole words only:
          "boiling" / "boiler" do not count)
      +2  "water" + ("advisory" / "notice" / "warning" / "lifted" /
          "rescinded")
      +2  URL slug contains "boil" (whole word)
      +1  title contains "boil" (whole word)
      -4  recipe / appliance phrasing ("bring water to a boil" /
          "boil the water" / "bring it to a boil" / "to a boil" /
          "boiling water" / "water is boiling" / "water boiler"),
          applied only when no strong advisory signal matched
          (a +5/+4 phrase or "do not drink"), mirroring the junk
          gate below: a real advisory that tells people to
          "boil the water" must not be penalised for its own
          instructions, while a recipe with only weak +2 signals
          still gets the -4
      -3  junk class ("water park" / "bottled water" / "waterfront" /
          "drink-driving", or "drinking water" as the only water
          mention), applied once and only when no positive advisory
          signal matched -- i.e. the junk phrase is the text's
          only water content. A real advisory routinely says
          "use bottled water", so an unconditional -3 would
          reject real advisories that score on weak signals.
    Signals stack: a text reading "precautionary boil water advisory"
    scores 5 + 2 (same sentence) + 2 (water + advisory) = 9.
    """
    low = (text or "").lower()
    score = 0
    signals = []
    # Strong signals: an exact advisory phrase, or do-not-drink.
    # The recipe penalty below is gated on none of these matching.
    strong = False
    for phrase in _STRONG_PHRASES_5:
        if phrase in low:
            score += 5
            signals.append(f"phrase:{phrase}:+5")
            strong = True
            break
    for phrase in _STRONG_PHRASES_4:
        if phrase in low:
            score += 4
            signals.append(f"phrase:{phrase}:+4")
            strong = True
            break
    if "do not drink" in low and _has_word(low, "water"):
        score += 4
        signals.append("do-not-drink+water:+4")
        strong = True
    if _same_sentence(low, "boil", "water"):
        score += 2
        signals.append("boil+water same sentence:+2")
    if _has_word(low, "water") and any(word in low
                                       for word in _WATER_CONTEXT_WORDS):
        score += 2
        signals.append("water+advisory-word:+2")
    if url:
        path = re.sub(r"^https?://[^/]+", "", str(url).lower())
        if _has_word(path, "boil"):
            score += 2
            signals.append("url slug contains boil:+2")
    if title and _has_word(str(title).lower(), "boil"):
        score += 1
        signals.append("title contains boil:+1")
    positive = score  # positive signals only; recipe/junk come next
    if not strong:
        for phrase in _RECIPE_PHRASES:
            if phrase in low:
                score -= 4
                signals.append(f"recipe:{phrase}:-4")
                break
    junk_hit = None
    if positive == 0:
        for phrase in _JUNK_PHRASES:
            if phrase in low:
                junk_hit = phrase
                break
        if junk_hit is None and "drinking water" in low:
            junk_hit = "drinking water (only water mention)"
    if junk_hit is not None:
        score -= 3
        signals.append(f"junk:{junk_hit}:-3")
    if score >= PASS_THRESHOLD:
        band = "pass"
    elif score >= REVIEW_THRESHOLD:
        band = "review"
    else:
        band = "reject"
    return score, band, signals


def passes_content_gate(text):
    """Cheap pre-LLM screen: does this fetched text read like a water advisory?

    Compatibility wrapper around score_content: True when the text
    scores >= 4. Kept because extract_all and the older tests call it;
    new code should use score_content so the url/title signals and the
    review band work. The LLM remains the real classifier: passing the
    gate only buys the article an LLM call, and pages that pass it but
    are not advisories (e.g. recipes for boiling water) still come back
    all-null from the model.
    """
    return score_content(text)[0] >= PASS_THRESHOLD


def detect_blocked(text):
    """Challenge/paywall detection. Returns (blocked, marker).

    A text is BLOCKED, not gate-rejected, when it carries a Cloudflare
    challenge marker, or paywall language with no substantial article
    body (under _PAYWALL_LONG_TEXT chars). The distinction matters for
    retry decisions: a blocked page may fetch fine later, a rejected
    one will not.
    """
    low = (text or "").lower()
    for marker in _CLOUDFLARE_MARKERS:
        if marker in low:
            return True, marker
    if len(text or "") < _PAYWALL_LONG_TEXT:
        for phrase in _PAYWALL_PHRASES:
            if phrase in low:
                return True, phrase
    return False, None


def find_repeated_line(text, threshold=_REPEATED_LINE_THRESHOLD):
    """Most-repeated non-empty line, or (None, 0).

    When one line repeats >= 5 times the extraction failed (usually
    navigation or a subscribe banner trafilatura did not strip).
    """
    counts = Counter(line.strip() for line in (text or "").splitlines()
                     if line.strip())
    common = counts.most_common(1)
    if common and common[0][1] >= threshold:
        return common[0]
    return None, 0


_LANGDETECT_WARNED = False


def check_language(text):
    """Language check on text that already passed or is under review.

    Returns (keep, lang, probability). Keeps English and
    unknown/low-confidence detections; drops only confident
    non-English (probability >= 0.90). Never translates. If langdetect
    is not installed, warns once and keeps everything rather than
    crashing. Only call this after scoring: langdetect costs 10-50 ms
    per text, and rejects never reach an LLM call anyway.
    """
    global _LANGDETECT_WARNED
    try:
        from langdetect import detect_langs
    except ImportError:
        if not _LANGDETECT_WARNED:
            warnings.warn("langdetect is not installed; skipping "
                          "language check (add langdetect to "
                          "requirements.txt)")
            _LANGDETECT_WARNED = True
        return True, None, None
    try:
        langs = detect_langs((text or "")[:20000])
    except Exception:
        return True, None, None
    if not langs:
        return True, None, None
    top = langs[0]
    if top.lang == "en" or top.prob < _LANG_CONFIDENCE:
        return True, top.lang, float(top.prob)
    return False, top.lang, float(top.prob)


def simhash64(text):
    """64-bit SimHash of normalized extracted text (word unigrams)."""
    tokens = Counter(re.findall(r"[a-z0-9]+", (text or "").lower()))
    vector = [0] * 64
    for token, weight in tokens.items():
        value = int.from_bytes(
            hashlib.md5(token.encode("utf-8")).digest()[:8], "little")
        for bit in range(64):
            vector[bit] += weight if (value >> bit) & 1 else -weight
    fingerprint = 0
    for bit in range(64):
        if vector[bit] > 0:
            fingerprint |= 1 << bit
    return fingerprint


def hamming_distance(a, b):
    """Number of differing bits between two fingerprints."""
    return bin(a ^ b).count("1")


class SimHashIndex:
    """Banded SimHash lookup: 4 x 16-bit bands over the 64-bit hash.

    A new fingerprint only needs checking against entries sharing at
    least one band, which is a handful of candidates, not the whole
    corpus. Banding is exact for the <= 3 threshold: with at most 3
    differing bits spread over 4 bands, at least one band is identical.
    """

    def __init__(self, max_distance=_SIMHASH_MAX_DISTANCE):
        self.max_distance = max_distance
        self.fingerprints = {}
        self._bands = [dict() for _ in range(4)]

    @staticmethod
    def _band_keys(fingerprint):
        return [(fingerprint >> (16 * i)) & 0xFFFF for i in range(4)]

    def add(self, fingerprint, key):
        self.fingerprints[key] = fingerprint
        for i, band in enumerate(self._band_keys(fingerprint)):
            self._bands[i].setdefault(band, set()).add(key)

    def find_near(self, fingerprint):
        """[(key, distance)] within max_distance, closest first."""
        candidates = set()
        for i, band in enumerate(self._band_keys(fingerprint)):
            candidates.update(self._bands[i].get(band, ()))
        near = [(key, hamming_distance(fingerprint, self.fingerprints[key]))
                for key in candidates
                if hamming_distance(fingerprint, self.fingerprints[key])
                <= self.max_distance]
        return sorted(near, key=lambda item: item[1])


def _simhash_sidecar_path(texts_csv):
    """Fingerprint file next to the texts file, named after it."""
    stem = Path(texts_csv).stem
    suffix = stem[len("texts"):] if stem.startswith("texts") else ""
    return Path(texts_csv).parent / f"simhash{suffix}.json"


def _load_simhash_index(path):
    """Rebuild the banded index from a sidecar file, if it exists."""
    index = SimHashIndex()
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                stored = json.load(f)
            for url, hex_fp in stored.items():
                index.add(int(hex_fp, 16), url)
        except (OSError, ValueError):
            pass
    return index


def _save_simhash_index(path, index):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({url: format(fp, "016x")
                   for url, fp in index.fingerprints.items()},
                  f, indent=1, sort_keys=True)


def _is_loaded(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "t", "yes", "y"}


def _clean_date(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _gate_log_paths(texts_csv, out_dir, gate_only=False):
    """Gate log paths, named after the texts file.

    Gate-only runs use the gate_* names; a normal run uses the
    extract_* names for the same files. Returns a dict keyed
    "passed", "rejected", "review", "blocked", "duplicates". The
    passed entry is None for a normal run (passing URLs go to the
    LLM, not a log).
    """
    stem = Path(texts_csv).stem
    suffix = stem[len("texts"):] if stem.startswith("texts") else ""
    if gate_only:
        return {"passed": out_dir / f"gate_passed{suffix}.csv",
                "rejected": out_dir / f"gate_rejected{suffix}.csv",
                "review": out_dir / f"gate_review{suffix}.csv",
                "blocked": out_dir / f"gate_blocked{suffix}.csv",
                "duplicates": out_dir / f"gate_duplicates{suffix}.csv"}
    return {"passed": None,
            "rejected": out_dir / "extract_rejected.csv",
            "review": out_dir / "extract_review.csv",
            "blocked": out_dir / "extract_blocked.csv",
            "duplicates": out_dir / "extract_duplicates.csv"}


def _append_csv(path, fieldnames, row):
    """Append one row, writing the header if the file is new or empty.

    If the file already exists with an older header (e.g. the v2.9
    two-column gate logs), rewrite it with the new header, padding the
    old rows with empty values, so columns stay aligned.

    Only the first line is read to decide that: reading the whole
    file per append is O(n^2) over a backfill-scale log (measured
    14 ms/append on a 3,000-row log). The full read happens only on
    the one-time header-mismatch rewrite.
    """
    if path.exists() and path.stat().st_size > 0:
        with open(path, newline="", encoding="utf-8") as f:
            header_line = f.readline()
        old_fields = (next(csv.reader([header_line]))
                      if header_line.strip() else [])
        if old_fields != fieldnames:
            with open(path, newline="", encoding="utf-8") as f:
                old_rows = list(csv.DictReader(f))
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for old in old_rows:
                    writer.writerow({name: old.get(name, "")
                                     for name in fieldnames})
    header = (not path.exists() or path.stat().st_size == 0)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if header:
            writer.writeheader()
        writer.writerow({name: row.get(name, "") for name in fieldnames})


def _signals_text(signals):
    return "; ".join(signals)


def _log_passed(passed_path, url, publish_date, score=None, signals=None):
    _append_csv(passed_path, ["url", "publish_date", "score", "signals"],
                {"url": url, "publish_date": publish_date or "",
                 "score": "" if score is None else score,
                 "signals": _signals_text(signals or [])})


def _log_rejected(rejected_path, url,
                  reason="no boil-water keywords in text",
                  score=None, signals=None):
    _append_csv(rejected_path, ["url", "reason", "score", "signals"],
                {"url": url, "reason": reason,
                 "score": "" if score is None else score,
                 "signals": _signals_text(signals or [])})


def _log_review(review_path, url, publish_date, score, signals, reason):
    _append_csv(review_path,
                ["url", "publish_date", "score", "signals", "reason"],
                {"url": url, "publish_date": publish_date or "",
                 "score": score, "signals": _signals_text(signals),
                 "reason": reason})


def _log_blocked(blocked_path, url, reason, score=None):
    _append_csv(blocked_path, ["url", "reason", "score"],
                {"url": url, "reason": reason,
                 "score": "" if score is None else score})


def _log_duplicate(duplicates_path, url, score, duplicate_of):
    _append_csv(duplicates_path,
                ["url", "reason", "score", "duplicate_of"],
                {"url": url, "reason": "near-duplicate of " + duplicate_of,
                 "score": score, "duplicate_of": duplicate_of})


def _urls_in(path):
    """URLs already recorded in a gate log file (resume safety)."""
    if path is None or not path.exists():
        return set()
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return {r.get("url", "") for r in csv.DictReader(f)}
    except OSError:
        return set()


def _row_title(row):
    """Title from the texts row, if the input file carries one.

    texts.csv has no title column, so this is usually None and the
    title signal simply does not fire.
    """
    for col in ("title", "Title"):
        if col in row.index:
            value = row[col]
            if value is not None and not (isinstance(value, float)
                                          and pd.isna(value)):
                value = str(value).strip()
                if value:
                    return value
    return None


def _gate_article(text, url=None, title=None, sim_index=None):
    """Run the full pre-LLM gate on one article.

    Returns a dict with keys action ("pass", "review", "reject",
    "blocked" or "duplicate"), score, signals, reason and
    duplicate_of. Checks run cheapest-first: blocked page, length
    floor, repeated lines, weighted score, length ceiling, then (only
    for pass/review texts, the ones that would cost an LLM call)
    language and near-duplicate. A passing article's fingerprint is
    added to sim_index. Review-band texts are logged for hand-tuning,
    not sent to the LLM; use --no-gate to send everything. An exact
    SimHash match (distance 0) is a duplicate; a near match at
    distance 1-3 is review, not a drop, so a different advisory
    sharing a utility's boilerplate is never silently lost.
    Over-50k texts get the language check too before their review
    verdict, so a confident non-English long text is rejected
    rather than parked in review with no language verdict.
    """
    text = text or ""
    blocked, marker = detect_blocked(text)
    if blocked:
        return {"action": "blocked", "score": None, "signals": [],
                "reason": f"blocked page ({marker})", "duplicate_of": None}
    if len(text) < config.MIN_TEXT_CHARS:
        return {"action": "reject", "score": None, "signals": [],
                "reason": f"text too short ({len(text)} chars)",
                "duplicate_of": None}
    line, count = find_repeated_line(text)
    if line is not None:
        return {"action": "reject", "score": None, "signals": [],
                "reason": f"repeated line {count}x (extraction failed): "
                          f"{line[:80]}", "duplicate_of": None}
    score, band, signals = score_content(text, url=url, title=title)
    if len(text) > MAX_TEXT_CHARS:
        keep, lang, prob = check_language(text)
        if not keep:
            return {"action": "reject", "score": score,
                    "signals": signals,
                    "reason": f"non-English (langdetect: {lang} "
                              f"p={prob:.2f})", "duplicate_of": None}
        return {"action": "review", "score": score, "signals": signals,
                "reason": f"text over {MAX_TEXT_CHARS} chars "
                          f"({len(text)} chars)",
                "duplicate_of": None}
    if band == "reject":
        return {"action": "reject", "score": score, "signals": signals,
                "reason": "score below review band", "duplicate_of": None}
    keep, lang, prob = check_language(text)
    if not keep:
        return {"action": "reject", "score": score, "signals": signals,
                "reason": f"non-English (langdetect: {lang} "
                          f"p={prob:.2f})", "duplicate_of": None}
    if band == "review":
        return {"action": "review", "score": score, "signals": signals,
                "reason": "review band (score 2-3)", "duplicate_of": None}
    if sim_index is not None:
        fingerprint = simhash64(text)
        near = sim_index.find_near(fingerprint)
        if near:
            key, dist = near[0]
            if dist == 0:
                return {"action": "duplicate", "score": score,
                        "signals": signals, "reason": "near-duplicate",
                        "duplicate_of": key}
            # Distance 1-3: too close to trust as a reprint, too
            # far to drop unseen -- a different advisory from the
            # same utility template lands here. Route to review
            # and do not add its fingerprint, so it cannot mask
            # a later copy of the original.
            return {"action": "review", "score": score,
                    "signals": signals,
                    "reason": f"near-duplicate of {key} "
                              f"(Hamming distance {dist}) - hand check",
                    "duplicate_of": key}
        sim_index.add(fingerprint, url)
    return {"action": "pass", "score": score, "signals": signals,
            "reason": "", "duplicate_of": None}


def extract_all(texts_csv, out_csv, limit=None, gate=True, gate_only=False):
    """Run extraction over texts.csv and append structured rows to out_csv.

    Returns the number of advisories extracted in this run. With the
    content gate on (default), articles whose fetched text does not read
    like a water advisory are logged to extract_rejected.csv next to the
    output and never sent to the LLM; review-band texts go to
    extract_review.csv, blocked pages to extract_blocked.csv and
    near-duplicates to extract_duplicates.csv. With gate_only=True, no
    LLM call is made at all (and no API key is needed): gate-passing
    URLs go to gate_passed[_suffix].csv, failures to
    gate_rejected[_suffix].csv, and the return value is the number that
    passed. Near-duplicate fingerprints are kept in a
    simhash[_suffix].json sidecar next to the texts file.
    """
    if gate_only:
        gate = True
    if not gate_only and not config.OPEN_AI_API_KEY:
        sys.exit("OPEN_AI_API_KEY is not set. Add it to a .env file at the repo head:\n"
                 "    OPEN_AI_API_KEY=sk-...")

    client = None if gate_only else OpenAI(api_key=config.OPEN_AI_API_KEY)

    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    texts = pd.read_csv(texts_csv)
    if "Loaded" in texts.columns:
        texts = texts[texts["Loaded"].map(_is_loaded)]
    if limit:
        texts = texts.head(limit)
    texts = texts.reset_index(drop=True)

    log_paths = _gate_log_paths(texts_csv, out_path.parent,
                                gate_only=gate_only)
    rejected = _urls_in(log_paths["rejected"])
    passed = _urls_in(log_paths["passed"])
    reviewed = _urls_in(log_paths["review"])
    blocked = _urls_in(log_paths["blocked"])
    duplicates = _urls_in(log_paths["duplicates"])

    sim_path = _simhash_sidecar_path(texts_csv)
    sim_index = _load_simhash_index(sim_path) if gate else None
    sim_dirty = False

    done = set()
    columns = None
    if out_path.exists():
        try:
            columns = list(pd.read_csv(out_path, nrows=0).columns)
            done = set(pd.read_csv(out_path, usecols=["Source URL"])["Source URL"].astype(str))
        except Exception:
            done = set()
            columns = None
    write_header = (not out_path.exists()) or out_path.stat().st_size == 0

    total = len(texts)
    processed = 0
    gated = 0
    review_n = 0
    blocked_n = 0
    dup_n = 0
    passed_n = 0
    for idx, row in texts.iterrows():
        link = str(row["Link"]).strip()
        if link in done or link in rejected or link in passed \
                or link in reviewed or link in blocked \
                or link in duplicates:
            continue
        text = row["Text"] if isinstance(row["Text"], str) else ""
        outcome = {"score": None, "signals": None}
        if gate:
            outcome = _gate_article(text, url=link,
                                    title=_row_title(row),
                                    sim_index=sim_index)
            action = outcome["action"]
            if action == "blocked":
                _log_blocked(log_paths["blocked"], link,
                             outcome["reason"], outcome["score"])
                blocked.add(link)
                blocked_n += 1
                print(f"Blocked row {idx} ({link}): {outcome['reason']}")
                continue
            if action == "review":
                _log_review(log_paths["review"], link,
                            _clean_date(row.get("publish_date")),
                            outcome["score"], outcome["signals"],
                            outcome["reason"])
                reviewed.add(link)
                review_n += 1
                print(f"Review row {idx} ({link}): score "
                      f"{outcome['score']} ({outcome['reason']})")
                continue
            if action == "duplicate":
                _log_duplicate(log_paths["duplicates"], link,
                               outcome["score"], outcome["duplicate_of"])
                duplicates.add(link)
                dup_n += 1
                print(f"Near-duplicate row {idx} ({link}): matches "
                      f"{outcome['duplicate_of']}")
                continue
            if action == "reject":
                _log_rejected(log_paths["rejected"], link,
                              outcome["reason"], outcome["score"],
                              outcome["signals"])
                rejected.add(link)
                gated += 1
                print(f"Gated out row {idx} ({link}): "
                      f"{outcome['reason']}")
                continue
            sim_dirty = True
        if gate_only:
            _log_passed(log_paths["passed"], link,
                        _clean_date(row.get("publish_date")),
                        outcome["score"], outcome["signals"])
            passed.add(link)
            passed_n += 1
            continue
        try:
            record = extract_advisory(
                client,
                text,
                publish_date=_clean_date(row.get("publish_date")),
                source_url=link,
                model=config.LLM_MODEL,
            )
        except Exception as e:
            print(f"Failed row {idx} ({link}): {e}")
            continue
        flat = flatten_dict(record)
        flat["Source URL"] = link
        row_df = pd.DataFrame([flat])
        if columns is None:
            columns = list(row_df.columns)
        row_df = row_df.reindex(columns=columns)
        row_df.to_csv(out_path, mode="a", header=write_header, index=False)
        write_header = False
        done.add(link)
        processed += 1
        print(f"Finished row {idx} ({processed}/{total} done this run)")

    if sim_index is not None and sim_dirty:
        _save_simhash_index(sim_path, sim_index)
    summary = []
    if gated:
        summary.append(f"{gated} gated out by the content gate")
    if review_n:
        summary.append(f"{review_n} for review")
    if blocked_n:
        summary.append(f"{blocked_n} blocked")
    if dup_n:
        summary.append(f"{dup_n} near-duplicates")
    detail = "; ".join(summary)
    if gate_only:
        print(f"Gate-only: {passed_n} passed"
              + (f" ({detail})" if summary else "")
              + " (no LLM calls made)")
        return passed_n

    if processed == 0 and (not out_path.exists()
                          or out_path.stat().st_size == 0):
        pd.DataFrame(columns=columns or _empty_columns()).to_csv(
            out_path, index=False)
        print(f"Wrote header-only {out_path} (no advisories extracted)")
    print(f"Processed {processed} advisories"
          + (f" ({detail})" if summary else ""))
    return processed


def main():
    parser = argparse.ArgumentParser(
        description="Extract structured advisories from fetched article texts.")
    parser.add_argument("--texts", default=str(config.DATA_DIR / "texts.csv"),
                        help="input texts.csv from the fetch stage")
    parser.add_argument("--out", default=str(config.DATA_DIR / "structured.csv"),
                        help="output structured.csv (appended to; resume-safe)")
    parser.add_argument("--limit", type=int, default=None,
                        help="only process the first N loaded articles (smoke test)")
    parser.add_argument("--no-gate", action="store_true",
                        help="disable the pre-LLM content gate: send every "
                             "loaded article to the LLM (costs more on noisy "
                             "discovery; useful if the gate is ever suspected "
                             "of dropping real advisories)")
    parser.add_argument("--gate-only", action="store_true",
                        help="run only the content gate, with no LLM calls "
                             "and no API key: write gate_passed[_suffix].csv, "
                             "gate_rejected[_suffix].csv, gate_review, "
                             "gate_blocked and gate_duplicates logs next to "
                             "the output (suffix from the texts filename) "
                             "and write no structured.csv")
    args = parser.parse_args()
    extract_all(args.texts, args.out, limit=args.limit,
                gate=not args.no_gate, gate_only=args.gate_only)


if __name__ == "__main__":
    main()
