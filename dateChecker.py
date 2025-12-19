import re
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

import requests
from bs4 import BeautifulSoup
import warnings
from dateutil import parser as dateparser
from dateutil.parser import UnknownTimezoneWarning



def get_response_or_error(url, headers, timeout):
    try:
        resp = requests.get(url, headers=headers, timeout = timeout, allow_redirects=True)
    except requests.RequestException as e:
        return None, {"url": url, "status_code": None, "error": str(e)}

    if resp.status_code != 200:
        return None, {"url": url, "status_code": resp.status_code,
                      "error": f"HTTP {resp.status_code}"}
    return resp, None

def _to_ordinal_or_default(date_str: str, default: int) -> int:
    """
    Convert 'MM/DD/YYYY' to a comparable ordinal int.
    Returns `default` if parsing fails.
    """
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").toordinal()
    except Exception:
        return default



def _parse_date(s: str) -> str | None:
    """
    Parse any date/time string into 'MM/DD/YYYY', ignoring timezone info.
    Suppresses UnknownTimezoneWarning (e.g., tzname 'VDH').
    """
    if not s:
        return None
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)
            dt = dateparser.parse(s, fuzzy=True, ignoretz=True)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return None



def _add_candidate(bucket: Dict[str, List[Tuple[int, str, datetime, str]]],
                   kind: str, weight: int, raw: str, dt: Optional[datetime], source: str):
    """
    Collect candidates with (weight, raw, parsed_dt, source).
    Higher weight = higher preference.
    """
    if dt:
        bucket.setdefault(kind, []).append((weight, raw, dt, source))


def _from_meta_content(tag) -> Optional[str]:
    if not tag:
        return None
    val = tag.get("content") or tag.get("value") or tag.get("datetime")
    return (val or "").strip() or None


def _iter_jsonld_objects(soup: BeautifulSoup):
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            # Some pages include multiple JSON objects concatenated or an array
            txt = node.string or node.get_text() or ""
            if not txt.strip():
                continue
            data = json.loads(txt)
            if isinstance(data, list):
                for item in data:
                    yield item
            else:
                yield data
        except Exception:
            # Ignore malformed JSON-LD blocks
            continue


def _search_jsonld_for_dates(obj: Any) -> Dict[str, str]:
    """
    Search JSON-LD object recursively for datePublished/dateModified/uploadDate fields.
    Returns dict with keys 'published' and/or 'updated' if found.
    """
    found = {}

    def walk(o):
        if isinstance(o, dict):
            # Normalize keys to lower-case for robust matching
            lower = {str(k).lower(): v for k, v in o.items()}
            for key, v in lower.items():
                if key in ("datepublished", "uploaddate"):
                    if "published" not in found and isinstance(v, (str, int, float)):
                        found["published"] = str(v)
                if key in ("datemodified", "updated"):
                    if "updated" not in found and isinstance(v, (str, int, float)):
                        found["updated"] = str(v)
            # Keep walking
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(obj)
    return found


def extract_webpage_dates(url: str, timeout: tuple = (8, 20)) -> Dict[str, Any]:
    """
    Try multiple signals to determine publish and last-update dates for a webpage.
    Returns:
        {
            "url": str,
            "published": {"value": ISO8601, "source": str, "raw": str} | None,
            "updated": {"value": ISO8601, "source": str, "raw": str} | None,
            "http_last_modified": ISO8601 | None,
            "all_candidates": {
                "published": [(weight, raw, ISO8601, source), ...],
                "updated": [(weight, raw, ISO8601, source), ...],
            }
        }
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DateFinder/1.0; +https://example.org/bot)"
    }

    # GET (prefer over HEAD because some servers mishandle HEAD and JSON-LD is in body)
    resp, err = get_response_or_error(url, headers=headers, timeout=timeout)
    if resp == None:
        return None
    # HTTP Last-Modified (fallback only)
    last_mod_hdr = resp.headers.get("Last-Modified") or resp.headers.get("last-modified")
    http_last_mod_dt = _parse_date(last_mod_hdr) if last_mod_hdr else None

    # Parse HTML
    soup = BeautifulSoup(resp.text, features="lxml")

    candidates: Dict[str, List[Tuple[int, str, datetime, str]]] = {}

    # 1) JSON-LD (schema.org) — usually most reliable
    for obj in _iter_jsonld_objects(soup):
        dates = _search_jsonld_for_dates(obj)
        if "published" in dates:
            dt = _parse_date(dates["published"])
            _add_candidate(candidates, "published", weight=100, raw=dates["published"], dt=dt,
                           source="jsonld.datePublished/uploadDate")
        if "updated" in dates:
            dt = _parse_date(dates["updated"])
            _add_candidate(candidates, "updated", weight=95, raw=dates["updated"], dt=dt,
                           source="jsonld.dateModified/updated")

    # 2) Open Graph & common meta tags
    meta_queries = [
        ("published", "property", "article:published_time", 90, "meta[property=article:published_time]"),
        ("updated",   "property", "article:modified_time",  88, "meta[property=article:modified_time]"),
        ("updated",   "property", "og:updated_time",        85, "meta[property=og:updated_time]"),

        ("published", "name", "pubdate",                    70, "meta[name=pubdate]"),
        ("published", "name", "publish_date",               70, "meta[name=publish_date]"),
        ("published", "name", "date",                       65, "meta[name=date]"),
        ("published", "name", "dcterms.date",               75, "meta[name=dcterms.date]"),
        ("published", "name", "dc.date.issued",             75, "meta[name=DC.date.issued]"),
        ("published", "itemprop", "datePublished",          92, "meta[itemprop=datePublished]"),

        ("updated",   "itemprop", "dateModified",           90, "meta[itemprop=dateModified]"),
        ("updated",   "name", "last-modified",              60, "meta[name=last-modified]"),
        ("updated",   "name", "modified",                   60, "meta[name=modified]"),
    ]
    for kind, attr, val, weight, label in meta_queries:
        tag = soup.find("meta", attrs={attr: val})
        raw = _from_meta_content(tag)
        if raw:
            dt = _parse_date(raw)
            _add_candidate(candidates, kind, weight, raw, dt, label)

    # 3) <time> elements
    for t in soup.find_all("time"):
        raw = t.get("datetime") or t.get_text(strip=True)
        if not raw:
            continue
        # Heuristic: if surrounding text mentions "updated" or "published", bias appropriately
        context = (t.get("class") or []) + [t.get("id")] if t.get("id") else []
        ctx_str = " ".join(filter(None, [t.get_text(" ", strip=True)] + context)).lower()
        kind = "updated" if any(k in ctx_str for k in ["updated", "modified", "last updated"]) else "published"
        weight = 70 if kind == "published" else 68
        dt = _parse_date(raw)
        _add_candidate(candidates, kind, weight, raw, dt, "time element")

    # 4) Heuristic text scan for dates near "Published"/"Updated" labels (lightweight)
    text = soup.get_text(" ", strip=True)
    patterns = [
        (r"(?:published|posted)\s*[:\-–]\s*([A-Za-z0-9,\s:/\-+.]+)", "published", 50),
        (r"(?:updated|last updated|modified)\s*[:\-–]\s*([A-Za-z0-9,\s:/\-+.]+)", "updated", 50),
    ]
    for pat, kind, weight in patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            raw = m.group(1)[:80]  # keep it short
            dt = _parse_date(raw)
            _add_candidate(candidates, kind, weight, raw, dt, f"text:{pat}")

    # 5) HTTP Last-Modified as a final fallback (often unreliable for publish date)
    if http_last_mod_dt:
        _add_candidate(candidates, "updated", 40, last_mod_hdr, http_last_mod_dt, "http.Last-Modified")

    def choose_best(kind: str) -> Optional[Dict[str, str]]:
        if kind not in candidates or not candidates[kind]:
            return None
        # Sort by weight desc, then by most recent for 'updated' or earliest for 'published'
        items = sorted(candidates[kind], key=lambda x: (-x[0], x[2]))
        if kind == "updated":
            # Among equal weights, prefer the most recent for updates
            items = sorted(items, key=lambda x: (-x[0], -_to_ordinal_or_default(x[2],0)))
        best = items[0]
        return best[2]
        # return {
        #     "value": best[2],
        #     "source": best[3],
        #     "raw": best[1],
        # }

    result = {
        "url": resp.url,  # final URL after redirects
        "published": choose_best("published"),
        "updated": choose_best("updated"),
        "http_last_modified": http_last_mod_dt if http_last_mod_dt else None,
        "all_candidates": {
            "published": [(w, raw, dt, src) for (w, raw, dt, src) in candidates.get("published", [])],
            "updated":   [(w, raw, dt, src) for (w, raw, dt, src) in candidates.get("updated", [])],
        }
    }
    return result
