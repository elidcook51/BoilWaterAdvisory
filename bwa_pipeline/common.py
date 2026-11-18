"""Small shared helpers: URL canonicalization and date parsing."""

import pandas as pd
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid",
}


def canonicalize_url(url):
    """Normalize a URL for dedup: lowercase host, drop tracking params."""
    url = (url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
             if k.lower() not in TRACKING_PARAMS]
    rebuilt = parsed._replace(
        netloc=parsed.netloc.lower(),
        path=parsed.path.rstrip("/") or "/",
        query=urlencode(query),
    )
    return urlunparse(rebuilt)


def parse_date(value):
    """Parse a date string to a datetime.date, or None if unparseable."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"null", "none", "nan", "nat"}:
        return None
    try:
        return pd.to_datetime(text, errors="raise").date()
    except Exception:
        return None
