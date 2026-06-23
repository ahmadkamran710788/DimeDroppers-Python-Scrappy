"""Helpers for extracting the Next.js ``__NEXT_DATA__`` JSON blob that MaxPreps
embeds in every page. Parsing this structured blob is far more robust than
scraping hashed CSS class names.
"""
import json
import re

# The script tag carries extra attributes (``crossorigin``), so match loosely.
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)


def extract_next_data(html: str):
    """Return the parsed ``__NEXT_DATA__`` dict, or ``None`` if absent/invalid."""
    if not html:
        return None
    match = _NEXT_DATA_RE.search(html)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except (ValueError, TypeError):
        return None


def page_props(html: str) -> dict:
    """Return ``props.pageProps`` (the useful payload) or an empty dict."""
    data = extract_next_data(html)
    if not data:
        return {}
    return (data.get("props") or {}).get("pageProps") or {}
