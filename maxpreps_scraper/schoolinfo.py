"""The single ``__NEXT_DATA__`` -> school row mapping.

Two places have to turn a MaxPreps school page's ``props.pageProps`` into the same 26
``SCHOOL_FIELDS``:

* the crawler (``spiders/maxpreps.py``), one ``SchoolItem`` per school page it visits, and
* the post-crawl opponent harvest (``enrich_opponent.py``), one CSV row per opponent the
  crawl never reached -- typically a school in another state, which
  ``_maybe_follow_school``'s state gate structurally excludes.

Keeping the mapping here is what stops those two from drifting and emitting rows with
subtly different columns.
"""
from .export import SCHOOL_FIELDS

__all__ = ["SCHOOL_FIELDS", "school_row"]


def school_row(page_props, url="", discovered_via=""):
    """Map a school page's ``pageProps`` to a dict keyed by ``SCHOOL_FIELDS``.

    ``sports`` comes back as a LIST -- the flat sinks join it with "; " (see
    ``pipelines.LIST_FIELDS``), the JSON export keeps it a list. Every other value is
    passed through as-is.

    Returns ``None`` when the blob carries no school id, which is how a page that didn't
    parse (or isn't a school page at all) is signalled to the caller. That mirrors the
    crawler, which has always keyed emission on a truthy ``schoolId``.
    """
    ctx = (page_props or {}).get("schoolContext") or {}
    info = ctx.get("schoolInfo") or {}
    links = (page_props or {}).get("schoolLinksData") or {}

    school_id = ctx.get("schoolId") or info.get("schoolId")
    if not school_id:
        return None

    # distinct "Sport (Gender)" labels offered by the school
    sports = sorted({
        f"{s.get('sport')} ({s.get('gender')})".strip()
        for s in (ctx.get("sportSeasons") or []) if s.get("sport")
    })

    return {
        # identity
        "school_id": school_id,
        "name": info.get("name") or info.get("formattedNameWithoutState"),
        "city": info.get("city"),
        "state": info.get("stateCode") or info.get("state"),
        "state_name": info.get("stateName"),
        "url": info.get("canonicalUrl") or url,
        # contact / location
        "mascot": info.get("mascot"),
        "address": info.get("address"),
        "zip_code": info.get("zipCode") or info.get("zip"),
        "phone": info.get("phone"),
        # branding
        "color1": (info.get("color1") or "").strip(),
        "color2": (info.get("color2") or "").strip(),
        "color3": (info.get("color3") or "").strip(),
        "mascot_url": info.get("mascotUrl"),
        # affiliation
        "league_name": info.get("leagueName"),
        "association_name": info.get("associationName"),
        "governing_body_name": info.get("associationGoverningBodyName"),
        "governing_body_url": info.get("associationGoverningBodyUrl"),
        # web presence
        "website": links.get("website") or info.get("websiteUrl"),
        "facebook": links.get("facebook"),
        "instagram": links.get("instagram"),
        "twitter": links.get("twitter"),
        "youtube": links.get("youtube"),
        # sports offered
        "sports": sports,
        "sports_count": len(sports),
        # provenance
        "discovered_via": discovered_via,
    }
