"""The single ``__NEXT_DATA__`` -> school row mapping.

Two places have to turn a MaxPreps school page's ``props.pageProps`` into the same 28
``SCHOOL_FIELDS``:

* the crawler (``spiders/maxpreps.py``), one ``SchoolItem`` per school page it visits, and
* the post-crawl opponent harvest (``enrich_opponent.py``), one CSV row per opponent the
  crawl never reached -- typically a school in another state, which
  ``_maybe_follow_school``'s state gate structurally excludes.

Keeping the mapping here is what stops those two from drifting and emitting rows with
subtly different columns.

It also extracts the GoFan / NFHS Network links MaxPreps itself publishes for a school
(``partner_links`` below). Those are separate from -- and never overwrite -- the
``go_fan_ticket_url`` / ``nfhs_url`` columns that ``enrich_gofan_scrapy.py`` and
``enrich_nfhs.py`` resolve by catalog matching; see ``worker.py``'s phase 4.5.
"""
import re
from collections import deque

from .export import SCHOOL_FIELDS

__all__ = ["SCHOOL_FIELDS", "school_row", "partner_links"]

# --------------------------------------------------------------------------- #
# Partner links (GoFan tickets / NFHS Network) as published by MaxPreps
#
# Scanned key-agnostically -- every URL anywhere in the pageProps blob is considered,
# rather than reading one guessed key. MaxPreps is a Next.js app whose payload shape is
# not contractual, and it has already moved fields once; a scan survives that, and costs
# microseconds on a blob we have already parsed.
#
# The ACCEPT patterns carry the real weight. MaxPreps is a CBS/PlayOnSports property and
# links both partners from site chrome, so a naive "any gofan.co URL" match would stamp
# the same nav/footer URL onto every row -- a column that looks populated but identifies
# nothing. Only paths that name a specific school or event are taken.
# --------------------------------------------------------------------------- #
_URL_RE = re.compile(r"https?://[^\s\"'<>\\)]+", re.I)

# Where MaxPreps puts the school's own partner links. Reading this beats scanning: an
# Evanston page carries 81 URLs on partner hosts and only these 2 belong to the school --
# the other 79 are schoolVideos[*] highlight clips of OTHER schools' games, which is what
# contaminated nfhs_url before. The scan below is kept only as a fallback.
PARTNER_INFO_FIELDS = {"gofan": "ticketingUrl", "nfhs": "streamingUrl"}

# Each entry: the host, and the accepted paths in PREFERENCE order.
#
# SCHOOL PAGES ONLY. Two things are load-bearing here:
#
# * GoFan serves a school at BOTH `/school/<id>` and `/app/school/<id>` (both verified
#   200), and MaxPreps publishes the SHORT form -- `partnerInfo.ticketingUrl` for Evanston
#   is `https://gofan.co/school/WY22664`. Requiring `/app/` rejected every GoFan link on
#   every page (0 captured across a 109-row WY run). Do not narrow this back.
# * A blocklist cannot work for gofan.co: it returns 200 for every path, including
#   /pricing, /support, /about, /events and nonsense slugs. Only an explicit school-path
#   whitelist keeps nav and footer chrome out.
#
# Event links stay rejected: MaxPreps' "related content" module links OTHER schools'
# games (Rich UT -> tintic-eureka-ut, Kimball/Mitchell NE -> pine-bluffs-wy, Rigby ID ->
# canyon-ridge-id). An event URL is one game, not a school. Do not add /events/ here.
PARTNERS = {
    "gofan": {
        "host": re.compile(r"(^|\.)gofan\.co$", re.I),
        "accept": (re.compile(r"^/(app/)?school/[^/]+", re.I),),
    },
    "nfhs": {
        "host": re.compile(r"(^|\.)nfhsnetwork\.com$", re.I),
        "accept": (re.compile(r"^/schools?/[^/]+", re.I),),
    },
}

# Marks a MaxPreps cross-promo link rather than the school's own. Match ONLY the campaign:
# `utm_medium=referral` rides on every MaxPreps outbound partner link including the
# legitimate `utm_campaign=school-home` button, so keying on it would reject the good ones.
_REFERRAL_RE = re.compile(r"utm_campaign=related", re.I)

# Hard ceilings so a pathological/recursive blob cannot stall a crawl. A school page's
# pageProps is a few thousand nodes; these are far above that and far below "hangs".
_MAX_DEPTH = 12
_MAX_NODES = 20000


def _iter_urls(node):
    """Yield every ``http(s)://`` string found anywhere in a JSON-ish structure.

    Breadth-first, so shallower nodes come first: a top-level ``ticketsUrl`` is seen
    before one buried inside a per-game array. Deterministic given the same blob, which
    matters because the first accepted URL of a given rank is the one recorded.
    """
    queue = deque([(node, 0)])
    seen = 0
    while queue:
        cur, depth = queue.popleft()
        seen += 1
        if seen > _MAX_NODES or depth > _MAX_DEPTH:
            continue
        if isinstance(cur, str):
            if cur[:4].lower() == "http":
                yield cur
        elif isinstance(cur, dict):
            queue.extend((v, depth + 1) for v in cur.values())
        elif isinstance(cur, (list, tuple)):
            queue.extend((v, depth + 1) for v in cur)


def _split_url(url):
    """``(host, path)`` for a URL, lowercased host. ``(None, None)`` if unparseable."""
    rest = url.split("://", 1)[-1]
    hostport, _, tail = rest.partition("/")
    host = hostport.split("@")[-1].split(":")[0].lower()
    if not host:
        return None, None
    path = "/" + tail.split("?")[0].split("#")[0]
    return host, path


def _match_partner(url):
    """``(partner, rank)`` if this URL names a school/event page, else ``None``.

    Lower rank is a better fit for a school row -- see ``PARTNERS``.
    """
    host, path = _split_url(url)
    if not host or _REFERRAL_RE.search(url):
        return None
    for key, rule in PARTNERS.items():
        if not rule["host"].search(host):
            continue
        for rank, pattern in enumerate(rule["accept"]):
            if pattern.match(path):
                return key, rank
    return None


def partner_links(page_props, html=""):
    """GoFan / NFHS links MaxPreps publishes for this school.

    Returns ``{"gofan": url_or_empty, "nfhs": url_or_empty}``, kept verbatim so we report
    what MaxPreps published rather than a normalized guess at it.

    ``schoolContext.partnerInfo`` is read first and is authoritative. Only if it is absent
    or says nothing about a partner do we fall back to scanning the blob, then ``html``.

    Every candidate -- however it was found -- must still name a school page. That is what
    drops the schools MaxPreps flags as partners but links to the partner's *homepage*
    (``streamingUrl == "https://www.nfhsnetwork.com/"``): a homepage identifies no school,
    so an empty column is the honest answer.
    """
    found = {"gofan": "", "nfhs": ""}
    best = {"gofan": None, "nfhs": None}

    info = ((page_props or {}).get("schoolContext") or {}).get("partnerInfo") or {}
    for key, field in PARTNER_INFO_FIELDS.items():
        url = info.get(field)
        if isinstance(url, str) and _match_partner(url):
            found[key], best[key] = url, -1  # outranks anything the scan can turn up
    if all(found.values()):
        return found

    sources = [_iter_urls(page_props or {})]
    if html:
        sources.append(_URL_RE.findall(html))

    for urls in sources:
        for url in urls:
            hit = _match_partner(url)
            if not hit:
                continue
            key, rank = hit
            if best[key] is None or rank < best[key]:
                best[key], found[key] = rank, url
        # The blob is the authoritative source; only fall through to the raw HTML for a
        # partner it said nothing about.
        if all(found.values()):
            break
    return found


def school_row(page_props, url="", discovered_via="", html=""):
    """Map a school page's ``pageProps`` to a dict keyed by ``SCHOOL_FIELDS``.

    ``sports`` comes back as a LIST -- the flat sinks join it with "; " (see
    ``pipelines.LIST_FIELDS``), the JSON export keeps it a list. Every other value is
    passed through as-is.

    ``html`` is optional and only widens the partner-link search to anchors rendered
    outside ``__NEXT_DATA__``; pass ``response.text`` when you have it.

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

    partners = partner_links(page_props, html)

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
        # partner links as published by MaxPreps itself. NOT the same thing as
        # go_fan_ticket_url / nfhs_url, which are resolved later by catalog matching --
        # these only ever FILL those when the match came back empty (worker.py phase 4.5).
        "maxpreps_gofan_url": partners["gofan"],
        "maxpreps_nfhs_url": partners["nfhs"],
        # sports offered
        "sports": sports,
        "sports_count": len(sports),
        # provenance
        "discovered_via": discovered_via,
    }
