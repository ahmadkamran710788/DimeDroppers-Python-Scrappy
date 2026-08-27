#!/usr/bin/env python
"""HTTP layer for the two GoFan JSON endpoints the middle-school flow needs.

GoFan's public site (``gofan.co/app/school/<id>``) is an Expo / React-Native-Web SPA:
the HTML carries no school or event content at all, so there is nothing to parse and
no reason to drive a browser. Everything the UI renders comes from ``api.gofan.co``,
which is unauthenticated. This module wraps exactly two calls.

**Search** -- what the search box on gofan.co actually calls::

    GET /v2/schools/search?q=<name>&limit=20&offset=0

    -> [ {huddleId, name, city, state, zipCode, industryCode, logoUrl}, ... ]

The paging parameters are ``limit``/``offset``. Passing ``page``/``size`` instead --
the convention every *other* endpoint on this host uses -- returns HTTP 500, and
omitting them entirely returns HTTP 409 "Keyword must not be empty". Both look like
the endpoint is broken; it isn't. Keep ``limit``/``offset``.

This search is the ONLY way to reach a middle school. The bulk catalog
(``/v2/schools/?page=N&size=2000``, used by ``enrich_gofan.load_catalog``) holds
25,777 schools that are effectively all high schools -- Albertville Middle School
(AL25500) is not in it, and neither is any other ``industryCode="Middle School"``
record. ``sitemap-schools.xml`` resolves to that same set. So the catalog+match
approach in ``enrich_gofan.py`` cannot be reused here, only its ideas.

**Events** -- what a school page calls to list its schedule::

    GET /v2/events/search/paginated/<huddleId>/<page>

    -> {content: [event, ...], last: bool, totalElements: int}

Only upcoming / on-sale events are returned. There is no history and no scores:
a ``startDate`` filter is not honoured, and a school with a full season returns
today-forward only. Any "result"/"score" column would be empty on every row, which
is why the schedule CSV this feeds doesn't have one.
"""
import json
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request

# Shared name vocabulary. It lives in gofan_match because that module is about what a
# school name *means*; this module only borrows it to decide what to type into the
# search box. gofan_match imports nothing from here, so there is no cycle.
from gofan_match import GENERIC_WORDS

API = "https://api.gofan.co/v2"
SCHOOL_URL = "https://gofan.co/app/school/{}"
EVENT_URL = "https://gofan.co/event/{}"
# Filename marker for GoFan's own wordmark, which the API serves as the logo for any
# school that never uploaded one. Matched on the FILENAME, not the path: a handful of
# real school logos live outside the usual /logo/<schoolId>/ prefix, so a path-shaped
# test would wrongly discard them. Across the 25,300-school catalog this is the only
# shared asset filename, and it covers 15,451 of them.
PLACEHOLDER_LOGO = "gofan-logo"

# Hard server-side ceiling on /schools/search results. Asking for more returns no more
# (limit=500 still yields 200), and `offset` does not paginate -- page 1 repeats page 0 --
# so 200 is genuinely all we can see for one query. A result set AT this size must be
# assumed truncated, which is what drives the narrower retry in search_schools.
SEARCH_CAP = 200

_PUNCT_TO_SPACE = re.compile(r"[^a-z0-9]+")

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT = 30
TRIES = 4
# Statuses worth retrying: throttling and transient upstream failures. A 4xx that
# isn't 429 means the request itself is wrong, so retrying it just wastes budget.
RETRY_STATUS = {429, 500, 502, 503, 504}
# Small pause after every call. With 8 workers this keeps us near ~80 req/s peak,
# which the API absorbed without a single 429 across the sample runs.
PAUSE = 0.05


class GofanError(RuntimeError):
    """A GoFan call that failed every retry."""


def _get(url, tries=TRIES):
    """GET ``url`` and return parsed JSON, retrying transient failures.

    Raises GofanError once the retries are exhausted; callers treat that as
    "no data for this row" rather than letting it fail the whole job.
    """
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": UA, "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                data = json.load(resp)
            time.sleep(PAUSE)
            return data
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in RETRY_STATUS:
                raise GofanError(f"{exc.code} for {url}") from exc
        except Exception as exc:  # noqa: BLE001 - retry anything transient
            last = exc
        # Exponential backoff with jitter so 8 workers that trip a 429 together
        # don't retry in lockstep and trip it again.
        time.sleep(min(8.0, 0.75 * (2**i)) + random.random() * 0.25)
    raise GofanError(f"giving up on {url}: {last}")


def logo_url(raw):
    """Return a usable logo URL for a school, or "" if GoFan has no real logo.

    Two things have to happen to GoFan's ``logoUrl`` before it is fit to publish.

    **It usually needs percent-encoding.** GoFan stores whatever filename the school
    uploaded, so 4,418 of the 25,300 catalog URLs carry characters that are illegal in a
    URL -- 8,954 spaces, plus parentheses, ``&``, commas, apostrophes, brackets, and 17
    instances of U+202F (narrow no-break space, courtesy of macOS screenshot names).
    Left raw they are not merely ugly, they are broken: curl returns 000 and Python
    raises ``InvalidURL: URL can't contain control characters``. Encoded, they 200.

    **The encoding has to treat everything after the host as one opaque path.**
    ``urllib.parse.urlsplit`` would read the ``#`` in ``LOGO#1.png`` (three such URLs
    exist) as a fragment delimiter and rebuild a URL that 404s. Splitting on ``://``
    and quoting the remainder keeps those intact. ``%`` stays in the safe set so the
    one already-encoded URL in the catalog isn't double-encoded, which also makes this
    idempotent -- verified over 8,000 catalog URLs.

    Returns "" for GoFan's generic wordmark (see PLACEHOLDER_LOGO) so a non-empty value
    always means a genuine school logo.
    """
    url = (raw or "").strip()
    if not url or PLACEHOLDER_LOGO in url.rsplit("/", 1)[-1]:
        return ""
    if "://" not in url:
        return ""
    scheme, rest = url.split("://", 1)
    host, sep, path = rest.partition("/")
    if not sep:
        return url
    return f"{scheme}://{host}/{urllib.parse.quote(path, safe='/%')}"


def _search_raw(q, limit=SEARCH_CAP):
    """One raw call to the search endpoint. Returns [] on any failure."""
    q = (q or "").strip()
    if not q:
        return []
    url = f"{API}/schools/search?" + urllib.parse.urlencode(
        {"q": q, "limit": limit, "offset": 0}
    )
    try:
        data = _get(url)
    except GofanError:
        return []
    # The endpoint returns a bare list; tolerate a paged envelope in case that changes.
    if isinstance(data, dict):
        data = data.get("content") or []
    return [d for d in data if isinstance(d, dict)]


def _query_terms(name):
    """(broad, narrow) query strings for a school name.

    ``broad`` is the first distinctive word -- the shortest thing still likely to be a
    substring of GoFan's own name for the school. ``narrow`` is the first two, used only
    to escape a truncated result set.
    """
    words = [w for w in _PUNCT_TO_SPACE.sub(" ", (name or "").lower()).split() if w]
    distinctive = [w for w in words if w not in GENERIC_WORDS]
    if not distinctive:
        return (name or "").strip(), ""
    broad = distinctive[0]
    # A very short first word ("st", "mt", "e") is too weak on its own, so pull in the
    # next one to keep the result set meaningful.
    if len(broad) < 4 and len(distinctive) > 1:
        broad = f"{distinctive[0]} {distinctive[1]}"
    narrow = " ".join(distinctive[:2]) if len(distinctive) > 1 else (name or "").strip()
    return broad, narrow


def search_schools(name, limit=SEARCH_CAP):
    """Return GoFan's search hits for a school name (possibly empty).

    Each hit carries everything needed to verify a match without a second call:
    ``{huddleId, name, city, state, zipCode, industryCode, logoUrl}``. That is the same
    "City, ST" line the search UI shows under every result.

    **The query is deliberately much shorter than the school's name.** This endpoint does
    a *contiguous substring* match against GoFan's name -- ``q="ort Caroline"`` returns
    Fort Caroline Middle School -- so every extra word NCES carries that GoFan does not
    makes the whole query miss. Searching the full name returned nothing at all for a
    large share of the file::

        "DARNELL COOKMAN MIDDLE/HIGH SCHOOL"           -> 0   "DARNELL"              -> FL25620
        "DUNCAN U. FLETCHER MIDDLE SCHOOL"             -> 0   "DUNCAN FLETCHER"      -> FL25623
        "JAMES WELDON JOHNSON COLLEGE PREPARATORY ..." -> 0   "JAMES WELDON JOHNSON" -> FL25628
        "MAYPORT COASTAL SCIENCE MIDDLE SCHOOL"        -> 0   "MAYPORT"              -> FL25636

    Even a dropped middle initial breaks it ("ALFRED DUPONT" -> 0, "ALFRED I. DUPONT"
    -> 1), so trimming to the first distinctive word is the only reliable query. The
    broader result set costs nothing: ``gofan_match.pick`` still gates every candidate on
    state, city/zip and name, and it does that locally.

    ``limit`` caps server-side at 200 and ``offset`` does not paginate (page 1 repeats
    page 0), so a broad query CAN silently truncate. When it does, retry once with the
    first two words -- narrower, hence fewer results, hence not truncated. Measured cost
    across a real sample: 1.11 requests per row.
    """
    broad, narrow = _query_terms(name)
    results = _search_raw(broad, limit)
    if len(results) >= SEARCH_CAP and narrow and narrow != broad:
        narrowed = _search_raw(narrow, limit)
        if narrowed:
            return narrowed
    return results


def schools_by_ids(ids, batch=1000):
    """Resolve GoFan school ids to their detail records, in bulk.

    ``POST /v2/schools/searchByIds`` takes a JSON array of huddleIds and returns the
    full record for each (name, city, state, zipCode, industryCode, ...). Batches of
    2,000 succeed but take ~8s; 1,000 is the better latency/throughput trade.

    Used to name the opponent on a *home* event, where the API gives us only
    ``opponentSchoolId``. Ids GoFan doesn't know are simply absent from the response,
    so the result is keyed by id and callers must tolerate misses.
    """
    out = {}
    wanted = [i for i in dict.fromkeys(ids) if i]
    for i in range(0, len(wanted), batch):
        chunk = wanted[i : i + batch]
        body = json.dumps(chunk).encode("utf-8")
        req = urllib.request.Request(
            f"{API}/schools/searchByIds",
            data=body,
            headers={
                "User-Agent": UA,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.load(resp)
        except Exception:  # noqa: BLE001 - opponent names are a nicety, never fatal
            continue
        if isinstance(data, dict):
            data = data.get("content") or []
        for rec in data:
            if isinstance(rec, dict) and rec.get("huddleId"):
                out[rec["huddleId"]] = rec
        time.sleep(PAUSE)
    return out


def school_events(huddle_id, max_pages=20):
    """Return every upcoming event for a GoFan school id.

    Pages until the envelope reports ``last``. ``max_pages`` is a runaway guard --
    the default page size is 200, so 20 pages is 4,000 events for one school, far
    beyond anything real.
    """
    sid = (huddle_id or "").strip()
    if not sid:
        return []
    out, page = [], 0
    while page < max_pages:
        try:
            data = _get(f"{API}/events/search/paginated/{urllib.parse.quote(sid)}/{page}")
        except GofanError:
            break
        content = (data or {}).get("content") or []
        out.extend(c for c in content if isinstance(c, dict))
        if (data or {}).get("last") is not False or not content:
            break
        page += 1
    return out
