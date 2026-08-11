#!/usr/bin/env python
"""Opponent reconciliation, in two phases.

    harvest   every opponent in the schedule gets a row in the TEAMS csv   (network)
    resolve   every game gets its opponent's school name + logo            (no network)

Why two phases. The crawler's ``_maybe_follow_school`` gates discovered urls on the
requested states, so an opponent filed under another state is never crawled -- its game
lands in the schedule CSV with no matching teams row. Fixing that in the crawl would spend
crawl budget and still not be guaranteed under truncation, so it is done here instead,
where it is bounded and deterministic.

The two outputs have opposite ordering constraints: the school rows must exist BEFORE the
GoFan/NFHS steps so those cover them, but the opponent's display name is only final AFTER
GoFan has written ``original_name``. So the page is fetched once by ``harvest``, which
parks what it learned in ``opponents.json``; ``resolve`` then runs after GoFan/NFHS off
that cache with no further requests. See the phase list in ``worker.py``.

``harvest`` only fetches opponents that are NOT already teams rows -- one whose root url is
already in the CSV is read straight from it. That makes this strictly cheaper than the
single pass it replaces, which fetched every distinct opponent unconditionally.

Both phases are best-effort and idempotent, and write atomically (tmp + ``os.replace``), so
a crash or a timeout can never leave a half-written CSV.

Add each game's opponent school name + logo to a schedule CSV, in place.

The schedule table only gives us the opponent's short display text ("Saint Mary's") and a
link to that opponent's team page. This step follows the link and turns it into two columns:

    original_opponent_school_name   the opponent's real school name, cross-referenced against
                                    the teams CSV so both files agree on one name
    original_opponent_school_logo   the opponent school's logo URL (written straight through)

How the name is resolved
------------------------
1. **Scrape** the opponent's team page and read the breadcrumb crumb shaped
   ``<School Name> <Sport>``, minus the sport -- "Star Valley High School Football" ->
   "Star Valley High School". That crumb is the LAST one on a varsity page but the
   second-to-last on a level page ("/football/freshman/" appends its own "Freshman" crumb),
   so it is located by matching the sport, never by position.
2. **Cross-reference** that against the teams CSV ``name`` column. On a match the teams row's
   ``original_name`` (the GoFan-verified name, written by ``enrich_gofan_scrapy.py``) wins, so
   a school is spelled identically in both CSVs. On no match the scraped name is kept as-is.

The comparison is normalized (``enrich_gofan.normalize`` -- lowercase, punctuation stripped,
"high school"/"hs" dropped) because MaxPreps' breadcrumb and its ``name`` field disagree about
the "High School" suffix; an exact comparison would miss almost every row. It is also scoped
to the opponent's state (taken from its URL) so a same-named school in another state can never
be picked, mirroring the GoFan and NFHS matchers.

Every unique opponent URL is fetched exactly once, no matter how many games reference it.

    python enrich_opponent.py output/max_prep_schedule.csv output/max_prep_School.csv

Like the other enrichment steps this is best-effort and idempotent: it writes atomically (tmp
file + ``os.replace``) so a crash or timeout can never leave a half-written schedule CSV, and
re-running just recomputes the two columns rather than appending them twice.

NOTE: unlike ``enrich_gofan_scrapy.py`` / ``enrich_nfhs.py``, these requests go to
**maxpreps.com**, so this uses the project's own polite, robots-obeying Scrapy settings rather
than the permissive ones used for arbitrary school homepages.
"""
import csv
import json
import os
import re
import sys

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Reuse the proven name normalizer rather than re-implementing it.
from enrich_gofan import normalize
from maxpreps_scraper.nextdata import page_props
from maxpreps_scraper.schoolinfo import school_row
from maxpreps_scraper.states import STATES

NAME_COLUMN = "original_opponent_school_name"
LOGO_COLUMN = "original_opponent_school_logo"

# "Boys"/"Girls" can sit between the school name and the sport in a breadcrumb.
_GENDER = r"(?:boys|girls|coed|co-ed)"
# A trailing "(Albany, CA)" on a <title>, e.g. "Saint Mary's High School (Albany, CA)".
_PAREN_TAIL = re.compile(r"\s*\([^)]*\)\s*$")
_WS = re.compile(r"\s+")

# Team levels. MaxPreps appends one as its own final crumb on a level-specific team page
# ("/football/freshman/" -> "... / Star Valley High School Football / Freshman"), and works it
# into that page's <title>. A bare level is never a school name, so it must never be written.
_LEVELS = frozenset({
    "varsity", "jv", "junior varsity", "freshman", "freshmen", "fresh",
    "sophomore", "b team", "c team", "middle school",
})
_LEVEL_TAIL = re.compile(
    r"\s*\b(?:varsity|jv|junior\s+varsity|freshm[ae]n|sophomore|[bc]\s+team)\b\s*$", re.I
)
# A crumb like "WY Football" names the state's sport hub, not a school.
_PLACE_WORDS = frozenset(
    [code.lower() for code in STATES] + [name.lower() for name in STATES.values()]
)


def _clean(text):
    return _WS.sub(" ", (text or "")).strip()


def _is_level(text):
    """True for a bare team-level label ("Freshman", "JV") -- never a school name."""
    return _clean(text).lower().strip(". ") in _LEVELS


def strip_level(text):
    """'Star Valley Braves Freshman' -> 'Star Valley Braves'."""
    text = _clean(text)
    return _clean(_LEVEL_TAIL.sub("", text)) or text


def strip_sport(crumb, sport):
    """'Saint Mary's High School Basketball' + 'Basketball' -> "Saint Mary's High School".

    The sport comes from the schedule row that produced this URL, so we strip the exact word
    rather than guessing at where the school name ends. An optional gender word in front of it
    ("Girls Basketball") is absorbed too. If the suffix isn't there the crumb is returned
    unchanged -- a school-root breadcrumb carries no sport at all.
    """
    text = _clean(crumb)
    if not text or not sport:
        return text
    pattern = re.compile(
        r"\s*(?:" + _GENDER + r")?\s*" + re.escape(_clean(sport)) + r"\s*$", re.I
    )
    return _clean(pattern.sub("", text)) or text


# --------------------------------------------------------------------------- #
# URL helpers
# --------------------------------------------------------------------------- #
def url_path_parts(url):
    """['ca', 'albany', 'saint-marys-panthers', ...] for a MaxPreps URL."""
    path = re.sub(r"^https?://[^/]+", "", (url or "").strip())
    return [p for p in path.split("/") if p]


def state_of(url):
    """Opponent's state code from its URL's first path segment ('CA'), or ''."""
    parts = url_path_parts(url)
    return parts[0].upper() if parts else ""


def is_school_url(url):
    """True for a real school/team URL, i.e. ``/{state}/{city}/{school}/...``.

    MaxPreps renders placeholder opponents ("N Non Varsity Opponent") as a link to
    ``/utility/about_pseudo_schools.aspx``. That isn't a school, so there is nothing worth
    fetching -- those rows keep the raw opponent text instead.
    """
    parts = url_path_parts(url)
    return len(parts) >= 3 and parts[0].lower() in STATES


# --------------------------------------------------------------------------- #
# Teams CSV index
# --------------------------------------------------------------------------- #
class NameIndex:
    """normalized teams ``name`` -> that row's ``original_name``.

    Two lookups: one keyed on (state, name) -- the safe one, used first -- and a state-less
    fallback for the rare opponent whose URL carries no usable state. The fallback deliberately
    drops any name claimed by two different schools, since without a state there's nothing left
    to tell them apart.
    """

    def __init__(self, by_state, by_name):
        self.by_state = by_state
        self.by_name = by_name

    @classmethod
    def from_csv(cls, teams_csv):
        by_state, by_name, ambiguous = {}, {}, set()
        with open(teams_csv, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                norm = normalize(row.get("name"))
                if not norm:
                    continue
                # original_name is written by the GoFan step; fall back to name when this CSV
                # hasn't been through it (e.g. running this script by hand on a raw crawl).
                resolved = (row.get("original_name") or row.get("name") or "").strip()
                if not resolved:
                    continue
                by_state.setdefault(((row.get("state") or "").strip().upper(), norm), resolved)
                if norm in by_name and by_name[norm] != resolved:
                    ambiguous.add(norm)
                by_name.setdefault(norm, resolved)
        for norm in ambiguous:
            by_name.pop(norm, None)
        return cls(by_state, by_name)

    def resolve(self, scraped_name, state):
        """The teams CSV's name for this school, or the scraped name when it isn't in there."""
        norm = normalize(scraped_name)
        if not norm:
            return scraped_name
        hit = self.by_state.get((state, norm))
        # The state-less index is ONLY for an opponent whose state we couldn't read. Falling
        # back to it when we do know the state would hand a same-named school in the wrong
        # state straight back, which is exactly what keying on state is meant to prevent.
        if hit is None and not state:
            hit = self.by_name.get(norm)
        return hit or scraped_name


# --------------------------------------------------------------------------- #
# Page extraction
# --------------------------------------------------------------------------- #
def breadcrumb_crumbs(response):
    """Every breadcrumb crumb, in order, or [].

    A level page yields
    ``['Football', 'WY Football', 'Star Valley High School Football', 'Freshman']``.

    MaxPreps' CSS class names are hashed, so anchor on things that don't churn: the schema.org
    BreadcrumbList first, then an accessible <nav aria-label="breadcrumb">, then any ordered
    list. The two DOM paths select ``li`` ELEMENTS and join each one's text separately --
    flattening every text node into one list would smear the crumbs together.
    """
    for blob in response.xpath('//script[@type="application/ld+json"]/text()').getall():
        try:
            data = json.loads(blob)
        except ValueError:
            continue
        for node in (data if isinstance(data, list) else [data]):
            if not isinstance(node, dict) or node.get("@type") != "BreadcrumbList":
                continue
            crumbs = []
            for el in node.get("itemListElement") or []:
                if not isinstance(el, dict):
                    continue
                item = el.get("item")
                name = el.get("name") or (item.get("name") if isinstance(item, dict) else None)
                if name:
                    crumbs.append(_clean(name))
            if crumbs:
                return crumbs

    for xp in (
        # aria-label casing varies ("Breadcrumb"/"breadcrumb"); fold it before comparing.
        '//nav[contains(translate(@aria-label, "BREADCUM", "breadcum"), "breadcrumb")]//li',
        '//ol[.//a]/li',
    ):
        crumbs = []
        for li in response.xpath(xp):
            text = _clean(" ".join(li.xpath(".//text()").getall()))
            if text and text != "/":
                crumbs.append(text)
        if crumbs:
            return crumbs
    return []


def school_from_crumbs(crumbs, sport):
    """The ``<School Name> <Sport>`` crumb, minus the sport, or "".

    Walks BACKWARDS and anchors on the sport rather than on position, because the school crumb
    is last on a varsity page but second-to-last on a level-specific one:

        varsity   ... / CA Basketball / Saint Mary's High School Basketball
        freshman  ... / WY Football   / Star Valley High School Football / Freshman

    Taking the last crumb outright would return "Freshman"/"JV". Anchoring on the sport handles
    both layouts and naturally skips the site's own "<Sport>" and "<State> <Sport>" hub crumbs,
    which strip down to nothing and to a state name respectively.
    """
    for crumb in reversed(crumbs):
        name = strip_sport(crumb, sport)
        if name == _clean(crumb):
            continue  # didn't end with the sport -> a level crumb like "Freshman"
        if not name or name.lower() in _PLACE_WORDS or _is_level(name):
            continue  # the bare "Football" / "WY Football" hub crumbs
        return name
    return ""


def title_name(response, sport=""):
    """"Saint Mary's High School (Albany, CA)" -> "Saint Mary's High School".

    Last resort, so it strips hard: a level page's title reads "Star Valley Braves Freshman
    Football (Afton, WY)" and must not come back as anything ending in "Freshman".
    """
    title = _clean(response.xpath("//title/text()").get())
    if not title:
        return ""
    return strip_level(strip_sport(_clean(_PAREN_TAIL.sub("", title)), sport))


def school_name(response, sport):
    """Best available school name for an opponent page, or "".

    Every source is guarded by ``_is_level`` so a bare level word can never be written as a
    school name -- if a source yields one, we fall through to the next rather than trust it.
    """
    name = school_from_crumbs(breadcrumb_crumbs(response), sport)
    if name:
        return name

    # Structured fallback: the Next.js blob the rest of this project reads.
    info = (page_props(response.text).get("schoolContext") or {}).get("schoolInfo") or {}
    for key in ("formattedNameWithoutState", "name", "formattedName"):
        value = _clean(_PAREN_TAIL.sub("", _clean(info.get(key))))
        if value and not _is_level(value):
            return value

    title = title_name(response, sport)
    return "" if _is_level(title) else title


def school_logo(response):
    """Best available school logo URL for an opponent page, or ""."""
    info = (page_props(response.text).get("schoolContext") or {}).get("schoolInfo") or {}
    mascot_url = _clean(info.get("mascotUrl"))
    if mascot_url:
        return response.urljoin(mascot_url)

    for xp in (
        '//img[contains(@src, "mascot")]/@src',
        '//img[contains(@src, "school")]/@src',
        '//meta[@property="og:image"]/@content',
    ):
        src = _clean(response.xpath(xp).get())
        if src:
            return response.urljoin(src)
    return ""


# --------------------------------------------------------------------------- #
# URL + CSV helpers
# --------------------------------------------------------------------------- #
_ORIGIN = re.compile(r"^(https?://[^/]+)")


def school_root_url(url):
    """Opponent TEAM url -> that school's ROOT url, or "".

    ``https://www.maxpreps.com/ca/albany/saint-marys-panthers/basketball/``
      -> ``https://www.maxpreps.com/ca/albany/saint-marys-panthers/``

    The root is the page ``spiders/maxpreps.py`` itself parses, so it is the one that
    reliably carries a full ``schoolContext.schoolInfo``. A team page may carry a reduced
    payload, which is why the harvest fetches roots rather than the link as-found.
    """
    origin = _ORIGIN.match((url or "").strip())
    parts = url_path_parts(url)
    if not origin or len(parts) < 3:
        return ""
    return f"{origin.group(1)}/{parts[0]}/{parts[1]}/{parts[2]}/"


def _norm_url(url):
    return (url or "").strip().rstrip("/").lower()


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader), list(reader.fieldnames or [])


def _write_csv(path, fieldnames, rows):
    """Atomic rewrite: a crash or timeout can never leave a half-written CSV."""
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", restval="")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def cache_path_for(schedule_csv):
    """Where the harvest parks what it learned, for ``resolve`` to reuse."""
    return os.path.join(os.path.dirname(os.path.abspath(schedule_csv)), "opponents.json")


def opponent_roots(schedule_rows):
    """Distinct school-root urls referenced as opponents, root -> a sport that used it.

    The sport is only needed for the breadcrumb-based name fallback. Placeholder opponents
    (``/utility/about_pseudo_schools.aspx``) and rows with no link are skipped by
    ``is_school_url`` -- there is no school behind them, so they keep their raw text.
    """
    roots = {}
    for row in schedule_rows:
        url = (row.get("opponent_url") or "").strip()
        if not url.lower().startswith(("http://", "https://")) or not is_school_url(url):
            continue
        root = school_root_url(url)
        if root:
            roots.setdefault(root, row.get("sport"))
    return roots


# --------------------------------------------------------------------------- #
# Phase A: harvest -- the only step that touches the network
# --------------------------------------------------------------------------- #
class HarvestSpider(scrapy.Spider):
    """Fetch opponent schools the crawl never reached and append them to the teams CSV.

    Only opponents ABSENT from the teams CSV are fetched. One whose root url is already a
    teams row needs no request at all -- its name and logo are read straight out of that
    row. That makes this pass strictly cheaper than the old one, which fetched every
    distinct opponent unconditionally.
    """

    name = "opponent_harvest"

    def __init__(self, schedule_csv=None, teams_csv=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.teams_csv = teams_csv
        self.cache_path = cache_path_for(schedule_csv)

        schedule_rows, _ = _read_csv(schedule_csv)
        self.teams_rows, self.teams_fields = _read_csv(teams_csv)

        self.known_ids = {(r.get("school_id") or "").strip()
                          for r in self.teams_rows if (r.get("school_id") or "").strip()}
        by_url = {_norm_url(r.get("url")): r for r in self.teams_rows if r.get("url")}

        # root url -> {school_id, name, logo}; consumed by resolve()
        self.cache = {}
        self.to_fetch = {}
        for root, sport in opponent_roots(schedule_rows).items():
            hit = by_url.get(_norm_url(root))
            if hit:                                   # already a teams row -> no request
                self.cache[root] = {
                    "school_id": (hit.get("school_id") or "").strip(),
                    "name": _clean(hit.get("name")),
                    "logo": _clean(hit.get("mascot_url")),
                }
            else:
                self.to_fetch[root] = sport

        self.added = 0

    def start_requests(self):
        for root, sport in self.to_fetch.items():
            yield scrapy.Request(
                root,
                callback=self.parse,
                errback=self.errback,
                cb_kwargs={"root": root, "sport": sport},
                dont_filter=True,
                meta={"download_timeout": 20},
            )

    def parse(self, response, root, sport):
        row = school_row(page_props(response.text), url=root,
                         discovered_via="opponent:schedule")
        if row:
            name, logo = _clean(row["name"]), _clean(row["mascot_url"])
            sid = (row["school_id"] or "").strip()
        else:
            # No schoolContext on the page. Still salvage a name/logo for the schedule
            # columns via the breadcrumb path, but there is no school row to append.
            name, logo, sid = school_name(response, sport), school_logo(response), ""

        self.cache[root] = {"school_id": sid, "name": name, "logo": logo}

        # dedup: school_id is authoritative, and guards against two different opponent
        # urls (slug changes, alternate spellings) resolving to the same school.
        if row and sid and sid not in self.known_ids:
            self.known_ids.add(sid)
            flat = dict(row)
            flat["sports"] = "; ".join(row["sports"])
            # phase 2 already copied name -> original_name for the crawled rows; do the
            # same here so the GoFan step downstream has a search value for this school.
            if "original_name" in self.teams_fields:
                flat["original_name"] = flat["name"]
            self.teams_rows.append(flat)
            self.added += 1

    def errback(self, failure):
        root = failure.request.cb_kwargs.get("root")
        self.logger.debug("harvest fetch failed root=%s: %r", root, failure.value)

    def closed(self, reason):
        if self.added:
            _write_csv(self.teams_csv, self.teams_fields, self.teams_rows)
        with open(self.cache_path, "w", encoding="utf-8") as fh:
            json.dump(self.cache, fh)
        self.logger.info(
            "harvest: %d distinct opponents (%d already in teams, %d fetched) -> "
            "added %d schools, teams CSV now %d rows",
            len(self.cache), len(self.cache) - len(self.to_fetch), len(self.to_fetch),
            self.added, len(self.teams_rows),
        )


def _settings():
    """The project's own polite settings -- these requests hit maxpreps.com."""
    s = get_project_settings()
    # Nothing here produces items; leaving the project pipeline in place would write the
    # canonical schools.csv / schedule.csv / maxpreps.db.
    s.set("ITEM_PIPELINES", {})
    # The project default is env-driven (4 h on Render) -- far too long for an enrichment pass.
    s.set("CLOSESPIDER_TIMEOUT", int(os.environ.get("OPPONENT_CLOSESPIDER_TIMEOUT", "1500")))
    return s


def harvest(schedule_csv, teams_csv):
    """Append missing opponent schools to the teams CSV and cache what was learned.

    Best-effort: never raises out to the caller. Both outputs are written only from a
    clean ``closed``, and the CSV rewrite is atomic, so a crash or timeout leaves the
    existing teams CSV exactly as it was.

    IMPORTANT: calls ``CrawlerProcess.start()`` (starts+stops the reactor), so run this
    AT MOST ONCE per process -- launch it in its own subprocess.
    """
    for path in (schedule_csv, teams_csv):
        if not os.path.exists(path):
            print(f"harvest: no such file: {path}", file=sys.stderr)
            return
    try:
        process = CrawlerProcess(_settings())
        process.crawl(HarvestSpider, schedule_csv=schedule_csv, teams_csv=teams_csv)
        process.start()
    except Exception as exc:  # noqa: BLE001 - enrichment must never fail the job
        print(f"harvest: failed, leaving CSVs unchanged: {exc!r}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# Phase B: resolve -- pure CSV work, no network
# --------------------------------------------------------------------------- #
def resolve(schedule_csv, teams_csv):
    """Write the two opponent columns into the schedule CSV.

    Runs AFTER the GoFan step, because the name a school is finally known by is the teams
    row's ``original_name``. Matching is by ``school_id`` where the harvest cached one --
    exact, rather than the normalised-name comparison ``NameIndex`` has to fall back on.

    Best-effort and idempotent: every row is pre-filled with the raw opponent text, so a
    missing cache or an unmatched opponent degrades to what the schedule itself said.
    """
    for path in (schedule_csv, teams_csv):
        if not os.path.exists(path):
            print(f"resolve: no such file: {path}", file=sys.stderr)
            return
    try:
        rows, fieldnames = _read_csv(schedule_csv)
        out_fields = fieldnames + [c for c in (NAME_COLUMN, LOGO_COLUMN)
                                   if c not in fieldnames]

        teams_rows, _ = _read_csv(teams_csv)
        # a school's final display name: GoFan-verified original_name, else the raw name
        by_id = {(r.get("school_id") or "").strip():
                 (_clean(r.get("original_name")) or _clean(r.get("name")))
                 for r in teams_rows if (r.get("school_id") or "").strip()}
        index = NameIndex.from_csv(teams_csv)   # fallback when there is no school_id

        cache = {}
        path = cache_path_for(schedule_csv)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                cache = json.load(fh)
        else:
            print(f"resolve: no {path}; falling back to raw opponent text", file=sys.stderr)

        named = with_logo = 0
        for row in rows:
            row[NAME_COLUMN] = _clean(row.get("opponent"))
            row[LOGO_COLUMN] = ""

            entry = cache.get(school_root_url((row.get("opponent_url") or "").strip()))
            if not entry:
                continue

            resolved = by_id.get(entry.get("school_id") or "")
            if not resolved and entry.get("name"):
                resolved = index.resolve(entry["name"], state_of(row.get("opponent_url")))
            if resolved:
                row[NAME_COLUMN] = resolved
                named += 1
            if entry.get("logo"):
                row[LOGO_COLUMN] = entry["logo"]
                with_logo += 1

        _write_csv(schedule_csv, out_fields, rows)
        print(f"resolve: wrote {len(rows)} rows -> {schedule_csv} "
              f"({len(cache)} cached opponents, {named} named, {with_logo} with a logo)")
    except Exception as exc:  # noqa: BLE001 - enrichment must never fail the job
        print(f"resolve: failed, leaving CSV unchanged: {exc!r}", file=sys.stderr)


def run_enrich(schedule_csv, teams_csv):
    """Both phases back to back -- the by-hand path documented in the README.

    Safe in one process: only ``harvest`` starts a reactor. In ``worker.py`` the two are
    split so GoFan/NFHS can run in between and cover the newly added schools.
    """
    harvest(schedule_csv, teams_csv)
    resolve(schedule_csv, teams_csv)


def main():
    argv = sys.argv[1:]
    phase = None
    if argv and argv[0] in ("harvest", "resolve"):
        phase, argv = argv[0], argv[1:]
    if len(argv) < 2:
        print("usage: python enrich_opponent.py [harvest|resolve] <schedule-csv> <teams-csv>",
              file=sys.stderr)
        raise SystemExit(2)
    {"harvest": harvest, "resolve": resolve, None: run_enrich}[phase](argv[0], argv[1])


if __name__ == "__main__":
    main()
