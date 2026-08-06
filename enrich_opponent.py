#!/usr/bin/env python
"""Add each game's opponent school name + logo to a schedule CSV, in place.

The schedule table only gives us the opponent's short display text ("Saint Mary's") and a
link to that opponent's team page. This step follows the link and turns it into two columns:

    original_opponent_school_name   the opponent's real school name, cross-referenced against
                                    the teams CSV so both files agree on one name
    original_opponent_school_logo   the opponent school's logo URL (written straight through)

How the name is resolved
------------------------
1. **Scrape** the opponent's team page and read the last breadcrumb crumb, minus the sport --
   "Saint Mary's High School Basketball" -> "Saint Mary's High School".
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

NAME_COLUMN = "original_opponent_school_name"
LOGO_COLUMN = "original_opponent_school_logo"

# "Boys"/"Girls" can sit between the school name and the sport in a breadcrumb.
_GENDER = r"(?:boys|girls|coed|co-ed)"
# A trailing "(Albany, CA)" on a <title>, e.g. "Saint Mary's High School (Albany, CA)".
_PAREN_TAIL = re.compile(r"\s*\([^)]*\)\s*$")
_WS = re.compile(r"\s+")


def _clean(text):
    return _WS.sub(" ", (text or "")).strip()


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
def breadcrumb_last(response):
    """The last breadcrumb crumb ("Saint Mary's High School Basketball"), or "".

    MaxPreps' CSS class names are hashed, so anchor on things that don't churn: the schema.org
    BreadcrumbList first, then an accessible <nav aria-label="breadcrumb">, then any ordered
    list's final item.
    """
    for blob in response.xpath('//script[@type="application/ld+json"]/text()').getall():
        try:
            data = json.loads(blob)
        except ValueError:
            continue
        for node in (data if isinstance(data, list) else [data]):
            if not isinstance(node, dict) or node.get("@type") != "BreadcrumbList":
                continue
            items = node.get("itemListElement") or []
            if not items:
                continue
            last = items[-1] if isinstance(items[-1], dict) else {}
            item = last.get("item")
            name = last.get("name") or (item.get("name") if isinstance(item, dict) else None)
            if name:
                return _clean(name)

    for xp in (
        # aria-label casing varies ("Breadcrumb"/"breadcrumb"); fold it before comparing.
        '//nav[contains(translate(@aria-label, "BREADCUM", "breadcum"), "breadcrumb")]'
        '//li[last()]//text()',
        '//nav[contains(translate(@aria-label, "BREADCUM", "breadcum"), "breadcrumb")]'
        '//text()',
        '//ol[.//a]/li[last()]//text()',
    ):
        parts = [_clean(t) for t in response.xpath(xp).getall()]
        parts = [p for p in parts if p and p != "/"]
        if parts:
            return parts[-1]
    return ""


def title_name(response):
    """"Saint Mary's High School (Albany, CA)" -> "Saint Mary's High School"."""
    title = _clean(response.xpath("//title/text()").get())
    return _clean(_PAREN_TAIL.sub("", title)) if title else ""


def school_name(response, sport):
    """Best available school name for an opponent page, or ""."""
    name = strip_sport(breadcrumb_last(response), sport)
    if name:
        return name

    # Structured fallback: the Next.js blob the rest of this project reads.
    info = (page_props(response.text).get("schoolContext") or {}).get("schoolInfo") or {}
    for key in ("formattedNameWithoutState", "name", "formattedName"):
        value = _clean(info.get(key))
        if value:
            return _clean(_PAREN_TAIL.sub("", value))

    return title_name(response)


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
# Spider
# --------------------------------------------------------------------------- #
class OpponentSpider(scrapy.Spider):
    name = "opponent_enrich"

    def __init__(self, csv_path=None, index=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.csv_path = csv_path
        self.index = index
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            self.rows = list(reader)
            self.fieldnames = list(reader.fieldnames or [])

        # Append each column exactly once (idempotent on re-run).
        extra = [c for c in (NAME_COLUMN, LOGO_COLUMN) if c not in self.fieldnames]
        self.out_fields = self.fieldnames + extra

        # Pre-fill EVERY row with its fallback so no row is ever left blank: rows whose
        # opponent has no link, or whose page fails to load, simply keep these values.
        for row in self.rows:
            row[NAME_COLUMN] = _clean(row.get("opponent"))
            row[LOGO_COLUMN] = ""

        # One fetch per distinct opponent URL, however many games reference it.
        self.by_url = {}
        for i, row in enumerate(self.rows):
            url = (row.get("opponent_url") or "").strip()
            if not url.lower().startswith(("http://", "https://")):
                continue
            entry = self.by_url.setdefault(url, {"sport": row.get("sport"), "rows": []})
            entry["rows"].append(i)

        self.scraped = 0

    def start_requests(self):
        for url, entry in self.by_url.items():
            yield scrapy.Request(
                url,
                callback=self.parse,
                errback=self.errback,
                cb_kwargs={"url": url, "sport": entry["sport"]},
                dont_filter=True,
                meta={"download_timeout": 20},
            )

    def parse(self, response, url, sport):
        name = school_name(response, sport)
        logo = school_logo(response)
        if not (name or logo):
            return  # nothing usable on the page -> every row keeps its fallback

        state = state_of(url)
        resolved = self.index.resolve(name, state) if name else None
        if name:
            self.scraped += 1
        for i in self.by_url[url]["rows"]:
            if resolved:
                self.rows[i][NAME_COLUMN] = resolved
            if logo:
                self.rows[i][LOGO_COLUMN] = logo

    def errback(self, failure):
        url = failure.request.cb_kwargs.get("url")
        # Rows already carry their fallbacks; nothing to undo.
        self.logger.debug("opponent fetch failed url=%s: %r", url, failure.value)

    def closed(self, reason):
        tmp = self.csv_path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=self.out_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(self.rows)
        os.replace(tmp, self.csv_path)
        with_logo = sum(1 for r in self.rows if (r.get(LOGO_COLUMN) or "").strip())
        self.logger.info(
            "opponent: wrote %d rows -> %s (%d unique opponent pages, %d named, "
            "%d rows with a logo)",
            len(self.rows), self.csv_path, len(self.by_url), self.scraped, with_logo,
        )


# --------------------------------------------------------------------------- #
def _settings():
    """The project's own polite settings -- these requests hit maxpreps.com."""
    s = get_project_settings()
    # Nothing here produces items; leaving the project pipeline in place would write the
    # canonical schools.csv / schedule.csv / maxpreps.db.
    s.set("ITEM_PIPELINES", {})
    # The project default is env-driven (4 h on Render) -- far too long for an enrichment pass.
    s.set("CLOSESPIDER_TIMEOUT", int(os.environ.get("OPPONENT_CLOSESPIDER_TIMEOUT", "1500")))
    return s


def run_enrich(schedule_csv, teams_csv):
    """Run one opponent enrichment crawl to completion (one Twisted reactor).

    Best-effort: never raises out to the caller. On any error the schedule CSV is left
    untouched (the atomic ``os.replace`` only runs after a clean ``closed``).

    IMPORTANT: calls ``CrawlerProcess.start()`` (starts+stops the reactor), so run this
    AT MOST ONCE per process -- launch it in its own subprocess.
    """
    for path in (schedule_csv, teams_csv):
        if not os.path.exists(path):
            print(f"opponent: no such file: {path}", file=sys.stderr)
            return
    try:
        index = NameIndex.from_csv(teams_csv)
        process = CrawlerProcess(_settings())
        process.crawl(OpponentSpider, csv_path=schedule_csv, index=index)
        process.start()
    except Exception as exc:  # noqa: BLE001 - enrichment must never fail the job
        print(f"opponent: failed, leaving CSV unchanged: {exc!r}", file=sys.stderr)


def main():
    if len(sys.argv) < 3:
        print("usage: python enrich_opponent.py <schedule-csv> <teams-csv>", file=sys.stderr)
        raise SystemExit(2)
    run_enrich(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
