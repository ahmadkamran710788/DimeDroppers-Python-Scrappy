#!/usr/bin/env python
"""Fill in roster rows for the schools the crawl never visited.

Why this exists
---------------
``spiders/maxpreps.py``'s ``_maybe_follow_school`` gates every discovered school on the
requested states, so an opponent filed under another state is never crawled -- it has
games in the schedule CSV but no roster (and, until ``enrich_opponent.py harvest`` runs,
no teams row either). Harvest closes the teams-row gap; this closes the roster gap, using
the same shape: post-crawl, bounded, deterministic, one page per school we actually need.

It is deliberately keyed on "has no roster rows yet" rather than on
``discovered_via == "opponent:schedule"``. That covers the harvested opponents AND any
school whose roster fetch failed or was cut short during the crawl, so re-running the
step is self-healing instead of re-doing work it already did.

Best-effort, like every enrichment step: never raises out to the caller, and the CSV is
rewritten atomically only from a clean ``closed()``, so a crash or a SIGKILL on the
subprocess timeout leaves the existing roster CSV exactly as it was.

    python enrich_roster.py <roster-csv> <teams-csv> [sports] [levels]

    sports  comma-separated, e.g. "Football" -- empty means every sport
    levels  "all" (default) or e.g. "Varsity"

IMPORTANT: ``run_enrich`` calls ``CrawlerProcess.start()``, so run it AT MOST ONCE per
process -- launch it in its own subprocess (see worker.py).
"""
import csv
import os
import sys

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from maxpreps_scraper.export import ROSTER_FIELDS
from maxpreps_scraper.nextdata import page_props
from maxpreps_scraper.rosterinfo import roster_rows, team_subpage
from maxpreps_scraper.states import STATES


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


def is_school_url(url):
    """True for a real MaxPreps school URL, i.e. ``/{state}/{city}/{school}/``."""
    path = (url or "").strip()
    path = path.split("://", 1)[-1]
    parts = [p for p in path.split("/")[1:] if p]
    return len(parts) >= 3 and parts[0].lower() in STATES


def _parse_filter(value):
    """A comma-separated arg -> lowercased set, or ``None`` for "no filter"."""
    if not value or str(value).strip().lower() == "all":
        return None
    items = {v.strip().lower() for v in str(value).split(",") if v.strip()}
    return items or None


class RosterSpider(scrapy.Spider):
    """Fetch Roster + Staff tabs for teams CSV schools that have no roster rows yet."""

    name = "roster_enrich"

    def __init__(self, roster_csv=None, teams_csv=None, sports=None, levels=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.roster_csv = roster_csv
        self.target_sports = _parse_filter(sports)
        self.levels = _parse_filter(levels)

        self.rows, self.fieldnames = _read_csv(roster_csv)
        teams_rows, _ = _read_csv(teams_csv)

        covered = {(r.get("school_id") or "").strip()
                   for r in self.rows if (r.get("school_id") or "").strip()}
        # school root url -> the {school_id, name, state} block roster_rows() needs
        self.to_fetch = {}
        for row in teams_rows:
            sid = (row.get("school_id") or "").strip()
            url = (row.get("url") or "").strip()
            if not sid or sid in covered or not is_school_url(url):
                continue
            self.to_fetch[url] = {
                "school_id": sid,
                # the crawl's own roster rows carry the MaxPreps name, so use `name`
                # rather than `original_name` (which GoFan may later rewrite) to keep
                # school_name consistent across every roster row.
                "name": (row.get("name") or "").strip(),
                "state": (row.get("state") or "").strip(),
            }
            covered.add(sid)  # one school, one fetch, even with duplicate rows

        self.added = 0

    def start_requests(self):
        for url, school in self.to_fetch.items():
            yield scrapy.Request(
                url,
                callback=self.parse_school,
                errback=self.errback,
                cb_kwargs={"school": school},
                dont_filter=True,
                meta={"download_timeout": 20},
            )

    def parse_school(self, response, school):
        """Read the school's sport-seasons, then queue each one's Roster + Staff tab."""
        ctx = page_props(response.text).get("schoolContext") or {}
        for ss in ctx.get("sportSeasons") or []:
            if self.levels and (ss.get("level") or "").lower() not in self.levels:
                continue
            if self.target_sports and (ss.get("sport") or "").strip().lower() \
                    not in self.target_sports:
                continue
            team_url = ss.get("canonicalUrl")
            if not team_url:
                continue
            for category, page in (("player", "roster"), ("staff", "staff")):
                url = team_subpage(team_url, page)
                if url:
                    yield scrapy.Request(
                        url,
                        callback=self.parse_roster,
                        errback=self.errback,
                        cb_kwargs={"team": ss, "school": school, "category": category},
                        dont_filter=True,
                        meta={"download_timeout": 20},
                    )

    def parse_roster(self, response, team, school, category):
        # Same mapper the spider uses, so these rows are byte-compatible with crawled ones.
        for row in roster_rows(response, team, school, category):
            self.rows.append(row)
            self.added += 1

    def errback(self, failure):
        self.logger.debug("roster fetch failed: %r", failure.value)

    def closed(self, reason):
        if self.added:
            _write_csv(self.roster_csv, self.fieldnames or ROSTER_FIELDS, self.rows)
        self.logger.info(
            "roster enrich: %d schools without roster rows -> added %d rows, "
            "roster CSV now %d rows",
            len(self.to_fetch), self.added, len(self.rows),
        )


def _settings():
    """The project's own polite settings -- these requests hit maxpreps.com."""
    s = get_project_settings()
    # Nothing here produces items; leaving the project pipeline in place would write the
    # canonical schools.csv / schedule.csv / roster.csv / maxpreps.db.
    s.set("ITEM_PIPELINES", {})
    # The project default is env-driven (4 h on Render) -- far too long for an enrichment
    # pass, and it must stay UNDER worker.py's subprocess kill so this self-closes and
    # writes rather than being SIGKILLed mid-run.
    s.set("CLOSESPIDER_TIMEOUT", int(os.environ.get("ROSTER_CLOSESPIDER_TIMEOUT", "1000")))
    return s


def run_enrich(roster_csv, teams_csv, sports=None, levels=None):
    """Append roster rows for every teams-CSV school that has none (one reactor)."""
    for path in (roster_csv, teams_csv):
        if not os.path.exists(path):
            print(f"roster enrich: no such file: {path}", file=sys.stderr)
            return
    try:
        process = CrawlerProcess(_settings())
        process.crawl(RosterSpider, roster_csv=roster_csv, teams_csv=teams_csv,
                      sports=sports, levels=levels)
        process.start()
    except Exception as exc:  # noqa: BLE001 - enrichment must never fail the job
        print(f"roster enrich: failed, leaving CSV unchanged: {exc!r}", file=sys.stderr)


def main():
    argv = sys.argv[1:]
    if len(argv) < 2:
        print("usage: python enrich_roster.py <roster-csv> <teams-csv> [sports] [levels]",
              file=sys.stderr)
        raise SystemExit(2)
    run_enrich(argv[0], argv[1],
               sports=argv[2] if len(argv) > 2 else None,
               levels=argv[3] if len(argv) > 3 else None)


if __name__ == "__main__":
    main()
