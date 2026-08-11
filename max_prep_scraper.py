#!/usr/bin/env python
"""Pull MaxPreps schools + schedules for specific SPORTS in specific STATES.

A focused, standalone launcher around the existing ``MaxPrepsSpider``. You give it one or
more states and one or more sports; it crawls those states, writes EVERY school it finds,
and pulls schedules for just the requested sports.

The sport filter applies to SCHEDULES ONLY. It never narrows the school list, and it never
narrows which schools can be *discovered* either -- see DISCOVERY_SCHEDULES_PER_SCHOOL
below. Results go to two dedicated CSVs:

    output/max_prep_School.csv      one row per matching school
    output/max_prep_schedule.csv    one row per game (only the requested sports)

It does NOT touch the existing code or the canonical schools.csv / schedule.csv /
maxpreps.db -- it reuses the spider read-only and swaps in its own pipeline.

Examples:
    python max_prep_scraper.py ny Football
    python max_prep_scraper.py ny,ca,tx Football,Basketball
    python max_prep_scraper.py ca "Football,Flag Football" --levels all
    python max_prep_scraper.py ny Soccer --no-discover
"""
import argparse
import csv
import os

import scrapy
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from maxpreps_scraper.export import GAME_FIELDS, SCHOOL_FIELDS
from maxpreps_scraper.items import ScheduleGameItem, SchoolItem
from maxpreps_scraper.spiders.maxpreps import MaxPrepsSpider

LIST_FIELDS = {"sports"}  # lists -> "a; b; c" in the flat CSV (same as the main pipeline)

# How many EXTRA schedule pages may be fetched per school purely to discover new schools,
# when a sport filter is active. These emit no games.
#
# Why this exists: opponent links on schedule pages are one of the two vectors that reach
# past MaxPreps' 200-schools-per-state directory cap (nearbySchools is the other). Dropping
# off-sport schedule requests therefore used to shrink the set of schools that could ever be
# FOUND -- a school that doesn't play the requested sport never appears as an opponent on it,
# so it was reachable only via the <=200 directory seeds or a nearbySchools link. That is the
# "schools are getting missed when a sport is selected" bug.
#
# Bounded on purpose: a school offers many sport-seasons, so fetching them all would multiply
# the crawl's page count and, on a time-capped run (CLOSESPIDER_TIMEOUT), would find FEWER
# schools rather than more. Raise it with `-a discovery_schedules=N`, or "all" for an uncapped
# local/backfill run where wall-clock isn't the binding constraint.
DISCOVERY_SCHEDULES_PER_SCHOOL = 2

# Near-universal sports make the best discovery seeds -- their schedules touch the most
# distinct opponents. Preferred when choosing which off-sport schedules to spend the budget on.
CONNECTOR_SPORTS = ("basketball", "volleyball", "soccer", "baseball", "softball", "cross country")


# --------------------------------------------------------------------------- #
# Spider: existing crawler + a sport filter (sport names matched exactly,
# case-insensitively, so "Football" never picks up "Flag Football").
#
# The sport filter applies to SCHEDULES ONLY. It never narrows the school list, and
# never narrows which schools can be discovered (see DISCOVERY_SCHEDULES_PER_SCHOOL).
# --------------------------------------------------------------------------- #
class FilteredMaxPrepsSpider(MaxPrepsSpider):
    name = "maxpreps_filtered"

    def __init__(self, sports=None, discovery_schedules=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_sports = (
            None if not sports
            else {s.strip().lower() for s in str(sports).split(",") if s.strip()}
        )
        self.discovery_schedules = self._parse_budget(discovery_schedules)

    @staticmethod
    def _parse_budget(value):
        """Per-school discovery-fetch budget. ``None`` means unlimited ("all")."""
        if value in (None, ""):
            return DISCOVERY_SCHEDULES_PER_SCHOOL
        if str(value).strip().lower() == "all":
            return None
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return DISCOVERY_SCHEDULES_PER_SCHOOL

    def _season_matches(self, sport):
        return bool(self.target_sports) and (sport or "").strip().lower() in self.target_sports

    def parse_school(self, response, discovered_via):
        # Reuse the parent's logic entirely and just re-route what it yields:
        #   - keep EVERY school (the teams list is never narrowed by the sport filter),
        #   - yield schedule requests for the requested sports as-is (these produce games),
        #   - turn a bounded number of the REMAINING schedule requests into discovery-only
        #     fetches rather than dropping them, so school coverage stays independent of
        #     the sport filter,
        #   - let nearby-school discovery requests through untouched.
        off_sport = []
        for out in super().parse_school(response, discovered_via=discovered_via):
            if isinstance(out, scrapy.Request) and out.callback == self.parse_schedule:
                team = out.cb_kwargs.get("team") or {}
                if not self.target_sports or self._season_matches(team.get("sport")):
                    yield out
                else:
                    off_sport.append(out)
            else:
                yield out

        # Schedules for sports we weren't asked about: fetch a few anyway, purely to harvest
        # their opponent links. parse_schedule emits no games for these, so schedule.csv is
        # byte-for-byte unaffected -- only teams.csv grows.
        if not (self.discover and off_sport):
            return
        for req in self._discovery_picks(off_sport):
            yield req.replace(cb_kwargs={**req.cb_kwargs, "discovery_only": True})

    def _discovery_picks(self, requests):
        """The off-sport schedule requests worth spending the discovery budget on."""
        ordered = sorted(requests, key=self._connector_rank)
        if self.discovery_schedules is None:
            return ordered
        return ordered[:self.discovery_schedules]

    @staticmethod
    def _connector_rank(request):
        sport = ((request.cb_kwargs.get("team") or {}).get("sport") or "").strip().lower()
        return CONNECTOR_SPORTS.index(sport) if sport in CONNECTOR_SPORTS else len(CONNECTOR_SPORTS)


# --------------------------------------------------------------------------- #
# Pipeline: write ONLY these two CSVs (no SQLite / JSON, no canonical files).
# --------------------------------------------------------------------------- #
class MaxPrepTwoFilePipeline:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("OUTPUT_DIR", "output"))

    def open_spider(self, spider):
        os.makedirs(self.output_dir, exist_ok=True)
        self._files, self._writers = {}, {}
        for kind, fields, name in (
            ("school", SCHOOL_FIELDS, "max_prep_School"),
            ("game", GAME_FIELDS, "max_prep_schedule"),
        ):
            fh = open(os.path.join(self.output_dir, f"{name}.csv"), "w",
                      newline="", encoding="utf-8")
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            self._files[kind] = fh
            self._writers[kind] = writer
        self._counts = {"school": 0, "game": 0}

    def process_item(self, item, spider):
        if isinstance(item, SchoolItem):
            self._write("school", SCHOOL_FIELDS, item)
        elif isinstance(item, ScheduleGameItem):
            self._write("game", GAME_FIELDS, item)
        return item

    def _write(self, kind, fields, item):
        adapter = ItemAdapter(item)
        row = {}
        for f in fields:
            v = adapter.get(f)
            row[f] = "; ".join(v) if f in LIST_FIELDS and isinstance(v, list) else v
        self._writers[kind].writerow(row)
        self._counts[kind] += 1

    def close_spider(self, spider):
        for fh in self._files.values():
            fh.close()
        spider.logger.info(
            "Wrote %d schools and %d games -> %s/max_prep_School.csv, max_prep_schedule.csv",
            self._counts["school"], self._counts["game"], self.output_dir,
        )


def run_crawl(states, sports, levels="all", discover=True, output_dir=None):
    """Run exactly one sports-filtered crawl to completion (one Twisted reactor).

    Shared by the CLI (``main``) and the API worker (``worker.py``). Writes only the
    two CSVs via ``MaxPrepTwoFilePipeline`` -- never the canonical CSV/JSON/DB.

    IMPORTANT: this calls ``CrawlerProcess.start()``, which starts and then stops the
    Twisted reactor. A reactor cannot be restarted, so call this AT MOST ONCE per
    process -- run each crawl in its own subprocess (see worker.py / api.py).
    """
    settings = get_project_settings()
    # only our two-file pipeline runs -> nothing writes to the canonical CSV/JSON/DB
    settings.set("ITEM_PIPELINES", {MaxPrepTwoFilePipeline: 300})
    if output_dir:
        settings.set("OUTPUT_DIR", output_dir)
        # Disk-backed scheduler queue + dupefilter: a discovery crawl of a large state
        # is an unbounded graph walk whose pending-request queue would otherwise live
        # entirely in RAM and OOM the instance. JOBDIR spills it to disk. Each API job
        # gets a unique output_dir, so this dir is always fresh (no accidental resume).
        settings.set("JOBDIR", os.path.join(output_dir, ".crawljob"))

    process = CrawlerProcess(settings)
    process.crawl(
        FilteredMaxPrepsSpider,
        states=states,
        sports=sports,
        schedules="1",  # we want schedules
        discover="1" if discover else "0",
        levels=levels,
    )
    process.start()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("states", help="comma-separated state codes, e.g. ny or ny,ca,tx")
    parser.add_argument("sports", help='comma-separated sport names, e.g. Football '
                                       'or "Football,Basketball" (match MaxPreps labels)')
    parser.add_argument("--levels", default="all",
                        help='team levels to pull schedules for: "all" (default: Varsity + '
                             'JV + Freshman) or e.g. "Varsity" for varsity-only (~3x faster)')
    parser.add_argument("--no-discover", action="store_true",
                        help="disable the graph crawl that reaches past the 200/state cap")
    parser.add_argument("--output-dir", default=None, help="override output directory")
    args = parser.parse_args()

    run_crawl(
        states=args.states,
        sports=args.sports,
        levels=args.levels,
        discover=not args.no_discover,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
