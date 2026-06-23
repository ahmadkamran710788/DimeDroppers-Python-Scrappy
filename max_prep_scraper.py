#!/usr/bin/env python
"""Pull MaxPreps schools + schedules for specific SPORTS in specific STATES.

A focused, standalone launcher around the existing ``MaxPrepsSpider``. You give it
one or more states and one or more sports; it crawls those states, keeps only the
schools that offer at least one of the requested sports, and pulls just those sports'
schedules. Results go to two dedicated CSVs:

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


# --------------------------------------------------------------------------- #
# Spider: existing crawler + a sport filter (sport names matched exactly,
# case-insensitively, so "Football" never picks up "Flag Football").
# --------------------------------------------------------------------------- #
class FilteredMaxPrepsSpider(MaxPrepsSpider):
    name = "maxpreps_filtered"

    def __init__(self, sports=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_sports = (
            None if not sports
            else {s.strip().lower() for s in str(sports).split(",") if s.strip()}
        )

    def _season_matches(self, sport):
        return bool(self.target_sports) and (sport or "").strip().lower() in self.target_sports

    def _school_offers_target(self, item):
        # SchoolItem.sports looks like ["Football (Boys)", "Basketball (Girls)", ...]
        for label in item.get("sports") or []:
            if label.split(" (")[0].strip().lower() in self.target_sports:
                return True
        return False

    def parse_school(self, response, discovered_via):
        # Reuse the parent's logic entirely and just filter what it yields:
        #   - drop schools that don't offer any requested sport,
        #   - drop schedule requests for non-requested sports,
        #   - let discovery requests through so coverage still expands.
        for out in super().parse_school(response, discovered_via=discovered_via):
            if isinstance(out, SchoolItem):
                if self.target_sports and not self._school_offers_target(out):
                    continue
                yield out
            elif isinstance(out, scrapy.Request) and out.callback == self.parse_schedule:
                team = out.cb_kwargs.get("team") or {}
                if self.target_sports and not self._season_matches(team.get("sport")):
                    continue
                yield out
            else:
                yield out


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


def run_crawl(states, sports, levels="Varsity", discover=True, output_dir=None):
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
    parser.add_argument("--levels", default="Varsity",
                        help='team levels to pull schedules for: "Varsity" (default) or '
                             '"all" to include JV/Freshman')
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
