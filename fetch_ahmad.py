#!/usr/bin/env python
"""Standalone, self-contained crawl: SCHOOLS ONLY for DE, DC and AK -> ``ahmad.csv``.

This does NOT touch any existing code or output. It reuses the existing
``MaxPrepsSpider`` but swaps in its own one-file pipeline so that:

* only **schools** are fetched (schedules are turned off), and
* school rows are written to a brand-new CSV named ``ahmad.csv`` —
  the canonical ``schools.csv`` / ``schedule.csv`` / ``maxpreps.db`` are left alone.

Run it:

    python fetch_ahmad.py                       # -> output/ahmad.csv
    python fetch_ahmad.py path/to/ahmad.csv     # custom output path
"""
import csv
import os
import sys

from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from maxpreps_scraper.export import SCHOOL_FIELDS
from maxpreps_scraper.items import SchoolItem
from maxpreps_scraper.spiders.maxpreps import MaxPrepsSpider

# the three states to fetch (Delaware, District of Columbia, Alaska)
TARGET_STATES = "de,dc,ak"
LIST_FIELDS = {"sports"}  # lists -> "a; b; c" in the flat CSV (same as the main pipeline)


class AhmadSchoolsPipeline:
    """Writes only ``SchoolItem`` rows to a single CSV (ignores schedule games)."""

    def __init__(self, csv_path):
        self.csv_path = csv_path

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get("AHMAD_CSV", os.path.join("output", "ahmad.csv")))

    def open_spider(self, spider):
        os.makedirs(os.path.dirname(self.csv_path) or ".", exist_ok=True)
        self._fh = open(self.csv_path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=SCHOOL_FIELDS, extrasaction="ignore")
        self._writer.writeheader()
        self._count = 0

    def process_item(self, item, spider):
        if isinstance(item, SchoolItem):
            adapter = ItemAdapter(item)
            row = {}
            for f in SCHOOL_FIELDS:
                v = adapter.get(f)
                row[f] = "; ".join(v) if f in LIST_FIELDS and isinstance(v, list) else v
            self._writer.writerow(row)
            self._count += 1
        return item

    def close_spider(self, spider):
        self._fh.close()
        spider.logger.info("Wrote %d schools (%s) -> %s",
                            self._count, TARGET_STATES.upper(), self.csv_path)


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join("output", "ahmad.csv")

    settings = get_project_settings()
    # replace the project's multi-format pipeline with our schools-only CSV pipeline,
    # so nothing writes to schools.csv / schedule.csv / maxpreps.db.
    settings.set("ITEM_PIPELINES", {AhmadSchoolsPipeline: 300})
    settings.set("AHMAD_CSV", csv_path)

    process = CrawlerProcess(settings)
    # schedules=0 -> schools (+ their sports list) only, no schedule pages
    process.crawl(MaxPrepsSpider, states=TARGET_STATES, schedules="0")
    process.start()


if __name__ == "__main__":
    main()
