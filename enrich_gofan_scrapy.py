#!/usr/bin/env python
"""Add each school's GoFan ticket link to a schools CSV, keyed on ``original_name``.

This is the Scrapy-based GoFan enrichment step. It runs AFTER ``worker.py`` has copied
the MaxPreps ``name`` column into ``original_name`` as the initial search value. For each
row it resolves the GoFan ticket link AND replaces ``original_name`` with the verified
GoFan school name, producing two updated columns:

    go_fan_ticket_url   https://gofan.co/app/school/{huddleId}   (or empty)
    original_name       the school's name as it appears on GoFan  (or the MaxPreps name
                        when no GoFan match / URL verification fails)

How the link is found
---------------------
GoFan has no usable server-side search endpoint, so a link is resolved in two steps:

1. **Match** ``original_name`` (= MaxPreps ``name``) against GoFan's local catalog. The
   match is gated on BOTH ``state`` and ``city`` so it mirrors the "City, ST" result line
   shown on GoFan's own search UI. This yields a ``huddleId`` AND the GoFan school name.
2. **Verify** ``https://gofan.co/app/school/{huddleId}`` with Scrapy (HTTP 200 check).
   On success both columns are written; on non-200 / error both keep their fallback values.

GoFan's school pages are React SPAs -- the school name is not in the HTML. Instead the
verified name comes from GoFan's own catalog (already downloaded locally), so no extra
network round-trip is needed.

    python enrich_gofan_scrapy.py output/max_prep_School.csv
    python enrich_gofan_scrapy.py output/max_prep_School.csv --refresh   # re-download catalog
"""
import csv
import os
import sys

import scrapy
from scrapy.crawler import CrawlerProcess

# Reuse the proven catalog + matching logic rather than re-implementing it.
from enrich_gofan import TICKET_URL, Matcher, build_index, load_catalog
# Reuse the Scrapy settings used for the website-name enrichment.
from enrich_website_name import _settings

NEW_COLUMN = "go_fan_ticket_url"
ORIGINAL_NAME_COLUMN = "original_name"


def candidate_huddle_id(matcher, row):
    """GoFan huddleId + catalog name for a row. Returns ``(id, source, gofan_name)``.

    Every match is state-scoped and city-verified. Attempt order:
      1. ``original_name`` (= MaxPreps name copied by worker.py)
      2. ``name`` fallback if original_name is blank

    ``gofan_name`` is the school's name as stored in GoFan's own catalog -- used to
    overwrite ``original_name`` after the URL is verified.
    """
    state = (row.get("state") or "").strip()
    original = (row.get("original_name") or "").strip()
    name = (row.get("name") or "").strip()

    # Attempt 1: original_name (skip if it equals name -- same lookup, avoid duplicate).
    if original and original != name:
        school_id, _kind = matcher.match({"name": original, "state": state,
                                          "city": row.get("city"), "zip_code": row.get("zip_code")})
        if school_id:
            return school_id, "original", matcher.id_to_name.get(school_id, "")

    # Attempt 2: name column.
    if name:
        school_id, _kind = matcher.match(row)
        if school_id:
            source = "original" if original and original == name else "name"
            return school_id, source, matcher.id_to_name.get(school_id, "")

    return None, None, ""


class GofanTicketSpider(scrapy.Spider):
    name = "gofan_ticket_enrich"

    def __init__(self, csv_path=None, matcher=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.csv_path = csv_path
        self.matcher = matcher
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            self.rows = list(reader)
            self.fieldnames = list(reader.fieldnames or [])

        # Ensure both output columns exist exactly once (idempotent on re-run).
        extra = [c for c in (ORIGINAL_NAME_COLUMN, NEW_COLUMN) if c not in self.fieldnames]
        self.out_fields = self.fieldnames + extra

        # Pre-fill go_fan_ticket_url to empty for every row; parse() overwrites on 200.
        # original_name is already populated (worker.py copied name → original_name before
        # launching this script), so we leave it intact here -- parse() overwrites it with
        # the GoFan catalog name only on a verified 200.
        for row in self.rows:
            row[NEW_COLUMN] = ""

        self.matched = 0          # rows that resolved to a candidate huddleId
        self.matched_original = 0  # ... via the original_name column
        self.matched_name = 0      # ... via the name fallback

    def start_requests(self):
        for i, row in enumerate(self.rows):
            school_id, source, gofan_name = candidate_huddle_id(self.matcher, row)
            if not school_id:
                continue  # no GoFan match -> both columns keep their fallback values
            self.matched += 1
            if source == "name":
                self.matched_name += 1
            else:
                self.matched_original += 1
            ticket_url = TICKET_URL.format(school_id)
            yield scrapy.Request(
                ticket_url,
                callback=self.parse,
                errback=self.errback,
                cb_kwargs={"idx": i, "url": ticket_url, "gofan_name": gofan_name},
                dont_filter=True,
                meta={"download_timeout": 15},
            )

    def parse(self, response, idx, url, gofan_name):
        if response.status == 200:
            self.rows[idx][NEW_COLUMN] = url
            # Replace original_name with GoFan's own school name (from the local catalog).
            # GoFan pages are React SPAs so the name isn't in the HTML; the catalog already
            # has it, so no extra HTTP request is needed.
            if gofan_name:
                self.rows[idx][ORIGINAL_NAME_COLUMN] = gofan_name
        # non-200 -> both columns keep their fallback values (name for original_name, "" for url)

    def errback(self, failure):
        idx = failure.request.cb_kwargs.get("idx")
        # Row already carries the empty fallback; nothing to do.
        self.logger.debug("gofan verify failed idx=%s: %r", idx, failure.value)

    def closed(self, reason):
        tmp = self.csv_path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=self.out_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(self.rows)
        os.replace(tmp, self.csv_path)
        verified = sum(1 for r in self.rows if (r.get(NEW_COLUMN) or "").strip())
        self.logger.info(
            "gofan: wrote %d rows -> %s (matched %d [orig %d, name-fallback %d], "
            "verified %d, empty %d)",
            len(self.rows), self.csv_path, self.matched, self.matched_original,
            self.matched_name, verified, len(self.rows) - verified,
        )


def run_enrich(csv_path, refresh=False):
    """Run one GoFan ticket-link enrichment crawl to completion (one Twisted reactor).

    Best-effort: never raises out to the caller. On any error the original CSV is left
    untouched (the atomic ``os.replace`` only runs after a clean ``closed``).

    IMPORTANT: calls ``CrawlerProcess.start()`` (starts+stops the reactor), so run this
    AT MOST ONCE per process -- launch it in its own subprocess.
    """
    if not os.path.exists(csv_path):
        print(f"gofan: no such file: {csv_path}", file=sys.stderr)
        return
    try:
        catalog = load_catalog(refresh=refresh)
        matcher = Matcher(build_index(catalog))
        process = CrawlerProcess(_settings())
        process.crawl(GofanTicketSpider, csv_path=csv_path, matcher=matcher)
        process.start()
    except Exception as exc:  # noqa: BLE001 - enrichment must never fail the job
        print(f"gofan: failed, leaving CSV unchanged: {exc!r}", file=sys.stderr)


def main():
    argv = [a for a in sys.argv[1:] if a != "--refresh"]
    refresh = "--refresh" in sys.argv[1:]
    if not argv:
        print("usage: python enrich_gofan_scrapy.py <csv-path> [--refresh]", file=sys.stderr)
        raise SystemExit(2)
    run_enrich(argv[0], refresh=refresh)


if __name__ == "__main__":
    main()
