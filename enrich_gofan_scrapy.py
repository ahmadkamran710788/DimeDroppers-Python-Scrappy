#!/usr/bin/env python
"""Add each school's GoFan ticket link to a schools CSV, keyed on ``original_name``.

This is the Scrapy-based GoFan enrichment step. It runs AFTER ``enrich_website_name.py``
has populated the ``original_name`` column (the school's name as it appears on its own
website). For each row it resolves the GoFan ticket link and appends one new column:

    go_fan_ticket_url   https://gofan.co/app/school/{huddleId}   (or empty)

How the link is found
---------------------
GoFan has no usable server-side search endpoint (``api.gofan.co/v2/schools?q=...`` ignores
the query and returns the full unfiltered catalog), so a link is resolved in two steps:

1. **Match** ``original_name`` (+ ``state``, with city/zip disambiguation) against GoFan's
   full catalog locally -- reusing the proven matcher from ``enrich_gofan.py``. This yields
   a GoFan ``huddleId``. (Falls back to the row's ``name`` if ``original_name`` is blank, so
   it's safe on CSVs that predate the ``original_name`` column.)
2. **Verify** the candidate ``https://gofan.co/app/school/{huddleId}`` with **Scrapy** -- one
   request per matched row. Only URLs that resolve (HTTP 200) are written; anything else
   leaves the column empty.

Like the rest of this project, each Scrapy run owns one Twisted reactor that cannot be
restarted -- so this MUST run in its own process:

    python enrich_gofan_scrapy.py output/max_prep_School.csv
    python enrich_gofan_scrapy.py output/max_prep_School.csv --refresh   # re-download catalog

It is idempotent (re-running just recomputes the column) and writes atomically (tmp file +
``os.replace``), so a crash or timeout never leaves a half-written CSV.
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


def candidate_huddle_id(matcher, row):
    """GoFan huddleId for a row, with a two-attempt match. Returns ``(id, source)``.

    Every match is state-scoped: ``Matcher.match`` only ever searches GoFan schools in the
    row's ``state`` column, so a candidate can never come from a different state.

    Attempt order:
      1. ``original_name`` -- match a copy of the row whose ``name`` is the scraped
         ``original_name`` (when it's non-blank).
      2. ``name`` fallback -- if attempt 1 found nothing (or ``original_name`` was blank),
         match the row on its real ``name`` column.

    ``source`` is ``"original"``, ``"name"``, or ``None`` (no match) -- used only for stats.
    """
    state = (row.get("state") or "").strip()
    original = (row.get("original_name") or "").strip()
    name = (row.get("name") or "").strip()

    # Attempt 1: original_name (only when it differs from name -- otherwise it's the same
    # lookup as the fallback, so skip the redundant call).
    if original and original != name:
        school_id, _kind = matcher.match({"name": original, "state": state,
                                          "city": row.get("city"), "zip_code": row.get("zip_code")})
        if school_id:
            return school_id, "original"

    # Attempt 2: fall back to the row's name column.
    if name:
        school_id, _kind = matcher.match(row)
        if school_id:
            # Distinguish "name happened to equal original_name" from a true fallback.
            return school_id, ("original" if original and original == name else "name")

    return None, None


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

        # Append the new column exactly once (idempotent on re-run).
        self.out_fields = self.fieldnames + (
            [] if NEW_COLUMN in self.fieldnames else [NEW_COLUMN]
        )
        # Pre-fill EVERY row to empty so no row is ever lost (no-match / errored / non-200
        # rows simply keep this value); ``parse`` overwrites only on a verified 200.
        for row in self.rows:
            row[NEW_COLUMN] = ""

        self.matched = 0          # rows that resolved to a candidate huddleId
        self.matched_original = 0  # ... via the original_name column
        self.matched_name = 0      # ... via the name fallback

    def start_requests(self):
        for i, row in enumerate(self.rows):
            school_id, source = candidate_huddle_id(self.matcher, row)
            if not school_id:
                continue  # no GoFan match (original_name AND name) -> column stays empty
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
                cb_kwargs={"idx": i, "url": ticket_url},
                dont_filter=True,
                meta={"download_timeout": 15},
            )

    def parse(self, response, idx, url):
        if response.status == 200:
            self.rows[idx][NEW_COLUMN] = url
        # non-200 -> keep the empty fallback

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
