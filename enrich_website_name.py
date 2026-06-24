#!/usr/bin/env python
"""Add each school's "original name" (scraped from its website) to a teams CSV, in place.

This is the post-crawl enrichment step. After ``max_prep_scraper.run_crawl`` writes
``max_prep_School.csv`` (the "teams" CSV), this opens each row's ``website`` URL with
Scrapy and tries to read the school/organization name off the page (og:site_name,
og:title, a cleaned <title>, or the first <h1>). The result is appended as one new
column:

    original_name   the scraped school name, or -- when nothing clean can be read --
                    a fallback to the row's existing ``name`` column.

So ``original_name`` is **never blank** (as long as ``name`` is set): it's either a
confident scraped name, or the MaxPreps ``name`` we already had. Many school sites are
behind bot walls / JS challenges / dead domains, so the fallback fires often and that's
expected and correct.

Like the rest of this project, each Scrapy run owns one Twisted reactor and that reactor
cannot be restarted -- so this MUST run in its own process (it's launched as a second
subprocess by ``worker.py`` after the crawl, and can also be run by hand):

    python enrich_website_name.py output/max_prep_School.csv

It is idempotent (re-running just recomputes the column) and writes atomically (tmp file
+ ``os.replace``), so a crash or timeout never leaves a half-written or empty teams CSV.
"""
import csv
import os
import re
import sys

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

NEW_COLUMN = "original_name"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_SEP = re.compile(r"\s*[|\-–—•»·:]\s*")        # title separators
_WS = re.compile(r"\s+")
_HOME = re.compile(r"^(home|welcome(\s+to)?)\b[\s:|\-–—]*", re.I)

# Lowercased contains-match: if any appears in a scraped name, the page is a
# bot-challenge / error / interstitial / parked-spam screen, not a school -> fall back.
JUNK_SUBSTRINGS = (
    "client challenge", "just a moment", "attention required",
    "checking your browser", "are you human", "access denied",
    "request blocked", "captcha", "cloudflare", "ddos",
    "page not found", "not found", "forbidden", "error",
    "service unavailable", "temporarily unavailable", "bad gateway",
    "gateway timeout", "internal server", "maintenance",
    "page cannot be displayed", "under construction", "domain for sale",
    "this site can", "account suspended",
    "poker", "casino", "slot", "togel", "judi", "betting", "viagra",
)


def _is_junk(name):
    low = (name or "").lower()
    return any(j in low for j in JUNK_SUBSTRINGS)


def clean_title(raw):
    """Turn a raw <title>/og:title into a best-guess organization name.

    Strips a leading "Home"/"Welcome to", splits on separators (| - : • ...) and keeps
    the longest segment -- school/district names tend to be the longest meaningful part,
    while slogans and section labels are short.
    """
    s = _WS.sub(" ", (raw or "")).strip()
    s = _HOME.sub("", s).strip()
    if not s:
        return ""
    segments = [seg.strip() for seg in _SEP.split(s) if seg.strip()]
    if not segments:
        return ""
    return max(segments, key=len)


def extract_raw_candidate(response):
    """First non-junk name candidate in priority order, cleaned but NOT name-validated.

    Keeps raw/doubtful school names so a successful scrape is never thrown away in favour
    of the ``name`` column, but skips bot-challenge / error / parked-spam screens via
    ``_is_junk``. Returns "" when the page yields nothing usable (-> ``name`` fallback).
    """
    for xp in (
        '//meta[@property="og:site_name"]/@content',
        '//meta[@property="og:title"]/@content',
        '//meta[@name="og:site_name"]/@content',
        '//meta[@name="og:title"]/@content',
    ):
        val = response.xpath(xp).get()
        cleaned = clean_title(val)
        if cleaned and not _is_junk(cleaned):
            return cleaned

    cleaned = clean_title(response.xpath("//title/text()").get())
    if cleaned and not _is_junk(cleaned):
        return cleaned

    h1 = (response.xpath("//h1//text()").get() or "").strip()
    if h1 and not _is_junk(h1):
        return h1

    return ""


class WebsiteNameSpider(scrapy.Spider):
    name = "website_name_enrich"

    def __init__(self, csv_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.csv_path = csv_path
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            self.rows = list(reader)
            self.fieldnames = list(reader.fieldnames or [])

        # Append the new column exactly once (idempotent on re-run).
        self.out_fields = self.fieldnames + (
            [] if NEW_COLUMN in self.fieldnames else [NEW_COLUMN]
        )
        # Pre-fill EVERY row with the fallback so no row is ever lost (errored or
        # blank-URL rows simply keep this value); ``parse`` overwrites on success.
        for i, row in enumerate(self.rows):
            row[NEW_COLUMN] = (row.get("name") or "").strip()
            row["_idx"] = i  # scratch key; dropped on write via extrasaction="ignore"

    def start_requests(self):
        for i, row in enumerate(self.rows):
            url = (row.get("website") or "").strip()
            if not url.lower().startswith(("http://", "https://")):
                continue  # blank / non-http -> fallback already set
            yield scrapy.Request(
                url,
                callback=self.parse,
                errback=self.errback,
                cb_kwargs={"idx": i},
                dont_filter=True,
                meta={"download_timeout": 15},
            )

    def parse(self, response, idx):
        ctype = (response.headers.get("Content-Type") or b"").decode("latin-1").lower()
        if "html" not in ctype and "xml" not in ctype:
            return  # binary/PDF/etc. -> keep fallback
        raw = extract_raw_candidate(response)
        if raw:
            self.rows[idx][NEW_COLUMN] = raw  # site opened & scraped -> use raw value
        # else: nothing extractable -> keep the name-column fallback set in __init__

    def errback(self, failure):
        idx = failure.request.cb_kwargs.get("idx")
        # Row already carries the fallback from __init__; nothing to do.
        self.logger.debug("website fetch failed idx=%s: %r", idx, failure.value)

    def closed(self, reason):
        tmp = self.csv_path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=self.out_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(self.rows)
        os.replace(tmp, self.csv_path)
        scraped = sum(
            1 for r in self.rows
            if (r.get(NEW_COLUMN) or "").strip() != (r.get("name") or "").strip()
        )
        self.logger.info(
            "enrich: wrote %d rows -> %s (scraped %d, fallback %d)",
            len(self.rows), self.csv_path, scraped, len(self.rows) - scraped,
        )


def _settings():
    s = Settings()
    s.setdict({
        "ROBOTSTXT_OBEY": False,        # arbitrary public school homepages; robots
                                        #   blocks would needlessly force the fallback
        "USER_AGENT": UA,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.25,
        "DOWNLOAD_TIMEOUT": 15,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 1,               # best-effort: don't burn time on dead hosts
        "REDIRECT_ENABLED": True,       # follow http->https / www redirects
        "HTTPERROR_ALLOW_ALL": True,    # 4xx/5xx still reach parse -> fallback, not drop
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "COOKIES_ENABLED": True,        # some sites set a cookie then serve real HTML
        "TELNETCONSOLE_ENABLED": False,  # errors continuously on Render's network
        "EXTENSIONS": {"scrapy.extensions.telnet.TelnetConsole": None},
        "LOG_LEVEL": "INFO",
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    })
    return s


def run_enrich(csv_path):
    """Run one website-name enrichment crawl to completion (one Twisted reactor).

    Best-effort: never raises out to the caller. On any error the original CSV is left
    untouched (the atomic ``os.replace`` only runs after a clean ``closed``), so the
    teams CSV is always still downloadable.

    IMPORTANT: calls ``CrawlerProcess.start()`` (starts+stops the reactor), so run this
    AT MOST ONCE per process -- launch it in its own subprocess.
    """
    if not os.path.exists(csv_path):
        print(f"enrich: no such file: {csv_path}", file=sys.stderr)
        return
    try:
        process = CrawlerProcess(_settings())
        process.crawl(WebsiteNameSpider, csv_path=csv_path)
        process.start()
    except Exception as exc:  # noqa: BLE001 - enrichment must never fail the job
        print(f"enrich: failed, leaving CSV unchanged: {exc!r}", file=sys.stderr)


def main():
    if len(sys.argv) < 2:
        print("usage: python enrich_website_name.py <csv-path>", file=sys.stderr)
        raise SystemExit(2)
    run_enrich(sys.argv[1])


if __name__ == "__main__":
    main()
