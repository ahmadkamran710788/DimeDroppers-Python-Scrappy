#!/usr/bin/env python
"""MaxPreps crawl seeded from an uploaded CSV of school links.

Where ``max_prep_scraper.py`` finds schools by crawling states, this crawls exactly the
schools in a CSV the user supplies (only its ``maxPrepsLink`` column is read). For each
school and each selected sport it fetches every team listed on the school page -- every
level (Varsity / JV / Freshman) and gender (Boys / Girls), current season -- and that
team's Schedule, Roster and Staff tabs. Opponent links are **not** followed; opponent
schools get their teams rows afterwards from the existing harvest step (``worker.py``
phase 2.5, run by ``csv_worker.py``).

It reuses the existing spider wholesale (``FilteredMaxPrepsSpider`` -> ``MaxPrepsSpider``)
and the shared row mappers, so rows are byte-compatible with every other path. What it
adds is what a long, list-driven run needs:

* **Resume-safe storage.** Rows go to SQLite (``INSERT OR REPLACE`` on the canonical
  PKs) instead of the API pipeline's ``"w"``-mode CSVs, and the scheduler queue lives in
  ``JOBDIR`` -- so an interrupted run continues where it stopped, with no duplicates.
  ``csv_worker.py`` exports the three API-named CSVs from the DB at the end.
* **Coverage.** Every page it is responsible for -- each uploaded school and each team's
  schedule / roster / staff page -- gets a row in a ``coverage`` table:
  ``pending`` when requested, then ``ok`` / ``empty`` / ``failed`` / ``no_data``.
  Nothing can be silently missing: a page that never resolved is still ``pending``.
* **Retry mode.** ``retry`` re-requests every ``pending`` and retryable ``failed`` row
  (fresh dupefilter, no JOBDIR) -- the second chance that makes "no schedule missed" real.
* **Every schedule table.** The base spider takes ``//table[...][1]`` (ARCHITECTURE
  §9.3); this parses every qualifying table on the page with one running ``game_index``
  and skips a table only when it is an exact duplicate render of one already parsed.

Usage (normally launched by ``csv_worker.py``, one mode per process):

    python csv_seed_scraper.py crawl <out_dir> <input.csv> <sports>
    python csv_seed_scraper.py retry <out_dir> <input.csv> <sports>

``sports`` is a comma list matched exactly against MaxPreps' labels ("Basketball"
covers both Boys and Girls), or ``all`` for every sport.
"""
import csv
import fcntl
import hashlib
import json
import os
import sqlite3
import sys
import time
from urllib.parse import urlsplit

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import IgnoreRequest
from scrapy.selector import SelectorList
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.project import get_project_settings

from max_prep_scraper import FilteredMaxPrepsSpider
from maxpreps_scraper.export import GAME_FIELDS, ROSTER_FIELDS, SCHOOL_FIELDS
from maxpreps_scraper.items import RosterItem, ScheduleGameItem, SchoolItem
from maxpreps_scraper.nextdata import page_props
from maxpreps_scraper.pipelines import LIST_FIELDS, MultiFormatPipeline
from maxpreps_scraper.rosterinfo import roster_rows
from maxpreps_scraper.states import STATES

DB_FILE = "maxpreps.db"
BLOCK_MARKER = "blocked.json"
LINK_COLUMN = "maxpreps link"  # matched case/space-insensitively: "maxPrepsLink" etc.

# Same anchoring as the base spider (header text, never CSS classes) -- but ALL matches.
SCHEDULE_TABLE_XPATH = '//table[.//th[contains(., "Opponent")] and .//th[contains(., "Date")]]'

# MaxPreps' own internal test schools appear in real exports ("MaxPreps A", "Metcalfe"
# in city "Max"). Their schedules are fixtures, not games.
TEST_CITY_SLUGS = {"max", "maxhq4"}
TEST_SCHOOL_PREFIX = "maxpreps-"

# Statuses that end a page's story. "pending" means requested, not yet resolved.
FINAL_OK = ("ok", "empty")
COVERAGE_FIELDS = ["kind", "status", "detail", "retryable", "url", "school_id",
                   "school_name", "state", "sport", "gender", "level", "season",
                   "updated_at"]
TEAM_KEYS = ("sport", "gender", "level", "season", "year", "canonicalUrl")


# --------------------------------------------------------------------------- #
# Input CSV
# --------------------------------------------------------------------------- #
def _norm_header(name):
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def normalize_link(raw):
    """A school link -> its canonical school-root URL, or ``(None, reason)``.

    Accepts any maxpreps.com URL under a school (a team or schedule link is trimmed back
    to ``/{state}/{city}/{school}/``). Rejects anything that isn't one.
    """
    raw = (raw or "").strip()
    if not raw:
        return None, "blank link"
    if "://" not in raw:
        raw = "https://" + raw.lstrip("/")
    parts = urlsplit(raw)
    host = (parts.hostname or "").lower()
    if host not in ("maxpreps.com", "www.maxpreps.com"):
        return None, "not a maxpreps.com link"
    segs = [s for s in parts.path.split("/") if s]
    if len(segs) < 3:
        return None, "not a school link (need /state/city/school/)"
    state, city, school = (s.lower() for s in segs[:3])
    if state not in STATES:
        return None, f"unknown state '{state}'"
    if city in TEST_CITY_SLUGS or school.startswith(TEST_SCHOOL_PREFIX):
        return None, "MaxPreps internal test school"
    return f"https://www.maxpreps.com/{state}/{city}/{school}/", None


def load_links(input_csv):
    """Read the uploaded CSV's link column.

    Returns ``{"column", "rows", "links", "skipped"}``: ``links`` are distinct canonical
    school URLs in file order; ``skipped`` lists ``(line_no, raw, reason)`` for rows that
    were not usable (blank, foreign, malformed, test school, duplicate).
    Raises ``ValueError`` if the CSV has no link column.
    """
    with open(input_csv, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        column = next((c for c in (reader.fieldnames or [])
                       if _norm_header(c) == _norm_header(LINK_COLUMN)), None)
        if column is None:
            raise ValueError("CSV must have a maxPrepsLink column")
        links, seen, skipped, rows = [], set(), [], 0
        for line_no, row in enumerate(reader, start=2):
            rows += 1
            raw = row.get(column) or ""
            url, reason = normalize_link(raw)
            if url is None:
                skipped.append((line_no, raw.strip(), reason))
            elif url in seen:
                skipped.append((line_no, raw.strip(), "duplicate link"))
            else:
                seen.add(url)
                links.append(url)
    return {"column": column, "rows": rows, "links": links, "skipped": skipped}


# --------------------------------------------------------------------------- #
# Coverage table
# --------------------------------------------------------------------------- #
class CoverageItem(scrapy.Item):
    """One page this crawl is responsible for, and what became of it."""
    kind = scrapy.Field()        # school | schedule | roster | staff | input
    url = scrapy.Field()         # the URL as requested (the key; redirects don't change it)
    status = scrapy.Field()      # pending | ok | empty | failed | no_data | skipped
    detail = scrapy.Field()      # "12 games", "http 404", "TimeoutError: ...", ...
    retryable = scrapy.Field()   # 1 if a later attempt could plausibly succeed
    school_id = scrapy.Field()
    school_name = scrapy.Field()
    state = scrapy.Field()
    sport = scrapy.Field()
    gender = scrapy.Field()
    level = scrapy.Field()
    season = scrapy.Field()
    context = scrapy.Field()     # JSON needed to rebuild the request (retry mode)


_COVERAGE_COLUMNS = ["url", "kind", "status", "detail", "retryable", "school_id",
                     "school_name", "state", "sport", "gender", "level", "season",
                     "context", "updated_at"]


def connect_db(path):
    """SQLite connection tuned for one writer (the crawl) + concurrent readers (the
    worker's progress poll, the API's mid-run snapshot)."""
    db = sqlite3.connect(path, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def ensure_coverage_table(db):
    db.execute(
        "CREATE TABLE IF NOT EXISTS coverage ("
        + ", ".join(f'"{c}" {"REAL" if c == "updated_at" else "TEXT"}' for c in _COVERAGE_COLUMNS)
        + ', PRIMARY KEY ("url"))'
    )
    db.commit()


def write_coverage(db, row):
    """Record one page's status. ``pending`` never overwrites anything (it can arrive
    after the page already resolved, e.g. on resume); every other status replaces."""
    values = dict(row)
    values["updated_at"] = time.time()
    verb = "INSERT OR IGNORE" if values.get("status") == "pending" else "INSERT OR REPLACE"
    db.execute(
        f'{verb} INTO coverage ({",".join(chr(34) + c + chr(34) for c in _COVERAGE_COLUMNS)}) '
        f'VALUES ({",".join("?" for _ in _COVERAGE_COLUMNS)})',
        [values.get(c, "") if c != "updated_at" else values["updated_at"]
         for c in _COVERAGE_COLUMNS],
    )


def init_job_db(db_path):
    """Open (creating if needed) a job DB with the canonical tables + coverage.

    Reuses ``MultiFormatPipeline._init_db`` so the schema -- PKs and column migrations --
    is the canonical one, not a copy. Returns the open connection.
    """
    pipeline = CsvSeedPipeline(os.path.dirname(db_path) or ".", os.path.basename(db_path))
    pipeline._db = connect_db(db_path)
    pipeline._init_db()
    ensure_coverage_table(pipeline._db)
    return pipeline._db


# Order coverage.csv the way a person reads it: each school, then its teams' pages.
_KIND_ORDER = "CASE kind WHEN 'input' THEN 0 WHEN 'school' THEN 1 WHEN 'schedule' THEN 2 " \
              "WHEN 'roster' THEN 3 WHEN 'staff' THEN 4 ELSE 5 END"


def export_coverage(db_path, csv_path):
    """Write the coverage table to ``csv_path`` (atomic). Returns the row count."""
    rows = _read_rows(
        db_path,
        f"SELECT {','.join(COVERAGE_FIELDS)} FROM coverage "
        f"ORDER BY {_KIND_ORDER}, school_name, sport, gender, level, url",
    )
    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COVERAGE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            row["updated_at"] = (time.strftime("%Y-%m-%d %H:%M:%S",
                                               time.gmtime(float(row["updated_at"])))
                                 if row.get("updated_at") else "")
            writer.writerow(row)
    os.replace(tmp, csv_path)
    return len(rows)


def _read_rows(db_path, sql, params=()):
    if not db_path or not os.path.exists(db_path):
        return []
    db = sqlite3.connect(db_path, timeout=60)
    try:
        db.row_factory = sqlite3.Row
        return [dict(r) for r in db.execute(sql, params)]
    except sqlite3.OperationalError:  # no coverage table yet
        return []
    finally:
        db.close()


def load_done_urls(db_path):
    """URLs already resolved ok/empty -- never worth fetching again."""
    rows = _read_rows(db_path, "SELECT url FROM coverage WHERE status IN (?, ?)", FINAL_OK)
    return {r["url"] for r in rows}


def load_retry_rows(db_path):
    """Everything that still needs an attempt: unresolved, or failed-but-retryable."""
    return _read_rows(
        db_path,
        "SELECT * FROM coverage WHERE kind != 'input' AND "
        "(status = 'pending' OR (status = 'failed' AND retryable = '1'))",
    )


def _team_cov(kind, url, team, school, category=None):
    return {
        "kind": kind,
        "url": url,
        "school_id": school.get("school_id") or "",
        "school_name": school.get("name") or "",
        "state": school.get("state") or "",
        "sport": team.get("sport") or "",
        "gender": team.get("gender") or "",
        "level": team.get("level") or "",
        "season": f"{team.get('season', '')} {team.get('year', '')}".strip(),
        "context": json.dumps({
            "team": {k: team.get(k) for k in TEAM_KEYS},
            "school": {k: school.get(k) for k in ("school_id", "name", "state")},
            "category": category,
        }),
    }


def _has_next_data(response):
    # Every real MaxPreps page is a Next.js page. A 200 without the blob is an error /
    # interstitial page, and must count as a failure, never as "empty".
    return 'id="__NEXT_DATA__"' in response.text


def _table_signature(table):
    text = " ".join(t.strip() for t in table.xpath(".//text()").getall() if t.strip())
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Spider
# --------------------------------------------------------------------------- #
class CsvSeedMaxPrepsSpider(FilteredMaxPrepsSpider):
    name = "maxpreps_csv_seed"

    def __init__(self, input_csv=None, sports=None, mode="crawl", db_path=None, **kwargs):
        sports = (sports or "").strip()
        if not sports:
            raise ValueError("sports is required (e.g. 'Basketball', or 'all')")
        super().__init__(
            sports=None if sports.lower() == "all" else sports,
            discovery_schedules="0",
            states=None,            # unused: start_requests() is overridden
            schedules="1",
            discover="0",           # no nearby schools, no opponent following
            levels="all",           # Varsity + JV + Freshman
            rosters="1",            # Roster + Staff tabs for every team
            **kwargs,
        )
        if mode not in ("crawl", "retry"):
            raise ValueError(f"unknown mode {mode!r}")
        self.input_csv = input_csv
        self.mode = mode
        self.db_path = db_path
        # Resolved pages from earlier runs of this job: never re-fetched.
        self._done_urls = load_done_urls(db_path)

    # -- seeding ---------------------------------------------------------------- #
    def start_requests(self):
        if self.mode == "retry":
            yield from self._retry_requests()
            return
        for url in load_links(self.input_csv)["links"]:
            if url not in self._done_urls:
                yield self._school_seed(url, "input")

    def _school_seed(self, url, discovered_via, dont_filter=False):
        return scrapy.Request(
            url,
            callback=self.parse_school,
            errback=self.on_error,
            cb_kwargs={"discovered_via": discovered_via},
            meta={"cov": {"kind": "school", "url": url,
                          "context": json.dumps({"discovered_via": discovered_via})}},
            dont_filter=dont_filter,
        )

    def _retry_requests(self):
        rows = load_retry_rows(self.db_path)
        self.logger.info("retry: %d pages to re-request", len(rows))
        for row in rows:
            ctx = json.loads(row.get("context") or "{}")
            kind, url = row["kind"], row["url"]
            if kind == "school":
                yield self._school_seed(url, ctx.get("discovered_via") or "input",
                                        dont_filter=True)
                continue
            team, school = ctx.get("team") or {}, ctx.get("school") or {}
            cov = {k: row.get(k) for k in ("kind", "url", "school_id", "school_name", "state",
                                           "sport", "gender", "level", "season", "context")}
            if kind == "schedule":
                callback, kwargs = self.parse_schedule, {"team": team, "school": school}
            elif kind in ("roster", "staff"):
                callback = self.parse_roster
                kwargs = {"team": team, "school": school,
                          "category": "player" if kind == "roster" else "staff"}
            else:
                continue
            yield scrapy.Request(url, callback=callback, errback=self.on_error,
                                 cb_kwargs=kwargs, meta={"cov": cov}, dont_filter=True)

    # -- coverage helpers ------------------------------------------------------- #
    @staticmethod
    def _cov(cov, status, detail="", retryable=0, **extra):
        return CoverageItem(**{**cov, **extra, "status": status, "detail": detail,
                               "retryable": str(int(bool(retryable)))})

    def _track(self, request):
        """Tag a team-page request for coverage. ``None`` if it's already resolved."""
        team = request.cb_kwargs.get("team") or {}
        school = request.cb_kwargs.get("school") or {}
        if request.callback == self.parse_schedule:
            kind, category = "schedule", None
        else:
            category = request.cb_kwargs.get("category")
            kind = "roster" if category == "player" else "staff"
        if request.url in self._done_urls:
            return None
        cov = _team_cov(kind, request.url, team, school, category)
        tracked = request.replace(errback=self.on_error, meta={**request.meta, "cov": cov})
        return tracked, self._cov(cov, "pending", retryable=1)

    # -- callbacks -------------------------------------------------------------- #
    def parse_school(self, response, discovered_via):
        cov = response.meta.get("cov") or {"kind": "school", "url": response.url}
        if not _has_next_data(response):
            yield self._cov(cov, "failed", "unexpected page (no __NEXT_DATA__)", retryable=1)
            return
        ctx = page_props(response.text).get("schoolContext") or {}
        info = ctx.get("schoolInfo") or {}
        school_id = ctx.get("schoolId") or info.get("schoolId")
        if not school_id:
            yield self._cov(cov, "no_data", "no school data on this page")
            return

        teams = 0
        for out in super().parse_school(response, discovered_via=discovered_via):
            if not isinstance(out, scrapy.Request):
                yield out
                continue
            if out.callback == self.parse_schedule:
                teams += 1
            tracked = self._track(out)
            if tracked:
                request, pending = tracked
                yield pending
                yield request

        identity = {"school_id": school_id,
                    "school_name": info.get("name") or info.get("formattedName") or "",
                    "state": info.get("stateCode") or info.get("state") or ""}
        if teams:
            yield self._cov(cov, "ok", f"{teams} team(s) for selected sport(s)", **identity)
        else:
            yield self._cov(cov, "empty", "no team for the selected sport(s)", **identity)

    def parse_schedule(self, response, team, school, discovery_only=False):
        cov = response.meta.get("cov") or _team_cov("schedule", response.url, team, school)
        if not _has_next_data(response):
            yield self._cov(cov, "failed", "unexpected page (no __NEXT_DATA__)", retryable=1)
            return
        tables = response.xpath(SCHEDULE_TABLE_XPATH)
        kept, signatures = [], set()
        for table in tables:
            sig = _table_signature(table)
            if sig not in signatures:  # an exact duplicate render (e.g. mobile + desktop)
                signatures.add(sig)
                kept.append(table)

        games = 0
        for item in self._parse_games(response, SelectorList(kept), team, school):
            games += 1
            yield item
        # Opponent links are deliberately NOT followed (discover=0): opponents get their
        # teams rows from the harvest step after the crawl.
        if not tables:
            yield self._cov(cov, "empty", "no schedule table on page")
        elif games:
            detail = f"{games} games" + (f" from {len(kept)} tables" if len(kept) > 1 else "")
            yield self._cov(cov, "ok", detail)
        else:
            yield self._cov(cov, "empty", "0 games")

    def parse_roster(self, response, team, school, category):
        kind = "roster" if category == "player" else "staff"
        cov = response.meta.get("cov") or _team_cov(kind, response.url, team, school, category)
        if not _has_next_data(response):
            yield self._cov(cov, "failed", "unexpected page (no __NEXT_DATA__)", retryable=1)
            return
        rows = roster_rows(response, team, school, category)
        for row in rows:
            yield RosterItem(**row)
        if rows:
            yield self._cov(cov, "ok", f"{len(rows)} {'players' if kind == 'roster' else 'staff'}")
        else:
            yield self._cov(cov, "empty", f"no {kind} listed")

    def on_error(self, failure):
        request = failure.request
        cov = request.meta.get("cov") or {"kind": "unknown", "url": request.url}
        if failure.check(HttpError):
            status = failure.value.response.status
            yield self._cov(cov, "failed", f"http {status}", retryable=status not in (404, 410))
        elif failure.check(IgnoreRequest):
            yield self._cov(cov, "failed", f"ignored: {failure.value}"[:300], retryable=0)
        else:
            yield self._cov(cov, "failed",
                            f"{type(failure.value).__name__}: {failure.value}"[:300],
                            retryable=1)

    def closed(self, reason):
        """Leave the close reason where csv_worker.py can read it (resume vs done)."""
        out_dir = self.settings.get("OUTPUT_DIR")
        stats = self.crawler.stats.get_stats() if self.crawler and self.crawler.stats else {}
        payload = {
            "reason": reason,
            "mode": self.mode,
            "finished_at": time.time(),
            "stats": {k: v for k, v in stats.items() if isinstance(v, (int, float))},
        }
        try:
            path = os.path.join(out_dir, f"crawl_result_{self.mode}.json")
            with open(path + ".tmp", "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(path + ".tmp", path)
        except OSError as exc:
            self.logger.warning("could not write crawl result: %r", exc)


# --------------------------------------------------------------------------- #
# Pipeline: SQLite only (resume-safe), coverage included
# --------------------------------------------------------------------------- #
class CsvSeedPipeline(MultiFormatPipeline):
    """``MultiFormatPipeline``'s SQLite store and schema, without its live CSV / JSON.

    The API pipeline (``MaxPrepTwoFilePipeline``) opens its CSVs in ``"w"`` mode, so a
    resumed run would truncate everything gathered before. SQLite with ``INSERT OR
    REPLACE`` on the canonical PKs is what makes resuming safe: rows only accumulate,
    never duplicate. The CSVs are exported from here by ``csv_worker.py``.
    """

    COMMIT_EVERY_ROWS = 100
    COMMIT_EVERY_SECONDS = 15

    def open_spider(self, spider):
        os.makedirs(self.output_dir, exist_ok=True)
        self._db = connect_db(os.path.join(self.output_dir, self.db_file))
        self._init_db()                 # schools / games / roster (+ column migrations)
        ensure_coverage_table(self._db)
        self._pending = 0
        self._last_commit = time.time()

    def process_item(self, item, spider):
        if isinstance(item, CoverageItem):
            write_coverage(self._db, dict(item))
        elif isinstance(item, SchoolItem):
            self._row("schools", SCHOOL_FIELDS, item)
        elif isinstance(item, ScheduleGameItem):
            self._row("games", GAME_FIELDS, item)
        elif isinstance(item, RosterItem):
            self._row("roster", ROSTER_FIELDS, item)
        else:
            return item
        self._tick()
        return item

    def _row(self, table, fields, item):
        flat = {}
        for f in fields:
            v = item.get(f)
            flat[f] = "; ".join(v) if f in LIST_FIELDS and isinstance(v, list) else v
        self._db.execute(
            f'INSERT OR REPLACE INTO {table} ({",".join(chr(34) + f + chr(34) for f in fields)}) '
            f'VALUES ({",".join("?" for _ in fields)})',
            [flat.get(f) for f in fields],
        )

    def _tick(self):
        self._pending += 1
        if (self._pending >= self.COMMIT_EVERY_ROWS
                or time.time() - self._last_commit > self.COMMIT_EVERY_SECONDS):
            self._db.commit()
            self._pending = 0
            self._last_commit = time.time()

    def close_spider(self, spider):
        self._db.commit()
        self._db.close()


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def run_csv_crawl(mode, out_dir, input_csv, sports):
    """Run ONE crawl (``crawl``) or retry pass (``retry``) for a job directory.

    Calls ``CrawlerProcess.start()``: at most once per process (ARCHITECTURE §3.1), which
    is why ``csv_worker.py`` runs each mode in its own subprocess.
    """
    settings = get_project_settings()
    settings.set("ITEM_PIPELINES", {CsvSeedPipeline: 300})
    settings.set("OUTPUT_DIR", out_dir)
    settings.set("SQLITE_FILE", DB_FILE)
    # The work is bounded by the uploaded list, and a wall-clock cap is exactly how
    # schedules get dropped -- so no cap. Resumability covers restarts instead.
    settings.set("CLOSESPIDER_TIMEOUT", 0)
    settings.set("RETRY_TIMES", int(os.environ.get("CSV_RETRY_TIMES", "8")))
    settings.set("MAXPREPS_BLOCK_MARKER", os.path.join(out_dir, BLOCK_MARKER))
    if mode == "crawl":
        # Disk-backed queue + persisted dupefilter: a relaunch on the same out_dir resumes.
        # (The retry pass is short and must re-request already-seen URLs, so it has none.)
        settings.set("JOBDIR", os.path.join(out_dir, ".crawljob"))

    process = CrawlerProcess(settings)
    process.crawl(CsvSeedMaxPrepsSpider, input_csv=input_csv, sports=sports, mode=mode,
                  db_path=os.path.join(out_dir, DB_FILE))
    process.start()


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 4 or argv[0] not in ("crawl", "retry"):
        print("usage: python csv_seed_scraper.py <crawl|retry> <out_dir> <input.csv> <sports>",
              file=sys.stderr)
        raise SystemExit(2)
    mode, out_dir, input_csv, sports = argv
    os.makedirs(out_dir, exist_ok=True)
    # One crawl per job directory at a time: two processes sharing a JOBDIR and DB would
    # corrupt the queue. Waits briefly for a predecessor that is still shutting down.
    lock = open(os.path.join(out_dir, "crawl.lock"), "a+")
    for _ in range(120):
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            time.sleep(1)
    else:
        print("csv_seed_scraper: another crawl holds this job directory", file=sys.stderr)
        raise SystemExit(4)
    run_csv_crawl(mode, out_dir, input_csv, sports)


if __name__ == "__main__":
    main()
