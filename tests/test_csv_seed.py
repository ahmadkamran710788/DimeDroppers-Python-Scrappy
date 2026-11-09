"""Offline tests for the CSV-seeded MaxPreps job (no network).

    .venv/bin/python -m unittest tests.test_csv_seed -v

Covers the spider's callbacks against small HTML fixtures shaped like MaxPreps pages,
the coverage semantics that back "no schedule missed", link loading, the access
middlewares, header parity with the /scrape output, and csv_worker's phase/resume logic
(with the crawl and enrichment subprocesses stubbed out).
"""
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import scrapy
from scrapy.http import HtmlResponse
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.python.failure import Failure

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

import csv_seed_scraper as css  # noqa: E402
from maxpreps_scraper import access  # noqa: E402
from maxpreps_scraper.export import GAME_FIELDS, ROSTER_FIELDS, SCHOOL_FIELDS, export_db  # noqa: E402
from maxpreps_scraper.items import RosterItem, ScheduleGameItem, SchoolItem  # noqa: E402

SCHOOL_URL = "https://www.maxpreps.com/ca/stockton/humphreys-able-charter-legacy/"


def next_data(page_props):
    blob = json.dumps({"props": {"pageProps": page_props}})
    return f'<script id="__NEXT_DATA__" type="application/json">{blob}</script>'


def season(sport, gender, level, path):
    return {"sport": sport, "gender": gender, "level": level, "season": "Winter",
            "year": "25-26", "canonicalUrl": SCHOOL_URL + path}


SCHOOL_PAGE = "<html><body>" + next_data({
    "schoolContext": {
        "schoolId": "school-1",
        "schoolInfo": {"name": "Humphreys Able Charter", "city": "Stockton",
                       "stateCode": "CA", "canonicalUrl": SCHOOL_URL},
        "sportSeasons": [
            season("Basketball", "Boys", "Varsity", "basketball/"),
            season("Basketball", "Boys", "JV", "basketball/jv/"),
            season("Basketball", "Boys", "Freshman", "basketball/freshman/"),
            season("Basketball", "Girls", "Varsity", "basketball/girls/"),
            season("Basketball", "Girls", "JV", "basketball/girls/jv/"),
            season("Football", "Boys", "Varsity", "football/"),
        ],
    },
    "nearbySchools": [{"canonicalUrl": "https://www.maxpreps.com/ca/lodi/lodi-academy/"}],
}) + "</body></html>"


def schedule_table(rows):
    body = "".join(f"<tr><td>{d}</td><td>{o}</td><td>{g}</td></tr>" for d, o, g in rows)
    return ("<table><thead><tr><th>Date</th><th>Opponent</th><th>Game Info</th></tr></thead>"
            f"<tbody>{body}</tbody></table>")


REGULAR = [
    ("12/16 7:30pm", '@ <a href="/ca/lodi/elliot-christian-eagles/basketball/">Elliot Christian*</a>', "W 55-40"),
    ("1/7 5:00pm", 'vs <a href="/ca/lodi/lodi-academy-titans/basketball/">Lodi Academy*</a>', "Preview Game"),
]
PLAYOFFS = [
    ("2/20 TBA", 'vs <a href="/ca/stockton/stockton-christian/basketball/">Stockton Christian</a>', "L 40-50"),
]
# Regular season, playoffs, then a duplicate render of the regular season (the kind of
# mobile/desktop double render that must not double-count games).
SCHEDULE_PAGE = ("<html><body>" + next_data({}) + "<div>" + schedule_table(REGULAR)
                 + "</div><section>" + schedule_table(PLAYOFFS) + "</section><aside>"
                 + schedule_table(REGULAR) + "</aside></body></html>")

ROSTER_PAGE = ("<html><body>" + next_data({}) + "<table><thead><tr><th>#</th><th>Player</th>"
               "<th>Grade</th><th>Position</th><th>Height</th><th>Weight</th></tr></thead><tbody>"
               "<tr><td>3</td><td>Sam Lee</td><td>Jr.</td><td>G</td><td>6'0\"</td><td>170 lbs</td></tr>"
               "<tr><td>10</td><td>Ali Khan</td><td>Sr.</td><td>F</td><td>6'4\"</td><td>190 lbs</td></tr>"
               "</tbody></table></body></html>")


def response(url, html, meta=None, cb_kwargs=None):
    req = scrapy.Request(url, meta=meta or {}, cb_kwargs=cb_kwargs or {})
    return HtmlResponse(url=url, body=html.encode("utf-8"), encoding="utf-8", request=req)


def split(outputs):
    items = {"requests": [], "schools": [], "games": [], "roster": [], "coverage": []}
    for out in outputs:
        if isinstance(out, scrapy.Request):
            items["requests"].append(out)
        elif isinstance(out, SchoolItem):
            items["schools"].append(out)
        elif isinstance(out, ScheduleGameItem):
            items["games"].append(out)
        elif isinstance(out, RosterItem):
            items["roster"].append(out)
        elif isinstance(out, css.CoverageItem):
            items["coverage"].append(out)
    return items


class TempDirCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="csvseed-test-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write_input(self, links, extra_cols=True):
        path = os.path.join(self.tmp, "input.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["id", "name", "maxPrepsLink"] if extra_cols else ["maxPrepsLink"])
            for i, link in enumerate(links):
                w.writerow([f"id{i}", f"School {i}", link] if extra_cols else [link])
        return path

    def spider(self, sports="Basketball", db_path=None):
        return css.CsvSeedMaxPrepsSpider(input_csv=self.write_input([SCHOOL_URL]),
                                         sports=sports, db_path=db_path)


# --------------------------------------------------------------------------- #
class LoadLinksTest(TempDirCase):
    def test_only_link_column_dedup_trim_and_skips(self):
        path = self.write_input([
            SCHOOL_URL,
            "https://www.maxpreps.com/ca/stockton/humphreys-able-charter-legacy/",  # dup
            "https://www.maxpreps.com/CA/Lodi/lodi-academy-titans/basketball/schedule/",  # team link
            "  https://maxpreps.com/fl/miami/miami-stingarees  ",  # no www, no slash
            "https://www.maxpreps.com/ca/max/maxpreps-a-alligators/",  # MaxPreps test school
            "https://www.maxpreps.com/ca/el-dorado-hills/maxpreps-preppies/",  # test school
            "",  # blank
            "https://example.com/ca/x/y/",  # foreign
            "https://www.maxpreps.com/zz/x/y/",  # bad state
        ])
        info = css.load_links(path)
        self.assertEqual(info["column"], "maxPrepsLink")
        self.assertEqual(info["rows"], 9)
        self.assertEqual(info["links"], [
            SCHOOL_URL,
            "https://www.maxpreps.com/ca/lodi/lodi-academy-titans/",
            "https://www.maxpreps.com/fl/miami/miami-stingarees/",
        ])
        reasons = [r for _n, _raw, r in info["skipped"]]
        self.assertEqual(reasons.count("duplicate link"), 1)
        self.assertEqual(reasons.count("MaxPreps internal test school"), 2)
        self.assertIn("blank link", reasons)
        self.assertIn("not a maxpreps.com link", reasons)
        self.assertTrue(any(r.startswith("unknown state") for r in reasons))

    def test_link_column_alone_and_missing_column(self):
        self.assertEqual(css.load_links(self.write_input([SCHOOL_URL], extra_cols=False))["links"],
                         [SCHOOL_URL])
        path = os.path.join(self.tmp, "bad.csv")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("id,url\n1,x\n")
        with self.assertRaises(ValueError):
            css.load_links(path)


# --------------------------------------------------------------------------- #
class SpiderCallbacksTest(TempDirCase):
    def test_school_page_every_level_and_gender_for_selected_sport(self):
        sp = self.spider()
        out = split(sp.parse_school(response(SCHOOL_URL, SCHOOL_PAGE,
                                             meta={"cov": {"kind": "school", "url": SCHOOL_URL}}),
                                    discovered_via="input"))
        self.assertEqual(len(out["schools"]), 1)
        self.assertEqual(out["schools"][0]["discovered_via"], "input")
        # 5 basketball teams (Boys V/JV/F, Girls V/JV) x schedule + roster + staff; no
        # football, no nearby school, nothing else.
        self.assertEqual(len(out["requests"]), 15)
        urls = {r.url for r in out["requests"]}
        self.assertFalse(any("football" in u or "lodi" in u for u in urls))
        for path in ("basketball/", "basketball/jv/", "basketball/freshman/",
                     "basketball/girls/", "basketball/girls/jv/"):
            for page in ("schedule/", "roster/", "staff/"):
                self.assertIn(SCHOOL_URL + path + page, urls)
        for r in out["requests"]:
            self.assertEqual(r.errback, sp.on_error)
            self.assertIn("cov", r.meta)
        pending = [c for c in out["coverage"] if c["status"] == "pending"]
        self.assertEqual(len(pending), 15)
        final = [c for c in out["coverage"] if c["kind"] == "school"]
        self.assertEqual(final[0]["status"], "ok")
        self.assertEqual(final[0]["school_id"], "school-1")

    def test_school_with_no_team_for_sport_is_empty_not_failed(self):
        sp = self.spider(sports="Lacrosse")
        out = split(sp.parse_school(response(SCHOOL_URL, SCHOOL_PAGE), discovered_via="input"))
        self.assertEqual(out["requests"], [])
        self.assertEqual(len(out["schools"]), 1)  # the school row is still written
        self.assertEqual(out["coverage"][-1]["status"], "empty")

    def test_pages_without_next_data_fail_retryably(self):
        sp = self.spider()
        bad = "<html><body>Access denied</body></html>"
        for out in (sp.parse_school(response(SCHOOL_URL, bad), discovered_via="input"),
                    sp.parse_schedule(response(SCHOOL_URL + "basketball/schedule/", bad),
                                      team={}, school={})):
            cov = split(out)["coverage"]
            self.assertEqual(cov[0]["status"], "failed")
            self.assertEqual(cov[0]["retryable"], "1")

    def test_schedule_reads_every_table_once(self):
        sp = self.spider()
        team = season("Basketball", "Boys", "Varsity", "basketball/")
        school = {"school_id": "school-1", "name": "Humphreys Able Charter", "state": "CA"}
        url = SCHOOL_URL + "basketball/schedule/"
        out = split(sp.parse_schedule(response(url, SCHEDULE_PAGE), team=team, school=school))
        games = out["games"]
        self.assertEqual([g["game_index"] for g in games], [0, 1, 2])
        self.assertEqual([g["opponent"] for g in games],
                         ["Elliot Christian", "Lodi Academy", "Stockton Christian"])
        self.assertEqual([g["home_away"] for g in games], ["away", "home", "home"])
        self.assertEqual((games[0]["result"], games[0]["score"]), ("W", "55-40"))
        self.assertEqual(games[0]["opponent_url"],
                         "https://www.maxpreps.com/ca/lodi/elliot-christian-eagles/basketball/")
        self.assertEqual(games[0]["season"], "Winter 25-26")
        self.assertEqual(out["requests"], [])  # opponents are NOT followed
        self.assertEqual(out["coverage"][-1]["status"], "ok")
        self.assertEqual(out["coverage"][-1]["detail"], "3 games from 2 tables")

    def test_schedule_without_table_is_empty(self):
        sp = self.spider()
        out = split(sp.parse_schedule(response(SCHOOL_URL + "x/schedule/",
                                               "<html>" + next_data({}) + "</html>"),
                                      team={}, school={}))
        self.assertEqual(out["coverage"][-1]["status"], "empty")

    def test_roster(self):
        sp = self.spider()
        team = season("Basketball", "Boys", "Varsity", "basketball/")
        out = split(sp.parse_roster(response(SCHOOL_URL + "basketball/roster/", ROSTER_PAGE),
                                    team=team, school={"school_id": "school-1"},
                                    category="player"))
        self.assertEqual([r["name"] for r in out["roster"]], ["Sam Lee", "Ali Khan"])
        self.assertEqual(out["coverage"][-1]["status"], "ok")
        self.assertEqual(out["coverage"][-1]["kind"], "roster")

    def test_errback_records_failure_and_retryability(self):
        sp = self.spider()
        for status, retryable in ((404, "0"), (403, "1")):
            req = scrapy.Request(SCHOOL_URL + "basketball/schedule/",
                                 meta={"cov": {"kind": "schedule",
                                               "url": SCHOOL_URL + "basketball/schedule/"}})
            failure = Failure(HttpError(HtmlResponse(url=req.url, status=status, request=req)))
            failure.request = req
            cov = split(sp.on_error(failure))["coverage"][0]
            self.assertEqual((cov["status"], cov["detail"], cov["retryable"]),
                             ("failed", f"http {status}", retryable))

    def test_sports_required(self):
        with self.assertRaises(ValueError):
            css.CsvSeedMaxPrepsSpider(input_csv="x.csv", sports="  ")
        self.assertIsNone(css.CsvSeedMaxPrepsSpider(input_csv="x.csv", sports="all").target_sports)


# --------------------------------------------------------------------------- #
class StorageTest(TempDirCase):
    def test_pending_never_overwrites_and_retry_selection(self):
        db_path = os.path.join(self.tmp, css.DB_FILE)
        db = css.init_job_db(db_path)
        ok = {"kind": "schedule", "url": "u1", "status": "ok", "detail": "3 games",
              "retryable": "0"}
        css.write_coverage(db, ok)
        css.write_coverage(db, {**ok, "status": "pending", "retryable": "1"})  # late pending
        css.write_coverage(db, {"kind": "schedule", "url": "u2", "status": "failed",
                                "detail": "http 404", "retryable": "0"})
        css.write_coverage(db, {"kind": "roster", "url": "u3", "status": "failed",
                                "detail": "http 503", "retryable": "1"})
        css.write_coverage(db, {"kind": "staff", "url": "u4", "status": "pending",
                                "retryable": "1"})
        css.write_coverage(db, {"kind": "input", "url": "line 3: x", "status": "skipped",
                                "retryable": "0"})
        db.commit()
        db.close()
        self.assertEqual(css.load_done_urls(db_path), {"u1"})
        self.assertEqual({r["url"] for r in css.load_retry_rows(db_path)}, {"u3", "u4"})

    def test_pipeline_rows_and_export_header_parity(self):
        pipe = css.CsvSeedPipeline(self.tmp, css.DB_FILE)
        pipe.open_spider(None)
        row = {f: "" for f in SCHOOL_FIELDS}
        row.update(school_id="school-1", name="Humphreys", sports=["Basketball (Boys)"])
        for _ in range(2):  # resume re-writes the same row: no duplicate
            pipe.process_item(SchoolItem(**row), None)
        game = {f: "" for f in GAME_FIELDS}
        game.update(school_id="school-1", schedule_url="s", game_index=0, opponent="X")
        pipe.process_item(ScheduleGameItem(**game), None)
        pipe.close_spider(None)

        names = ("max_prep_School", "max_prep_schedule", "max_prep_roster")
        self.assertEqual(export_db(self.tmp, css.DB_FILE, names=names, write_json=False),
                         (1, 1, 0))
        for name, fields in zip(names, (SCHOOL_FIELDS, GAME_FIELDS, ROSTER_FIELDS)):
            with open(os.path.join(self.tmp, name + ".csv"), newline="") as fh:
                self.assertEqual(next(csv.reader(fh)), fields)
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "max_prep_School.json")))


# --------------------------------------------------------------------------- #
class AccessTest(TempDirCase):
    def test_settings_defaults_unchanged_without_env(self):
        env = {k: v for k, v in os.environ.items() if not k.startswith("MAXPREPS_")}
        code = ("import json; from maxpreps_scraper import settings as s; "
                "print(json.dumps([s.USER_AGENT, s.DOWNLOAD_DELAY, s.DEFAULT_REQUEST_HEADERS, "
                "s.MAXPREPS_AUTH_HEADER]))")
        out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env,
                             capture_output=True, text=True, check=True).stdout
        ua, delay, headers, auth = json.loads(out.strip().splitlines()[-1])
        self.assertEqual(ua, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        self.assertEqual(delay, 0.75)
        self.assertEqual(headers, {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"})
        self.assertEqual(auth, "")

    def test_auth_header_only_on_maxpreps(self):
        mw = access.MaxPrepsHeadersMiddleware(access.parse_header("X-Token: abc"))
        mp = scrapy.Request(SCHOOL_URL)
        gofan = scrapy.Request("https://gofan.co/app/school/CA123")
        mw.process_request(mp, None)
        mw.process_request(gofan, None)
        self.assertEqual(mp.headers.get("X-Token"), b"abc")
        self.assertIsNone(gofan.headers.get("X-Token"))
        noop = access.MaxPrepsHeadersMiddleware(access.parse_header(""))
        req = scrapy.Request(SCHOOL_URL)
        noop.process_request(req, None)
        self.assertNotIn(b"X-Token", req.headers)

    def _breaker(self, window=10):
        crawler = mock.Mock()
        marker = os.path.join(self.tmp, "blocked.json")
        return access.MaxPrepsBlockBreaker(crawler, window, 0.8, marker), crawler, marker

    def _feed(self, breaker, statuses, url=SCHOOL_URL):
        spider = mock.Mock()
        for status in statuses:
            req = scrapy.Request(url)
            breaker.process_response(req, HtmlResponse(url=url, status=status, request=req),
                                     spider)

    def test_breaker_trips_on_block_and_writes_marker(self):
        br, crawler, marker = self._breaker()
        self._feed(br, [403] * 10)
        self.assertTrue(br.tripped)
        crawler.engine.close_spider.assert_called_once()
        self.assertEqual(crawler.engine.close_spider.call_args[0][1], "maxpreps_blocked")
        self.assertEqual(access.read_block_marker(marker)["status"], 403)

    def test_small_run_refused_entirely_is_marked_blocked_at_close(self):
        # A one-state /scrape: one directory URL, 5 attempts, window never fills.
        br, crawler, marker = self._breaker(window=50)
        self._feed(br, [403] * 5)
        self.assertFalse(br.tripped)
        br.spider_closed(mock.Mock(), "finished")
        self.assertEqual(access.read_block_marker(marker)["status"], 403)

    def test_small_run_with_any_real_page_is_not_blocked(self):
        br, crawler, marker = self._breaker(window=50)
        self._feed(br, [403, 403, 200, 403])
        br.spider_closed(mock.Mock(), "finished")
        self.assertFalse(os.path.exists(marker))

    def test_breaker_ignores_scattered_403s_robots_and_other_hosts(self):
        br, crawler, marker = self._breaker()
        self._feed(br, [200, 403, 200, 200, 403, 200, 200, 404, 200, 403] * 3)
        self._feed(br, [403] * 20, url="https://www.maxpreps.com/robots.txt")
        self._feed(br, [403] * 20, url="https://gofan.co/app/school/X")
        self.assertFalse(br.tripped)
        crawler.engine.close_spider.assert_not_called()
        self.assertFalse(os.path.exists(marker))


# --------------------------------------------------------------------------- #
class SnapshotHeaderTest(TempDirCase):
    def test_snapshot_headers_match_final_enriched_headers(self):
        import csv_seed_api as api_mod

        with mock.patch.object(api_mod, "CSV_JOBS_DIR", self.tmp):
            job_dir = os.path.join(self.tmp, "job1")
            os.makedirs(job_dir)
            db = css.init_job_db(os.path.join(job_dir, css.DB_FILE))
            db.close()
            expect = {
                "teams": SCHOOL_FIELDS + ["original_name", "go_fan_ticket_url", "nfhs_url"],
                "schedule": GAME_FIELDS + ["original_opponent_school_name",
                                           "original_opponent_school_logo"],
                "roster": ROSTER_FIELDS,
                "coverage": css.COVERAGE_FIELDS,
            }
            for kind, header in expect.items():
                with open(api_mod._snapshot("job1", kind), newline="") as fh:
                    self.assertEqual(next(csv.reader(fh)), header, kind)
            self.assertEqual(len(expect["teams"]), 32)
            self.assertEqual(len(expect["schedule"]), 18)
            self.assertEqual(len(expect["roster"]), 16)


# --------------------------------------------------------------------------- #
class WorkerPhasesTest(TempDirCase):
    """csv_worker's orchestration with the crawl/enrichment subprocesses stubbed."""

    def setUp(self):
        super().setUp()
        import csv_worker

        self.cw = csv_worker
        self.job = os.path.join(self.tmp, "job")
        os.makedirs(self.job)
        self.input = self.write_input([SCHOOL_URL, "https://www.maxpreps.com/ca/max/maxpreps-a-x/"])

    def fake_scraper(self, outcomes):
        """outcomes: list of (mode, reason, mark_schools_ok) consumed per launch."""
        calls = []

        def run(mode, job_dir, input_csv, sports, progress):
            expected_mode, reason, ok = outcomes.pop(0)
            assert mode == expected_mode, (mode, expected_mode)
            calls.append(mode)
            if ok:
                db = css.init_job_db(os.path.join(job_dir, css.DB_FILE))
                css.write_coverage(db, {"kind": "school", "url": SCHOOL_URL, "status": "ok",
                                        "detail": "5 team(s)", "retryable": "0"})
                db.commit()
                db.close()
            return 0, {"reason": reason} if reason else None, False

        return run, calls

    def test_full_run_then_resume_is_a_noop(self):
        run, calls = self.fake_scraper([("crawl", "shutdown", False),   # interrupted
                                        ("crawl", "finished", True)])  # resumed, done
        with mock.patch.object(self.cw, "_run_scraper", run), \
                mock.patch.object(self.cw, "run_enrichment") as enrich:
            self.assertEqual(self.cw.run_job(self.job, self.input, "Basketball"), 0)
        self.assertEqual(calls, ["crawl", "crawl"])  # nothing left to retry -> no retry pass
        enrich.assert_called_once()
        self.assertFalse(enrich.call_args.kwargs["run_roster"])
        with open(os.path.join(self.job, "progress.json")) as fh:
            prog = json.load(fh)
        self.assertEqual((prog["state"], prog["input"]["links"], prog["input"]["skipped"]),
                         ("done", 1, 1))
        with open(os.path.join(self.job, "coverage.csv"), newline="") as fh:
            rows = list(csv.DictReader(fh))
        self.assertEqual([(r["kind"], r["status"]) for r in rows],
                         [("input", "skipped"), ("school", "ok")])
        for name in ("max_prep_School.csv", "max_prep_schedule.csv", "max_prep_roster.csv"):
            self.assertTrue(os.path.exists(os.path.join(self.job, name)))

        # Relaunching a finished job redoes nothing.
        with mock.patch.object(self.cw, "_run_scraper") as again, \
                mock.patch.object(self.cw, "run_enrichment") as enrich2:
            self.assertEqual(self.cw.run_job(self.job, self.input, "Basketball"), 0)
        again.assert_not_called()
        enrich2.assert_not_called()

    def test_blocked_crawl_fails_fast_with_message(self):
        def run(mode, job_dir, *_a):
            with open(os.path.join(job_dir, css.BLOCK_MARKER), "w") as fh:
                json.dump({"status": 403, "message": access.blocked_message(403)}, fh)
            return 0, {"reason": "maxpreps_blocked"}, False

        with mock.patch.object(self.cw, "_run_scraper", run), \
                mock.patch.object(self.cw, "run_enrichment") as enrich:
            self.assertEqual(self.cw.run_job(self.job, self.input, "Basketball"), 3)
        enrich.assert_not_called()
        with open(os.path.join(self.job, "job_error.txt")) as fh:
            self.assertIn("refused access (HTTP 403)", fh.read())

    def test_all_school_pages_failed_is_an_error_not_done(self):
        run, _calls = self.fake_scraper([("crawl", "finished", False),
                                         ("retry", "finished", False)])
        with mock.patch.object(self.cw, "_run_scraper", run), \
                mock.patch.object(self.cw, "run_enrichment"):
            self.assertEqual(self.cw.run_job(self.job, self.input, "Basketball"), 1)
        with open(os.path.join(self.job, "progress.json")) as fh:
            self.assertEqual(json.load(fh)["state"], "error")


if __name__ == "__main__":
    unittest.main()
