#!/usr/bin/env python
"""Run one CSV-seeded MaxPreps job, start to finish -- resumably.

    python csv_worker.py <job_dir> <input.csv> <sports>

Produces, in ``job_dir``, exactly what a ``/scrape`` job produces:
``max_prep_School.csv`` / ``max_prep_schedule.csv`` / ``max_prep_roster.csv``, with the
same columns, plus ``coverage.csv`` (what was fetched, empty or failed) and
``progress.json`` (live status for the API).

Phases, each recorded in ``state.json`` once complete so a relaunch skips it:

1. **seed**     -- read the upload's ``maxPrepsLink`` column; register every school as
                   ``pending`` coverage (unusable rows as ``skipped``).
2. **crawl**    -- ``csv_seed_scraper.py crawl`` in a subprocess (one reactor per
                   process). Resumes from ``JOBDIR`` + SQLite whenever it stops early:
                   memory guard, host restart, or the stall watchdog below.
3. **retry**    -- ``csv_seed_scraper.py retry``: re-request every page still pending or
                   failed-but-retryable, until a round stops helping.
4. **export**   -- SQLite -> the three API-named CSVs (the canonical column lists).
5. **enrich**   -- ``worker.run_enrichment`` (shared with ``/scrape``): opponent school
                   rows (harvest), GoFan, NFHS, MaxPreps link fill, opponent names/logos.
                   Phase 2.6 (rosters) is skipped: rosters were fetched in-crawl, and 2.6
                   would add opponents' rosters, which this job excludes.
6. **finalize** -- ``coverage.csv``, summary, final state.

Relaunching on the same ``job_dir`` resumes at the first unfinished phase. That is how
``csv_seed_api.py`` survives a deploy/restart: the job directory lives on the persistent
disk and the API relaunches unfinished jobs at startup.

Exit codes: 0 done, 3 MaxPreps refused this server, 4 another worker owns the job,
1 any other failure, 128+N interrupted by signal N (resumable).
"""
import fcntl
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time

from csv_seed_scraper import (
    BLOCK_MARKER, DB_FILE, export_coverage, init_job_db, load_links, load_retry_rows,
    write_coverage,
)
from maxpreps_scraper.access import read_block_marker
from maxpreps_scraper.export import export_db
from worker import (
    EXIT_BLOCKED, JOB_ERROR_FILE, ROSTER_CSV, SCHEDULE_CSV, TEAMS_CSV, run_enrichment,
)

HERE = os.path.dirname(os.path.abspath(__file__))
PHASES = ("seed", "crawl", "retry", "export", "enrich", "finalize")
PROGRESS_JSON = "progress.json"
STATE_JSON = "state.json"
COVERAGE_CSV = "coverage.csv"
LOCK_FILE = "worker.lock"
EXIT_BUSY = 4

POLL_SECONDS = int(os.environ.get("CSV_POLL_SECONDS", "15"))
HEARTBEAT_SECONDS = 30
# The crawl subprocess is restarted (and resumes) if no page resolves for this long. A
# single page can't legitimately take this long: AutoThrottle caps the delay at 30 s and
# retries are bounded. Covers a wedged reactor, not slow progress.
CRAWL_STALL_SECONDS = int(os.environ.get("CSV_CRAWL_STALL_SECONDS", "1800"))
MAX_CRAWL_LAUNCHES = int(os.environ.get("CSV_MAX_CRAWL_LAUNCHES", "20"))
RETRY_ROUNDS = int(os.environ.get("CSV_RETRY_ROUNDS", "2"))

# Enrichment caps for CSV-sized jobs (thousands of schools). /scrape keeps its own.
TIMEOUTS = {
    "harvest": int(os.environ.get("CSV_HARVEST_TIMEOUT_SECONDS", "7200")),
    "gofan": int(os.environ.get("CSV_GOFAN_TIMEOUT_SECONDS", "3600")),
    "nfhs": int(os.environ.get("CSV_NFHS_TIMEOUT_SECONDS", "3600")),
    "resolve": int(os.environ.get("CSV_RESOLVE_TIMEOUT_SECONDS", "900")),
}

API_NAMES = tuple(name[:-len(".csv")] for name in (TEAMS_CSV, SCHEDULE_CSV, ROSTER_CSV))


class JobFailed(Exception):
    def __init__(self, message, code=1):
        super().__init__(message)
        self.code = code


# --------------------------------------------------------------------------- #
# progress.json / state.json
# --------------------------------------------------------------------------- #
class Progress:
    """Thread-safe, atomically-written progress.json (read by the API)."""

    def __init__(self, job_dir):
        self.path = os.path.join(job_dir, PROGRESS_JSON)
        self.lock = threading.Lock()
        try:
            with open(self.path, encoding="utf-8") as fh:
                self.data = json.load(fh)
        except (OSError, ValueError):
            self.data = {}

    def update(self, **fields):
        with self.lock:
            self.data.update(fields)
            self.data["updated_at"] = time.time()
            self._write()

    def beat(self):
        with self.lock:
            self.data["heartbeat_at"] = time.time()
            self._write()

    def _write(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.data, fh)
        os.replace(tmp, self.path)


def _load_state(job_dir):
    try:
        with open(os.path.join(job_dir, STATE_JSON), encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {"phases_done": [], "crawl_launches": 0}


def _save_state(job_dir, state):
    path = os.path.join(job_dir, STATE_JSON)
    with open(path + ".tmp", "w", encoding="utf-8") as fh:
        json.dump(state, fh)
    os.replace(path + ".tmp", path)


def db_counts(db_path):
    """Live counters from the job DB (safe while the crawl writes: WAL readers)."""
    out = {"schools": {}, "schedule": {}, "roster": {}, "staff": {}, "input": {},
           "rows": {"schools": 0, "games": 0, "roster": 0}}
    if not os.path.exists(db_path):
        return out
    db = sqlite3.connect(db_path, timeout=30)
    try:
        for kind, status, n in db.execute(
                "SELECT kind, status, COUNT(*) FROM coverage GROUP BY kind, status"):
            key = "schools" if kind == "school" else kind
            out.setdefault(key, {})[status] = n
        for table, key in (("schools", "schools"), ("games", "games"), ("roster", "roster")):
            out["rows"][key] = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except sqlite3.OperationalError:
        pass  # tables not created yet
    finally:
        db.close()
    for key in ("schools", "schedule", "roster", "staff"):
        statuses = out[key]
        statuses["total"] = sum(v for k, v in statuses.items() if k != "total")
    return out


def _resolved(counts):
    """Pages no longer pending -- the crawl's forward-progress measure."""
    return sum(n for key in ("schools", "schedule", "roster", "staff")
               for status, n in counts[key].items() if status not in ("pending", "total"))


# --------------------------------------------------------------------------- #
# Subprocess control
# --------------------------------------------------------------------------- #
_child = {"proc": None}


def _on_signal(signum, _frame):
    """Stop the crawl gracefully (Scrapy persists JOBDIR on SIGTERM), then exit resumable."""
    proc = _child["proc"]
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            proc.kill()
    raise SystemExit(128 + signum)


def _run_scraper(mode, job_dir, input_csv, sports, progress):
    """Run ``csv_seed_scraper.py <mode>`` to completion, watching for stalls.

    Returns ``(returncode, crawl_result_or_None, stalled)``.
    """
    db_path = os.path.join(job_dir, DB_FILE)
    result_path = os.path.join(job_dir, f"crawl_result_{mode}.json")
    if os.path.exists(result_path):
        os.remove(result_path)

    with open(os.path.join(job_dir, f"{mode}.log"), "a", encoding="utf-8") as log:
        log.write(f"\n===== {mode} launch at {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n")
        log.flush()
        proc = subprocess.Popen(
            [sys.executable, "csv_seed_scraper.py", mode, job_dir, input_csv, sports],
            cwd=HERE, stdout=log, stderr=subprocess.STDOUT,
        )
        _child["proc"] = proc
        last, last_change, stalled = -1, time.time(), False
        try:
            while proc.poll() is None:
                time.sleep(POLL_SECONDS)
                counts = db_counts(db_path)
                progress.update(counts=counts)
                resolved = _resolved(counts)
                if resolved != last:
                    last, last_change = resolved, time.time()
                elif time.time() - last_change > CRAWL_STALL_SECONDS:
                    stalled = True
                    print(f"csv_worker: {mode} made no progress for {CRAWL_STALL_SECONDS}s; "
                          "restarting it (it resumes)", file=sys.stderr)
                    proc.terminate()
                    try:
                        proc.wait(timeout=120)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
        finally:
            _child["proc"] = None

    try:
        with open(result_path, encoding="utf-8") as fh:
            result = json.load(fh)
    except (OSError, ValueError):
        result = None
    return proc.returncode, result, stalled


def _log_tail(job_dir, mode, lines=15):
    try:
        with open(os.path.join(job_dir, f"{mode}.log"), encoding="utf-8", errors="replace") as fh:
            return "".join(fh.readlines()[-lines:])
    except OSError:
        return ""


# --------------------------------------------------------------------------- #
# Phases
# --------------------------------------------------------------------------- #
def phase_seed(job_dir, input_csv, progress):
    try:
        info = load_links(input_csv)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise JobFailed(f"could not read the uploaded CSV: {exc}")
    if not info["links"]:
        raise JobFailed("the uploaded CSV has no usable maxPrepsLink values")

    db_path = os.path.join(job_dir, DB_FILE)
    db = init_job_db(db_path)
    try:
        for url in info["links"]:
            write_coverage(db, {"kind": "school", "url": url, "status": "pending",
                                "retryable": "1",
                                "context": json.dumps({"discovered_via": "input"})})
        for line_no, raw, reason in info["skipped"]:
            # Keyed by line so two blank rows don't collide; never retried.
            write_coverage(db, {"kind": "input", "url": f"line {line_no}: {raw}",
                                "status": "skipped", "detail": reason, "retryable": "0"})
        db.commit()
    finally:
        db.close()
    progress.update(input={"rows": info["rows"], "links": len(info["links"]),
                           "skipped": len(info["skipped"])})


def _fail_if_blocked(job_dir):
    marker = read_block_marker(os.path.join(job_dir, BLOCK_MARKER))
    if marker:
        raise JobFailed(marker.get("message") or "MaxPreps refused access from this server.",
                        code=EXIT_BLOCKED)


def phase_crawl(job_dir, input_csv, sports, progress, state):
    marker = os.path.join(job_dir, BLOCK_MARKER)
    if os.path.exists(marker):
        os.remove(marker)  # a previous blocked attempt; this run decides afresh
    while state["crawl_launches"] < MAX_CRAWL_LAUNCHES:
        state["crawl_launches"] += 1
        _save_state(job_dir, state)
        progress.update(crawl_launches=state["crawl_launches"])
        rc, result, stalled = _run_scraper("crawl", job_dir, input_csv, sports, progress)
        _fail_if_blocked(job_dir)
        reason = (result or {}).get("reason")
        if reason == "finished":
            return
        if result is None and rc and rc > 0 and not stalled:
            # Died before Scrapy could even close: a bug or bad input, not an interruption.
            raise JobFailed(f"crawl process failed (exit {rc}):\n{_log_tail(job_dir, 'crawl')}")
        print(f"csv_worker: crawl stopped early ({reason or f'exit {rc}'}); resuming",
              file=sys.stderr)
    raise JobFailed(f"crawl did not finish after {MAX_CRAWL_LAUNCHES} launches")


def phase_retry(job_dir, input_csv, sports, progress):
    db_path = os.path.join(job_dir, DB_FILE)
    for round_no in range(1, RETRY_ROUNDS + 1):
        before = len(load_retry_rows(db_path))
        if not before:
            return
        progress.update(phase="retry", retry_round=round_no, retry_pending=before)
        _run_scraper("retry", job_dir, input_csv, sports, progress)
        marker = os.path.join(job_dir, BLOCK_MARKER)
        if os.path.exists(marker):
            # A retry pass contains ONLY problem pages, so a run of individually-forbidden
            # pages can trip the breaker without any network block. The crawl's data
            # stands; what's left stays "failed" in coverage.csv for the user to see.
            os.remove(marker)
            print("csv_worker: retry pass stopped by the block breaker; leaving the rest "
                  "as failed", file=sys.stderr)
            return
        if len(load_retry_rows(db_path)) >= before:
            return  # a round that fixes nothing won't be helped by another


def phase_export(job_dir):
    export_db(job_dir, DB_FILE, names=API_NAMES, write_json=False)


def phase_enrich(job_dir, sports, progress):
    run_enrichment(job_dir, sports, "all", run_roster=False, timeouts=TIMEOUTS,
                   on_phase=lambda name: progress.update(phase=f"enrich: {name}"))


def phase_finalize(job_dir, progress):
    db_path = os.path.join(job_dir, DB_FILE)
    export_coverage(db_path, os.path.join(job_dir, COVERAGE_CSV))
    counts = db_counts(db_path)
    progress.update(counts=counts)
    schools = counts["schools"]
    if not (schools.get("ok", 0) + schools.get("empty", 0)):
        failed = schools.get("failed", 0) + schools.get("pending", 0)
        raise JobFailed(f"none of the {failed} school pages could be fetched; "
                        "see coverage.csv for each URL's error")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def _acquire_lock(job_dir):
    # Patient: a predecessor that was just told to stop may need ~90 s to shut its crawl
    # down gracefully (JOBDIR flush) before it releases the job.
    fh = open(os.path.join(job_dir, LOCK_FILE), "a+")
    for _ in range(240):
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fh
        except BlockingIOError:
            time.sleep(0.5)
    fh.close()
    return None


def run_job(job_dir, input_csv, sports):
    os.makedirs(job_dir, exist_ok=True)
    lock = _acquire_lock(job_dir)
    if lock is None:
        print("csv_worker: another worker is running this job", file=sys.stderr)
        return EXIT_BUSY

    progress = Progress(job_dir)
    state = _load_state(job_dir)
    progress.update(state="running", pid=os.getpid(), error=None,
                    started_at=progress.data.get("started_at") or time.time(),
                    resumed_at=time.time() if state["phases_done"] else None)
    error_file = os.path.join(job_dir, JOB_ERROR_FILE)
    if os.path.exists(error_file):
        os.remove(error_file)

    stop = threading.Event()

    def heartbeat():
        while not stop.wait(HEARTBEAT_SECONDS):
            progress.beat()

    progress.beat()
    threading.Thread(target=heartbeat, daemon=True).start()

    steps = {
        "seed": lambda: phase_seed(job_dir, input_csv, progress),
        "crawl": lambda: phase_crawl(job_dir, input_csv, sports, progress, state),
        "retry": lambda: phase_retry(job_dir, input_csv, sports, progress),
        "export": lambda: phase_export(job_dir),
        "enrich": lambda: phase_enrich(job_dir, sports, progress),
        "finalize": lambda: phase_finalize(job_dir, progress),
    }
    try:
        for name in PHASES:
            if name in state["phases_done"]:
                continue
            progress.update(phase=name)
            steps[name]()
            state["phases_done"].append(name)
            _save_state(job_dir, state)
    except JobFailed as exc:
        message = str(exc)
        with open(error_file, "w", encoding="utf-8") as fh:
            fh.write(message)
        progress.update(state="error", error=message, finished_at=time.time())
        print(f"csv_worker: {message}", file=sys.stderr)
        return exc.code
    except SystemExit:
        progress.update(interrupted_at=time.time())  # state stays "running": resumable
        raise
    except Exception as exc:  # noqa: BLE001 - surface any crash as a job error
        message = f"worker crashed in phase {progress.data.get('phase')}: {exc!r}"
        with open(error_file, "w", encoding="utf-8") as fh:
            fh.write(message)
        progress.update(state="error", error=message, finished_at=time.time())
        raise
    finally:
        stop.set()

    progress.update(state="done", phase="done", finished_at=time.time())
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if len(argv) != 3:
        print("usage: python csv_worker.py <job_dir> <input.csv> <sports>", file=sys.stderr)
        raise SystemExit(2)
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    job_dir, input_csv, sports = argv
    raise SystemExit(run_job(os.path.abspath(job_dir), os.path.abspath(input_csv), sports))


if __name__ == "__main__":
    main()
