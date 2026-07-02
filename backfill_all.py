#!/usr/bin/env python
"""Re-crawl every state to *completion* and merge into the canonical DB.

Why this exists
---------------
The public ``/{state}/schools/`` directory is hard-capped at 200 schools. The spider
already reaches past that cap via graph-discovery (``nearbySchools`` + schedule
opponents), but the normal run is cut short by ``CLOSESPIDER_TIMEOUT`` (30 min), the
memory guard, and -- on the deployed API -- the job watchdog. So most states end up
only partially covered (e.g. FL had 417 of ~600-900; DE/AK/DC had ~1 each).

This orchestrator runs each state as its own subprocess with those caps removed and a
per-state ``JOBDIR`` so the discovery BFS runs to full convergence and is resumable.

No regression, by construction
------------------------------
The pipeline writes with ``INSERT OR REPLACE`` on the ``school_id`` primary key
(maxpreps_scraper/pipelines.py) -- it only ever *adds or updates* rows, never deletes.
Re-crawling can only grow the DB. Before/after per-state counts are logged and a
decrease is flagged loudly as a tripwire.

Usage
-----
    python backfill_all.py                 # all 51 states + DC, resumable
    python backfill_all.py fl              # just Florida (canary)
    python backfill_all.py fl,de,ak,dc     # a subset
    python backfill_all.py --force fl      # re-crawl even if marked complete
    python backfill_all.py --mem 12000     # raise the per-crawl memory cap (MB)

Resume / re-run
---------------
A completed state is recorded in ``.jobs/completed.txt`` and skipped on re-run. An
interrupted state (Ctrl-C / crash) is NOT marked complete, so re-running resumes it
from its ``.jobs/<code>`` JOBDIR. To force a clean re-crawl of a state, delete its
``.jobs/<code>`` dir (and its line in ``.jobs/completed.txt``) or pass ``--force``.
"""
import argparse
import os
import sqlite3
import subprocess
import sys
import time

from maxpreps_scraper.settings import OUTPUT_DIR, SQLITE_FILE
from maxpreps_scraper.states import ALL_STATE_CODES

HERE = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.join(HERE, ".jobs")
COMPLETED_FILE = os.path.join(JOBS_DIR, "completed.txt")
DB_PATH = os.path.join(HERE, OUTPUT_DIR, SQLITE_FILE)
DEFAULT_MEM_MB = int(os.environ.get("MEMUSAGE_LIMIT_MB", "8000"))


def school_count(state_code):
    """Rows in the canonical DB for a state (uppercased, as stored)."""
    if not os.path.exists(DB_PATH):
        return 0
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.execute(
            "SELECT COUNT(*) FROM schools WHERE state = ?", (state_code.upper(),)
        )
        return cur.fetchone()[0]
    except sqlite3.OperationalError:
        return 0  # table not created yet
    finally:
        con.close()


def load_completed():
    if not os.path.exists(COMPLETED_FILE):
        return set()
    with open(COMPLETED_FILE, encoding="utf-8") as fh:
        return {ln.strip().lower() for ln in fh if ln.strip()}


def mark_completed(code):
    os.makedirs(JOBS_DIR, exist_ok=True)
    with open(COMPLETED_FILE, "a", encoding="utf-8") as fh:
        fh.write(code.lower() + "\n")


def crawl_state(code, mem_mb):
    """Run one state's crawl to completion (uncapped + resumable) in a subprocess.

    One subprocess == one Twisted reactor (a reactor cannot restart in-process), which
    is why we don't loop the crawl inside this Python process. Schedules and discovery
    are ON by default in run.py; we only override the truncating caps here.
    """
    jobdir = os.path.join(JOBS_DIR, code.lower())
    os.makedirs(jobdir, exist_ok=True)
    cmd = [
        sys.executable, os.path.join(HERE, "run.py"), code.lower(),
        "-s", f"JOBDIR={jobdir}",
        "-s", "CLOSESPIDER_TIMEOUT=0",       # remove the 30-min wall-clock cap
        "-s", f"MEMUSAGE_LIMIT_MB={mem_mb}",  # raise the graceful-stop memory cap
    ]
    # stdout/stderr inherit the parent's -> the Scrapy log streams live to the console.
    return subprocess.run(cmd, cwd=HERE).returncode


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "states", nargs="?", default="all",
        help='comma-separated state codes (e.g. "fl" or "fl,de,ak"), or "all"',
    )
    parser.add_argument("--force", action="store_true",
                        help="re-crawl states even if already marked complete")
    parser.add_argument("--mem", type=int, default=DEFAULT_MEM_MB,
                        help=f"per-crawl MEMUSAGE_LIMIT_MB (default {DEFAULT_MEM_MB})")
    args = parser.parse_args()

    if args.states in (None, "", "all"):
        codes = list(ALL_STATE_CODES)
    else:
        codes = [c.strip().lower() for c in args.states.split(",") if c.strip()]
        unknown = [c for c in codes if c not in ALL_STATE_CODES]
        if unknown:
            parser.error(f"unknown state code(s): {', '.join(unknown)}")

    completed = set() if args.force else load_completed()
    os.makedirs(JOBS_DIR, exist_ok=True)

    todo = [c for c in codes if c not in completed]
    skipped = [c for c in codes if c in completed]
    print(f"DB: {DB_PATH}")
    print(f"States to crawl: {len(todo)}  |  already complete (skipped): {len(skipped)}")
    if skipped:
        print(f"  skipping (use --force to redo): {', '.join(skipped)}")

    results = []  # (code, before, after, rc)
    try:
        for i, code in enumerate(todo, 1):
            before = school_count(code)
            print("\n" + "=" * 72)
            print(f"[{i}/{len(todo)}] {code.upper()}  (currently {before} schools) -> crawling to completion")
            print("=" * 72, flush=True)
            t0 = time.time()
            rc = crawl_state(code, args.mem)
            after = school_count(code)
            dt = time.time() - t0
            results.append((code, before, after, rc))

            status = "OK" if rc == 0 else f"FAILED(rc={rc})"
            print(f"\n--- {code.upper()} {status}: {before} -> {after} "
                  f"({after - before:+d}) in {dt/60:.1f} min ---", flush=True)
            if after < before:
                print(f"!!! REGRESSION TRIPWIRE: {code.upper()} count DECREASED "
                      f"({before} -> {after}). Investigate before continuing. Backup: "
                      f"output/{SQLITE_FILE}.bak", flush=True)
            if rc == 0:
                mark_completed(code)  # only mark clean, fully-converged runs
            else:
                print(f"    {code.upper()} not marked complete; re-run to resume from JOBDIR.",
                      flush=True)
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run the same command to resume unfinished states "
              "(each resumes from its .jobs/<state> JOBDIR).", flush=True)

    # ---- final audit table ----
    print("\n" + "=" * 72)
    print("SUMMARY (state: before -> after (delta) status)")
    print("=" * 72)
    grew = same = failed = 0
    for code, before, after, rc in results:
        flag = "" if rc == 0 else "  <-- FAILED"
        if after > before:
            grew += 1
        elif after == before:
            same += 1
        if rc != 0:
            failed += 1
        print(f"  {code.upper():<3} {before:>5} -> {after:>5} ({after - before:+d}){flag}")
    total = school_count_all()
    print("-" * 72)
    print(f"  states crawled: {len(results)}  grew: {grew}  unchanged: {same}  failed: {failed}")
    print(f"  grand total schools now: {total}")
    print("\nNext: python -m maxpreps_scraper.export   # rebuild clean CSV/JSON from the DB")


def school_count_all():
    if not os.path.exists(DB_PATH):
        return 0
    con = sqlite3.connect(DB_PATH)
    try:
        return con.execute("SELECT COUNT(*) FROM schools").fetchone()[0]
    except sqlite3.OperationalError:
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    main()
