#!/usr/bin/env python
"""Run exactly ONE sports-filtered MaxPreps crawl, then exit.

This exists so the FastAPI service (``api.py``) can launch each crawl in its own
process. Scrapy/Twisted can only run one reactor per process and that reactor
cannot be restarted -- so every job gets a fresh ``python worker.py ...``.

Usage (invoked by api.py, not by hand):

    python worker.py <output_dir> <states> <sports> <levels> <discover>

    output_dir  directory the two CSVs are written to (max_prep_School.csv,
                max_prep_schedule.csv)
    states      comma-separated state codes, e.g. "ny" or "ny,ca,tx"
    sports      comma-separated sport names, e.g. "Football" or "Football,Basketball"
    levels      "Varsity" (default) or "all"
    discover    "1" to graph-crawl past the 200/state cap, "0" to disable

Exit code 0 = crawl finished; non-zero = it failed (api.py marks the job "error").
"""
import os
import subprocess
import sys

from max_prep_scraper import run_crawl

# Hard cap on the post-crawl website-name enrichment (seconds). Bounds total runtime on
# large crawls; on timeout the (un-enriched) teams CSV is left intact and still usable.
ENRICH_TIMEOUT_SECONDS = 1800


def main():
    if len(sys.argv) < 4:
        print(
            "usage: python worker.py <output_dir> <states> <sports> [levels] [discover]",
            file=sys.stderr,
        )
        raise SystemExit(2)

    output_dir = sys.argv[1]
    states = sys.argv[2]
    sports = sys.argv[3]
    levels = sys.argv[4] if len(sys.argv) > 4 else "Varsity"
    discover = (sys.argv[5] if len(sys.argv) > 5 else "1") == "1"

    run_crawl(
        states=states,
        sports=sports,
        levels=levels,
        discover=discover,
        output_dir=output_dir,
    )

    # Second phase: scrape each school's website name into an "original_name" column.
    # Best-effort -- a Scrapy reactor can't be restarted in this process, so it runs as
    # its own subprocess. Failures/timeouts are swallowed (check=False + try/except) so
    # this NEVER changes the worker's exit code: the crawl already produced the teams
    # CSV, and enrichment merely augments it. api.py marks the job done on exit code 0.
    teams_csv = os.path.join(output_dir, "max_prep_School.csv")
    if os.path.exists(teams_csv):
        here = os.path.dirname(os.path.abspath(__file__))
        try:
            subprocess.run(
                [sys.executable, "enrich_website_name.py", teams_csv],
                cwd=here,
                timeout=ENRICH_TIMEOUT_SECONDS,
                check=False,
            )
        except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
            print(f"worker: enrichment skipped/failed: {exc!r}", file=sys.stderr)


if __name__ == "__main__":
    main()
