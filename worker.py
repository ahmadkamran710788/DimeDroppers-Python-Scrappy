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
import csv
import os
import subprocess
import sys

from max_prep_scraper import run_crawl

# Hard cap on the GoFan ticket-link enrichment (seconds). It verifies one URL per matched
# school and overwrites original_name with the GoFan catalog name on success; on timeout
# the teams CSV is left intact (with original_name = MaxPreps name) and still usable.
GOFAN_TIMEOUT_SECONDS = 900

# Hard cap on the NFHS link enrichment (seconds). Matches locally against a cached catalog
# (no per-school requests), so it's fast once the catalog is built; on timeout the teams
# CSV is left intact and still usable.
NFHS_TIMEOUT_SECONDS = 900


def _copy_name_to_original_name(csv_path):
    """Write name → original_name for every row, creating the column if absent.

    Idempotent: safe to call on a CSV that already has original_name (e.g. a re-run).
    The GoFan step will overwrite original_name with the verified GoFan school name for
    every row it successfully matches; rows with no GoFan match keep this MaxPreps name.
    """
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    out_fields = fieldnames + ([] if "original_name" in fieldnames else ["original_name"])
    for row in rows:
        row["original_name"] = (row.get("name") or "").strip()

    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, csv_path)


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

    teams_csv = os.path.join(output_dir, "max_prep_School.csv")
    if os.path.exists(teams_csv):
        here = os.path.dirname(os.path.abspath(__file__))

        # Second phase: copy the MaxPreps "name" column into "original_name".
        # This gives the GoFan step a search value and acts as the fallback for rows
        # where no GoFan match is found. Done inline (no subprocess) -- it's a plain
        # CSV read+write, no Scrapy reactor involved.
        try:
            _copy_name_to_original_name(teams_csv)
        except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
            print(f"worker: original_name copy failed: {exc!r}", file=sys.stderr)

        # Third phase: match each school against GoFan's catalog (state + city gated),
        # verify the candidate URL (HTTP 200), write "go_fan_ticket_url", and replace
        # "original_name" with the school's verified GoFan name. Own-subprocess because
        # Scrapy's Twisted reactor can only run once per process.
        try:
            subprocess.run(
                [sys.executable, "enrich_gofan_scrapy.py", teams_csv],
                cwd=here,
                timeout=GOFAN_TIMEOUT_SECONDS,
                check=False,
            )
        except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
            print(f"worker: gofan enrichment skipped/failed: {exc!r}", file=sys.stderr)

        # Fourth phase: resolve each school's NFHS Network page into an "nfhs_url" column.
        # Matches original_name (state + city gated) against NFHS's cached catalog. Own
        # subprocess for isolation; same best-effort rules (failures swallowed, atomic
        # write means a partial run never corrupts the teams CSV).
        try:
            subprocess.run(
                [sys.executable, "enrich_nfhs.py", teams_csv],
                cwd=here,
                timeout=NFHS_TIMEOUT_SECONDS,
                check=False,
            )
        except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
            print(f"worker: nfhs enrichment skipped/failed: {exc!r}", file=sys.stderr)


if __name__ == "__main__":
    main()
