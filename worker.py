#!/usr/bin/env python
"""Run exactly ONE sports-filtered MaxPreps crawl, then exit.

This exists so the FastAPI service (``api.py``) can launch each crawl in its own
process. Scrapy/Twisted can only run one reactor per process and that reactor
cannot be restarted -- so every job gets a fresh ``python worker.py ...``.

Usage (invoked by api.py, not by hand):

    python worker.py <output_dir> <states> <sports> <levels> <discover>

    output_dir  directory the three CSVs are written to (max_prep_School.csv,
                max_prep_schedule.csv, max_prep_roster.csv)
    states      comma-separated state codes, e.g. "ny" or "ny,ca,tx"
    sports      comma-separated sport names, e.g. "Football" or "Football,Basketball"
    levels      "all" (default: Varsity + JV + Freshman) or e.g. "Varsity"
    discover    "1" to graph-crawl past the 200/state cap, "0" to disable

Exit code 0 = crawl finished; non-zero = it failed (api.py marks the job "error").
"""
import csv
import os
import subprocess
import sys

from max_prep_scraper import run_crawl
from maxpreps_scraper.access import read_block_marker

# Hard cap on the GoFan ticket-link enrichment (seconds). It verifies one URL per matched
# school and overwrites original_name with the GoFan catalog name on success; on timeout
# the teams CSV is left intact (with original_name = MaxPreps name) and still usable.
GOFAN_TIMEOUT_SECONDS = 900

# Hard cap on the NFHS link enrichment (seconds). Matches locally against a cached catalog
# (no per-school requests), so it's fast once the catalog is built; on timeout the teams
# CSV is left intact and still usable.
NFHS_TIMEOUT_SECONDS = 900

# Hard cap on the opponent HARVEST (seconds). This is the only opponent step that touches the
# network: it fetches the school page of every opponent that is not already a teams row, so it
# needs a bigger budget than the other steps. On timeout the teams CSV is left exactly as it
# was (the harvest writes atomically, only from a clean close). Anything raised here must also
# be reflected in JOB_MAX_RUNTIME_SECONDS (api.py / render.yaml), which has to exceed the sum
# of every cap.
OPPONENT_HARVEST_TIMEOUT_SECONDS = 1800

# Hard cap on the ROSTER enrichment (seconds). It fetches the school page plus a Roster and
# a Staff page for every teams-CSV school that has no roster rows yet -- in practice the
# opponent schools phase 2.5 just appended, which the crawl's state gate never visited. On
# timeout the roster CSV keeps exactly the rows the crawl itself wrote (the append is
# atomic and only happens on a clean close), so it stays complete and usable.
ROSTER_TIMEOUT_SECONDS = 1200

# Hard cap on the opponent RESOLVE (seconds). Pure CSV work off the harvest's cache -- no
# requests -- so this is generous. On timeout the schedule CSV keeps the fallback columns
# written by the inline prefill step below, so it is still complete and usable.
OPPONENT_RESOLVE_TIMEOUT_SECONDS = 300


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


# Where MaxPreps publishes a partner link itself -> which enrichment column it may fill.
# Strictly a FALLBACK: the catalog-matched value always wins when it is non-empty.
MAXPREPS_LINK_FALLBACKS = (
    ("maxpreps_gofan_url", "go_fan_ticket_url"),
    ("maxpreps_nfhs_url", "nfhs_url"),
)


def _fill_links_from_maxpreps(csv_path):
    """Fill blank go_fan_ticket_url / nfhs_url from the links MaxPreps published.

    The crawler records the GoFan / NFHS links present on a school's own MaxPreps page
    (``maxpreps_scraper/schoolinfo.py``). Those cost no extra requests -- the page was
    already fetched and parsed -- and they cover schools whose GoFan/NFHS catalog match
    failed on a name or city mismatch.

    Fill only, never overwrite, and each column decided independently: a value the GoFan
    step actually verified returns HTTP 200 is strictly better evidence than an unverified
    link, so it is kept. If only one of the two columns is empty, only that one is filled.

    Runs AFTER phases 3 and 4 because both of those set their column unconditionally --
    to "" when they find no match -- so anything written earlier would be clobbered.
    Doing it here means neither enrichment script needs modifying.

    Idempotent, and a no-op on a CSV that predates the source columns.
    """
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    pairs = [(src, dst) for src, dst in MAXPREPS_LINK_FALLBACKS if src in fieldnames]
    if not pairs:
        return  # nothing to fill from (e.g. a CSV from an older crawl)

    out_fields = fieldnames + [d for _s, d in pairs if d not in fieldnames]
    filled = {dst: 0 for _src, dst in pairs}
    for row in rows:
        for src, dst in pairs:
            if not (row.get(dst) or "").strip() and (row.get(src) or "").strip():
                row[dst] = row[src].strip()
                filled[dst] += 1

    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, csv_path)
    print("worker: filled from MaxPreps links -> "
          + ", ".join(f"{dst} +{n}" for dst, n in filled.items()))


def _prefill_opponent_columns(csv_path):
    """Seed the two opponent columns from the schedule's own data, creating them if absent.

    Guarantees the columns exist on every row before the (network-bound, timeout-capped)
    enrichment runs -- without this a timed-out enrichment would leave the API serving a
    schedule CSV with no such columns at all. ``enrich_opponent.py`` overwrites these with the
    scraped school name and logo for every opponent whose page it manages to read.

    Idempotent: safe to call on a CSV that already has the columns (e.g. a re-run).
    """
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    out_fields = fieldnames + [
        c for c in ("original_opponent_school_name", "original_opponent_school_logo")
        if c not in fieldnames
    ]
    for row in rows:
        row["original_opponent_school_name"] = (row.get("opponent") or "").strip()
        row["original_opponent_school_logo"] = ""

    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, csv_path)


def _step(label, args, timeout, here, env=None):
    """Run one enrichment subprocess, best-effort.

    Own subprocess because Scrapy's Twisted reactor can only run once per process, and
    ``check=False`` + swallowed exceptions because no enrichment step may ever fail the
    job -- each writes atomically, so a killed step leaves its input intact.
    """
    try:
        subprocess.run([sys.executable, *args], cwd=here, timeout=timeout, check=False,
                       env={**os.environ, **env} if env else None)
    except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
        print(f"worker: {label} skipped/failed: {exc!r}", file=sys.stderr)


# Default subprocess caps for the enrichment chain, keyed for run_enrichment(). These are
# the /scrape job's values (mirrored in api.py's JOB_MAX_RUNTIME_SECONDS ladder); the
# CSV-seed worker passes its own, larger ones for its bigger jobs.
DEFAULT_TIMEOUTS = {
    "harvest": OPPONENT_HARVEST_TIMEOUT_SECONDS,
    "roster": ROSTER_TIMEOUT_SECONDS,
    "gofan": GOFAN_TIMEOUT_SECONDS,
    "nfhs": NFHS_TIMEOUT_SECONDS,
    "resolve": OPPONENT_RESOLVE_TIMEOUT_SECONDS,
}

# The three files every MaxPreps job writes (MaxPrepTwoFilePipeline / the CSV-seed export).
TEAMS_CSV = "max_prep_School.csv"
SCHEDULE_CSV = "max_prep_schedule.csv"
ROSTER_CSV = "max_prep_roster.csv"

# Written next to the CSVs when a job fails for a reason api.py should show verbatim.
JOB_ERROR_FILE = "job_error.txt"
# Exit code for "MaxPreps refused this server" (api.py maps it to job_error.txt's text).
EXIT_BLOCKED = 3


def run_enrichment(output_dir, sports, levels, *, run_roster=True, timeouts=None,
                   on_phase=None):
    """Run the post-crawl enrichment chain over a job directory's three CSVs.

    Shared by this /scrape worker and ``csv_worker.py`` so both produce byte-compatible
    CSVs with the same extra columns. The phase ORDER is load-bearing (ARCHITECTURE
    invariant 4) -- each phase's comment says why it sits where it does.

    ``run_roster=False`` skips phase 2.6 (the CSV-seed job already fetched every roster it
    wants in-crawl, and 2.6 would otherwise fetch rosters for opponent schools).
    ``timeouts`` overrides any of ``DEFAULT_TIMEOUTS``' subprocess caps. ``on_phase`` is
    called with each phase's name before it runs (progress reporting).
    Best-effort throughout: nothing here raises or fails the job.
    """
    timeouts = {**DEFAULT_TIMEOUTS, **(timeouts or {})}
    phase = on_phase or (lambda _name: None)
    here = os.path.dirname(os.path.abspath(__file__))
    teams_csv = os.path.join(output_dir, TEAMS_CSV)
    schedule_csv = os.path.join(output_dir, SCHEDULE_CSV)
    roster_csv = os.path.join(output_dir, ROSTER_CSV)
    have_both = os.path.exists(teams_csv) and os.path.exists(schedule_csv)

    if os.path.exists(teams_csv):
        # Phase 2: copy the MaxPreps "name" column into "original_name".
        # This gives the GoFan step a search value and acts as the fallback for rows
        # where no GoFan match is found. Done inline (no subprocess) -- it's a plain
        # CSV read+write, no Scrapy reactor involved.
        try:
            _copy_name_to_original_name(teams_csv)
        except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
            print(f"worker: original_name copy failed: {exc!r}", file=sys.stderr)

    # Phase 2.5: give every opponent in the schedule a row in the teams CSV. The crawler
    # gates discovery on the requested states, so an opponent filed under another state is
    # never crawled -- its game is in the schedule with no teams row. This is the ONLY
    # opponent step that touches the network, and it must run BEFORE GoFan/NFHS so the
    # schools it adds get those columns like every other row. Deduped on school_id.
    if have_both:
        phase("opponent harvest")
        # The harvest spider self-closes on its own CLOSESPIDER_TIMEOUT so it writes before
        # this subprocess cap kills it; keep that inner cap 5 min under the kill whenever
        # the caller raises the harvest budget (the /scrape default keeps its env value).
        harvest_env = None
        if timeouts["harvest"] != OPPONENT_HARVEST_TIMEOUT_SECONDS:
            harvest_env = {"OPPONENT_CLOSESPIDER_TIMEOUT": str(max(60, timeouts["harvest"] - 300))}
        _step("opponent harvest",
              ["enrich_opponent.py", "harvest", schedule_csv, teams_csv],
              timeouts["harvest"], here, env=harvest_env)

    # Phase 2.6: give every school in the teams CSV roster rows. Same gap as phase 2.5 and
    # for the same reason -- the crawl never visits an out-of-state opponent, so the rows
    # harvest just appended have no roster. Must run AFTER 2.5 so those rows exist. Writes
    # only the roster CSV, so it cannot interfere with phases 3/4/4.5.
    if run_roster and os.path.exists(roster_csv) and os.path.exists(teams_csv):
        phase("roster enrichment")
        _step("roster enrichment",
              ["enrich_roster.py", roster_csv, teams_csv, sports, levels],
              timeouts["roster"], here)

    if os.path.exists(teams_csv):
        # Phase 3: match each school against GoFan's catalog (state + city gated),
        # verify the candidate URL (HTTP 200), write "go_fan_ticket_url", and replace
        # "original_name" with the school's verified GoFan name.
        phase("gofan enrichment")
        _step("gofan enrichment", ["enrich_gofan_scrapy.py", teams_csv],
              timeouts["gofan"], here)

        # Phase 4: resolve each school's NFHS Network page into an "nfhs_url" column.
        # Matches original_name (state + city gated) against NFHS's cached catalog.
        phase("nfhs enrichment")
        _step("nfhs enrichment", ["enrich_nfhs.py", teams_csv],
              timeouts["nfhs"], here)

        # Phase 4.5: for schools those two catalogs couldn't match, fall back to the
        # GoFan/NFHS links MaxPreps published on the school's own page (captured during
        # the crawl, so no extra requests). Fills blanks only -- a verified catalog match
        # is never overwritten -- which is why it runs after them rather than before.
        try:
            _fill_links_from_maxpreps(teams_csv)
        except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
            print(f"worker: maxpreps link fill failed: {exc!r}", file=sys.stderr)

    # Phase 5: the only step that rewrites the SCHEDULE csv. Resolves each game's opponent
    # into "original_opponent_school_name" + "original_opponent_school_logo". Runs last
    # because it uses the teams CSV's "original_name", which isn't final until GoFan has run.
    if have_both:
        # 5a, inline: seed both columns from the schedule's own opponent text so they exist
        # even if 5b is killed by its timeout.
        try:
            _prefill_opponent_columns(schedule_csv)
        except Exception as exc:  # noqa: BLE001 - never fail the job on enrichment
            print(f"worker: opponent column prefill failed: {exc!r}", file=sys.stderr)

        # 5b: no network -- reads the opponents.json the harvest parked in phase 2.5 and
        # matches it against the now-final teams CSV, by school_id where one was cached.
        phase("opponent resolve")
        _step("opponent resolve",
              ["enrich_opponent.py", "resolve", schedule_csv, teams_csv],
              timeouts["resolve"], here)


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
    # "all" (not "Varsity") so JV and Freshman schedules are included -- the API is the
    # path that feeds the frontend's CSVs, and those are expected to carry every level.
    # Costs ~3x the schedule pages; run.py / backfill_all.py keep the Varsity default
    # because they exist to maximise SCHOOL coverage, not schedule depth.
    levels = sys.argv[4] if len(sys.argv) > 4 else "all"
    discover = (sys.argv[5] if len(sys.argv) > 5 else "1") == "1"

    run_crawl(
        states=states,
        sports=sports,
        levels=levels,
        discover=discover,
        output_dir=output_dir,
    )

    # The crawl's block breaker (maxpreps_scraper/access.py) drops this marker when
    # MaxPreps refused most requests. Fail the job with that reason instead of running
    # enrichment over (near-)empty CSVs and reporting "done" -- which is what a blocked
    # job used to look like.
    blocked = read_block_marker(os.path.join(output_dir, "blocked.json"))
    if blocked:
        message = blocked.get("message") or "MaxPreps refused access from this server."
        with open(os.path.join(output_dir, JOB_ERROR_FILE), "w", encoding="utf-8") as fh:
            fh.write(message)
        print(f"worker: {message}", file=sys.stderr)
        raise SystemExit(EXIT_BLOCKED)

    run_enrichment(output_dir, sports, levels)


if __name__ == "__main__":
    main()
