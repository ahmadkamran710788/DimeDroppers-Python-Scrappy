#!/usr/bin/env python
"""Run ONE GoFan middle-school enrichment over an uploaded CSV, then exit.

This is the middle-school counterpart to ``worker.py``, and it is deliberately
separate from it: nothing here imports or mutates the MaxPreps pipeline, and
``middle_school_api.py`` launches it in its own subprocess exactly as ``api.py``
launches ``worker.py``.

Usage (invoked by middle_school_api.py, not by hand):

    python gofan_ms_worker.py <job_dir> <input_csv> [limit]

    job_dir    directory the two CSVs and progress.json are written to
    input_csv  the CSV the user uploaded; must have a SCH_NAME column
    limit      optional max rows to process ("0"/absent = every row)

Two phases, two outputs:

  Phase 1 -> gofan_schools.csv    every column of the uploaded CSV, unchanged, plus
                                  eight gofan_* columns naming the matched school.
  Phase 2 -> gofan_schedule.csv   one row per upcoming GoFan event, for every school
                                  phase 1 matched.

Unlike a MaxPreps crawl, the amount of work is known before it starts (it is just the
row count), so this writes a real ``progress.json`` and the UI can show a true
progress bar instead of indeterminate dots.

Exit code 0 = finished; non-zero = failed (the API marks the job "error").
"""
import csv
import datetime
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import gofan_client
from gofan_match import pick

try:  # stdlib on 3.9+, but tzdata can be missing on a slim container
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

SCHOOLS_CSV = "gofan_schools.csv"
SCHEDULE_CSV = "gofan_schedule.csv"
PROGRESS_JSON = "progress.json"

# Uploaded rows carry ~120 columns of NCES data, some of them long free text.
csv.field_size_limit(10**7)

# Concurrency for the GoFan calls. 8 measured ~106 ms/row end-to-end with zero 429s;
# the API is the bottleneck, not the CPU, so this is I/O-bound and threads are right.
WORKERS = 8
# How often to flush progress.json. Small enough that the UI feels live, large enough
# that we aren't doing a write per row.
PROGRESS_EVERY = 25

# Columns appended to the uploaded CSV. Pre-filled to "" for every row BEFORE any
# work starts, so a killed job still leaves a CSV whose header has them -- the
# frontend sees blanks rather than `undefined` (see ARCHITECTURE.md's note on the
# GoFan/NFHS steps that get this wrong).
GOFAN_COLUMNS = [
    "gofan_url",
    "gofan_school_id",
    "gofan_name",
    "gofan_city",
    "gofan_state",
    "gofan_zip",
    "gofan_school_type",
    "gofan_match",
    "gofan_match_score",
]

# The GoFan analogue of maxpreps_scraper/export.py's GAME_FIELDS. The leading block
# mirrors it so the two schedule CSVs read the same way. GoFan serves only upcoming,
# on-sale events, so there is deliberately no result/score column -- it would be empty
# on every row. In exchange we get ticketing and venue detail MaxPreps doesn't have.
GAME_FIELDS = [
    "sch_name", "nces_school_id", "state", "city",
    "gofan_school_id", "gofan_school_name", "gofan_url",
    "sport", "is_athletic", "gender", "level", "event_index",
    "date", "time", "start_datetime_utc", "timezone",
    "home_away", "opponent", "event_title",
    "venue_name", "venue_address", "venue_city", "venue_state", "venue_zip",
    "event_id", "event_url", "min_price", "is_postseason", "canceled",
]


# --------------------------------------------------------------------------- #
# Progress
# --------------------------------------------------------------------------- #
def _write_progress(job_dir, **fields):
    """Write progress.json atomically so a poll never reads a half-written file."""
    path = os.path.join(job_dir, PROGRESS_JSON)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(fields, fh)
        os.replace(tmp, path)
    except OSError:
        pass  # progress is advisory; never fail the job over it


# --------------------------------------------------------------------------- #
# Phase 1 -- resolve each row to a GoFan school
# --------------------------------------------------------------------------- #
def _resolve(row):
    """Search GoFan for one row and return the gofan_* values to write."""
    blank = {c: "" for c in GOFAN_COLUMNS}
    blank["gofan_match"] = "none"
    name = (row.get("SCH_NAME") or "").strip()
    if not name:
        return blank
    candidate, kind, score = pick(gofan_client.search_schools(name), row)
    if not candidate:
        return blank
    sid = candidate.get("huddleId") or ""
    return {
        "gofan_url": gofan_client.SCHOOL_URL.format(sid) if sid else "",
        "gofan_school_id": sid,
        "gofan_name": candidate.get("name") or "",
        "gofan_city": candidate.get("city") or "",
        "gofan_state": candidate.get("state") or "",
        "gofan_zip": candidate.get("zipCode") or "",
        "gofan_school_type": candidate.get("industryCode") or "",
        "gofan_match": kind,
        "gofan_match_score": score,
    }


def link_schools(job_dir, input_csv, limit):
    """Phase 1: add the gofan_* columns to the uploaded CSV. Returns the rows."""
    with open(input_csv, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        rows = []
        for row in reader:
            rows.append(row)
            if limit and len(rows) >= limit:
                break

    if "SCH_NAME" not in fieldnames:
        raise SystemExit("uploaded CSV has no SCH_NAME column")

    # Idempotent column addition, matching the idiom used across this repo.
    out_fields = fieldnames + [c for c in GOFAN_COLUMNS if c not in fieldnames]
    for row in rows:
        for c in GOFAN_COLUMNS:
            row.setdefault(c, "")
        row["gofan_match"] = "none"

    total = len(rows)
    done = matched = 0
    _write_progress(job_dir, phase="link", done=0, total=total, matched=0, events=0)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for row, result in zip(rows, pool.map(_resolve, rows)):
            row.update(result)
            done += 1
            if result.get("gofan_school_id"):
                matched += 1
            if done % PROGRESS_EVERY == 0 or done == total:
                _write_progress(
                    job_dir, phase="link", done=done, total=total,
                    matched=matched, events=0,
                )

    path = os.path.join(job_dir, SCHOOLS_CSV)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)

    print(f"phase 1: {total} rows | matched {matched} | -> {path}", flush=True)
    return rows, matched


# --------------------------------------------------------------------------- #
# Phase 2 -- pull each matched school's schedule
# --------------------------------------------------------------------------- #
def _local_parts(event):
    """(date, time, raw_utc, tz) rendered in the event's own timezone.

    A game at 4:00 PM Central must not read as 21:00 -- GoFan sends the instant in
    +0000 and names the timezone separately, so we convert before formatting and keep
    the raw value in its own column for anyone who needs the instant.
    """
    raw = event.get("startDateTime") or ""
    tz = event.get("timeZone") or ""
    if not raw:
        return "", "", "", tz
    try:
        dt = datetime.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        try:
            dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return "", "", raw, tz
    if tz and ZoneInfo is not None:
        try:
            dt = dt.astimezone(ZoneInfo(tz))
        except Exception:  # noqa: BLE001 - unknown zone -> leave in UTC
            pass
    return dt.strftime("%Y-%m-%d"), dt.strftime("%I:%M %p").lstrip("0"), raw, tz


def _levels(event):
    """(level, gender) flattened from GoFan's nested level/gender structure."""
    levels = event.get("resolvedLevels") or event.get("levels") or []
    names, genders = [], []
    for lv in levels:
        if not isinstance(lv, dict):
            continue
        if lv.get("levelName"):
            names.append(str(lv["levelName"]))
        for g in lv.get("genders") or []:
            if g and g not in genders:
                genders.append(str(g))
    if not genders:
        genders = [str(g) for g in (event.get("resolvedGenders") or event.get("genders") or []) if g]
    return "; ".join(dict.fromkeys(names)), "; ".join(dict.fromkeys(genders))


def _min_price(event):
    prices = [
        t.get("price")
        for t in (event.get("ticketTypes") or [])
        if isinstance(t, dict) and t.get("isEnabled") and isinstance(t.get("price"), (int, float))
    ]
    return f"{min(prices):.2f}" if prices else ""


def _event_row(school, event, index, names):
    """Flatten one GoFan event into a schedule row for ``school``."""
    sid = school["gofan_school_id"]
    host_id = event.get("schoolHuddleId") or ""
    home = host_id == sid
    # Away: the host school is the opponent, and its name is already on the event.
    # Home: we are the host, so the opponent is opponentSchoolId, resolved in bulk.
    if home:
        opp_id = event.get("opponentSchoolId") or ""
        opponent = (names.get(opp_id) or {}).get("name") or ""
    else:
        opponent = event.get("financialSchoolName") or (names.get(host_id) or {}).get("name") or ""

    date, time_, raw, tz = _local_parts(event)
    level, gender = _levels(event)
    venue = event.get("venue") or {}
    eid = event.get("id") or ""

    return {
        "sch_name": school["sch_name"],
        "nces_school_id": school["nces_school_id"],
        "state": school["state"],
        "city": school["city"],
        "gofan_school_id": sid,
        "gofan_school_name": school["gofan_name"],
        "gofan_url": gofan_client.SCHOOL_URL.format(sid),
        "sport": (event.get("activity") or {}).get("name") or event.get("eventTypeName") or "",
        # A school's GoFan page also sells non-game items (season pass cards, theatre,
        # registration). They are real rows on that page, so they are not dropped --
        # this flag lets a consumer filter to games without losing anything.
        "is_athletic": "yes" if (event.get("activity") or {}).get("isAthletic") else "no",
        "gender": gender,
        "level": level,
        "event_index": index,
        "date": date,
        "time": time_,
        "start_datetime_utc": raw,
        "timezone": tz,
        "home_away": "Home" if home else "Away",
        "opponent": opponent,
        "event_title": event.get("title") or event.get("shortenName") or "",
        "venue_name": venue.get("name") or "",
        "venue_address": venue.get("streetAddress") or "",
        "venue_city": venue.get("city") or "",
        "venue_state": venue.get("state") or "",
        "venue_zip": venue.get("zip") or "",
        "event_id": eid,
        "event_url": gofan_client.EVENT_URL.format(eid) if eid else "",
        "min_price": _min_price(event),
        "is_postseason": "yes" if event.get("isPostSeason") else "no",
        "canceled": "yes" if event.get("canceled") else "no",
    }


def scrape_schedules(job_dir, rows, total_rows, matched):
    """Phase 2: one CSV row per upcoming event, for every matched school."""
    # One entry per distinct GoFan school -- several uploaded rows can legitimately
    # resolve to the same GoFan page, and we must not fetch or emit it twice.
    schools, seen = [], set()
    for row in rows:
        sid = (row.get("gofan_school_id") or "").strip()
        if not sid or sid in seen:
            continue
        seen.add(sid)
        schools.append({
            "gofan_school_id": sid,
            "gofan_name": row.get("gofan_name") or "",
            "sch_name": row.get("SCH_NAME") or "",
            "nces_school_id": row.get("NCESSCH") or "",
            "state": row.get("MSTATE") or row.get("ST") or "",
            "city": row.get("MCITY") or "",
        })

    total = len(schools)
    _write_progress(
        job_dir, phase="schedule", done=0, total=total,
        matched=matched, events=0, rows=total_rows,
    )

    fetched, done = [], 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for school, events in zip(
            schools, pool.map(lambda s: gofan_client.school_events(s["gofan_school_id"]), schools)
        ):
            fetched.append((school, events))
            done += 1
            if done % PROGRESS_EVERY == 0 or done == total:
                _write_progress(
                    job_dir, phase="schedule", done=done, total=total,
                    matched=matched, events=sum(len(e) for _, e in fetched), rows=total_rows,
                )

    # Resolve every referenced school id to a name in one bulk call, so opponents on
    # home games aren't left blank. Cheap: one POST per 1,000 ids.
    wanted = set()
    for _, events in fetched:
        for ev in events:
            for key in ("schoolHuddleId", "opponentSchoolId"):
                if ev.get(key):
                    wanted.add(ev[key])
    names = gofan_client.schools_by_ids(sorted(wanted)) if wanted else {}

    out_rows = []
    for school, events in fetched:
        for i, ev in enumerate(
            sorted(events, key=lambda e: e.get("startDateTime") or ""), start=1
        ):
            out_rows.append(_event_row(school, ev, i, names))

    path = os.path.join(job_dir, SCHEDULE_CSV)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=GAME_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out_rows)
    os.replace(tmp, path)

    print(f"phase 2: {total} schools | {len(out_rows)} events | -> {path}", flush=True)
    return len(out_rows)


# --------------------------------------------------------------------------- #
def main():
    if len(sys.argv) < 3:
        print(
            "usage: python gofan_ms_worker.py <job_dir> <input_csv> [limit]",
            file=sys.stderr,
        )
        sys.exit(2)
    job_dir, input_csv = sys.argv[1], sys.argv[2]
    limit = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else 0
    os.makedirs(job_dir, exist_ok=True)

    rows, matched = link_schools(job_dir, input_csv, limit)
    events = scrape_schedules(job_dir, rows, len(rows), matched)

    _write_progress(
        job_dir, phase="done", done=len(rows), total=len(rows),
        matched=matched, events=events, rows=len(rows),
    )


if __name__ == "__main__":
    main()
