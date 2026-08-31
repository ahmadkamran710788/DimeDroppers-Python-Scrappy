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
                                  nine gofan_* columns naming the matched school.
  Phase 2 -> gofan_schedule.csv   one row per upcoming GoFan event, for every school
                                  phase 1 matched.

**Both phases stream.** Rows are read, enriched and written a chunk at a time, and
nothing is ever accumulated across the whole file, so peak memory is set by CHUNK_ROWS
(a few MB) rather than by the row count. An earlier version read the whole CSV into a
list, which cost ~12 MB per 1,000 rows -- fine for 16k rows, but ~1.2 GB at 100k and an
OOM kill beyond that. Row count is now bounded only by disk and wall-clock.

Unlike a MaxPreps crawl, the amount of work is known before it starts, so this writes a
real ``progress.json`` and the UI can show a true progress bar. That file is also the
job's liveness signal: the API watches it advance and kills only a genuinely stalled
job, rather than capping total runtime -- which is what lets an arbitrarily large file
run to completion.

Exit code 0 = finished; non-zero = failed (the API marks the job "error").
"""
import csv
import datetime
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

import gofan_client
from gofan_match import parse_opponent, pick, pick_opponent

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
# Rows held in memory at once. The only thing that grows with file size is the set of
# matched school ids (phase 2's work list), which is a few bytes per matched school.
CHUNK_ROWS = 500
# Schools per schedule batch. Smaller than CHUNK_ROWS because each one can return
# hundreds of events, which are written out and dropped immediately after.
CHUNK_SCHOOLS = 100
# Heartbeat granularity, in completed items. This is deliberately far finer than a
# chunk: progress.json doubles as the job's liveness signal, and if it only advanced
# once per 500-row chunk then a chunk slowed by GoFan retries could sit still long
# enough to look stalled and be killed mid-run. It also keeps the UI progress bar
# moving smoothly on a multi-hour job instead of jumping a chunk at a time.
PROGRESS_EVERY = 25

# Columns appended to the uploaded CSV. Every row is written with all of them present
# (blank when unmatched), so a job killed midway still leaves a valid CSV whose header
# is complete -- the frontend sees blanks rather than `undefined`.
GOFAN_COLUMNS = [
    "gofan_url",
    "gofan_school_id",
    "gofan_name",
    "gofan_city",
    "gofan_state",
    "gofan_zip",
    "gofan_school_type",
    # Unprefixed by request. Empty unless GoFan has a real logo for the school -- its
    # generic wordmark is filtered out by gofan_client.logo_url.
    "logo_url",
    "gofan_match",
    "gofan_match_score",
]

# Colour columns. Unlike everything above, these already EXIST in the uploaded CSV (and
# are entirely empty), so they are filled in place rather than appended -- the idempotent
# column logic in link_schools only appends names that aren't already in the header.
#
# These are the one thing the search response cannot give us: colours come only from the
# school detail record, so they are filled from a bulk lookup per chunk rather than
# per row. Coverage is genuinely partial upstream -- across GoFan's catalog primaryColor
# is present for 39% of schools and secondaryColor for 24% -- so a blank colour on a
# matched row is correct, not a failure.
COLOR_COLUMNS = ["SCHOOL_COLORS", "PRIMARY_SCHOOL_COLOR", "SECONDARY_SCHOOL_COLOR"]

_HEX_COLOR = re.compile(r"^[0-9a-f]{6}$")

# The GoFan analogue of maxpreps_scraper/export.py's GAME_FIELDS. The leading block
# mirrors it so the two schedule CSVs read the same way. GoFan serves only upcoming,
# on-sale events, so there is deliberately no result/score column -- it would be empty
# on every row. In exchange we get ticketing and venue detail MaxPreps doesn't have.
GAME_FIELDS = [
    "sch_name", "nces_school_id", "state", "city",
    "gofan_school_id", "gofan_school_name", "gofan_url",
    "sport", "is_athletic", "gender", "level", "event_index",
    "date", "time", "start_datetime_utc", "timezone",
    "home_away", "opponent",
    # Who the opponent actually is on GoFan -- their page-header name, page URL and
    # logo. Resolved from the event's opponent id when it has one (~90% of rows, free);
    # otherwise by putting the title-parsed name through the search box, the way a
    # person would. opponent_match records which ("id" / "search" / "" = unresolved).
    "opponent_gofan_school_id", "opponent_gofan_name", "opponent_gofan_url",
    "opponent_logo_url", "opponent_match",
    "event_title",
    "venue_name", "venue_address", "venue_city", "venue_state", "venue_zip",
    "event_id", "event_url", "min_price", "is_postseason", "canceled",
]


# --------------------------------------------------------------------------- #
# Progress
# --------------------------------------------------------------------------- #
def _write_progress(job_dir, **fields):
    """Write progress.json atomically so a poll never reads a half-written file.

    The API treats a change in ``done`` as proof the job is alive, so this must keep
    being called throughout both phases even when nothing interesting has happened.
    """
    path = os.path.join(job_dir, PROGRESS_JSON)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(fields, fh)
        os.replace(tmp, path)
    except OSError:
        pass  # progress is advisory; never fail the job over it


def _chunks(iterable, size):
    """Yield lists of at most ``size`` items from an iterator, lazily."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            return
        yield batch


def count_rows(path, limit=0):
    """Count data rows without building dicts.

    Needed up front so the progress bar has a denominator. Uses csv.reader rather than
    counting newlines because quoted NCES fields can contain embedded newlines, which
    would inflate a naive line count. One extra read of the file, ~1s per 100k rows.
    """
    total = 0
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # header
        for _ in reader:
            total += 1
            if limit and total >= limit:
                break
    return total


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
        # Already in the search response we just made -- no extra request.
        "logo_url": gofan_client.logo_url(candidate.get("logoUrl")),
        "gofan_match": kind,
        "gofan_match_score": score,
    }


def _hex_color(raw):
    """Normalise one GoFan colour to '#rrggbb', or '' if it isn't a usable colour.

    GoFan stores bare 6-digit hex with inconsistent case ('9d0909', 'FF0000') and no
    leading '#'. Anything that isn't exactly six hex digits (there is at least one
    3-character value in the catalog) is dropped rather than guessed at.
    """
    v = str(raw or "").strip().lstrip("#").lower()
    return f"#{v}" if _HEX_COLOR.match(v) else ""


def _colors_for(detail):
    """{column: value} colour fields for one GoFan detail record."""
    primary = _hex_color((detail or {}).get("primaryColor"))
    secondary = _hex_color((detail or {}).get("secondaryColor"))
    return {
        "SCHOOL_COLORS": ", ".join(c for c in (primary, secondary) if c),
        "PRIMARY_SCHOOL_COLOR": primary,
        "SECONDARY_SCHOOL_COLOR": secondary,
    }


def _fill_colors(chunk):
    """Fill the colour columns for every matched row in a chunk, in one bulk request.

    Batched per chunk rather than per row: ``schools_by_ids`` takes up to 1,000 ids at
    a time, so a 500-row chunk costs exactly one extra POST no matter how many of its
    rows matched.
    """
    ids = sorted({r["gofan_school_id"] for r in chunk if r.get("gofan_school_id")})
    details = gofan_client.schools_by_ids(ids) if ids else {}
    for row in chunk:
        sid = row.get("gofan_school_id") or ""
        # Only write where we actually have a school. An unmatched row keeps whatever
        # the upload contained (empty, on the NCES file) rather than being blanked --
        # this step must never destroy a value it didn't supply.
        if sid and sid in details:
            row.update(_colors_for(details[sid]))
        else:
            for col in COLOR_COLUMNS:
                row.setdefault(col, "")


def link_schools(job_dir, input_csv, limit):
    """Phase 1, streaming: add the gofan_* columns to the uploaded CSV.

    Returns ``(total_rows, matched, work_list)`` where work_list is the de-duplicated
    set of matched schools phase 2 has to fetch. That list is the only thing carried
    across phases, and it holds a handful of short strings per *matched* school -- not
    per input row -- so it stays small even on a very large upload.
    """
    total = count_rows(input_csv, limit)
    out_path = os.path.join(job_dir, SCHOOLS_CSV)
    _write_progress(job_dir, phase="link", done=0, total=total, matched=0, events=0)

    done = matched = 0
    work_list, seen = [], set()

    with open(input_csv, newline="", encoding="utf-8-sig") as fin:
        reader = csv.DictReader(fin)
        fieldnames = list(reader.fieldnames or [])
        if "SCH_NAME" not in fieldnames:
            raise SystemExit("uploaded CSV has no SCH_NAME column")
        # Idempotent column addition, matching the idiom used across this repo. The
        # colour columns are included so they still get a header on the rare upload that
        # lacks them; on the normal NCES file they are already present and this is a
        # no-op, leaving them to be filled in place.
        out_fields = fieldnames + [
            c for c in (GOFAN_COLUMNS + COLOR_COLUMNS) if c not in fieldnames
        ]

        rows = islice(reader, limit) if limit else reader

        # Written progressively rather than atomically at the end: at 100k+ rows the
        # whole point is never to hold the result in memory, and a partially-written
        # CSV is strictly more useful than none if the job dies.
        with open(out_path, "w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=out_fields, extrasaction="ignore")
            writer.writeheader()

            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                for chunk in _chunks(rows, CHUNK_ROWS):
                    # pool.map yields in order as results arrive, so `done` advances
                    # row by row rather than only when the whole chunk lands.
                    for row, result in zip(chunk, pool.map(_resolve, chunk)):
                        row.update(result)
                        sid = result.get("gofan_school_id")
                        if sid:
                            matched += 1
                            if sid not in seen:
                                seen.add(sid)
                                work_list.append({
                                    "gofan_school_id": sid,
                                    "gofan_name": result.get("gofan_name") or "",
                                    "sch_name": row.get("SCH_NAME") or "",
                                    "nces_school_id": row.get("NCESSCH") or "",
                                    "state": row.get("MSTATE") or row.get("ST") or "",
                                    "city": row.get("MCITY") or "",
                                })
                        done += 1
                        if done % PROGRESS_EVERY == 0:
                            _write_progress(
                                job_dir, phase="link", done=done, total=total,
                                matched=matched, events=0,
                            )
                    # One bulk detail call for the whole chunk, just before it is
                    # written -- colours are the only field the search response omits.
                    _fill_colors(chunk)
                    writer.writerows(chunk)
                    fout.flush()
                    _write_progress(
                        job_dir, phase="link", done=done, total=total,
                        matched=matched, events=0,
                    )

    print(f"phase 1: {done} rows | matched {matched} | -> {out_path}", flush=True)
    return done, matched, work_list


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
        genders = [
            str(g)
            for g in (event.get("resolvedGenders") or event.get("genders") or [])
            if g
        ]
    return "; ".join(dict.fromkeys(names)), "; ".join(dict.fromkeys(genders))


def _min_price(event):
    prices = [
        t.get("price")
        for t in (event.get("ticketTypes") or [])
        if isinstance(t, dict)
        and t.get("isEnabled")
        and isinstance(t.get("price"), (int, float))
    ]
    return f"{min(prices):.2f}" if prices else ""


_BLANK_OPPONENT = {
    "opponent_gofan_school_id": "",
    "opponent_gofan_name": "",
    "opponent_gofan_url": "",
    "opponent_logo_url": "",
    "opponent_match": "",
}


def _opponent(school, event, home, host_id, names, resolve):
    """Identify the event's opponent: ``(opponent_* columns, display_name)``.

    Two paths, tried in order:

    **By id** -- authoritative and free. A home event names its opponent in
    ``opponentSchoolId``; an away event's opponent is the host, ``schoolHuddleId``.
    Either way the detail record is already in ``names`` (phase 2 bulk-fetches every id
    it sees), carrying the official name and logo. If the bulk lookup didn't know the
    id, the id and URL are still written -- the URL needs nothing but the id -- with an
    away row's name covered by ``financialSchoolName`` (that field names the HOST, so
    on a home row it would name us, never the opponent).

    **By search** -- the manual flow, for the ~10% of home events with no opponent id
    (tri-matches, "X vs Y" titles). Parse the opponent off the title and put it through
    the search box; ``resolve`` caches per (name, state) so a repeated opponent costs
    one request. Non-athletic items (season passes, theatre dues) are skipped -- they
    have no opponent to find. An unresolvable but parseable name is still returned as
    the display name, so the ``opponent`` column beats a blank even when the link
    couldn't be pinned down.
    """
    sid = school["gofan_school_id"]
    opp_id = (event.get("opponentSchoolId") or "") if home else host_id
    if opp_id:
        detail = names.get(opp_id) or {}
        name = detail.get("name") or (
            "" if home else (event.get("financialSchoolName") or "")
        )
        return {
            "opponent_gofan_school_id": opp_id,
            "opponent_gofan_name": name,
            "opponent_gofan_url": gofan_client.SCHOOL_URL.format(opp_id),
            "opponent_logo_url": gofan_client.logo_url(detail.get("logoUrl")),
            "opponent_match": "id",
        }, name

    if not (event.get("activity") or {}).get("isAthletic"):
        return dict(_BLANK_OPPONENT), ""
    own = (
        school["gofan_name"],
        school["sch_name"],
        (names.get(sid) or {}).get("mascot") or "",
    )
    parsed = parse_opponent(event.get("title") or event.get("shortenName") or "", own)
    if not parsed:
        return dict(_BLANK_OPPONENT), ""
    cand = resolve(parsed, school["state"])
    if not cand:
        return dict(_BLANK_OPPONENT), parsed
    cid = cand.get("huddleId") or ""
    return {
        "opponent_gofan_school_id": cid,
        "opponent_gofan_name": cand.get("name") or "",
        "opponent_gofan_url": gofan_client.SCHOOL_URL.format(cid) if cid else "",
        "opponent_logo_url": gofan_client.logo_url(cand.get("logoUrl")),
        "opponent_match": "search",
    }, cand.get("name") or parsed


def _event_row(school, event, index, names, resolve):
    """Flatten one GoFan event into a schedule row for ``school``."""
    sid = school["gofan_school_id"]
    host_id = event.get("schoolHuddleId") or ""
    home = host_id == sid
    opp_cols, opp_name = _opponent(school, event, home, host_id, names, resolve)
    # The away preference order (financialSchoolName first) predates the opponent_*
    # columns and is kept verbatim so existing opponent values don't shift.
    if home:
        opponent = opp_name
    else:
        opponent = event.get("financialSchoolName") or opp_name or ""

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
        "sport": (event.get("activity") or {}).get("name")
        or event.get("eventTypeName")
        or "",
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
        **opp_cols,
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


def scrape_schedules(job_dir, work_list, total_rows, matched):
    """Phase 2, streaming: one CSV row per upcoming event, for every matched school."""
    total = len(work_list)
    out_path = os.path.join(job_dir, SCHEDULE_CSV)
    _write_progress(
        job_dir, phase="schedule", done=0, total=total,
        matched=matched, events=0, rows=total_rows,
    )

    # id -> detail, for naming opponents on home games. Grows with the number of
    # *distinct* schools seen, not events, and is only ever added to once per id.
    names = {}
    done = written = 0

    # (parsed name, state) -> search hit or None, for the title-parse fallback. The
    # same opponents recur across a league's schedule, so most lookups are cache hits;
    # a None is cached too, so an unresolvable name is searched once, not per event.
    opp_cache = {}

    def resolve_opponent(parsed, state):
        key = (parsed.lower(), (state or "").strip().upper())
        if key not in opp_cache:
            hits, precise = gofan_client.search_opponent(parsed)
            opp_cache[key] = pick_opponent(hits, parsed, state, precise=precise)
        return opp_cache[key]

    with open(out_path, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=GAME_FIELDS, extrasaction="ignore")
        writer.writeheader()

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for batch in _chunks(work_list, CHUNK_SCHOOLS):
                fetched = []
                for school, events in zip(
                    batch,
                    pool.map(
                        lambda s: gofan_client.school_events(s["gofan_school_id"]),
                        batch,
                    ),
                ):
                    fetched.append((school, events))
                    done += 1
                    if done % PROGRESS_EVERY == 0:
                        # Heartbeat during the fetch too -- a batch of 100 schools is
                        # minutes of network on its own.
                        _write_progress(
                            job_dir, phase="schedule", done=done, total=total,
                            matched=matched, events=written, rows=total_rows,
                        )

                # Resolve this batch's unseen opponent ids in one POST, then write and
                # drop the events. Batching per chunk (rather than once globally at the
                # end) is what keeps events from piling up in memory.
                wanted = {
                    ev[key]
                    for _, events in fetched
                    for ev in events
                    for key in ("schoolHuddleId", "opponentSchoolId")
                    if ev.get(key) and ev[key] not in names
                }
                if wanted:
                    names.update(gofan_client.schools_by_ids(sorted(wanted)))

                for school, events in fetched:
                    for i, ev in enumerate(
                        sorted(events, key=lambda e: e.get("startDateTime") or ""),
                        start=1,
                    ):
                        writer.writerow(_event_row(school, ev, i, names, resolve_opponent))
                        written += 1

                fout.flush()
                _write_progress(
                    job_dir, phase="schedule", done=done, total=total,
                    matched=matched, events=written, rows=total_rows,
                )

    print(f"phase 2: {total} schools | {written} events | -> {out_path}", flush=True)
    return written


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

    total_rows, matched, work_list = link_schools(job_dir, input_csv, limit)
    events = scrape_schedules(job_dir, work_list, total_rows, matched)

    _write_progress(
        job_dir, phase="done", done=total_rows, total=total_rows,
        matched=matched, events=events, rows=total_rows,
    )


if __name__ == "__main__":
    main()
