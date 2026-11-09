"""Regenerate clean CSV + JSON from the canonical SQLite database.

SQLite is the durable source of truth during a crawl (it survives interruptions
via periodic commits + INSERT OR REPLACE). CSV/JSON are *derived* from it, so they
can always be rebuilt to a consistent state — even after an unexpected stop.

Run standalone anytime to snapshot current progress:

    python -m maxpreps_scraper.export                 # uses output/maxpreps.db
    python -m maxpreps_scraper.export output           # explicit output dir
"""
import csv
import json
import os
import sqlite3
import sys

# column order, kept in sync with pipelines.py
SCHOOL_FIELDS = [
    "school_id", "name", "city", "state", "state_name", "url",
    "mascot", "address", "zip_code", "state_linking", "phone",
    "color1", "color2", "color3", "mascot_url",
    "league_name", "association_name", "governing_body_name", "governing_body_url",
    "website", "facebook", "instagram", "twitter", "youtube",
    "maxpreps_gofan_url", "maxpreps_nfhs_url",
    "sports", "sports_count", "discovered_via",
]
GAME_FIELDS = [
    "school_id", "school_name", "state", "sport", "gender", "season", "level",
    "game_index", "date", "home_away", "opponent", "opponent_url",
    "result", "score", "game_info", "schedule_url",
]
# One row per person on a team's Roster or Staff tab. The leading block mirrors
# GAME_FIELDS so the three CSVs join on the same keys; "category" says which tab the
# row came from ("player" / "staff"), and the player-only columns are blank for staff.
ROSTER_FIELDS = [
    "school_id", "school_name", "state", "sport", "gender", "season", "level",
    "category", "row_index", "jersey_number", "name", "grade", "position",
    "height", "weight", "roster_url",
]
LIST_FIELDS = {"sports"}  # stored as "a; b; c" text in SQLite -> list in JSON


def _export_table(db, table, fields, csv_path, json_path=None):
    """Write one table to CSV (and JSON when ``json_path`` is given). Atomic per file."""
    rows = db.execute(f"SELECT {','.join(fields)} FROM {table}").fetchall()

    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(fields)
        w.writerows(rows)
    os.replace(tmp, csv_path)

    if not json_path:
        return len(rows)

    with open(json_path, "w", encoding="utf-8") as fh:
        fh.write("[")
        for i, row in enumerate(rows):
            rec = dict(zip(fields, row))
            for lf in LIST_FIELDS:
                if lf in rec and rec[lf] is not None:
                    rec[lf] = [s for s in str(rec[lf]).split("; ") if s]
            fh.write(("\n  " if i == 0 else ",\n  ") + json.dumps(rec, ensure_ascii=False, default=str))
        fh.write("\n]\n" if rows else "]\n")

    return len(rows)


# Default output basenames (the canonical CLI files). The CSV-seed worker passes the API
# names instead (max_prep_School / max_prep_schedule / max_prep_roster).
DEFAULT_NAMES = ("schools", "schedule", "roster")


def export_db(output_dir="output", db_file="maxpreps.db", names=DEFAULT_NAMES, write_json=True):
    """Rebuild schools/schedule/roster CSV (+ JSON) from the SQLite DB.

    ``names`` are the three output basenames, in schools/schedule/roster order.
    Returns ``(n_schools, n_games, n_roster)``.
    """
    db_path = os.path.join(output_dir, db_file)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"No database at {db_path}")
    db = sqlite3.connect(db_path, timeout=60)
    try:
        counts = []
        for table, fields, name in zip(("schools", "games", "roster"),
                                       (SCHOOL_FIELDS, GAME_FIELDS, ROSTER_FIELDS), names):
            counts.append(_export_table(
                db, table, fields,
                os.path.join(output_dir, f"{name}.csv"),
                os.path.join(output_dir, f"{name}.json") if write_json else None,
            ))
    finally:
        db.close()
    return tuple(counts)


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "output"
    s, g, r = export_db(out)
    print(f"Exported {s} schools, {g} games and {r} roster rows "
          f"from {out}/maxpreps.db -> CSV + JSON")
