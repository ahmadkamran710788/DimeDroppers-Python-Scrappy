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
    "mascot", "address", "zip_code", "phone",
    "color1", "color2", "color3", "mascot_url",
    "league_name", "association_name", "governing_body_name", "governing_body_url",
    "website", "facebook", "instagram", "twitter", "youtube",
    "sports", "sports_count", "discovered_via",
]
GAME_FIELDS = [
    "school_id", "school_name", "state", "sport", "gender", "season",
    "game_index", "date", "home_away", "opponent", "opponent_url",
    "result", "score", "game_info", "schedule_url",
]
LIST_FIELDS = {"sports"}  # stored as "a; b; c" text in SQLite -> list in JSON


def _export_table(db, table, fields, csv_path, json_path):
    rows = db.execute(f"SELECT {','.join(fields)} FROM {table}").fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(fields)
        w.writerows(rows)

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


def export_db(output_dir="output", db_file="maxpreps.db"):
    """Rebuild schools.{csv,json} and schedule.{csv,json} from the SQLite DB."""
    db_path = os.path.join(output_dir, db_file)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"No database at {db_path}")
    db = sqlite3.connect(db_path)
    try:
        n_schools = _export_table(
            db, "schools", SCHOOL_FIELDS,
            os.path.join(output_dir, "schools.csv"),
            os.path.join(output_dir, "schools.json"),
        )
        n_games = _export_table(
            db, "games", GAME_FIELDS,
            os.path.join(output_dir, "schedule.csv"),
            os.path.join(output_dir, "schedule.json"),
        )
    finally:
        db.close()
    return n_schools, n_games


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "output"
    s, g = export_db(out)
    print(f"Exported {s} schools and {g} games from {out}/maxpreps.db -> CSV + JSON")
