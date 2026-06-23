"""Durable, resume-safe output.

SQLite is the **canonical** store: rows are written with ``INSERT OR REPLACE`` and
committed frequently, so it survives interruptions and resumes cleanly. The CSV is
appended live (so you can watch it grow, and a resume never truncates it). JSON —
and a clean, authoritative copy of the CSV — are regenerated from the database when
the spider closes. You can also rebuild CSV/JSON from the DB anytime with
``python -m maxpreps_scraper.export``.
"""
import csv
import os
import sqlite3

from itemadapter import ItemAdapter

from .export import GAME_FIELDS, SCHOOL_FIELDS, export_db
from .items import ScheduleGameItem, SchoolItem

LIST_FIELDS = {"sports"}  # lists -> "a; b; c" for the flat sinks
COMMIT_EVERY = 100


class MultiFormatPipeline:
    def __init__(self, output_dir, db_file):
        self.output_dir = output_dir
        self.db_file = db_file

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            crawler.settings.get("OUTPUT_DIR", "output"),
            crawler.settings.get("SQLITE_FILE", "maxpreps.db"),
        )

    # ------------------------------------------------------------------ #
    def open_spider(self, spider):
        os.makedirs(self.output_dir, exist_ok=True)

        # CSV writers — append if the file already has rows (resume-safe)
        self._csv_files, self._csv_writers = {}, {}
        for kind, fields, name in (
            ("school", SCHOOL_FIELDS, "schools"),
            ("game", GAME_FIELDS, "schedule"),
        ):
            path = os.path.join(self.output_dir, f"{name}.csv")
            resuming = os.path.exists(path) and os.path.getsize(path) > 0
            fh = open(path, "a" if resuming else "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            if not resuming:
                writer.writeheader()
            self._csv_files[kind] = fh
            self._csv_writers[kind] = writer

        # SQLite — canonical store
        self._db = sqlite3.connect(os.path.join(self.output_dir, self.db_file))
        self._init_db()
        self._counts = {"school": 0, "game": 0}

    def _init_db(self):
        cur = self._db.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS schools ("
            + ", ".join(f'"{f}" TEXT' for f in SCHOOL_FIELDS)
            + ', PRIMARY KEY ("school_id"))'
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS games ("
            + ", ".join(f'"{f}" TEXT' for f in GAME_FIELDS)
            + ', PRIMARY KEY ("school_id", "schedule_url", "game_index"))'
        )
        self._db.commit()

    # ------------------------------------------------------------------ #
    def process_item(self, item, spider):
        if isinstance(item, SchoolItem):
            self._write("school", "schools", SCHOOL_FIELDS, item)
        elif isinstance(item, ScheduleGameItem):
            self._write("game", "games", GAME_FIELDS, item)
        return item

    def _write(self, kind, table, fields, item):
        adapter = ItemAdapter(item)
        flat = {}
        for f in fields:
            v = adapter.get(f)
            flat[f] = "; ".join(v) if f in LIST_FIELDS and isinstance(v, list) else v

        self._csv_writers[kind].writerow(flat)
        self._db.execute(
            f'INSERT OR REPLACE INTO {table} ({",".join(chr(34)+f+chr(34) for f in fields)}) '
            f'VALUES ({",".join("?" for _ in fields)})',
            [flat.get(f) for f in fields],
        )
        self._counts[kind] += 1
        if (self._counts[kind] % COMMIT_EVERY) == 0:
            self._db.commit()
            self._csv_files[kind].flush()

    # ------------------------------------------------------------------ #
    def close_spider(self, spider):
        for fh in self._csv_files.values():
            fh.close()
        self._db.commit()
        self._db.close()
        # rebuild clean, de-duplicated CSV + JSON from the canonical DB
        try:
            n_s, n_g = export_db(self.output_dir, self.db_file)
            spider.logger.info(
                "Finalized %d schools and %d games -> CSV + JSON (from SQLite)", n_s, n_g
            )
        except Exception as exc:  # never let export failure mask a finished crawl
            spider.logger.warning("Export from DB failed (%s); CSV still has appended rows", exc)
