#!/usr/bin/env python
"""Add each school's GoFan ticket link to a schools CSV, in place.

GoFan (https://gofan.co) is a separate ticketing site. There's no shared key with
MaxPreps, so we resolve the link by matching each school's **name + state** against
GoFan's own catalog. The catalog (~25.7k schools) is downloaded once, cached, and
matched locally. Three columns are appended to the CSV:

    gofan_ticket_url   https://gofan.co/app/school/{id}   (or empty)
    gofan_school_id    the GoFan "huddleId"               (or empty)
    gofan_match        exact | fuzzy | none

Many MaxPreps schools simply aren't on GoFan (it only lists schools that sell tickets
there), so blanks (``gofan_match=none``) are expected and correct.

Usage:
    python enrich_gofan.py output/ahmad.csv
    python enrich_gofan.py output/schools.csv
    python enrich_gofan.py output/ahmad.csv --refresh   # re-download the catalog
"""
import csv
import difflib
import json
import os
import re
import sys
import time
import urllib.request

API = "https://api.gofan.co/v2/schools"
CATALOG_CACHE = os.path.join("output", "gofan_catalog.json")
TICKET_URL = "https://gofan.co/app/school/{}"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
NEW_COLUMNS = ["gofan_ticket_url", "gofan_school_id", "gofan_match"]
FUZZY_CUTOFF = 0.87
PAGE_SIZE = 2000

# generic tokens dropped from a school name; distinguishing words (academy, christian,
# charter, catholic, ...) are intentionally kept so we don't merge different schools.
_DROP = re.compile(r"\b(high\s+school|high|hs|school)\b")
_PUNCT = re.compile(r"[^a-z0-9 ]")
_WS = re.compile(r"\s+")


def _get(url, tries=4):
    """GET a URL, returning parsed JSON, with simple backoff on transient errors."""
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except Exception as exc:  # noqa: BLE001 - retry anything transient
            last = exc
            time.sleep(1.5 * (i + 1))
    raise last


def normalize(name):
    s = (name or "").lower()
    s = _PUNCT.sub(" ", s)
    if s.strip().startswith("the "):
        s = s.strip()[4:]
    s = _DROP.sub(" ", s)
    return _WS.sub(" ", s).strip()


def load_catalog(refresh=False):
    """Return the full GoFan catalog [{huddleId,name,state}, ...], cached on disk."""
    if not refresh and os.path.exists(CATALOG_CACHE):
        with open(CATALOG_CACHE, encoding="utf-8") as fh:
            cat = json.load(fh)
        print(f"Loaded {len(cat)} GoFan schools from cache ({CATALOG_CACHE})")
        return cat

    print("Downloading GoFan catalog ...")
    cat, page = [], 0
    while True:
        d = _get(f"{API}/?page={page}&size={PAGE_SIZE}")
        cat.extend(d.get("content") or [])
        if d.get("last") or not d.get("content"):
            break
        page += 1
        time.sleep(0.4)
    os.makedirs(os.path.dirname(CATALOG_CACHE) or ".", exist_ok=True)
    with open(CATALOG_CACHE, "w", encoding="utf-8") as fh:
        json.dump(cat, fh)
    print(f"Downloaded {len(cat)} GoFan schools across {page + 1} pages -> {CATALOG_CACHE}")
    return cat


def build_index(catalog):
    """state -> list of {norm, name, id}."""
    by_state = {}
    for c in catalog:
        st = (c.get("state") or "").upper()
        by_state.setdefault(st, []).append(
            {"norm": normalize(c.get("name")), "name": c.get("name"), "id": c.get("huddleId")}
        )
    return by_state


class Matcher:
    def __init__(self, by_state):
        self.by_state = by_state
        self._detail = {}          # huddleId -> detail dict (memoized)
        self.detail_fetches = 0

    def detail(self, school_id):
        if school_id not in self._detail:
            self.detail_fetches += 1
            try:
                self._detail[school_id] = _get(f"{API}/{school_id}")
            except Exception:      # noqa: BLE001 - treat as no extra info
                self._detail[school_id] = {}
            time.sleep(0.15)
        return self._detail[school_id]

    @staticmethod
    def _agrees(detail, row, require_positive=False):
        """True if a candidate's city or zip corroborates the CSV row.

        Matching mirrors the GoFan search UI, which shows "City, ST" under every
        result: a candidate is accepted only when its city (or zip) lines up with
        the row's. A row zip equal to the candidate's zip, or a city that matches
        (exactly or on a 4-char prefix, to absorb "Flemington" vs "Fleming"-style
        spellings), corroborates the match.

        Edge cases:
          * Row has no city AND no zip   -> nothing to check, don't block.
          * Candidate detail has no city AND no zip -> unverifiable; don't block
            (name+state already identifies it and there's nothing to contradict).

        ``require_positive=True`` drops both permissive fallbacks: the candidate is
        accepted ONLY on a real zip/city hit. Used for weak name matches (e.g. a
        single-word school name) where city is the only thing keeping it honest.
        """
        city = (detail.get("city") or "").strip().lower()
        zc = (detail.get("zipCode") or "").strip()[:5]
        row_city = (row.get("city") or "").strip().lower()
        row_zip = (row.get("zip_code") or "").strip()[:5]
        if row_zip and zc and row_zip == zc:
            return True
        if row_city and city and (row_city == city or row_city[:4] == city[:4]):
            return True
        if require_positive:
            return False  # weak name match -> demand a real city/zip corroboration
        # Nothing on one side to compare against -> can't disprove, so allow it.
        if not (row_city or row_zip):
            return True
        if not (city or zc):
            return True
        # Both sides carry a city/zip but they disagree -> reject (wrong city).
        return False

    def match(self, row):
        """Return (huddleId, match_type) for a CSV row, or (None, 'none').

        Both ``state`` and ``city`` are verified before a match is returned:

          * **state** is enforced structurally -- candidates are only ever drawn
            from ``by_state[st]``, so a match can never cross state lines.
          * **city** (or zip) is checked via ``_agrees`` for *every* candidate,
            including a lone exact name match. A school whose name matches in the
            right state but whose city disagrees is rejected, so we never write a
            ticket URL for a same-named school in the wrong city.
        """
        st = (row.get("state") or "").strip().upper()
        n = normalize(row.get("name"))
        if not st or not n:
            return None, "none"
        cands = self.by_state.get(st, [])

        # Tier 1 -- exact name matches in-state, each gated on city/zip agreement.
        exact = [c for c in cands if c["norm"] == n]
        for c in exact:
            if self._agrees(self.detail(c["id"]), row):
                return c["id"], "exact"
        if exact:
            return None, "none"  # right name+state, but no candidate's city agreed

        # Tier 2 -- token containment. The row's words are the full set of a longer
        # catalog name (or vice-versa), e.g. "Little Snake River" vs "Little Snake
        # River Valley School", "St. Stephens" vs "St. Stephens Indian High School",
        # or "East" vs "Cheyenne East High School". difflib's whole-string ratio
        # penalises the length gap below the cutoff, so handle this subset case
        # explicitly. Candidates are ordered by fewest extra words and gated on
        # city/zip. A single-word row name is weak, so for it we demand a positive
        # city/zip hit (require_positive) -- the city is the only safeguard.
        row_toks = set(n.split())
        strict = len(row_toks) < 2
        subset = []
        for c in cands:
            cat_toks = set(c["norm"].split())
            if not cat_toks:
                continue
            if row_toks <= cat_toks or cat_toks <= row_toks:
                subset.append((abs(len(cat_toks) - len(row_toks)), c))
        for _extra, c in sorted(subset, key=lambda t: t[0]):
            if self._agrees(self.detail(c["id"]), row, require_positive=strict):
                return c["id"], "fuzzy"

        # Tier 3 -- fuzzy difflib match within the state, also verified by city/zip.
        names = [c["norm"] for c in cands]
        close = difflib.get_close_matches(n, names, n=1, cutoff=FUZZY_CUTOFF)
        if close:
            c = next(c for c in cands if c["norm"] == close[0])
            if self._agrees(self.detail(c["id"]), row):
                return c["id"], "fuzzy"
        return None, "none"


def enrich_csv(path, matcher):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fields = list(reader.fieldnames or [])

    out_fields = fields + [c for c in NEW_COLUMNS if c not in fields]  # idempotent

    counts = {"exact": 0, "fuzzy": 0, "none": 0}
    for row in rows:
        school_id, kind = matcher.match(row)
        row["gofan_ticket_url"] = TICKET_URL.format(school_id) if school_id else ""
        row["gofan_school_id"] = school_id or ""
        row["gofan_match"] = kind
        counts[kind] += 1

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)

    total = len(rows)
    matched = counts["exact"] + counts["fuzzy"]
    print(
        f"{path}: {total} rows | exact {counts['exact']} | fuzzy {counts['fuzzy']} "
        f"| none {counts['none']} | matched {matched} ({matched * 100 // total if total else 0}%) "
        f"| detail fetches {matcher.detail_fetches}"
    )


def main():
    argv = [a for a in sys.argv[1:] if a != "--refresh"]
    refresh = "--refresh" in sys.argv[1:]
    if not argv:
        print("usage: python enrich_gofan.py <csv-path> [--refresh]", file=sys.stderr)
        sys.exit(2)
    csv_path = argv[0]
    if not os.path.exists(csv_path):
        print(f"no such file: {csv_path}", file=sys.stderr)
        sys.exit(1)

    catalog = load_catalog(refresh=refresh)
    matcher = Matcher(build_index(catalog))
    enrich_csv(csv_path, matcher)


if __name__ == "__main__":
    main()
