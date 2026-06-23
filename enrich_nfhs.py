#!/usr/bin/env python
"""Add each school's NFHS Network link to a schools CSV, in place.

NFHS Network (https://www.nfhsnetwork.com) is, like GoFan, a PlayOnSports property,
and its school catalog carries a ``gofan_id`` field. So resolution is mostly an exact
join: if a row already has a ``gofan_school_id`` (added by enrich_gofan.py), we look up
the matching NFHS school directly and take its real ``slug``. Rows without a GoFan id
fall back to name + state matching (disambiguated by the catalog's own city).

The whole NFHS catalog (~25k schools) is downloaded once via the public search API,
cached, and matched locally — no per-school page requests, so links come straight from
NFHS's own data rather than being guessed. Three columns are appended:

    nfhs_url     https://www.nfhsnetwork.com/schools/{slug}   (or empty)
    nfhs_slug    the NFHS slug                                (or empty)
    nfhs_match   gofan_id | exact | fuzzy | none

Usage:
    python enrich_nfhs.py output/ahmad.csv
    python enrich_nfhs.py output/schools.csv
    python enrich_nfhs.py output/ahmad.csv --refresh   # re-download the catalog
"""
import csv
import difflib
import json
import os
import re
import sys
import time

import requests

from maxpreps_scraper.states import ALL_STATE_CODES

API = "https://search-api.nfhsnetwork.com/search/schools"
CATALOG_CACHE = os.path.join("output", "nfhs_catalog.json")
PROGRESS_CACHE = os.path.join("output", "nfhs_catalog.progress.json")
SCHOOL_URL = "https://www.nfhsnetwork.com/schools/{}"
# The search API is Elasticsearch-backed: offset paging is capped at from+size<=10000,
# there is no working cursor/sort/text-search, and the `state` filter returns only NFHS's
# curated subset. So we assemble the catalog from the two reachable slices and dedupe:
#   (a) the full catalog's first 10k rows (alphabetical), and
#   (b) every per-state result (each state is well under the 10k cap).
OFFSET_CAP = 10000
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nfhsnetwork.com",
    "Referer": "https://www.nfhsnetwork.com/",
}
NEW_COLUMNS = ["nfhs_url", "nfhs_slug", "nfhs_match"]
FUZZY_CUTOFF = 0.87
PAGE_SIZE = 500  # 1000 is the hard max, but large-total states (e.g. CA) 400 at >~900;
# 500 paginates every state reliably. (Still well under the from+size<=10000 offset cap.)

_DROP = re.compile(r"\b(high\s+school|high|hs|school)\b")
_PUNCT = re.compile(r"[^a-z0-9 ]")
_WS = re.compile(r"\s+")


def _get(params, tries=8):
    # The API blocks a connection with 400s after a burst, and the block sticks to that
    # connection. So we open a FRESH connection (Connection: close) every call and back
    # off patiently. Combined with the resumable catalog build below, this completes
    # reliably even when the limiter trips mid-run.
    headers = {**HEADERS, "Connection": "close"}
    last = None
    for i in range(tries):
        try:
            resp = requests.get(API, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            last = RuntimeError(f"HTTP {resp.status_code}")
        except Exception as exc:  # noqa: BLE001 - retry anything transient
            last = exc
        time.sleep(min(8 * (i + 1), 45))
    raise last


def normalize(name):
    s = (name or "").lower()
    s = _PUNCT.sub(" ", s)
    if s.strip().startswith("the "):
        s = s.strip()[4:]
    s = _DROP.sub(" ", s)
    return _WS.sub(" ", s).strip()


def _trim(i):
    return {
        "slug": i.get("slug"),
        "name": i.get("name"),
        "city": i.get("city"),
        "state_code": i.get("state_code"),
        "gofan_id": i.get("gofan_id"),
    }


def _page_through(params, stop_at=None):
    """Yield trimmed items, paging with start-offset until exhausted or the offset cap."""
    start = 0
    while True:
        d = _get({**params, "start": start, "size": PAGE_SIZE})
        items = d.get("items") or []
        total = d.get("total")
        for i in items:
            if i.get("slug"):
                yield _trim(i)
        start += len(items)
        limit = total if stop_at is None else min(total or 0, stop_at)
        if not items or (total is not None and start >= limit):
            break
        if start + PAGE_SIZE > OFFSET_CAP:  # Elasticsearch from+size<=10000
            break
        time.sleep(1.0)  # steady, unhurried cadence keeps us under the rate threshold


def _save_progress(by_slug, done_steps):
    os.makedirs(os.path.dirname(CATALOG_CACHE) or ".", exist_ok=True)
    with open(PROGRESS_CACHE, "w", encoding="utf-8") as fh:
        json.dump({"done": sorted(done_steps), "schools": list(by_slug.values())}, fh)


def load_catalog(refresh=False):
    """Return the NFHS catalog as [{slug,name,city,state_code,gofan_id}, ...], cached.

    Assembled (and deduped by slug) from the two reachable slices of the search API:
    the full catalog's first 10k rows plus every per-state result. The build is
    RESUMABLE -- progress is checkpointed after the full slice and after each state, so
    a run interrupted by the rate-limiter just continues where it left off on re-run.
    """
    if not refresh and os.path.exists(CATALOG_CACHE):
        with open(CATALOG_CACHE, encoding="utf-8") as fh:
            cat = json.load(fh)
        print(f"Loaded {len(cat)} NFHS schools from cache ({CATALOG_CACHE})")
        return cat, True

    # resume from a partial checkpoint if one exists
    by_slug, done = {}, set()
    if not refresh and os.path.exists(PROGRESS_CACHE):
        with open(PROGRESS_CACHE, encoding="utf-8") as fh:
            p = json.load(fh)
        for rec in p.get("schools", []):
            by_slug[rec["slug"]] = rec
        done = set(p.get("done", []))
        print(f"Resuming catalog build: {len(by_slug)} schools, {len(done)} steps done")

    steps = ["__full__"] + [c.upper() for c in ALL_STATE_CODES]
    for n, step in enumerate(steps, 1):
        if step in done:
            continue
        params = {} if step == "__full__" else {"state": step}
        stop = OFFSET_CAP if step == "__full__" else None
        try:
            fresh = {}
            for rec in _page_through(params, stop_at=stop):
                fresh[rec["slug"]] = rec
            for slug, rec in fresh.items():
                by_slug.setdefault(slug, rec)
            done.add(step)
            _save_progress(by_slug, done)
            print(f"  [{n}/{len(steps)}] {step}: {len(by_slug)} unique schools")
        except Exception as exc:  # noqa: BLE001 - skip; a re-run resumes this step
            print(f"  [{n}/{len(steps)}] {step}: FAILED ({exc}); will retry on re-run")
        time.sleep(0.5)

    missing = [s for s in steps if s not in done]
    if missing:
        # don't finalize a partial catalog as complete; keep the checkpoint for re-run
        print(f"Incomplete: {len(missing)} step(s) still pending {missing[:8]}"
              f"{' ...' if len(missing) > 8 else ''}. Re-run to finish.")
        return list(by_slug.values()), False

    cat = list(by_slug.values())
    with open(CATALOG_CACHE, "w", encoding="utf-8") as fh:
        json.dump(cat, fh)
    if os.path.exists(PROGRESS_CACHE):
        os.remove(PROGRESS_CACHE)
    print(f"Downloaded {len(cat)} unique NFHS schools -> {CATALOG_CACHE}")
    return cat, True


def build_indexes(catalog):
    """Return (by_gofan_id: id->slug, by_state: STATE->[{norm,name,city,slug}])."""
    by_gofan_id, by_state = {}, {}
    for c in catalog:
        if not c.get("slug"):
            continue
        gid = c.get("gofan_id")
        if gid and gid not in by_gofan_id:
            by_gofan_id[gid] = c["slug"]
        st = (c.get("state_code") or "").upper()
        by_state.setdefault(st, []).append({
            "norm": normalize(c.get("name")),
            "name": c.get("name"),
            "city": (c.get("city") or "").strip().lower(),
            "slug": c["slug"],
        })
    return by_gofan_id, by_state


def _city_agrees(cand_city, row_city):
    """True if cities corroborate, or there's nothing to check against."""
    if not row_city or not cand_city:
        return True
    return row_city == cand_city or row_city[:4] == cand_city[:4]


class Matcher:
    def __init__(self, by_gofan_id, by_state):
        self.by_gofan_id = by_gofan_id
        self.by_state = by_state

    def match(self, row):
        # 1) exact join on the GoFan id we already stored
        gid = (row.get("gofan_school_id") or "").strip()
        if gid and gid in self.by_gofan_id:
            return self.by_gofan_id[gid], "gofan_id"

        st = (row.get("state") or "").strip().upper()
        n = normalize(row.get("name"))
        if not st or not n:
            return None, "none"
        cands = self.by_state.get(st, [])
        row_city = (row.get("city") or "").strip().lower()

        exact = [c for c in cands if c["norm"] == n]
        if len(exact) == 1:
            return exact[0]["slug"], "exact"
        if len(exact) > 1:
            for c in exact:
                if _city_agrees(c["city"], row_city):
                    return c["slug"], "exact"
            return exact[0]["slug"], "exact"

        close = difflib.get_close_matches(n, [c["norm"] for c in cands], n=1, cutoff=FUZZY_CUTOFF)
        if close:
            c = next(c for c in cands if c["norm"] == close[0])
            if _city_agrees(c["city"], row_city):
                return c["slug"], "fuzzy"
        return None, "none"


def enrich_csv(path, matcher):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fields = list(reader.fieldnames or [])

    out_fields = fields + [c for c in NEW_COLUMNS if c not in fields]  # idempotent

    counts = {"gofan_id": 0, "exact": 0, "fuzzy": 0, "none": 0}
    for row in rows:
        slug, kind = matcher.match(row)
        row["nfhs_url"] = SCHOOL_URL.format(slug) if slug else ""
        row["nfhs_slug"] = slug or ""
        row["nfhs_match"] = kind
        counts[kind] += 1

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)

    total = len(rows)
    matched = total - counts["none"]
    print(
        f"{path}: {total} rows | gofan_id {counts['gofan_id']} | exact {counts['exact']} "
        f"| fuzzy {counts['fuzzy']} | none {counts['none']} "
        f"| matched {matched} ({matched * 100 // total if total else 0}%)"
    )


def main():
    args = [a for a in sys.argv[1:] if a != "--refresh"]
    refresh = "--refresh" in sys.argv[1:]
    if not args:
        print("usage: python enrich_nfhs.py <csv-path> [--refresh]", file=sys.stderr)
        sys.exit(2)
    csv_path = args[0]
    if not os.path.exists(csv_path):
        print(f"no such file: {csv_path}", file=sys.stderr)
        sys.exit(1)

    catalog, complete = load_catalog(refresh=refresh)
    if not complete:
        print("Catalog build is incomplete (rate-limited). Progress is saved; "
              "just run this command again to resume and finish. CSV not modified.",
              file=sys.stderr)
        sys.exit(3)
    by_gofan_id, by_state = build_indexes(catalog)
    print(f"index: {len(by_gofan_id)} schools with a gofan_id join key")
    enrich_csv(csv_path, Matcher(by_gofan_id, by_state))


if __name__ == "__main__":
    main()
