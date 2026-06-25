#!/usr/bin/env python
"""Add each school's NFHS Network link to a schools CSV, in place.

This is the final enrichment step, run AFTER the GoFan step. For each row it resolves the
school's NFHS Network page by matching ``original_name`` against NFHS's own catalog, gated
on BOTH ``state`` and ``city`` -- exactly like the GoFan step, mirroring the "School Name -
City, ST" line NFHS shows under each search result. One new column is appended:

    nfhs_url     https://www.nfhsnetwork.com/schools/{slug}   (or empty)

How the link is found
---------------------
The whole NFHS catalog (~16k schools) is downloaded once via the public search API,
cached, and matched locally -- no per-school page requests, so the slug comes straight
from NFHS's own data rather than being guessed. Matching is state-scoped (a candidate can
only come from the row's state) and every candidate's city must agree with the row's, so a
same-named school in a different city is never picked. Search value is ``original_name``
(the GoFan-verified name from the previous step), falling back to the row's ``name``.

Usage:
    python enrich_nfhs.py output/max_prep_School.csv
    python enrich_nfhs.py output/max_prep_School.csv --refresh   # re-download the catalog
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
NEW_COLUMN = "nfhs_url"
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


def build_index(catalog):
    """state -> [{norm, name, city, slug}]."""
    by_state = {}
    for c in catalog:
        if not c.get("slug"):
            continue
        st = (c.get("state_code") or "").upper()
        by_state.setdefault(st, []).append({
            "norm": normalize(c.get("name")),
            "name": c.get("name"),
            "city": (c.get("city") or "").strip().lower(),
            "slug": c["slug"],
        })
    return by_state


def _city_agrees(cand_city, row_city, require_positive=False):
    """True if a candidate's city corroborates the row's.

    A city matches exactly or on a 4-char prefix (absorbing "St."/"Saint"-style and
    "Flemington" vs "Fleming" variants). When one side has no city there's nothing to
    check, so the match isn't blocked -- unless ``require_positive`` is set (used for weak
    name matches, e.g. a single-word school name, where city is the only safeguard).
    """
    if row_city and cand_city:
        return row_city == cand_city or row_city[:4] == cand_city[:4]
    return not require_positive


class Matcher:
    def __init__(self, by_state):
        self.by_state = by_state

    def match(self, name, state, city):
        """Return (slug, match_type) for a name+state+city, or (None, 'none').

        State is enforced structurally (candidates only come from ``by_state[st]``) and
        city is verified for every candidate, mirroring the GoFan matcher.
        """
        st = (state or "").strip().upper()
        n = normalize(name)
        if not st or not n:
            return None, "none"
        cands = self.by_state.get(st, [])
        row_city = (city or "").strip().lower()

        # Tier 1 -- exact name match in-state, city-gated.
        exact = [c for c in cands if c["norm"] == n]
        for c in exact:
            if _city_agrees(c["city"], row_city):
                return c["slug"], "exact"
        if exact:
            return None, "none"

        # Tier 2 -- token containment ("Little Snake River" vs "Little Snake River Valley
        # School", "East" vs "Cheyenne East High School"). A single-word row name is weak,
        # so it demands a positive city hit. Ordered by fewest extra words.
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
            if _city_agrees(c["city"], row_city, require_positive=strict):
                return c["slug"], "fuzzy"

        # Tier 3 -- fuzzy difflib match within the state, city-gated.
        close = difflib.get_close_matches(n, [c["norm"] for c in cands], n=1, cutoff=FUZZY_CUTOFF)
        if close:
            c = next(c for c in cands if c["norm"] == close[0])
            if _city_agrees(c["city"], row_city):
                return c["slug"], "fuzzy"
        return None, "none"


def candidate_slug(matcher, row):
    """NFHS slug for a row, searching original_name first, then name. Or None."""
    state, city = row.get("state"), row.get("city")
    original = (row.get("original_name") or "").strip()
    name = (row.get("name") or "").strip()

    if original:
        slug, _kind = matcher.match(original, state, city)
        if slug:
            return slug
    if name and name != original:
        slug, _kind = matcher.match(name, state, city)
        if slug:
            return slug
    return None


def enrich_csv(path, matcher):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fields = list(reader.fieldnames or [])

    out_fields = fields + ([] if NEW_COLUMN in fields else [NEW_COLUMN])  # idempotent

    matched = 0
    for row in rows:
        slug = candidate_slug(matcher, row)
        row[NEW_COLUMN] = SCHOOL_URL.format(slug) if slug else ""
        if slug:
            matched += 1

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)

    total = len(rows)
    print(
        f"{path}: {total} rows | nfhs matched {matched} | empty {total - matched} "
        f"({matched * 100 // total if total else 0}%)"
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
    enrich_csv(csv_path, Matcher(build_index(catalog)))


if __name__ == "__main__":
    main()
