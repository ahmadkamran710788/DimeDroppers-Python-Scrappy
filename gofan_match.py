#!/usr/bin/env python
"""Pick the right GoFan school for one uploaded CSV row.

Pure functions, no network -- ``gofan_client`` does the fetching, this decides. That
split keeps the matching rules testable without hitting GoFan.

The rules mirror what a person does with the GoFan search box: type the school name,
then read the "City, ST" line under each result to tell same-named schools apart.
So a candidate must clear three gates:

  1. **state** -- structural. Candidates whose state differs are dropped outright, so
     a match can never cross state lines.
  2. **city / zip** -- ``agrees()``. A candidate in the right state but the wrong city
     is rejected; we never write a link for a same-named school two states' worth of
     highway away.
  3. **school type** -- preference, not a gate. GoFan's ``industryCode`` tells us
     whether a record is a middle school, and a real middle-school record is always
     preferred. But a middle school's ticketing often lives on the district's or the
     high school's GoFan page, so when no middle-school record agrees we fall back to
     any agreeing record rather than losing the row. ``match_type`` records which
     happened, so the CSV stays auditable.

Column note: match on ``MSTATE`` (or ``ST``), not ``STATENAME``. The CSV's
``STATENAME`` is "ALABAMA" while GoFan returns "AL"; ``MSTATE`` already holds the
two-letter code. ``STATENAME`` is used only as a last-resort fallback via NAME_TO_CODE.
"""
import difflib
import re

from maxpreps_scraper.states import STATES

# "ALABAMA" -> "AL". Only consulted when MSTATE and ST are both blank.
NAME_TO_CODE = {name.upper(): code.upper() for code, name in STATES.items()}

# Tokens that carry no distinguishing information on their own. Unlike
# enrich_gofan.normalize (which strips "high school"), "middle"/"junior"/"intermediate"
# are deliberately KEPT below -- they are exactly what separates a middle school from
# the same-named high school, which is the distinction this whole module exists to make.
_DROP = re.compile(r"\b(school|schools)\b")
_PUNCT = re.compile(r"[^a-z0-9 ]")
_WS = re.compile(r"\s+")

# industryCode values that mean "this really is a middle school".
_MIDDLE = ("middle", "junior", "jr high", "intermediate")

# Minimum normalized-name similarity for a candidate whose token set is neither a
# subset nor a superset of the row's.
#
# This floor exists because GoFan's search is fuzzy on more than the name: querying
# "Alexander City Middle School" returns "William J. Radney Junior High School",
# which is in the same city AND the same zip, so state and city/zip both pass. Without
# a name bar we would happily write that link. Measured over a 60-row Alabama sample,
# 36 of 37 accepted matches scored exactly 1.00 and the lone false positive scored
# 0.24 -- a gap wide enough that the exact cutoff barely matters. 0.62 sits in the
# middle of it and still admits real spelling drift ("Saint"/"St.", "Gulf Shores
# Middle" vs "Gulf Shores Middle School").
#
# A school GoFan lists under a genuinely different name is therefore left unmatched.
# That is the intended trade: a wrong ticket link is worse than a blank one, and
# gofan_match/gofan_match_score keep every decision auditable.
NAME_FLOOR = 0.62


def normalize(name):
    """Lowercase, strip punctuation and a generic trailing 'school'."""
    s = (name or "").lower()
    s = _PUNCT.sub(" ", s)
    s = s.strip()
    if s.startswith("the "):
        s = s[4:]
    s = _DROP.sub(" ", s)
    return _WS.sub(" ", s).strip()


def row_state(row):
    """Two-letter state code for an uploaded row: MSTATE -> ST -> STATENAME."""
    for key in ("MSTATE", "ST"):
        v = (row.get(key) or "").strip().upper()
        if len(v) == 2:
            return v
    return NAME_TO_CODE.get((row.get("STATENAME") or "").strip().upper(), "")


def is_middle(candidate):
    """True if GoFan classifies this record as a middle / junior high school."""
    code = " ".join(
        str(candidate.get(k) or "") for k in ("industryCode", "gofanSchoolType")
    ).lower()
    return any(m in code for m in _MIDDLE)


def agrees(candidate, row):
    """True if the candidate's city or zip corroborates the row.

    Ported from ``enrich_gofan.Matcher._agrees``. A zip5 match, or a city that is
    equal or shares a 4-character prefix (absorbing "Saint"/"St" and truncation
    differences), counts as corroboration. When one side has neither a city nor a
    zip there is nothing to contradict, so we allow it -- name+state already
    identifies the school. Only an actual city/zip *disagreement* rejects.
    """
    city = (candidate.get("city") or "").strip().lower()
    zc = (candidate.get("zipCode") or "").strip()[:5]
    row_city = (row.get("MCITY") or row.get("LCITY") or "").strip().lower()
    row_zip = str(row.get("MZIP") or row.get("LZIP") or "").strip()[:5]

    if row_zip and zc and row_zip == zc:
        return True
    if row_city and city and (row_city == city or row_city[:4] == city[:4]):
        return True
    if not (row_city or row_zip):
        return True
    if not (city or zc):
        return True
    return False


def name_score(row_name, candidate_name):
    """How strongly two school names agree: (passes_floor, similarity).

    Token containment counts as agreement regardless of ratio, because a longer
    catalog name that fully contains the row's words ("Gulf Shores Middle" vs "Gulf
    Shores Middle School") is the same school with extra words -- and difflib's
    whole-string ratio unfairly penalises that length gap. Otherwise the similarity
    must clear NAME_FLOOR.
    """
    a, b = normalize(row_name), normalize(candidate_name)
    if not a or not b:
        return False, 0.0
    sim = difflib.SequenceMatcher(None, a, b).ratio()
    ta, tb = set(a.split()), set(b.split())
    return (ta <= tb or tb <= ta or sim >= NAME_FLOOR), sim


def pick(candidates, row):
    """Choose the best GoFan school for a row.

    Returns ``(candidate, match_type, score)`` where match_type is:
        "middle" -- a middle/junior-high record that cleared every gate (ideal)
        "city"   -- some other record (high school, district) that cleared them
        "none"   -- nothing survived; candidate is None, score 0.0
    """
    st = row_state(row)
    target = (row.get("SCH_NAME") or "").strip()
    if not st or not normalize(target) or not candidates:
        return None, "none", 0.0

    scored = []
    for c in candidates:
        if (c.get("state") or "").strip().upper() != st:
            continue  # gate 1: state, structural -- never cross state lines
        if not agrees(c, row):
            continue  # gate 2: city / zip must corroborate
        ok, sim = name_score(target, c.get("name"))
        if not ok:
            continue  # gate 3: the names must actually resemble each other
        scored.append((is_middle(c), sim, c))

    if not scored:
        return None, "none", 0.0

    # Middle-school records first, then closest name. One pass gives "prefer middle,
    # fall back to any" without a second search.
    is_mid, sim, best = max(scored, key=lambda t: (t[0], t[1]))
    return best, ("middle" if is_mid else "city"), round(sim, 3)
