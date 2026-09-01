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
import unicodedata

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
# Share of the smaller distinctive-word set that must be common to both names, paired
# with NAME_FLOOR above. The second, laxer overlap needs a correspondingly higher
# similarity -- see name_score for the labelled pair that pins OVERLAP_SIM_FLOOR at 0.72.
OVERLAP_FLOOR = 0.6
# How many extra words the longer name may add before a ONE-word overlap stops counting
# as evidence. "Ruffner School" -> "Ruffner Middle School (MS)" adds one word and is the
# same school; "Marin County Juvenile Court" -> "The Marin School" shares only "marin"
# while three other words disagree, and is not. Two is the line between them.
SUBSET_MAX_EXTRA = 2
# Similarity at which a name counts as "the same name", letting it outrank the
# middle-school type preference in pick(). Below this the type preference decides.
EXACT_SIM = 0.95
WEAK_OVERLAP_FLOOR = 0.5
OVERLAP_SIM_FLOOR = 0.72

# "Boaz Pirates vs Albertville Middle" -- the delimiter GoFan event titles put between
# the two teams. Case-insensitive, optional dot, must be a standalone word ("vs" inside
# a school name like "Vestavia" can't match because of the surrounding whitespace).
_VS_SPLIT = re.compile(r"\s+vs\.?\s+", re.IGNORECASE)
# Separators between multiple opponents on ONE side of a title ("Albertville Middle &
# Scottsboro Middle", "Caver MS/Putnam"). " and " is deliberately not one -- it appears
# inside real school names ("Lewis and Clark").
_OPP_SEP = re.compile(r"\s*[&/]\s*")
# Title fragments that mean "no opponent named yet", never a school.
_NON_OPPONENT = frozenset({"tbd", "tba", "tbc", "multi", "multiple", "opponent"})

# Words that carry no distinguishing information in a school name. Used for two things:
# choosing the search query (gofan_client imports this) and measuring how much two names
# genuinely have in common. Note this is a SUPERSET of what ``normalize`` strips --
# normalize deliberately keeps "middle"/"junior" so a middle school is never conflated
# with the same-named high school, whereas here they are noise.
GENERIC_WORDS = frozenset({
    "school", "schools", "middle", "high", "junior", "jr", "sr", "senior",
    "elementary", "intermediate", "academy", "the", "of", "and", "at",
    "campus", "center", "centre",
})


def _fold(text):
    """Lowercase and strip accents.

    GoFan spells some schools with diacritics that NCES writes plain -- "Cesar Chavez
    Academy" vs "Cesar Chavez Academy" with acutes. Without folding, the punctuation
    regex shreds the accented form into "c sar ch vez", the two names share no words at
    all, and a correct match is thrown away.
    """
    decomposed = unicodedata.normalize("NFKD", (text or ""))
    return "".join(c for c in decomposed if not unicodedata.combining(c)).lower()


def normalize(name):
    """Lowercase, fold accents, strip punctuation and a generic trailing 'school'."""
    s = _fold(name)
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


# Words in a school's NAME that state its level. "int" and the initialisms cover the
# abbreviated NCES style ("CALDWELL INT", "FOO JR HIGH").
_NAME_MIDDLE = frozenset({"middle", "junior", "jr", "intermediate", "int", "ms", "jh", "jhs"})
_NAME_HIGH = frozenset({"high", "hs", "shs"})


def name_type(name):
    """"middle" / "high" / "" as stated by the name itself.

    A name containing both ("JR HIGH", "MIDDLE/HIGH") counts as middle -- the middle
    marker is the informative one in a middle-school file.
    """
    words = set(_PUNCT.sub(" ", _fold(name or "")).split())
    if words & _NAME_MIDDLE:
        return "middle"
    if words & _NAME_HIGH:
        return "high"
    return ""


def _prefer(candidate, row_type):
    """Whether pick() should prefer this candidate, given what the row's name states.

    GoFan's industryCode alone is not trustworthy here: real intermediate schools sit
    under "Elementary School" or the meaningless "Member School", so for the row
    "CALDWELL INT" the type preference never fired and raw similarity chose "Caldwell
    High School" (0.80) over "Caldwell Intermediate School" (0.73). Reading the level
    out of the candidate's NAME as well fixes that family. And when the row explicitly
    says middle, a candidate that says high is actively dispreferred rather than
    merely not preferred.
    """
    cand_middle = is_middle(candidate) or name_type(candidate.get("name")) == "middle"
    if row_type == "high":
        code = str(candidate.get("industryCode") or "").lower()
        return not cand_middle and ("high" in code or name_type(candidate.get("name")) == "high")
    # row says middle, or says nothing: a middle-school record is the one we want.
    return cand_middle


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


def _distinctive(name):
    """Set of a name's meaningful words, with generic school vocabulary removed."""
    words = _PUNCT.sub(" ", _fold(name)).split()
    return {w for w in words if w not in GENERIC_WORDS}


def name_score(row_name, candidate_name):
    """How strongly two school names agree: (passes, similarity).

    Token containment counts as agreement regardless of ratio, because a longer catalog
    name that fully contains the row's words ("Gulf Shores Middle" vs "Gulf Shores
    Middle School") is the same school with extra words -- and difflib's whole-string
    ratio unfairly penalises that length gap.

    **Containment alone is not enough when the smaller side is a single word.** Since
    the search query was widened to a single distinctive word, candidate pools are much
    larger and a one-word overlap stopped being evidence: it let
    "Marin County Juvenile Court" match "The Marin School" and "Duval Academy" match
    "Florida Virtual Academy At Duval County", both in the right city and state. So a
    subset only counts when the smaller set has at least two words; a lone word has to
    carry the whole name (R == C, e.g. "Oceanway School" vs "Oceanway Middle School").

    Everything else falls back to partial overlap plus string similarity, with a second,
    stricter pairing for weaker overlap. Calibrated against hand-labelled borderline
    pairs: this accepts "ALDEN ROAD EXCEP. STUDENT CENTER" -> "Alden Road Exceptional
    Child Center" (overlap 0.50, sim 0.76) while still rejecting "Edinburgh Comm Middle
    School" -> "Edinburgh Community High School" (overlap 0.50, sim 0.71). That pair is
    why OVERLAP_SIM_FLOOR is 0.72 and not 0.70.
    """
    a, b = normalize(row_name), normalize(candidate_name)
    if not a or not b:
        return False, 0.0
    sim = difflib.SequenceMatcher(None, a, b).ratio()
    if sim >= EXACT_SIM:
        # An essentially-identical string is the same name no matter how it tokenises.
        # Without this, "LaVergne Middle School" (sim 0.97 against GoFan's "La Vergne
        # Middle School") was REJECTED: the space split makes the token sets disjoint,
        # overlap is 0, and every token rule fails -- leaving the same-named high
        # school as the only survivor. Same story for "Ke Ana Laahana" vs GoFan's
        # "Ke Ana La Ahana".
        return True, sim

    R, C = _distinctive(row_name), _distinctive(candidate_name)
    if not R or not C:
        # Nothing distinctive on one side (e.g. a name that is all generic words) --
        # fall back to raw string similarity.
        return sim >= NAME_FLOOR, sim
    if R == C:
        return True, sim
    if (R <= C or C <= R) and (
        min(len(R), len(C)) >= 2 or abs(len(R) - len(C)) <= SUBSET_MAX_EXTRA
    ):
        return True, sim

    overlap = len(R & C) / min(len(R), len(C))
    if overlap >= OVERLAP_FLOOR and sim >= NAME_FLOOR:
        return True, sim
    if overlap >= WEAK_OVERLAP_FLOOR and sim >= OVERLAP_SIM_FLOOR:
        return True, sim
    return False, sim


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

    row_type = name_type(target)
    R = _distinctive(target)
    scored = []
    for c in candidates:
        if (c.get("state") or "").strip().upper() != st:
            continue  # gate 1: state, structural -- never cross state lines
        if not agrees(c, row):
            continue  # gate 2: city / zip must corroborate
        ok, sim = name_score(target, c.get("name"))
        if not ok:
            continue  # gate 3: the names must actually resemble each other
        C = _distinctive(c.get("name"))
        contained = bool(R and C and (R <= C or C <= R))
        scored.append((sim >= EXACT_SIM, contained, _prefer(c, row_type), sim, c))

    if not scored:
        return None, "none", 0.0

    # Rank: an essentially-exact name first, then token containment, then the type
    # preference, then closeness.
    #
    # The name has to outrank the type preference. Ordering by type first meant a
    # weakly-named middle school beat a perfectly-named one of another type -- "J.
    # Graham Brown School" was resolving to "Brown Middle School" purely because the
    # latter is typed Middle School. Preferring the right school type is still correct
    # when the names are comparably good, which is what the later keys do.
    #
    # Containment sits second because one name wholly containing the other is stronger
    # evidence than a partial overlap with a flattering character ratio: for "Barret
    # Traditional Middle", raw similarity marginally preferred "Johnson Traditional
    # Middle School" (0.745, sharing only "traditional") over "Barret Middle School"
    # (0.70, wholly contained in the row's name). Containment breaks that the right way.
    _exact, _cont, _pref, sim, best = max(scored, key=lambda t: (t[0], t[1], t[2], t[3]))
    # The match label stays keyed to GoFan's own classification, not our preference.
    return best, ("middle" if is_middle(best) else "city"), round(sim, 3)


def parse_opponent(title, own_names):
    """Read the opponent's name out of an event title, or "" if there isn't one.

    Only consulted when the event carries no opponent id, which in practice means an
    event *we* host -- away events always name their host. Handles the shapes found in
    the real blank-opponent rows: "Gulf Shores Dolphins vs WS Neal" (our side carries
    our mascot, not our name), "AthensGoldenEagles vs TBD" (our name concatenated into
    one word, opponent not yet decided), "Smith Middle vs Caver MS/Putnam" (two
    opponents; the first is taken, consistent with the single-value opponent column).

    ``own_names`` is every string known to identify us -- GoFan name, uploaded
    SCH_NAME, mascot. The side sharing tokens with any of them is ours; the other side
    is the opponent. When neither side is recognisably us, the LEFT side is treated as
    ours, because the host writes the title and lists itself first.
    """
    sides = [s.strip() for s in _VS_SPLIT.split(title or "") if s.strip()]
    if len(sides) < 2:
        return ""

    own_tokens = set()
    for n in own_names or ():
        own_tokens |= _distinctive(n)

    def us_score(side):
        # Token equality, plus substring containment for concatenated forms:
        # "athens" is not a token of "AthensGoldenEagles" but is a substring of it.
        # Substrings shorter than 4 chars match too promiscuously to count.
        toks = _distinctive(side)
        squashed = re.sub(r"[^a-z0-9]", "", _fold(side))
        return sum(
            1 for t in own_tokens if t in toks or (len(t) >= 4 and t in squashed)
        )

    scores = [us_score(s) for s in sides]
    us = max(range(len(sides)), key=lambda i: (scores[i], -i))
    for i, side in enumerate(sides):
        if i == us:
            continue
        for chunk in _OPP_SEP.split(side):
            c = chunk.strip(" -–—")
            if c and _fold(c).strip() not in _NON_OPPONENT:
                return c
    return ""


def pick_opponent(candidates, name, state, precise=True):
    """Resolve a title-parsed opponent name to one GoFan search hit, or None.

    This mirrors the manual flow for opponents: type what the title says into the
    search box; a single hit is taken as-is; several hits are narrowed to the row's
    state; whatever is left must actually carry the parsed name -- where "carry" also
    covers name+mascot, since the search matches mascots and a title like "Boaz
    Pirates" agrees with name "Boaz High School" + mascot "Pirates".

    The state gate applies even to a lone hit: "St. Clair County" (an Alabama
    opponent) returns exactly one hit -- St. Clair County Community College, in
    Michigan -- and a person reading "Port Huron, MI" under the result would not click
    it. A lone hit is only auto-accepted once it is in the right state.

    ``precise=False`` marks hits from the broad longest-word fallback query. Those are
    a superset by construction, so they never get the lone-hit shortcut and must pass
    the name gate like any other. Ambiguity returns None: a wrong school link is worse
    than a blank one, same principle as pick().
    """
    cands = [c for c in candidates if isinstance(c, dict)]
    st = (state or "").strip().upper()
    if st:
        cands = [c for c in cands if (c.get("state") or "").strip().upper() == st]
    if not cands:
        return None
    if precise and len(cands) == 1:
        return cands[0]

    # Rank survivors by how well their DISTINCTIVE tokens carry the parsed name, then
    # by school type, then raw similarity. Distinctive tokens first because a parsed
    # opponent like "Homewood" ties "Homewood High School" against "Homewood Middle
    # School" -- the raw ratio breaks that tie on nothing but name length, whereas
    # token similarity ties them honestly and lets the type key decide. The type key:
    # when the title states a type ("Homewood High School", "Foley Middle School"),
    # believe it; when it is silent ("Homewood"), prefer the middle school, because
    # every event reaching this resolver is a middle school's game.
    words = set(_PUNCT.sub(" ", _fold(name)).split())
    wants_middle = bool(words & {"middle", "junior", "jr", "intermediate"})
    wants_high = "high" in words and not wants_middle

    best = None
    r_toks = " ".join(sorted(_distinctive(name)))
    for c in cands:
        variants = [c.get("name") or ""]
        if c.get("mascot"):
            variants.append(f"{variants[0]} {c['mascot']}")
        ok, sim, tok_sim = False, 0.0, 0.0
        for v in variants:
            o, s = name_score(name, v)
            ok, sim = ok or o, max(sim, s)
            c_toks = " ".join(sorted(_distinctive(v)))
            tok_sim = max(
                tok_sim, difflib.SequenceMatcher(None, r_toks, c_toks).ratio()
            )
        if ok:
            mid = is_middle(c)
            if wants_middle or wants_high:
                pref = (wants_middle and mid) or (wants_high and not mid)
            else:
                pref = mid
            key = (round(tok_sim, 3), pref, sim)
            if best is None or key > best[0]:
                best = (key, c)
    return best[1] if best else None
