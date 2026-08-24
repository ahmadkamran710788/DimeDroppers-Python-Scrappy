"""The single roster/staff page -> ``ROSTER_FIELDS`` mapping.

Two places have to turn a team's Roster or Staff tab into the same rows:

* the crawler (``spiders/maxpreps.py``), for every team-season it visits, and
* the post-crawl pass (``enrich_roster.py``), for the opponent schools the crawl never
  reached -- typically schools in another state, which ``_maybe_follow_school``'s state
  gate structurally excludes.

Keeping the mapping here is what stops those two from drifting, the same way
``schoolinfo.school_row`` does for school rows.

Extraction strategy
-------------------
MaxPreps hashes its CSS class names, so the tables are located by their **header text**
(``Player``/``Grade``, ``Staff``/``Position``) exactly like the schedule table is. Unlike
the schedule parser, cells are then read **by header name** rather than by fixed index:
the roster table carries a leading ``#`` column that the staff table does not, and reading
by name means an added or reordered column cannot silently shift every value one place.
"""
import re

from .export import ROSTER_FIELDS

__all__ = ["ROSTER_FIELDS", "CATEGORIES", "cell_text", "roster_rows", "team_subpage"]

# category -> how to find its table and what its columns mean.
#
# "anchors" are header labels that must BOTH be present, which is what tells the two
# tables apart: the roster table has Player+Grade (and a Position column, but no Staff
# header), the staff table has Staff+Position (and neither Player nor Grade).
#
# "columns" maps the lowercased header text to the ROSTER_FIELDS name it feeds. Anything
# not listed here is ignored; anything listed but absent from the page stays "".
CATEGORIES = {
    "player": {
        "anchors": ("Player", "Grade"),
        "columns": {
            "#": "jersey_number",
            "player": "name",
            "grade": "grade",
            "position": "position",
            "height": "height",
            "weight": "weight",
        },
    },
    "staff": {
        "anchors": ("Staff", "Position"),
        "columns": {
            "staff": "name",
            "position": "position",
        },
    },
}

# Columns that only ever carry a value for players; blank on every staff row.
_PERSON_FIELDS = ("jersey_number", "name", "grade", "position", "height", "weight")

_WS = re.compile(r"\s+")


def cell_text(cell):
    """Collapsed visible text of a table cell (or any selector)."""
    text = " ".join(t.strip() for t in cell.xpath('.//text()').getall() if t.strip())
    return _WS.sub(" ", text).strip()


def team_subpage(team_url, page):
    """A sport-season's ``canonicalUrl`` -> its ``roster/`` or ``staff/`` page.

    Some (especially past-season) ``canonicalUrl`` values already end in ``/schedule``.
    That suffix has to be REMOVED before appending, not appended to -- otherwise the
    request goes to ``.../schedule/roster/``, which is not a page.
    """
    base = (team_url or "").rstrip("/")
    if not base:
        return ""
    if base.endswith("/schedule"):
        base = base[: -len("/schedule")]
    return f"{base}/{page}/"


def _table(response, anchors):
    """The first table in the document carrying all of ``anchors`` as header labels.

    Note the outer parentheses: ``(//table[pred])[1]`` is "the first match in the
    document". The unparenthesised ``//table[pred][1]`` means "every matching table that
    is first among its siblings", which can select more than one.
    """
    preds = " and ".join(f'.//th[contains(., "{a}")]' for a in anchors)
    return response.xpath(f"(//table[{preds}])[1]")


def _header_index(table, columns):
    """``{ROSTER_FIELDS name: cell index}`` built from the table's header row."""
    heads = table.xpath("(.//tr[th])[1]/th") or table.xpath(".//thead//th")
    index = {}
    for i, head in enumerate(heads):
        field = columns.get(cell_text(head).lower())
        if field and field not in index:  # first occurrence wins
            index[field] = i
    return index


def roster_rows(response, team, school, category):
    """One dict per person on this Roster/Staff page, keyed by ``ROSTER_FIELDS``.

    ``team`` is a raw ``schoolContext.sportSeasons`` entry and ``school`` the
    ``{school_id, name, state}`` block -- the same two the schedule parser receives, so a
    roster row joins to a schedule row on the same keys.

    Returns ``[]`` when the page carries no matching table (a team with no roster
    published, or a page that failed to render), which callers treat as "nothing to
    emit" rather than an error.
    """
    spec = CATEGORIES[category]
    table = _table(response, spec["anchors"])
    if not table:
        return []

    index = _header_index(table, spec["columns"])
    if "name" not in index:  # no name column -> nothing worth emitting
        return []

    base = {
        "school_id": school.get("school_id"),
        "school_name": school.get("name"),
        "state": school.get("state"),
        "sport": team.get("sport"),
        "gender": team.get("gender"),
        "season": f"{team.get('season', '')} {team.get('year', '')}".strip(),
        "level": team.get("level"),
        "category": category,
        "roster_url": response.url,
    }

    rows = table.xpath(".//tbody/tr") or table.xpath(".//tr[td]")
    out = []
    for row in rows:
        cells = row.xpath("./td")
        if not cells:
            continue
        values = {f: (cell_text(cells[i]) if i < len(cells) else "")
                  for f, i in index.items()}
        if not values.get("name"):
            continue  # spacer / section-header row
        # row_index counts EMITTED rows, so it stays contiguous -- it is part of the
        # SQLite primary key, and gaps there would make re-crawls key differently.
        out.append({**base, "row_index": len(out),
                    **{f: values.get(f, "") for f in _PERSON_FIELDS}})
    return out
