"""MaxPreps crawler.

Crawl flow
----------
1. ``/{state}/schools/``                     -> directory of (up to 200) schools per state
2. ``/{state}/{city}/{school}/``             -> full school detail + list of sport-seasons
3. ``{sport_canonical_url}schedule/``        -> one schedule (table of games) per team

Coverage note
-------------
The public directory at ``/{state}/schools/`` is capped at 200 schools per state
(verified against TX/CA, which both truncate). The complete list lives behind the
``/discovery/`` search API, which ``robots.txt`` disallows. To reach the remaining
schools *without* violating robots.txt, this spider also follows the ``nearbySchools``
links found on each school page and the opponent links found on each schedule
(both are on robots-allowed URLs). Because schools are densely connected to their
in-state neighbours and opponents, this graph crawl expands coverage well beyond the
200 seeds. Toggle it with ``-a discover=0``.

Usage examples
--------------
    # one small state, schedules on, default (Varsity) level
    scrapy crawl maxpreps -a states=wy

    # a few states, no schedule pages (schools + sports only -> fast)
    scrapy crawl maxpreps -a states=wy,vt,ri -a schedules=0

    # everything (all 50 states + DC) -- large, slow, resumable
    scrapy crawl maxpreps -s JOBDIR=.jobs/full
"""
import scrapy

from ..items import SchoolItem, ScheduleGameItem
from ..nextdata import page_props
from ..states import ALL_STATE_CODES, STATES

BASE = "https://www.maxpreps.com"

# Single-path segments under a school that are NOT sports (used as a safety net;
# the primary sports source is the structured ``sportSeasons`` list).
NON_SPORT_SLUGS = {
    "events", "news", "photos", "videos", "fans", "fan-poll", "calendar",
    "about", "store", "tickets", "scores", "schedule", "roster", "stats",
    "standings", "rankings", "home", "athletes", "teams", "coaches",
}


def _truthy(value, default=True):
    """Parse a spider argument that may arrive as a string ('0','false','no')."""
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off", ""}


class MaxPrepsSpider(scrapy.Spider):
    name = "maxpreps"
    allowed_domains = ["maxpreps.com"]

    def __init__(self, states=None, schedules="1", discover="1", levels="Varsity",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        # which states to crawl: comma-separated codes, or "all"
        if states in (None, "", "all"):
            self.state_codes = list(ALL_STATE_CODES)
        else:
            self.state_codes = [
                s.strip().lower() for s in states.split(",") if s.strip()
            ]
        self.target_states = {s.upper() for s in self.state_codes}

        self.crawl_schedules = _truthy(schedules, True)
        self.discover = _truthy(discover, True)
        # which team levels to fetch schedules for: "Varsity" (default) or "all"
        self.levels = (
            None if str(levels).lower() == "all"
            else {lvl.strip().lower() for lvl in levels.split(",") if lvl.strip()}
        )

        # de-dup of emitted schools (Scrapy's dupefilter also dedups requests)
        self._seen_schools = set()

    # ------------------------------------------------------------------ #
    # 1. state directories
    # ------------------------------------------------------------------ #
    def start_requests(self):
        for code in self.state_codes:
            yield scrapy.Request(
                f"{BASE}/{code}/schools/",
                callback=self.parse_directory,
                cb_kwargs={"state_code": code},
            )

    def parse_directory(self, response, state_code):
        pp = page_props(response.text)
        groupings = pp.get("groupings") or []
        self.logger.info(
            "Directory %s: %d schools (capped at 200 by the site)",
            state_code, len(groupings),
        )
        for g in groupings:
            url = g.get("canonicalUrl")
            if url:
                yield self._school_request(url, discovered_via=f"directory:{state_code}")

    # ------------------------------------------------------------------ #
    # 2. school detail pages
    # ------------------------------------------------------------------ #
    def _school_request(self, url, discovered_via):
        return scrapy.Request(
            url,
            callback=self.parse_school,
            cb_kwargs={"discovered_via": discovered_via},
        )

    def _maybe_follow_school(self, url, discovered_via):
        """Yield a request for a newly-discovered school if it's in scope."""
        if not url:
            return None
        path = url.replace(BASE, "").strip("/").split("/")
        if len(path) < 3:           # need /{state}/{city}/{school}
            return None
        state = path[0].upper()
        if self.target_states and state not in self.target_states:
            return None
        return self._school_request(url, discovered_via)

    def parse_school(self, response, discovered_via):
        pp = page_props(response.text)
        ctx = pp.get("schoolContext") or {}
        info = ctx.get("schoolInfo") or {}
        links = pp.get("schoolLinksData") or {}
        school_id = ctx.get("schoolId") or info.get("schoolId")

        # emit the school exactly once
        if school_id and school_id not in self._seen_schools:
            self._seen_schools.add(school_id)
            yield self._build_school_item(response, ctx, info, links, discovered_via)

        # 2a. schedules for each sport-season team
        sport_seasons = ctx.get("sportSeasons") or []
        if self.crawl_schedules:
            for ss in sport_seasons:
                if self.levels and (ss.get("level") or "").lower() not in self.levels:
                    continue
                team_url = ss.get("canonicalUrl")
                if not team_url:
                    continue
                # some (esp. past-season) canonicalUrls already end in /schedule/
                base = team_url.rstrip("/")
                schedule_url = base + "/" if base.endswith("/schedule") else base + "/schedule/"
                yield scrapy.Request(
                    schedule_url,
                    callback=self.parse_schedule,
                    cb_kwargs={"team": ss, "school": {
                        "school_id": school_id,
                        "name": info.get("name") or info.get("formattedName"),
                        "state": info.get("stateCode") or info.get("state"),
                    }},
                )

        # 2b. graph expansion via nearby schools
        if self.discover:
            for near in pp.get("nearbySchools") or []:
                url = near.get("canonicalUrl") if isinstance(near, dict) else None
                req = self._maybe_follow_school(url, discovered_via="nearby")
                if req:
                    yield req

    def _build_school_item(self, response, ctx, info, links, discovered_via):
        sport_seasons = ctx.get("sportSeasons") or []
        # distinct "Sport (Gender)" labels offered by the school
        sports = sorted({
            f"{s.get('sport')} ({s.get('gender')})".strip()
            for s in sport_seasons if s.get("sport")
        })
        return SchoolItem(
            school_id=ctx.get("schoolId") or info.get("schoolId"),
            name=info.get("name") or info.get("formattedNameWithoutState"),
            city=info.get("city"),
            state=info.get("stateCode") or info.get("state"),
            state_name=info.get("stateName"),
            url=info.get("canonicalUrl") or response.url,
            mascot=info.get("mascot"),
            address=info.get("address"),
            zip_code=info.get("zipCode") or info.get("zip"),
            phone=info.get("phone"),
            color1=(info.get("color1") or "").strip(),
            color2=(info.get("color2") or "").strip(),
            color3=(info.get("color3") or "").strip(),
            mascot_url=info.get("mascotUrl"),
            league_name=info.get("leagueName"),
            association_name=info.get("associationName"),
            governing_body_name=info.get("associationGoverningBodyName"),
            governing_body_url=info.get("associationGoverningBodyUrl"),
            website=links.get("website") or info.get("websiteUrl"),
            facebook=links.get("facebook"),
            instagram=links.get("instagram"),
            twitter=links.get("twitter"),
            youtube=links.get("youtube"),
            sports=sports,
            sports_count=len(sports),
            discovered_via=discovered_via,
        )

    # ------------------------------------------------------------------ #
    # 3. schedule pages
    # ------------------------------------------------------------------ #
    def parse_schedule(self, response, team, school):
        sport = team.get("sport")
        gender = team.get("gender")
        season = f"{team.get('season','')} {team.get('year','')}".strip()

        # locate the schedule table by its header labels (class names are hashed)
        table = response.xpath(
            '//table[.//th[contains(., "Opponent")]'
            ' and .//th[contains(., "Date")]][1]'
        )
        rows = table.xpath('.//tbody/tr | .//tr[td]')
        for i, row in enumerate(rows):
            cells = row.xpath('./td')
            if len(cells) < 2:
                continue
            date = self._cell_text(cells[0])
            opp_cell = cells[1]
            opp_link = opp_cell.xpath('.//a[contains(@href, "/")]/@href').get()
            opponent = self._cell_text(opp_cell)
            game_info = self._cell_text(cells[2]) if len(cells) > 2 else ""

            home_away = "away" if opponent.startswith("@") else "home"
            # strip the "@"/"vs" home-away marker and trailing "*" footnote markers
            opponent_clean = opponent.lstrip("@").strip()
            if opponent_clean.lower().startswith("vs "):
                opponent_clean = opponent_clean[3:]
            opponent_clean = opponent_clean.rstrip("*").strip()
            result, score = self._parse_result(game_info)

            yield ScheduleGameItem(
                school_id=school.get("school_id"),
                school_name=school.get("name"),
                state=school.get("state"),
                sport=sport,
                gender=gender,
                season=season,
                schedule_url=response.url,
                game_index=i,
                date=date,
                home_away=home_away,
                opponent=opponent_clean,
                opponent_url=response.urljoin(opp_link) if opp_link else "",
                result=result,
                score=score,
                game_info=game_info,
            )

        # graph expansion via opponents
        if self.discover:
            for href in table.xpath('.//tbody//a/@href').getall():
                url = response.urljoin(href)
                # opponent links point at a team page; trim to the school root
                parts = url.replace(BASE, "").strip("/").split("/")
                if len(parts) >= 3:
                    school_root = f"{BASE}/{parts[0]}/{parts[1]}/{parts[2]}/"
                    req = self._maybe_follow_school(school_root, discovered_via="opponent")
                    if req:
                        yield req

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _cell_text(cell):
        text = " ".join(t.strip() for t in cell.xpath('.//text()').getall() if t.strip())
        return text.strip()

    @staticmethod
    def _parse_result(game_info):
        """Pull a leading W/L/T and a digits-digits score out of a Game Info cell."""
        import re
        result = ""
        m = re.match(r'\s*([WLT])\b', game_info)
        if m:
            result = m.group(1)
        score_m = re.search(r'\b(\d{1,3}\s*[-–]\s*\d{1,3})\b', game_info)
        score = score_m.group(1).replace(" ", "") if score_m else ""
        return result, score
