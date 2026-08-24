"""Scrapy item definitions.

Three record types flow through the pipeline:

* ``SchoolItem``        -> one row per high school (with its list of sports)
* ``ScheduleGameItem``  -> one row per game on a team's schedule
* ``RosterItem``        -> one row per player/staff member on a team's roster

The CSV column order is ``export.py``'s ``SCHOOL_FIELDS`` / ``GAME_FIELDS`` /
``ROSTER_FIELDS``, NOT the field order declared here -- every writer builds its row by
iterating those lists with ``extrasaction="ignore"``. Declaring a field here that is
absent from the matching list silently drops it from every sink; the reverse (a name in
the list with no field here) raises ``KeyError`` when the spider constructs the item.
"""
import scrapy


class SchoolItem(scrapy.Item):
    # identity
    school_id = scrapy.Field()
    name = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    state_name = scrapy.Field()
    url = scrapy.Field()

    # contact / location
    mascot = scrapy.Field()
    address = scrapy.Field()
    zip_code = scrapy.Field()
    # "1725 North Main Spearfish, SD 57783" -- the address block MaxPreps prints under
    # the school name, joined from address + city + state + zip_code by
    # schoolinfo.state_linking(). Derived, not separately fetched.
    state_linking = scrapy.Field()
    phone = scrapy.Field()

    # branding
    color1 = scrapy.Field()
    color2 = scrapy.Field()
    color3 = scrapy.Field()
    mascot_url = scrapy.Field()

    # affiliation
    league_name = scrapy.Field()
    association_name = scrapy.Field()
    governing_body_name = scrapy.Field()
    governing_body_url = scrapy.Field()

    # web presence
    website = scrapy.Field()
    facebook = scrapy.Field()
    instagram = scrapy.Field()
    twitter = scrapy.Field()
    youtube = scrapy.Field()

    # partner links as published by MaxPreps (GoFan tickets / NFHS Network).
    # Distinct from the go_fan_ticket_url / nfhs_url columns the enrichment chain adds
    # by catalog matching -- see worker.py phase 4.5.
    maxpreps_gofan_url = scrapy.Field()
    maxpreps_nfhs_url = scrapy.Field()

    # sports offered (semicolon-joined in CSV, list in JSON)
    sports = scrapy.Field()
    sports_count = scrapy.Field()

    # provenance
    discovered_via = scrapy.Field()


class ScheduleGameItem(scrapy.Item):
    # which team/schedule this game belongs to
    school_id = scrapy.Field()
    school_name = scrapy.Field()
    state = scrapy.Field()
    sport = scrapy.Field()
    gender = scrapy.Field()
    season = scrapy.Field()
    level = scrapy.Field()        # Varsity / JV / Freshman
    schedule_url = scrapy.Field()

    # the game itself
    game_index = scrapy.Field()
    date = scrapy.Field()
    home_away = scrapy.Field()
    opponent = scrapy.Field()
    opponent_url = scrapy.Field()
    result = scrapy.Field()       # W / L / T / "" (scheduled)
    score = scrapy.Field()
    game_info = scrapy.Field()    # raw text of the "Game Info" cell


class RosterItem(scrapy.Item):
    # which team/season this person belongs to (same block as ScheduleGameItem)
    school_id = scrapy.Field()
    school_name = scrapy.Field()
    state = scrapy.Field()
    sport = scrapy.Field()
    gender = scrapy.Field()
    season = scrapy.Field()
    level = scrapy.Field()          # Varsity / JV / Freshman
    roster_url = scrapy.Field()

    # the person
    category = scrapy.Field()       # "player" (Roster tab) / "staff" (Staff tab)
    row_index = scrapy.Field()      # position within their table
    jersey_number = scrapy.Field()  # the "#" column; blank for staff
    name = scrapy.Field()           # the "Player" / "Staff" column
    grade = scrapy.Field()          # So. / Jr. / Sr. ...; blank for staff
    position = scrapy.Field()       # "WR", "SS, FS" / "Assistant Coach"
    height = scrapy.Field()         # as displayed (6'0"); blank for staff
    weight = scrapy.Field()         # as displayed (178 lbs); blank for staff
