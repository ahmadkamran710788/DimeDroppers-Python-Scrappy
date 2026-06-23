"""Scrapy item definitions.

Two record types flow through the pipeline:

* ``SchoolItem``        -> one row per high school (with its list of sports)
* ``ScheduleGameItem``  -> one row per game on a team's schedule

Field order here is also the column order used in the CSV output.
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
