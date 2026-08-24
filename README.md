# MaxPreps Scraper

A [Scrapy](https://scrapy.org) crawler that collects **high schools**, the **sports**
each school offers, every team's **game schedule**, and every team's **roster and
coaching staff** from [MaxPreps](https://www.maxpreps.com), across all 50 states + DC.

Every record is written to **three formats at once**: CSV, JSON, and a SQLite database.

---

## How it works

MaxPreps is a Next.js site that embeds clean structured data in a `__NEXT_DATA__`
JSON blob on every page — the crawler parses that instead of fragile, hashed CSS
classes. Schedules are read from the rendered schedule table.

Crawl flow:

```
/{state}/schools/                  ->  directory of schools per state (≤200, see caveat)
  └─ /{state}/{city}/{school}/      ->  full school detail + list of sport-seasons
       └─ .../{sport}/schedule/     ->  one schedule (table of games) per team
```

### Coverage caveat (important)

MaxPreps' public directory `/{state}/schools/` is **capped at 200 schools per
state** (verified — TX and CA both truncate at 200; small states like WY return
their true count). The full list sits behind the `/discovery/` search API, which
**`robots.txt` disallows**.

To reach the rest **without violating robots.txt**, the crawler also follows two
robots-allowed link sources and treats them as new seeds:

- `nearbySchools` listed on each school page
- **opponent** links on each schedule

Because schools are densely connected to their in-state neighbours and opponents,
this graph crawl expands coverage well past the 200 seeds. It's on by default;
disable with `-a discover=0`. (100% completeness for the biggest states isn't
guaranteed via public pages alone — this is an inherent MaxPreps limitation, not a
bug in the scraper.)

---

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

The easy way (`run.py` wraps the Scrapy flags):

```bash
python run.py wy                    # one state
python run.py wy,vt,ri              # a few states
python run.py wy --no-schedules     # schools + sports only (fast)
python run.py all --levels all      # everything, incl. JV/Freshman schedules
python run.py                       # all 50 states + DC  (large + slow)
```

Or call Scrapy directly for full control:

```bash
scrapy crawl maxpreps -a states=ny
scrapy crawl maxpreps -a states=ny,ca -a schedules=0
scrapy crawl maxpreps                       # all states (default)
```

### Spider arguments (`-a name=value`)

| Arg | Default | Meaning |
|-----|---------|---------|
| `states` | `all` | Comma-separated state codes (`ny,ca,tx`) or `all` |
| `schedules` | `1` | Crawl each team's schedule page (`0` = schools + sports only) |
| `discover` | `1` | Graph-crawl via nearby schools + opponents to beat the 200 cap |
| `levels` | `Varsity` | Team levels to fetch schedules for; `all` adds JV/Freshman |
| `rosters` | `0` | Also crawl each team's Roster + Staff tabs (`1` = on) |

`rosters` is **off** here on purpose: it adds two pages per team-season on top of the
schedule, so it roughly triples the crawl's page count, and on a time-capped run that
costs graph-discovered schools. The API path turns it on (see below); `run.py` and
`backfill_all.py` exist to maximise *school* coverage and leave it off. Enable it with
`python run.py wy -a rosters=1`.

### Make a big crawl resumable

Pass a `JOBDIR` so you can stop (Ctrl-C once) and restart where you left off:

```bash
scrapy crawl maxpreps -s JOBDIR=.jobs/full
```

---

## Output

Written to `output/` (override with `-s OUTPUT_DIR=...`):

| File | Contents |
|------|----------|
| `schools.csv` / `schools.json` | one row per school |
| `schedule.csv` / `schedule.json` | one row per game |
| `roster.csv` / `roster.json` | one row per player / staff member (header-only unless `-a rosters=1`) |
| `maxpreps.db` | SQLite with `schools`, `games` and `roster` tables |

**`schools`** columns: `school_id, name, city, state, state_name, url, mascot,
address, zip_code, state_linking, phone, color1-3, mascot_url, league_name,
association_name, governing_body_name/url, website, facebook, instagram, twitter,
youtube, maxpreps_gofan_url, maxpreps_nfhs_url, sports, sports_count, discovered_via`

`state_linking` is the address block MaxPreps prints under the school name on its page,
as one line — `1725 North Main Spearfish, SD 57783`. It is a formatted join of the
`address`, `city`, `state` and `zip_code` columns (the street address, **not** the
school's mailing address, which often differs), so it costs no extra request. Blank
pieces are skipped, so a school with no street still gets `Pinedale, WY 82941`.

`maxpreps_gofan_url` / `maxpreps_nfhs_url` are the GoFan ticket and NFHS Network
links **MaxPreps itself publishes** on the school's page, when it has them. They cost
no extra requests (the page is already fetched and parsed) and are empty when MaxPreps
links only to the partners' home pages rather than to that school.

**`games`** columns: `school_id, school_name, state, sport, gender, season, level,
game_index, date, home_away, opponent, opponent_url, result, score, game_info,
schedule_url`

**`roster`** columns: `school_id, school_name, state, sport, gender, season, level,
category, row_index, jersey_number, name, grade, position, height, weight, roster_url`

One row per person on a team's **Roster** tab (`category = player`) or its **Staff** tab
(`category = staff`). The leading block is identical to `games`, so a roster row joins to a
schedule row on the same keys — including `level` (`Varsity` / `JV` / `Freshman`) and
`sport`. `jersey_number`, `grade`, `height` and `weight` are player-only and blank on staff
rows; `name` is the Player or Staff column and `position` is `WR` / `SS, FS` for players and
`Head Coach` / `Assistant Coach` for staff. Values are kept exactly as MaxPreps displays
them (`6'0"`, `178 lbs`), and `row_index` counts emitted rows within one table.

JSON files are proper, streaming JSON arrays. SQLite uses `INSERT OR REPLACE`
keyed on the school / game, so re-running updates rows in place rather than
duplicating them. Query example:

```sql
SELECT school_name, COUNT(*) AS games, SUM(result='W') AS wins
FROM games WHERE sport='Basketball' GROUP BY school_id ORDER BY wins DESC;
```

---

## Politeness & scale

The crawler is configured to be considerate (see `settings.py`):
`ROBOTSTXT_OBEY=True`, AutoThrottle, a real browser User-Agent, modest
concurrency, and retries on `403/429/5xx`.

A true nationwide run is **hundreds of thousands of pages** and MaxPreps (a CBS /
PlayOnSports property) will throttle a single IP. For that scale you'll likely
want to:

- run it slowly with a `JOBDIR` (resume across days), and/or
- add **rotating proxies** — set `DOWNLOAD_DELAY`, lower concurrency, and plug a
  proxy middleware into `settings.py`.

Scrape responsibly and within MaxPreps' Terms of Service.

---

## HTTP API (for the Next.js frontend / Render)

`api.py` exposes the sports-filtered scraper as an HTTP service so a frontend can
trigger scrapes by **state(s)** + **sport** and download the resulting CSVs. Because
a crawl is long-running and Scrapy's reactor can't be restarted in-process, the API
uses an **async job model**: each crawl runs in its own subprocess (`worker.py`),
and results are transient CSV files under `jobs/<job_id>/` (no database; files are
deleted on `DELETE` or swept on restart).

This path produces **three** CSVs — `teams.csv`, `schedule.csv` and `roster.csv` — and,
unlike the canonical CLI, it crawls rosters by default (same reasoning as `levels=all`:
the frontend's CSVs are expected to be complete). Pass `--no-rosters` to
`max_prep_scraper.py` for a substantially faster crawl without them.

### Run locally

```bash
pip install -r requirements.txt
uvicorn api:app --reload          # http://localhost:8000
```

### Endpoints

| Method & path | Purpose |
|---|---|
| `POST /scrape` | Start a crawl. Body: `{ "states": "wy", "sports": "Football", "levels": "all", "discover": true }`. Returns `{ job_id, status }`. `levels` defaults to `all` (Varsity + JV + Freshman); pass `"Varsity"` for a ~3x faster varsity-only crawl. |
| `GET /scrape/{job_id}` | Poll status: `{ status: running\|done\|error, counts, error }`. |
| `GET /scrape/{job_id}/results?type=teams\|schedule\|roster` | Parsed CSV rows as JSON (when done). |
| `GET /scrape/{job_id}/download?type=teams\|schedule\|roster` | Download `teams.csv` / `schedule.csv` / `roster.csv`. |
| `DELETE /scrape/{job_id}` | Delete the job's temp files. |
| `GET /states` | `[{ code, name }]` for all 50 states + DC. |
| `GET /sports` | Common sport labels for a dropdown. |
| `GET /health` | Health check for Render. |

### Extra columns the API's CSVs carry

After the crawl, `worker.py` runs a chain of best-effort enrichment steps that append columns
the canonical CSV/SQLite output does **not** have. Each writes atomically and swallows its own
failures, so a job always returns usable data even if a step times out.

`teams.csv` (on top of the `schools` columns above):

| Column | Source |
|---|---|
| `original_name` | The school's name as GoFan spells it (`enrich_gofan_scrapy.py`), falling back to the MaxPreps `name` when there's no GoFan match |
| `go_fan_ticket_url` | `https://gofan.co/app/school/{huddleId}`, matched on state + city and verified to return HTTP 200. **Falls back to `maxpreps_gofan_url`** when that match finds nothing |
| `nfhs_url` | `https://www.nfhsnetwork.com/schools/{slug}`, matched against NFHS's cached catalog (`enrich_nfhs.py`). **Falls back to `maxpreps_nfhs_url`** when that match finds nothing |

The fallback fills blanks only, and each column is decided independently — a link the
GoFan step actually verified returns HTTP 200 is never replaced by MaxPreps' unverified
one. If only one of the two came back empty, only that one is filled. The catalog-matching
scripts themselves are untouched by this; the fill is a separate step in `worker.py` that
runs after them.

To see which links MaxPreps publishes for a given school (and check they aren't just its
site-wide nav links to those partners):

```bash
python scripts/probe_partner_links.py https://www.maxpreps.com/ca/stockton/lincoln-trojans/
```

`schedule.csv` (on top of the `games` columns above), both from `enrich_opponent.py`:

| Column | Source |
|---|---|
| `original_opponent_school_name` | The opponent's real school name, matched to its teams-CSV row by `school_id` so a school spelled one way in `teams.csv` is spelled the same way here; falls back to a normalized name match, then to the raw `opponent` text |
| `original_opponent_school_logo` | The opponent school's logo URL from its MaxPreps page |

Every opponent also gets its own row in `teams.csv`, tagged `discovered_via=opponent:schedule`
and deduplicated on `school_id`. This matters because the crawler only follows schools in the
states you asked for, so an opponent from a neighbouring state would otherwise appear in
`schedule.csv` with no corresponding school row. The two exceptions are opponents with no link
and MaxPreps' placeholder opponents (`/utility/about_pseudo_schools.aspx`) — there is no school
behind either, so those rows keep their raw opponent text.

`roster.csv` carries no extra columns — it has the same schema on both paths. It does get one
extra *step*: `enrich_roster.py` closes the same gap for rosters that the opponent harvest
closes for school rows. Those harvested opponent schools were never crawled, so they have no
roster; this step fetches the Roster + Staff tabs for every school in `teams.csv` that has no
roster rows yet, honouring the same sport and level filters as the crawl. Keying on "has no
rows yet" rather than on `discovered_via` also makes it self-healing — a school whose roster
fetch failed mid-crawl is picked up on the next run.

```bash
# rebuild opponent rosters by hand against an existing pair of CSVs
python enrich_roster.py output/max_prep_roster.csv output/max_prep_School.csv Football all
```

Both opponent columns can also be (re)built by hand against an existing pair of CSVs:

```bash
# both phases back to back
python enrich_opponent.py output/max_prep_schedule.csv output/max_prep_School.csv

# or separately -- worker.py splits them so GoFan/NFHS run in between and cover the
# schools the harvest adds. Only `harvest` touches the network.
python enrich_opponent.py harvest output/max_prep_schedule.csv output/max_prep_School.csv
python enrich_opponent.py resolve output/max_prep_schedule.csv output/max_prep_School.csv
```

```bash
# quick smoke test (Wyoming + Football is small/fast)
curl -X POST localhost:8000/scrape -H 'Content-Type: application/json' \
  -d '{"states":"wy","sports":"Football"}'
curl localhost:8000/scrape/<job_id>                       # until status: "done"
curl localhost:8000/scrape/<job_id>/download?type=teams -o teams.csv
```

The existing CLIs (`run.py`, `max_prep_scraper.py`) are unchanged and still work.

### Deploy on Render

`render.yaml` defines a single Python **web service**:

- Build: `pip install -r requirements.txt`
- Start: `uvicorn api:app --host 0.0.0.0 --port $PORT`
- Health check: `/health`
- Env: `FRONTEND_ORIGIN` (comma-separated allowed origins; default `*`),
  `MAX_CONCURRENT_JOBS` (default `2`).

In the Render dashboard: **New + -> Blueprint**, point at this repo, deploy. After
the frontend is deployed, set `FRONTEND_ORIGIN` to your Vercel URL to lock down CORS.

> Note: Render's free tier can kill long crawls on idle timeout, and the in-memory
> job table is lost on restart (by design — nothing is persisted). Small
> state + sport scrapes are fine; use a paid instance for large ones.
