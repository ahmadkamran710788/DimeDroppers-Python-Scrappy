# MaxPreps Scraper

A [Scrapy](https://scrapy.org) crawler that collects **high schools**, the **sports**
each school offers, and every team's **game schedule** from
[MaxPreps](https://www.maxpreps.com), across all 50 states + DC.

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
| `maxpreps.db` | SQLite with `schools` and `games` tables |

**`schools`** columns: `school_id, name, city, state, state_name, url, mascot,
address, zip_code, phone, color1-3, mascot_url, league_name, association_name,
governing_body_name/url, website, facebook, instagram, twitter, youtube,
sports, sports_count, discovered_via`

**`games`** columns: `school_id, school_name, state, sport, gender, season,
game_index, date, home_away, opponent, opponent_url, result, score, game_info,
schedule_url`

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

### Run locally

```bash
pip install -r requirements.txt
uvicorn api:app --reload          # http://localhost:8000
```

### Endpoints

| Method & path | Purpose |
|---|---|
| `POST /scrape` | Start a crawl. Body: `{ "states": "wy", "sports": "Football", "levels": "Varsity", "discover": true }`. Returns `{ job_id, status }`. |
| `GET /scrape/{job_id}` | Poll status: `{ status: running\|done\|error, counts, error }`. |
| `GET /scrape/{job_id}/results?type=teams\|schedule` | Parsed CSV rows as JSON (when done). |
| `GET /scrape/{job_id}/download?type=teams\|schedule` | Download `teams.csv` / `schedule.csv`. |
| `DELETE /scrape/{job_id}` | Delete the job's temp files. |
| `GET /states` | `[{ code, name }]` for all 50 states + DC. |
| `GET /sports` | Common sport labels for a dropdown. |
| `GET /health` | Health check for Render. |

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
