# Project Context — DimeDroppers MaxPreps Scraper

> Orientation doc for anyone (human or agent) picking this repo up cold.
> `README.md` is the **user-facing usage** guide; this file is **how the system
> actually works, why it is shaped this way, and what will bite you**.
> Read this first, then only the files you need.

Last verified against commit `e032b30`. ~3,160 LOC, 21 Python files, no tests, no CI.

---

## 1. One-paragraph summary

A Scrapy crawler that collects US **high schools**, the **sports** each offers, and
every team's **game schedule** from MaxPreps (all 50 states + DC), plus a chain of
best-effort enrichment steps that resolve each school to its **GoFan ticket page**,
its **NFHS Network page**, and each game's **opponent school name + logo**. It ships
in two shapes off one spider: a **CLI/batch crawler** (canonical CSV + JSON + SQLite)
and a **FastAPI HTTP service** on Render that a Next.js/Vercel frontend drives to
produce two transient CSVs per job.

---

## 2. File map

| Path | Role |
|---|---|
| **`maxpreps_scraper/`** | The Scrapy project proper |
| `spiders/maxpreps.py` | The only real spider. Directory → school → schedule |
| `items.py` | `SchoolItem`, `ScheduleGameItem`. **Field order = CSV column order** |
| `pipelines.py` | `MultiFormatPipeline` — CSV + SQLite live, JSON on close |
| `export.py` | `SCHOOL_FIELDS` / `GAME_FIELDS` (the column contract) + DB→CSV/JSON rebuild |
| `nextdata.py` | `__NEXT_DATA__` blob extractor — `page_props(html)` |
| `states.py` | 50 states + DC, code→name |
| `settings.py` | Politeness, throttling, memory/time guards |
| **Entry points** | |
| `api.py` | FastAPI service. Async job model, in-memory `JOBS` dict |
| `worker.py` | One crawl + the whole enrichment chain, one per subprocess |
| `max_prep_scraper.py` | `FilteredMaxPrepsSpider`, `MaxPrepTwoFilePipeline`, `run_crawl()` |
| `run.py` | Thin `scrapy crawl` wrapper for the canonical CLI |
| `backfill_all.py` | Per-state, uncapped, resumable full backfill orchestrator |
| **Enrichment** | |
| `enrich_gofan.py` | GoFan catalog + the 3-tier `Matcher`. **The matching brain** — reused elsewhere |
| `enrich_gofan_scrapy.py` | Scrapy step: verify ticket URL → `go_fan_ticket_url`, overwrite `original_name` |
| `enrich_nfhs.py` | NFHS catalog + matcher → `nfhs_url` |
| `enrich_opponent.py` | Opponent school name + logo for the schedule CSV |
| `enrich_website_name.py` | **Orphaned** — see §9.5. Still exports `_settings()` that others import |
| **Odds and ends** | |
| `fetch_ahmad.py` | One-off: schools-only crawl of DE/DC/AK → `output/ahmad.csv` |
| `sample.py` | 0-byte file. Dead |
| `render.yaml` | Render blueprint. **The real production tuning lives in its env vars** |

Environment: **Python 3.9.6**, Scrapy **2.12.0**, Twisted **24.11.0**, FastAPI **0.128.8**.

---

## 3. The three constraints that explain every design decision

Understand these and the rest of the codebase stops looking odd.

### 3.1 One Twisted reactor per process, and it cannot be restarted

This is the big one. Scrapy's reactor starts once and dies once. Therefore:

- Every crawl runs in its **own subprocess** (`api.py` → `Popen(worker.py …)`).
- Every Scrapy-based enrichment step is **also** its own subprocess
  (`worker.py` → `subprocess.run([sys.executable, "enrich_gofan_scrapy.py", …])`).
- Any function that calls `CrawlerProcess.start()` is documented "call AT MOST ONCE
  per process": `run_crawl()`, `enrich_gofan_scrapy.run_enrich()`,
  `enrich_opponent.run_enrich()`, `enrich_website_name.run_enrich()`.

**If you add a Scrapy step, it must be a new subprocess.** Do not try to chain two
crawls in one process.

### 3.2 MaxPreps caps its public directory at 200 schools per state

The complete list is behind `/discovery/`, which `robots.txt` **disallows**, and
`ROBOTSTXT_OBEY = True`. So the spider reaches the rest by treating two
robots-allowed link sources as new seeds:

- `pageProps.nearbySchools` on each school page → `discovered_via="nearby"`
- opponent anchors in each schedule table, trimmed to the school root →
  `discovered_via="opponent"`

`_maybe_follow_school()` gates every discovered URL on `self.target_states`, so the
BFS can't leave the requested states. Toggle with `-a discover=0`.

100% coverage of the largest states is **not guaranteed** via public pages — that is
a MaxPreps limitation, not a bug. `backfill_all.py` exists to get as close as
possible by running each state uncapped and resumable.

### 3.3 Enrichment is best-effort and must never fail the job

Every enrichment step: `check=False`, exceptions swallowed and printed to stderr,
writes atomically (`tmp` file + `os.replace`). A job always returns usable data even
if every enrichment step dies. See §9.1 for the one place this guarantee leaks.

---

## 4. The crawl

### 4.1 Extraction strategy

MaxPreps is Next.js and **hashes its CSS class names**. So:

- School data, sport-seasons, nearby schools → parsed from the `__NEXT_DATA__` JSON
  blob via `nextdata.page_props()`. Robust.
- The schedule **table** → DOM-scraped, but anchored on the `Opponent` / `Date`
  `<th>` **text**, never on classes.
- Breadcrumbs (`enrich_opponent.py`) → schema.org `BreadcrumbList` JSON-LD first,
  then `<nav aria-label="breadcrumb">`, then any `<ol>`.

**Rule: never anchor on a MaxPreps CSS class.** It will churn.

### 4.2 Flow

```
/{state}/schools/              → parse_directory  → groupings[].canonicalUrl
  └ /{state}/{city}/{school}/   → parse_school    → SchoolItem + sportSeasons
       └ …/{sport}/schedule/     → parse_schedule  → ScheduleGameItem per table row
```

### 4.3 Crawl order is BREADTH-FIRST, on purpose

`settings.py` overrides Scrapy's defaults:

```python
DEPTH_PRIORITY = 1
SCHEDULER_DISK_QUEUE   = "scrapy.squeues.PickleFifoDiskQueue"
SCHEDULER_MEMORY_QUEUE = "scrapy.squeues.FifoMemoryQueue"
```

Scrapy defaults to **LIFO queues = depth-first**. That is actively wrong here: the ≤200
directory schools per state are the authoritative seed list, while the nearby/opponent
graph is unbounded. Depth-first dives into the graph and leaves the seeds queued, so any
truncation (`CLOSESPIDER_TIMEOUT`, `MEMUSAGE_LIMIT_MB`, the `api.py` watchdog) drops the
*best* schools and keeps the speculative ones. A Florida run regressed from ~190 directory
schools to 9 exactly this way.

Depth 1 = directory school pages, depth 2 = schedules + `nearbySchools`, depth 3+ =
opponents. BFS guarantees every seed is crawled before any discovered school. The wider
frontier spills to disk via `JOBDIR`, so it costs disk, not RAM.

**Diagnostic:** compare the spider's `Directory <st>: N schools listed` log line against
the count of `discovered_via = directory:<st>` rows in the CSV. If the CSV has far fewer,
the crawl was truncated — not the directory.

### 4.4 Spider arguments

Scrapy `-a` always delivers **strings**, hence `_truthy()` in `spiders/maxpreps.py`.

| Arg | Default | Meaning |
|---|---|---|
| `states` | `all` | `ny,ca,tx` or `all` |
| `schedules` | `1` | `0` = schools + sports only (fast) |
| `discover` | `1` | the §3.2 graph crawl |
| `levels` | `Varsity` | `all` → `None` → no level filter (adds JV/Freshman) |

`FilteredMaxPrepsSpider` (in `max_prep_scraper.py`) adds `sports`. Empty/absent
`sports` means **no filtering**, not "no sports".

---

## 5. Two output sinks — do not confuse them

This is the most common source of confusion in the repo.

### 5.1 Canonical (CLI path) — `MultiFormatPipeline`

Used by `run.py`, `scrapy crawl maxpreps`, `backfill_all.py`.

- **SQLite is the source of truth.** `INSERT OR REPLACE`, committed every 100 rows.
  PKs: `schools(school_id)`, `games(school_id, schedule_url, game_index)`.
- CSV is appended live and **resume-safe** (opens `"a"` when the file is non-empty).
- On `close_spider`, `export_db()` regenerates clean, de-duplicated CSV **and**
  streaming JSON arrays **from the DB**.
- `_ensure_columns()` is a hand-rolled migration: `CREATE TABLE IF NOT EXISTS` won't
  add a later-introduced field, so it `ALTER TABLE ADD COLUMN`s anything missing.
  **This is what lets `backfill_all.py` re-crawl against an old `maxpreps.db`.**

Files: `output/schools.{csv,json}`, `output/schedule.{csv,json}`, `output/maxpreps.db`.

Because writes are `INSERT OR REPLACE` on a PK, **re-crawling can only add or update
rows, never delete.** `backfill_all.py` relies on this and logs a regression tripwire
if a per-state count ever decreases.

### 5.2 API path — `MaxPrepTwoFilePipeline`

Used by `api.py` → `worker.py` → `run_crawl()`.

- Writes **only** `max_prep_School.csv` + `max_prep_schedule.csv`, in `"w"` mode.
- **No SQLite, no JSON, no canonical files.** `run_crawl()` swaps `ITEM_PIPELINES`
  wholesale so nothing can leak into the canonical store.
- Sets `JOBDIR` inside the per-job `output_dir` so the pending-request queue spills to
  disk instead of OOMing the box (keeps a crawl at ~55–70 MB observed).

**Column contract:** both pipelines write `SCHOOL_FIELDS` / `GAME_FIELDS` from
`maxpreps_scraper/export.py`. If you add an item field, add it there too or it
silently won't reach any CSV (`extrasaction="ignore"`).

---

## 6. The HTTP API

```
POST   /scrape             → Popen(worker.py …); JOBS[id] = {status, started_at, proc}
GET    /scrape/{id}        → _refresh_status() reconciles proc.poll()
GET    /scrape/{id}/results?type=teams|schedule  → CSV rows as JSON
GET    /scrape/{id}/download?type=teams|schedule → FileResponse
DELETE /scrape/{id}        → terminate + rmtree
GET    /states  /sports  /health
```

State is a plain in-memory dict; `jobs/` is `rmtree`d on startup. **Nothing is
persisted, by design.** A restart loses the job table.

`_refresh_all()` runs *before* counting active jobs on POST, so a dead-but-unpolled
job can't pin a concurrency slot forever.

Note: subprocess handles are only reaped when someone polls (`proc.poll()`), so a
job started and never polled leaves a zombie until the next request.

### 6.1 The timeout ladder — keep it ordered

Four layers, deliberately nested. **Any change to one must preserve the ordering.**

| Layer | Setting | Default | Deployed | On expiry |
|---|---|---|---|---|
| Crawl (time) | `CLOSESPIDER_TIMEOUT` | 1800 | **14400** | Graceful self-close, CSV flushed, enrichment still runs |
| Crawl (mem) | `MEMUSAGE_LIMIT_MB` | 1600 | **1600** | Graceful self-close before host OOM |
| Enrichment | `GOFAN` / `NFHS` / `OPPONENT` | 900 / 900 / 1800 | same | `subprocess.run(timeout=)` → **SIGKILL** |
| Job | `JOB_MAX_RUNTIME_SECONDS` | 5700 | **19800** | `proc.terminate()`, job → `error` |

**Invariant: `JOB_MAX_RUNTIME_SECONDS` > `CLOSESPIDER_TIMEOUT` + 900 + 900 + 1800.**
Deployed: 19800 > 18000, ~30 min of margin. If you raise any inner cap, raise the
watchdog in both `api.py` and `render.yaml`.

`enrich_opponent._settings()` additionally clamps its own `CLOSESPIDER_TIMEOUT` to
1500s (env `OPPONENT_CLOSESPIDER_TIMEOUT`) — deliberately *under* its 1800s
subprocess kill, so it self-closes and writes before being killed.

### 6.2 Memory hazard, already documented in `render.yaml`

`MAX_CONCURRENT_JOBS=2` against `MEMUSAGE_LIMIT_MB=1600` on a 2 GB box: the memory
cap is **per process**, so two crawls both approaching it would exceed the box and
trigger an ungraceful OS OOM kill instead of the graceful self-close. In practice
`JOBDIR` keeps a crawl far below the cap. To be safe by construction, either drop
`MEMUSAGE_LIMIT_MB` to ~800 or set `MAX_CONCURRENT_JOBS=1`.

Rule of thumb: `MEMUSAGE_LIMIT_MB ≈ (RAM_MB − 400) / MAX_CONCURRENT_JOBS`.

---

## 7. The enrichment chain (`worker.py`)

Runs after the crawl, **strictly ordered** — each step feeds the next.

| # | Step | Process | Writes |
|---|---|---|---|
| 1 | `run_crawl()` | inline | the two CSVs |
| 2 | `_copy_name_to_original_name` | inline | `original_name` = MaxPreps `name` |
| 3 | `enrich_gofan_scrapy.py` | subprocess | `go_fan_ticket_url`; **overwrites** `original_name` with GoFan's spelling |
| 4 | `enrich_nfhs.py` | subprocess | `nfhs_url` |
| 5a | `_prefill_opponent_columns` | inline | seeds both opponent columns from raw `opponent` text |
| 5b | `enrich_opponent.py` | subprocess | `original_opponent_school_name`, `original_opponent_school_logo` |

**Why the order matters:** step 5 cross-references the teams CSV's `original_name`,
which isn't final until step 3 has run. Step 4 matches on `original_name` for the
same reason. **Do not reorder.**

**Why the inline steps exist:** steps 2 and 5a guarantee their columns exist on every
row *before* the network-bound, SIGKILL-capped subprocess runs. A killed subprocess
never reaches its `closed()` handler, so without the prefill the column would be
missing from the CSV header entirely. (Steps 3 and 4 lack this guard — see §9.1.)

### 7.1 Extra columns the API CSVs carry beyond the canonical schema

`teams.csv`: `original_name`, `go_fan_ticket_url`, `nfhs_url`
`schedule.csv`: `original_opponent_school_name`, `original_opponent_school_logo`

---

## 8. The matching logic

The most carefully-built code in the repo. `enrich_gofan.Matcher` and
`enrich_nfhs.Matcher` share the same shape; `enrich_opponent` reuses
`enrich_gofan.normalize`.

- **State is enforced structurally.** Candidates are only ever drawn from
  `by_state[st]`, so a cross-state match is *impossible by construction*, not merely
  checked. Preserve this property in any change.
- **Tier 1 — exact** normalized name, each candidate city/zip-gated. If exact names
  exist but none agree on city, return `none` rather than falling through. (Correct:
  don't fuzzy-match past a confident-but-wrong-city name.)
- **Tier 2 — token containment**, either direction: `"Little Snake River"` ⊂
  `"Little Snake River Valley School"`. Ordered by fewest extra words. Needed because
  `difflib`'s whole-string ratio penalises the length gap below the cutoff.
- **Tier 3 — `difflib.get_close_matches`** at `FUZZY_CUTOFF = 0.87`, city-gated.
- **`require_positive`** — a single-token row name (`"East"`) is weak, so it demands a
  *real* city/zip hit rather than the permissive "nothing to compare, allow it"
  fallback. City is the only safeguard there.
- **`normalize()`** drops only `high school|high|hs|school`. Distinguishing words
  (academy, christian, charter, catholic) are **intentionally kept** so different
  schools aren't merged.

### 8.1 Opponent breadcrumb parsing

`enrich_opponent.school_from_crumbs()` walks crumbs **backwards and anchors on the
sport**, never on position — because the school crumb is *last* on a varsity page but
*second-to-last* on a level page (`/football/freshman/` appends its own "Freshman"
crumb):

```
varsity   … / CA Basketball / Saint Mary's High School Basketball
freshman  … / WY Football   / Star Valley High School Football / Freshman
```

Taking the last crumb outright returns `"Freshman"`. Every name source is guarded by
`_is_level()` so a bare level word can never be written as a school name. Each
distinct opponent URL is fetched exactly once, however many games reference it.

### 8.2 Catalogs

| | GoFan | NFHS |
|---|---|---|
| Source | `api.gofan.co/v2/schools` | `search-api.nfhsnetwork.com/search/schools` |
| Size | ~25.7k | ~16k |
| Cache | `output/gofan_catalog.json` | `output/nfhs_catalog.json` |
| Build | simple paging, size 2000 | 52 steps (`__full__` + 51 states), size 500, deduped by slug |
| Resumable | no | yes — `output/nfhs_catalog.progress.json` |

NFHS is awkward because its Elasticsearch backend caps `from+size <= 10000`, has no
working cursor, and its `state` filter returns only a curated subset — so the catalog
is assembled from the two reachable slices and deduped. It also rate-limits per
connection, hence `Connection: close` on every call and patient backoff.

Refresh either with `--refresh`.

---

## 9. Known issues and gotchas

Read this section before changing anything in the enrichment chain.

### 9.1 GoFan/NFHS columns can be **absent**, not blank

`subprocess.run(timeout=)` sends **SIGKILL**. The CSV write lives in the spider's
`closed()` handler, so a timed-out GoFan step writes nothing — `go_fan_ticket_url`
is then missing from the header entirely. Same for `nfhs_url`, which is additionally
skipped whenever `enrich_nfhs.py` exits **3** on an incomplete catalog.

Steps 2 and 5a in `worker.py` exist precisely to prevent this for the *other*
columns; these two never got the same treatment. A frontend indexing
`row.go_fan_ticket_url` sees `undefined` rather than `""`.

**Fix shape:** add an inline prefill mirroring `_prefill_opponent_columns()`.

### 9.2 Catalog caches are ephemeral on Render

`CATALOG_CACHE` paths are **relative** (`output/…`). `output/` is gitignored and
Render's filesystem is ephemeral, so **both catalogs re-download on every
deploy/restart**. The NFHS build is 52 sequential API steps with 1s inter-page sleeps
and up to ~300s backoff per failing request, against a 900s cap — likely to be killed
mid-build on a cold instance, which then triggers §9.1. Its resume checkpoint lives
in the same ephemeral dir, so the resume never fires across restarts.

**Fix shape:** persistent disk, or bake the catalogs in at build time, or make the
cache dir env-configurable.

### 9.3 Schedule-table XPath selects more than one table

`maxpreps_scraper/spiders/maxpreps.py`:

```python
'//table[.//th[contains(., "Opponent")] and .//th[contains(., "Date")]][1]'
```

`//table[pred][1]` means "every table matching `pred` that is first **among its
siblings**" — not "the first match in the document". Verified: with two qualifying
tables under different parents this selects **2**, and `rows` unions both. Correct
form is `(//table[pred])[1]`. Latent today (pages render one table), but it would
merge two tables' games into one team's schedule under a single `enumerate` counter.

### 9.4 ~~`max_prep_scraper.py` docstring contradicts its code~~ — FIXED

The module docstring used to claim it "keeps only the schools that offer at least one of
the requested sports"; the code has always kept **every** school and filtered only which
*schedules* are fetched. Commit `6fc3feb feat: remove sport` changed the behavior without
updating the header. Docstring corrected, and the sport filter no longer narrows discovery
either (see §4.3 and `DISCOVERY_SCHEDULES_PER_SCHOOL`).

### 9.5 `enrich_website_name.py` is orphaned but undeletable

`worker.py` no longer calls it and the README doesn't mention it, so its spider is
dead code. It cannot simply be deleted because `enrich_gofan_scrapy.py` imports
`_settings` from it — and that settings function sets `ROBOTSTXT_OBEY: False`, which
was written for *arbitrary public school homepages* but is now applied to
**gofan.co**. Either resolve the orphan (delete the spider, move `_settings` to a
shared module) or re-instate the step deliberately.

### 9.6 `requests` is used but undeclared

`enrich_nfhs.py` imports `requests`; it is **not** in `requirements.txt` and resolves
only transitively via `Scrapy → tldextract → requests`. If that chain changes, the
NFHS step dies with an ImportError that `check=False` silently swallows.

### 9.7 Blocking I/O inside `start_requests`

`enrich_gofan_scrapy.GofanTicketSpider.start_requests` → `Matcher.match` →
`Matcher.detail`, which does a synchronous `urllib.request.urlopen` **plus
`time.sleep(0.15)`** per uncached candidate. `start_requests` is pulled by the Twisted
engine, so each call stalls the whole reactor. Memoized per `huddleId`, so bounded by
distinct candidates — but still a serial stall inside the 900s budget on a big state.

### 9.8 Smaller items

- `api.py` uses `@app.on_event("startup")`, deprecated in the installed FastAPI 0.128.
  Migrate to the `lifespan` context manager.
- `start_scrape(payload: dict)` has no Pydantic model — a JSON list for `sports`
  reaches `.strip()` and 500s. A request model would give a clean 422.
- `_count_rows` counts physical lines, so a quoted embedded newline inflates `counts`.
  `csv.reader` would be exact.
- `run.py` arg parsing is order-dependent: `python run.py -s X=1 wy` puts `wy` into
  passthrough instead of using it as the state code.
- `sample.py` is a 0-byte file.

### 9.9 Dependency pins are load-bearing — read the comments before bumping

- **`Scrapy < 2.13`**: 2.13 deprecated and 2.16 **removed** `Spider.start_requests()`
  in favour of an async `start()`. Every spider here uses `start_requests()`, so a
  newer Scrapy makes them **silently yield zero requests → empty CSVs**. Migrating
  the spiders is a prerequisite for bumping this.
- **`Twisted < 25`**: 25+ removed `_setAcceptableProtocols`, which Scrapy 2.12 imports.

---

## 10. Invariants — don't break these

1. **One `CrawlerProcess.start()` per process.** New Scrapy step ⇒ new subprocess.
2. **Enrichment never fails the job.** `check=False`, swallow exceptions, write
   atomically via `tmp` + `os.replace`.
3. **Every enriched column must exist on every row**, even when its step is killed.
   Prefill inline first (§9.1 is the outstanding violation).
4. **Enrichment order is fixed**: `original_name` copy → GoFan → NFHS → opponent.
5. **Watchdog > sum of inner caps** (§6.1), mirrored in `api.py` *and* `render.yaml`.
6. **Matcher state scoping stays structural** — candidates drawn from `by_state[st]`.
7. **The API path never writes canonical output.** Keep `ITEM_PIPELINES` swapped.
8. **Never anchor on a MaxPreps CSS class.** Use `__NEXT_DATA__`, header text, or
   schema.org.
8b. **Keep the crawl breadth-first** (§4.3). Reverting to Scrapy's LIFO defaults makes any
    truncation drop the authoritative directory seeds first.
8c. **The sport filter applies to schedules only** — never to which schools are emitted,
    and never to which schools can be discovered.
9. **Canonical writes are additive** (`INSERT OR REPLACE` on a PK). Never delete rows;
   `backfill_all.py`'s no-regression guarantee depends on it.
10. **New item field ⇒ add it to `export.py`'s field lists**, or it silently never
    reaches a CSV.

---

## 11. Commands

```bash
source .venv/bin/activate            # Python 3.9.6

# --- canonical CLI crawl (CSV + JSON + SQLite) ---
python run.py wy                     # one state
python run.py wy,vt,ri               # a few
python run.py wy --no-schedules      # schools + sports only (fast)
python run.py all --levels all       # everything incl. JV/Freshman
scrapy crawl maxpreps -a states=ny -s JOBDIR=.jobs/ny     # resumable

# --- sport-filtered two-CSV crawl (what the API runs) ---
python max_prep_scraper.py ny Football
python max_prep_scraper.py ca "Football,Flag Football" --levels all

# --- full backfill, uncapped + resumable, per state ---
python backfill_all.py fl            # canary
python backfill_all.py               # all 51

# --- the API ---
uvicorn api:app --reload             # http://localhost:8000
curl -X POST localhost:8000/scrape -H 'Content-Type: application/json' \
     -d '{"states":"wy","sports":"Football"}'
curl localhost:8000/scrape/<job_id>                        # poll → status: done
curl "localhost:8000/scrape/<job_id>/download?type=teams" -o teams.csv

# --- drive a job without the HTTP layer (best for debugging) ---
python worker.py /tmp/wyjob wy Football Varsity 1

# --- enrichment steps standalone ---
python enrich_gofan_scrapy.py output/max_prep_School.csv [--refresh]
python enrich_nfhs.py         output/max_prep_School.csv [--refresh]
python enrich_opponent.py     output/max_prep_schedule.csv output/max_prep_School.csv

# --- rebuild CSV/JSON from the canonical SQLite ---
python -m maxpreps_scraper.export output
```

**Smoke test of choice: Wyoming + Football.** Small and fast enough to iterate on.

---

## 12. Deployment

Single Render **web service** defined by `render.yaml` (New + → Blueprint → this repo).

- Build `pip install -r requirements.txt`; start `uvicorn api:app --host 0.0.0.0 --port $PORT`
- Health check `/health`
- Plan **standard** (2 GB / 1 CPU) — `starter` (512 MB) OOM-kills big-state discovery crawls
- Env: `FRONTEND_ORIGIN` (CORS; defaults `*`, tighten to the Vercel URL),
  `MAX_CONCURRENT_JOBS`, `MEMUSAGE_LIMIT_MB`, `CLOSESPIDER_TIMEOUT`,
  `JOB_MAX_RUNTIME_SECONDS`

Caveats: the free tier kills long crawls on idle timeout; the in-memory job table is
lost on restart (by design); guaranteed full coverage of the largest states is still
best done locally with `backfill_all.py`, which is fully uncapped.

---

## 13. Where to start for common tasks

| Task | Start here |
|---|---|
| Add a scraped school/game field | `items.py` → `export.py` field lists → `spiders/maxpreps.py` |
| Add an enrichment column | `worker.py` (prefill + subprocess), new `enrich_*.py`, §10 rules 2–4 |
| Change matching accuracy | `enrich_gofan.py::Matcher` (shared shape with `enrich_nfhs.py`) |
| Fix an opponent name/logo bug | `enrich_opponent.py` — `school_from_crumbs`, `school_name`, `NameIndex` |
| Change API shape | `api.py`; note the `FILENAMES` coupling to `MaxPrepTwoFilePipeline` |
| Tune crawl politeness/limits | `maxpreps_scraper/settings.py` + `render.yaml` env vars |
| Improve coverage | §3.2, then `backfill_all.py` |

**Highest-value tests to write first** (pure functions, no network): the tier logic in
`enrich_gofan.Matcher` / `enrich_nfhs.Matcher`, `normalize()`, and
`enrich_opponent`'s `school_from_crumbs` / `strip_sport` / `strip_level` / `_is_level`.
