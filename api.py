#!/usr/bin/env python
"""HTTP API around the MaxPreps sports-filtered scraper (deployable on Render).

A MaxPreps crawl is long-running (minutes to hours) and Scrapy's Twisted reactor
cannot be restarted in-process, so this service uses an async **job** model:

    POST /scrape                      -> { job_id, status: "running" }   (starts a crawl)
    GET  /scrape/{job_id}             -> { status, counts, error? }      (poll)
    GET  /scrape/{job_id}/results     -> [ {...}, ... ]   ?type=teams|schedule
    GET  /scrape/{job_id}/download    -> CSV file         ?type=teams|schedule
    DELETE /scrape/{job_id}           -> { deleted: true }  (frontend calls after download)
    GET  /states                      -> [ { code, name }, ... ]
    GET  /sports                      -> [ "Football", ... ]
    GET  /health                      -> { ok: true }

Each crawl runs in its OWN subprocess (``worker.py``) so the one-reactor-per-process
limit is respected. Results are transient CSV files under ``jobs/<job_id>/`` -- nothing
is persisted to a database; files are deleted on DELETE or swept on restart.
"""
import csv
import os
import shutil
import subprocess
import sys
import time
import uuid

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from maxpreps_scraper.states import STATES

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
JOBS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs")
# Pipeline (MaxPrepTwoFilePipeline) always writes these two filenames:
FILENAMES = {"teams": "max_prep_School.csv", "schedule": "max_prep_schedule.csv"}
# Cap simultaneous crawls so a burst of requests can't exhaust the instance.
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "2"))
# Wall-clock backstop: a job 'running' longer than this whose subprocess is gone is
# treated as failed, so a crashed/hung crawl can never pin a concurrency slot forever.
# Kept above worker.py's 1800s enrichment timeout so legitimate long jobs aren't killed.
JOB_MAX_RUNTIME_SECONDS = int(os.environ.get("JOB_MAX_RUNTIME_SECONDS", "2700"))
# Common high-school sports as MaxPreps labels them (for the frontend dropdown).
COMMON_SPORTS = [
    "Football", "Basketball", "Baseball", "Softball", "Soccer", "Volleyball",
    "Wrestling", "Track & Field", "Cross Country", "Tennis", "Golf", "Lacrosse",
    "Field Hockey", "Ice Hockey", "Swimming", "Flag Football",
]

# job_id -> { status, started_at, error, states, sports, proc }
# status: "running" | "done" | "error"
JOBS = {}

app = FastAPI(title="DimeDropper MaxPreps Scraper API")

# CORS: allow the Vercel frontend. Default "*" so the first deploy works before the
# frontend URL is known; tighten via FRONTEND_ORIGIN once the frontend is deployed.
_origin = os.environ.get("FRONTEND_ORIGIN", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _origin == "*" else [o.strip() for o in _origin.split(",")],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _job_dir(job_id):
    return os.path.join(JOBS_DIR, job_id)


def _csv_path(job_id, kind):
    return os.path.join(_job_dir(job_id), FILENAMES[kind])


def _validate_states(raw):
    """Return a normalized 'ny,ca' string, or raise 400. 'all' is allowed."""
    if not raw or not str(raw).strip():
        raise HTTPException(400, "states is required (e.g. 'ny' or 'ny,ca' or 'all')")
    if str(raw).strip().lower() == "all":
        return "all"
    codes = [c.strip().lower() for c in str(raw).split(",") if c.strip()]
    bad = [c for c in codes if c not in STATES]
    if bad:
        raise HTTPException(400, f"unknown state code(s): {', '.join(bad)}")
    if not codes:
        raise HTTPException(400, "no valid state codes provided")
    return ",".join(codes)


def _refresh_status(job_id):
    """Reconcile a 'running' job with its subprocess's actual exit state."""
    job = JOBS[job_id]
    if job["status"] != "running":
        return job
    proc = job.get("proc")
    if proc is None:
        # No live handle but still 'running' -> a previously-started job whose process
        # we lost track of. Fail it once it exceeds the runtime backstop so it can't
        # pin a concurrency slot forever.
        if time.time() - job.get("started_at", 0) > JOB_MAX_RUNTIME_SECONDS:
            job["status"] = "error"
            job["error"] = "job timed out"
        return job
    rc = proc.poll()
    if rc is None:
        # Still executing -- unless it has blown past the wall-clock backstop.
        if time.time() - job.get("started_at", 0) > JOB_MAX_RUNTIME_SECONDS:
            proc.terminate()
            job["status"] = "error"
            job["error"] = "job timed out"
            job["proc"] = None
        return job
    if rc == 0:
        job["status"] = "done"
    else:
        job["status"] = "error"
        job["error"] = f"crawl process exited with code {rc}"
    job["proc"] = None
    return job


def _refresh_all():
    """Reconcile every job against its subprocess. Call before counting active jobs so
    finished-but-unpolled crawls don't falsely consume concurrency slots."""
    for job_id in list(JOBS.keys()):
        _refresh_status(job_id)


def _count_rows(path):
    if not os.path.exists(path):
        return 0
    with open(path, newline="", encoding="utf-8") as fh:
        # subtract the header row
        return max(0, sum(1 for _ in fh) - 1)


def _read_rows(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


@app.on_event("startup")
def _sweep_stale_jobs():
    """Clear any leftover job dirs from a previous run (no persistence by design)."""
    if os.path.isdir(JOBS_DIR):
        shutil.rmtree(JOBS_DIR, ignore_errors=True)
    os.makedirs(JOBS_DIR, exist_ok=True)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/states")
def list_states():
    return [{"code": code, "name": name} for code, name in STATES.items()]


@app.get("/sports")
def list_sports():
    return COMMON_SPORTS


@app.post("/scrape")
def start_scrape(payload: dict):
    states = _validate_states(payload.get("states"))
    sports = (payload.get("sports") or "").strip()
    if not sports:
        raise HTTPException(400, "sports is required (e.g. 'Football' or 'Football,Basketball')")
    levels = (payload.get("levels") or "Varsity").strip() or "Varsity"
    discover = payload.get("discover", True)

    # Reconcile finished/dead subprocesses first so only genuinely-running crawls count
    # toward the cap (otherwise a fast-failing job stuck 'running' would block new ones).
    _refresh_all()
    active = sum(1 for j in JOBS.values() if j["status"] == "running")
    if active >= MAX_CONCURRENT_JOBS:
        raise HTTPException(429, "too many scrapes running; try again shortly")

    job_id = uuid.uuid4().hex
    out_dir = _job_dir(job_id)
    os.makedirs(out_dir, exist_ok=True)

    proc = subprocess.Popen(
        [
            sys.executable, "worker.py",
            out_dir, states, sports, levels, "1" if discover else "0",
        ],
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    JOBS[job_id] = {
        "status": "running",
        "started_at": time.time(),
        "error": None,
        "states": states,
        "sports": sports,
        "proc": proc,
    }
    return {"job_id": job_id, "status": "running"}


@app.get("/scrape/{job_id}")
def scrape_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "unknown job_id")
    job = _refresh_status(job_id)
    counts = None
    if job["status"] == "done":
        counts = {
            "teams": _count_rows(_csv_path(job_id, "teams")),
            "schedule": _count_rows(_csv_path(job_id, "schedule")),
        }
    return {
        "job_id": job_id,
        "status": job["status"],
        "states": job["states"],
        "sports": job["sports"],
        "counts": counts,
        "error": job["error"],
    }


@app.get("/scrape/{job_id}/results")
def scrape_results(job_id: str, type: str = Query("teams", pattern="^(teams|schedule)$")):
    if job_id not in JOBS:
        raise HTTPException(404, "unknown job_id")
    job = _refresh_status(job_id)
    if job["status"] != "done":
        raise HTTPException(409, f"job not done (status: {job['status']})")
    return _read_rows(_csv_path(job_id, type))


@app.get("/scrape/{job_id}/download")
def scrape_download(job_id: str, type: str = Query("teams", pattern="^(teams|schedule)$")):
    if job_id not in JOBS:
        raise HTTPException(404, "unknown job_id")
    job = _refresh_status(job_id)
    if job["status"] != "done":
        raise HTTPException(409, f"job not done (status: {job['status']})")
    path = _csv_path(job_id, type)
    if not os.path.exists(path):
        raise HTTPException(404, f"no {type} file for this job")
    filename = "teams.csv" if type == "teams" else "schedule.csv"
    return FileResponse(path, media_type="text/csv", filename=filename)


@app.delete("/scrape/{job_id}")
def delete_scrape(job_id: str):
    job = JOBS.pop(job_id, None)
    if job and job.get("proc") and job["proc"].poll() is None:
        job["proc"].terminate()
    shutil.rmtree(_job_dir(job_id), ignore_errors=True)
    return {"deleted": True}
