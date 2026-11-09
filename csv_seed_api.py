#!/usr/bin/env python
"""HTTP surface for CSV-seeded MaxPreps jobs, mounted at ``/scrape/csv``.

    POST   /scrape/csv                    multipart: file (CSV with maxPrepsLink), sports
    GET    /scrape/csv/{job_id}           status, phase, progress counters, error
    GET    /scrape/csv/{job_id}/results   ?type=teams|schedule|roster|coverage&offset&limit
    GET    /scrape/csv/{job_id}/download  ?type=teams|schedule|roster|coverage
    POST   /scrape/csv/{job_id}/resume    continue a job that ended in error (e.g. blocked)
    DELETE /scrape/csv/{job_id}

A job is many hours (≈15–18 h per sport for ~5,000 schools), so unlike ``/scrape`` it is
built to survive the server restarting underneath it:

* **Persistent registry.** Jobs are recorded in ``$CSV_JOBS_DIR/jobs.json`` and the job
  directories are never swept at startup. Point ``CSV_JOBS_DIR`` at a Render persistent
  disk (``/var/data/csv_jobs``) for this to survive deploys; without a disk it still
  works, but a deploy loses in-flight jobs.
* **Auto-resume.** ``resume_jobs()`` (called from api.py's startup) relaunches every job
  that was running. ``csv_worker.py`` resumes at its first unfinished phase, and the
  crawl resumes from its JOBDIR + SQLite -- no work redone, no duplicate rows.
* **Liveness, not a runtime cap.** A job fails only if its worker stops heartbeating for
  ``CSV_JOB_STALL_SECONDS`` (then it is restarted, which resumes it), or after
  ``CSV_JOB_MAX_RUNTIME_SECONDS`` if that is set (default 0 = never).
* **Partial results while running.** ``download`` / ``results`` on an unfinished job
  export a snapshot from the job's SQLite, with the same header as the final file.

Self-contained like ``middle_school_api.py``: its own registry, directory, concurrency
cap and timeouts, so it never consumes a ``/scrape`` slot or trips ``/scrape``'s watchdog.
"""
import csv
import fcntl
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

from csv_seed_scraper import DB_FILE, export_coverage, load_links
from csv_worker import COVERAGE_CSV, LOCK_FILE, PROGRESS_JSON
from maxpreps_scraper import access
from maxpreps_scraper.export import GAME_FIELDS, ROSTER_FIELDS, SCHOOL_FIELDS
from worker import EXIT_BLOCKED, JOB_ERROR_FILE, ROSTER_CSV, SCHEDULE_CSV, TEAMS_CSV

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_JOBS_DIR = os.environ.get("CSV_JOBS_DIR") or os.path.join(HERE, "csv_jobs")
REGISTRY = os.path.join(CSV_JOBS_DIR, "jobs.json")

FILENAMES = {"teams": TEAMS_CSV, "schedule": SCHEDULE_CSV, "roster": ROSTER_CSV,
             "coverage": COVERAGE_CSV}
RESULT_TYPES = f"^({'|'.join(FILENAMES)})$"

MAX_CONCURRENT = int(os.environ.get("CSV_MAX_CONCURRENT_JOBS", "1"))
STALL_SECONDS = int(os.environ.get("CSV_JOB_STALL_SECONDS", "1800"))
MAX_RUNTIME_SECONDS = int(os.environ.get("CSV_JOB_MAX_RUNTIME_SECONDS", "0"))
MAX_AUTO_RESTARTS = int(os.environ.get("CSV_MAX_AUTO_RESTARTS", "10"))
MAX_UPLOAD_BYTES = int(os.environ.get("CSV_MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))
UPLOAD_CHUNK = 1024 * 1024
MAX_PAGE = 1000

# Final-file headers = crawl columns + the columns enrichment adds (worker.py §7.1).
# Snapshots of an unfinished job use the same headers, so a consumer never has to care
# which one it got.
SNAPSHOT_EXTRA = {
    "teams": ["original_name", "go_fan_ticket_url", "nfhs_url"],
    "schedule": ["original_opponent_school_name", "original_opponent_school_logo"],
    "roster": [],
}
_TABLES = {"teams": ("schools", SCHOOL_FIELDS), "schedule": ("games", GAME_FIELDS),
           "roster": ("roster", ROSTER_FIELDS)}

# job_id -> registry record (persisted); live Popen handles are kept apart, unpersisted.
JOBS = {}
_PROCS = {}
_lock = threading.RLock()

router = APIRouter(prefix="/scrape/csv", tags=["csv-seed"])
csv.field_size_limit(10**7)


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
def _job_dir(job_id):
    return os.path.join(CSV_JOBS_DIR, job_id)


def _save():
    os.makedirs(CSV_JOBS_DIR, exist_ok=True)
    tmp = REGISTRY + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(JOBS, fh, indent=1)
    os.replace(tmp, REGISTRY)


def _load():
    try:
        with open(REGISTRY, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    # Only keep jobs whose directory still exists (a disk-less restart wipes them).
    return {k: v for k, v in data.items() if os.path.isdir(_job_dir(k))}


def _progress(job_id):
    try:
        with open(os.path.join(_job_dir(job_id), PROGRESS_JSON), encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def _job_error(job_id):
    try:
        with open(os.path.join(_job_dir(job_id), JOB_ERROR_FILE), encoding="utf-8") as fh:
            return fh.read().strip() or None
    except OSError:
        return None


def _worker_alive(job_id):
    """True if a csv_worker currently holds this job's lock (works after an API restart,
    when we have no Popen handle -- e.g. uvicorn --reload leaves the worker running)."""
    path = os.path.join(_job_dir(job_id), LOCK_FILE)
    if not os.path.exists(path):
        return False
    with open(path, "a+") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fh, fcntl.LOCK_UN)
        return False


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #
def _launch(job_id):
    job = JOBS[job_id]
    out_dir = _job_dir(job_id)
    log = open(os.path.join(out_dir, "worker.log"), "a", encoding="utf-8")
    _PROCS[job_id] = subprocess.Popen(
        [sys.executable, "csv_worker.py", out_dir, os.path.join(out_dir, "input.csv"),
         job["sports"]],
        cwd=HERE, stdout=log, stderr=subprocess.STDOUT,
    )
    log.close()  # the child has its own copy of the descriptor
    job.update(status="running", error=None, launched_at=time.time())
    _save()


def _finish(job_id, status, error=None):
    JOBS[job_id].update(status=status, error=error, finished_at=time.time())
    _PROCS.pop(job_id, None)
    _save()


def _relaunch_or_fail(job_id, why):
    job = JOBS[job_id]
    if job.get("restarts", 0) >= MAX_AUTO_RESTARTS:
        _finish(job_id, "error", f"{why}; gave up after {MAX_AUTO_RESTARTS} automatic restarts "
                                 "(POST /scrape/csv/{id}/resume to try again)")
        return
    job["restarts"] = job.get("restarts", 0) + 1
    print(f"csv_seed_api: job {job_id}: {why}; restarting (resumes)", file=sys.stderr)
    _launch(job_id)


def _settle(job_id, rc):
    """A worker we launched has exited with ``rc``: decide the job's fate."""
    progress = _progress(job_id)
    if rc == 0 and progress.get("state") == "done":
        _finish(job_id, "done")
    elif rc == EXIT_BLOCKED or progress.get("state") == "error":
        _finish(job_id, "error", _job_error(job_id) or progress.get("error")
                or f"worker exited with code {rc}")
    elif rc is not None and (rc < 0 or rc >= 128):
        _relaunch_or_fail(job_id, f"worker interrupted (exit {rc})")
    else:
        _finish(job_id, "error", _job_error(job_id) or f"worker exited with code {rc}")


def _stalled(progress, job):
    beat = progress.get("heartbeat_at") or job.get("launched_at") or time.time()
    return time.time() - beat > STALL_SECONDS


def _stop_worker(job_id, progress):
    proc = _PROCS.get(job_id)
    if proc is not None and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
        return
    pid = progress.get("pid")
    if pid and _worker_alive(job_id):  # the lock proves this pid is still our worker
        try:
            os.kill(int(pid), signal.SIGTERM)
        except (OSError, ValueError):
            return
        # Wait for it to let go of the job (it stops its crawl gracefully first), so a
        # replacement launched right after doesn't find the lock still held.
        deadline = time.time() + 150
        while time.time() < deadline and _worker_alive(job_id):
            time.sleep(1)


def _refresh(job_id):
    with _lock:
        job = JOBS[job_id]
        if job["status"] != "running":
            return job
        progress = _progress(job_id)
        proc = _PROCS.get(job_id)

        if proc is not None:
            rc = proc.poll()
            if rc is not None:
                _settle(job_id, rc)
                return job
        elif not _worker_alive(job_id):
            # No handle and no live worker: the API restarted, or the worker died.
            state = progress.get("state")
            if state == "done":
                _finish(job_id, "done")
            elif state == "error":
                _finish(job_id, "error", _job_error(job_id) or progress.get("error"))
            else:
                _relaunch_or_fail(job_id, "worker not running")
            return job

        # Worker is alive (ours or adopted). Guard against hangs and the optional cap.
        if MAX_RUNTIME_SECONDS and time.time() - job.get("created_at", 0) > MAX_RUNTIME_SECONDS:
            _stop_worker(job_id, progress)
            _finish(job_id, "error", "job exceeded CSV_JOB_MAX_RUNTIME_SECONDS")
        elif _stalled(progress, job):
            _stop_worker(job_id, progress)
            _relaunch_or_fail(job_id, f"no heartbeat for {STALL_SECONDS}s")
        return job


def _refresh_all():
    for job_id in list(JOBS):
        _refresh(job_id)


def resume_jobs():
    """Startup hook (api.py): reload the registry and resume every unfinished job."""
    with _lock:
        os.makedirs(CSV_JOBS_DIR, exist_ok=True)
        JOBS.clear()
        JOBS.update(_load())
        _save()
        for job_id, job in JOBS.items():
            if job["status"] == "running":
                job["restarts"] = 0  # a fresh process gets a fresh restart budget
        _refresh_all()


def _require(job_id):
    if job_id not in JOBS:
        raise HTTPException(404, "unknown job_id")
    return _refresh(job_id)


# --------------------------------------------------------------------------- #
# Files: final or snapshot
# --------------------------------------------------------------------------- #
def _snapshot(job_id, kind):
    """Export the current state of an unfinished job to ``snapshot/<file>``."""
    out_dir = _job_dir(job_id)
    db_path = os.path.join(out_dir, DB_FILE)
    snap_dir = os.path.join(out_dir, "snapshot")
    os.makedirs(snap_dir, exist_ok=True)
    path = os.path.join(snap_dir, FILENAMES[kind])
    if kind == "coverage":
        export_coverage(db_path, path)
        return path

    table, fields = _TABLES[kind]
    header = fields + SNAPSHOT_EXTRA[kind]
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        if os.path.exists(db_path):
            db = sqlite3.connect(db_path, timeout=60)
            try:
                for row in db.execute(f"SELECT {','.join(fields)} FROM {table}"):
                    rec = dict(zip(fields, row))
                    if kind == "teams":
                        extra = [rec.get("name") or "", "", ""]
                    elif kind == "schedule":
                        extra = [rec.get("opponent") or "", ""]
                    else:
                        extra = []
                    writer.writerow(list(row) + extra)
            except sqlite3.OperationalError:
                pass  # tables not created yet: header only
            finally:
                db.close()
    os.replace(tmp, path)
    return path


def _file_for(job_id, kind):
    """``(path, is_final)``: the finished file, or a fresh snapshot while unfinished."""
    job = JOBS[job_id]
    final = os.path.join(_job_dir(job_id), FILENAMES[kind])
    if job["status"] == "done" and os.path.exists(final):
        return final, True
    return _snapshot(job_id, kind), False


def _count_rows(path):
    if not os.path.exists(path):
        return 0
    with open(path, newline="", encoding="utf-8") as fh:
        return max(0, sum(1 for _ in csv.reader(fh)) - 1)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.post("")
async def start_csv_scrape(file: UploadFile = File(...), sports: str = Form(...)):
    """Upload a CSV of MaxPreps school links (``maxPrepsLink`` column) and start a job."""
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(400, "file must be a .csv")
    sports = (sports or "").strip()
    if not sports:
        raise HTTPException(400, "sports is required, e.g. 'Basketball' or 'Football,Soccer'")

    with _lock:
        _refresh_all()
        if sum(1 for j in JOBS.values() if j["status"] == "running") >= MAX_CONCURRENT:
            raise HTTPException(429, "a CSV scrape is already running; try again later")

    job_id = uuid.uuid4().hex
    out_dir = _job_dir(job_id)
    os.makedirs(out_dir, exist_ok=True)
    input_csv = os.path.join(out_dir, "input.csv")
    size = 0
    try:
        with open(input_csv, "wb") as fh:
            while chunk := await file.read(UPLOAD_CHUNK):
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(413, "uploaded file is too large")
                fh.write(chunk)
    except HTTPException:
        shutil.rmtree(out_dir, ignore_errors=True)
        raise
    finally:
        await file.close()

    # Fail fast on a file that can't work, before a worker is ever launched.
    try:
        info = load_links(input_csv)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        shutil.rmtree(out_dir, ignore_errors=True)
        raise HTTPException(400, f"could not use the uploaded CSV: {exc}")
    if not info["links"]:
        shutil.rmtree(out_dir, ignore_errors=True)
        sample = "; ".join(f"line {n}: {why}" for n, _raw, why in info["skipped"][:5])
        raise HTTPException(400, f"no usable maxPrepsLink values ({sample})")

    with _lock:
        _refresh_all()
        if sum(1 for j in JOBS.values() if j["status"] == "running") >= MAX_CONCURRENT:
            shutil.rmtree(out_dir, ignore_errors=True)
            raise HTTPException(429, "a CSV scrape is already running; try again later")
        JOBS[job_id] = {
            "status": "running",
            "created_at": time.time(),
            "filename": file.filename,
            "sports": sports,
            "input_rows": info["rows"],
            "links": len(info["links"]),
            "skipped": len(info["skipped"]),
            "restarts": 0,
            "error": None,
        }
        _launch(job_id)
    return {
        "job_id": job_id,
        "status": "running",
        "links": len(info["links"]),
        "skipped": len(info["skipped"]),
        # Advisory: whether MaxPreps looked reachable from this server at the last check.
        # The crawl itself is the authority -- a refused crawl fails the job within ~a minute.
        "maxpreps_access": access.cached_status().get("status"),
    }


@router.get("/{job_id}")
def csv_scrape_status(job_id: str):
    job = _require(job_id)
    progress = _progress(job_id)
    counts = None
    if job["status"] == "done":
        counts = {kind: _count_rows(os.path.join(_job_dir(job_id), name))
                  for kind, name in FILENAMES.items()}
    return {
        "job_id": job_id,
        "status": job["status"],
        "phase": progress.get("phase"),
        "filename": job.get("filename"),
        "sports": job.get("sports"),
        "input": {"rows": job.get("input_rows"), "links": job.get("links"),
                  "skipped": job.get("skipped")},
        # Live counters: coverage by status per page kind (schools / schedule / roster /
        # staff), and rows collected so far.
        "progress": progress.get("counts"),
        "counts": counts,
        "restarts": job.get("restarts", 0),
        "created_at": job.get("created_at"),
        "finished_at": job.get("finished_at"),
        "error": job.get("error"),
    }


@router.get("/{job_id}/results")
def csv_scrape_results(job_id: str, type: str = Query("teams", pattern=RESULT_TYPES),
                       offset: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=MAX_PAGE)):
    job = _require(job_id)
    path, final = _file_for(job_id, type)
    rows, total = [], 0
    with open(path, newline="", encoding="utf-8") as fh:
        for i, row in enumerate(csv.DictReader(fh)):
            total += 1
            if offset <= i < offset + limit:
                rows.append(row)
    return {"status": job["status"], "final": final, "offset": offset, "limit": limit,
            "total": total, "rows": rows}


@router.get("/{job_id}/download")
def csv_scrape_download(job_id: str, type: str = Query("teams", pattern=RESULT_TYPES)):
    job = _require(job_id)
    path, final = _file_for(job_id, type)
    name = f"{type}.csv" if final else f"{type}_partial.csv"
    return FileResponse(path, media_type="text/csv", filename=name,
                        headers={"X-Job-Status": job["status"]})


@router.post("/{job_id}/resume")
def csv_scrape_resume(job_id: str):
    """Continue a job that stopped in error (e.g. MaxPreps refused this server and access
    has since been sorted). Picks up where it stopped; nothing already fetched is redone."""
    job = _require(job_id)
    if job["status"] == "running":
        return {"job_id": job_id, "status": "running"}
    if job["status"] == "done":
        raise HTTPException(409, "job is already done")
    with _lock:
        if sum(1 for j in JOBS.values() if j["status"] == "running") >= MAX_CONCURRENT:
            raise HTTPException(429, "a CSV scrape is already running; try again later")
        job["restarts"] = 0
        job.pop("finished_at", None)
        _launch(job_id)
    return {"job_id": job_id, "status": "running"}


@router.delete("/{job_id}")
def csv_scrape_delete(job_id: str):
    with _lock:
        job = JOBS.pop(job_id, None)
        if job is not None:
            _stop_worker(job_id, _progress(job_id))
            _PROCS.pop(job_id, None)
            _save()
    shutil.rmtree(_job_dir(job_id), ignore_errors=True)
    return {"deleted": True}
