#!/usr/bin/env python
"""HTTP surface for the GoFan middle-school flow, mounted at ``/ms``.

Deliberately self-contained. The MaxPreps service in ``api.py`` keeps its own job
registry, filenames, concurrency cap and runtime ladder; this module keeps a parallel
set of its own and shares none of them, so a middle-school run can never consume a
MaxPreps concurrency slot, trip its timeout backstop, or appear in its job list.
``api.py`` touches this file in exactly two lines: an import and an include_router.

    POST   /ms/gofan                       multipart CSV upload -> { job_id, status }
    GET    /ms/gofan/{job_id}              poll: status, phase, progress, counts
    GET    /ms/gofan/{job_id}/results      ?type=schools|schedule  (capped preview)
    GET    /ms/gofan/{job_id}/download     ?type=schools|schedule  (full CSV)
    DELETE /ms/gofan/{job_id}

The work itself runs in ``gofan_ms_worker.py`` as a subprocess -- same pattern as
``worker.py`` -- so a long run never blocks the event loop and can be killed cleanly.
"""
import csv
import os
import shutil
import subprocess
import sys
import time
import uuid

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

from gofan_ms_worker import PROGRESS_JSON, SCHEDULE_CSV, SCHOOLS_CSV

# Own directory, swept at import. Keeping it out of api.py's ``jobs/`` means neither
# service's cleanup can delete the other's in-flight results.
MS_JOBS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ms_jobs")

FILENAMES = {"schools": SCHOOLS_CSV, "schedule": SCHEDULE_CSV}
RESULT_TYPES = f"^({'|'.join(FILENAMES)})$"

# One at a time. A run is steady outbound requests to GoFan for as long as it takes,
# and the worker already saturates its own concurrency budget, so a second parallel job
# would double our request rate against GoFan for no throughput gain.
MAX_CONCURRENT = int(os.environ.get("MS_MAX_CONCURRENT_JOBS", "1"))

# Liveness, not a runtime budget.
#
# There is deliberately NO total-runtime cap. Runtime here scales linearly with the
# uploaded row count (~106 ms/row), so any fixed ceiling silently becomes a row-count
# ceiling: the old 5400s limit killed anything past ~44k rows mid-run, and the user
# just saw a job "fail" with no reason. Instead the worker heartbeats progress.json
# after every chunk, and a job is failed only when that heartbeat stops advancing --
# which is what a hung job actually looks like, at any file size.
#
# The window must comfortably exceed the slowest single chunk. A chunk is 500 rows
# (phase 1) or 100 schools (phase 2); with GoFan's retry/backoff a pathological chunk
# is still minutes, not tens of minutes. 15 min is generous.
STALL_SECONDS = int(os.environ.get("MS_JOB_STALL_SECONDS", "900"))
# Absolute backstop for a job that heartbeats but never finishes (0 disables). Default
# 0: the stall detector is the real guard, and any finite value reintroduces the
# row-count ceiling this design exists to remove.
MAX_RUNTIME_SECONDS = int(os.environ.get("MS_JOB_MAX_RUNTIME_SECONDS", "0"))
# The uploaded file is streamed to disk in chunks -- never read whole into memory.
UPLOAD_CHUNK = 1024 * 1024
# 1 GB. The worker streams rows now, so the file size no longer drives memory; this is
# just a guard against a runaway upload filling the disk.
MAX_UPLOAD_BYTES = int(os.environ.get("MS_MAX_UPLOAD_BYTES", str(1024 * 1024 * 1024)))
# Rows returned by /results. The full schools CSV is 23 MB+ with ~134 columns; the
# UI only needs a preview, and the download endpoint serves the real thing.
PREVIEW_ROWS = 500

# job_id -> { status, started_at, error, filename, limit, proc, last_beat* }
JOBS = {}

router = APIRouter(prefix="/ms", tags=["middle-school"])

csv.field_size_limit(10**7)


def _sweep():
    """Drop leftovers from a previous process (no persistence by design)."""
    shutil.rmtree(MS_JOBS_DIR, ignore_errors=True)
    os.makedirs(MS_JOBS_DIR, exist_ok=True)


_sweep()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _job_dir(job_id):
    return os.path.join(MS_JOBS_DIR, job_id)


def _csv_path(job_id, kind):
    return os.path.join(_job_dir(job_id), FILENAMES[kind])


def _read_progress(job_id):
    import json

    path = os.path.join(_job_dir(job_id), PROGRESS_JSON)
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _stalled(job_id, job):
    """True if the worker's progress heartbeat has stopped advancing.

    Liveness is measured by the ``done`` counter in progress.json, not by elapsed time,
    so a legitimately long run (a 100k-row upload takes hours) is never mistaken for a
    hung one. Each observed advance resets the clock.
    """
    progress = _read_progress(job_id) or {}
    beat = (progress.get("phase"), progress.get("done"))
    now = time.time()
    if beat != job.get("last_beat"):
        job["last_beat"] = beat
        job["last_beat_at"] = now
        return False
    # No movement yet is normal at startup: the worker counts rows before its first
    # heartbeat, which on a very large file takes a few seconds. Fall back to
    # started_at so the window is measured from a real point in time either way.
    since = job.get("last_beat_at") or job.get("started_at", now)
    return (now - since) > STALL_SECONDS


def _refresh_status(job_id):
    """Reconcile a 'running' job with its subprocess's actual exit state."""
    job = JOBS[job_id]
    if job["status"] != "running":
        return job

    proc = job.get("proc")
    if proc is None:
        # No live handle but still 'running' -- a job whose process we lost track of.
        if _stalled(job_id, job):
            job["status"] = "error"
            job["error"] = "job stalled (no progress)"
        return job

    rc = proc.poll()
    if rc is None:
        # Still executing. Fail it only if it has genuinely stopped making progress,
        # or blown an explicitly-configured absolute ceiling (disabled by default).
        if MAX_RUNTIME_SECONDS and (
            time.time() - job.get("started_at", 0) > MAX_RUNTIME_SECONDS
        ):
            proc.terminate()
            job["status"] = "error"
            job["error"] = "job exceeded MS_JOB_MAX_RUNTIME_SECONDS"
            job["proc"] = None
        elif _stalled(job_id, job):
            proc.terminate()
            job["status"] = "error"
            job["error"] = f"job stalled (no progress for {STALL_SECONDS}s)"
            job["proc"] = None
        return job

    job["status"] = "done" if rc == 0 else "error"
    if rc != 0:
        job["error"] = f"worker exited with code {rc}"
    job["proc"] = None
    return job


def _refresh_all():
    for job_id in list(JOBS):
        _refresh_status(job_id)


def _count_rows(path):
    if not os.path.exists(path):
        return 0
    with open(path, newline="", encoding="utf-8") as fh:
        return max(0, sum(1 for _ in fh) - 1)


def _preview(path, limit=PREVIEW_ROWS):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            out.append(row)
            if len(out) >= limit:
                break
    return out


def _require(job_id):
    if job_id not in JOBS:
        raise HTTPException(404, "unknown job_id")
    return _refresh_status(job_id)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.post("/gofan")
async def start_gofan(file: UploadFile = File(...), limit: int = Form(0)):
    """Upload a middle-school CSV and start a GoFan enrichment job."""
    if not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(400, "file must be a .csv")

    _refresh_all()
    if sum(1 for j in JOBS.values() if j["status"] == "running") >= MAX_CONCURRENT:
        raise HTTPException(429, "a middle-school job is already running; try again shortly")

    job_id = uuid.uuid4().hex
    out_dir = _job_dir(job_id)
    os.makedirs(out_dir, exist_ok=True)
    input_csv = os.path.join(out_dir, "input.csv")

    # Stream to disk. The real file is ~23 MB and reading it into memory on a small
    # instance would be the difference between working and an OOM kill.
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

    # Fail fast on a CSV that can't work, rather than after a subprocess launch.
    try:
        with open(input_csv, newline="", encoding="utf-8-sig") as fh:
            header = next(csv.reader(fh), [])
    except (OSError, UnicodeDecodeError):
        shutil.rmtree(out_dir, ignore_errors=True)
        raise HTTPException(400, "could not read the uploaded file as CSV")
    if "SCH_NAME" not in header:
        shutil.rmtree(out_dir, ignore_errors=True)
        raise HTTPException(400, "CSV must have a SCH_NAME column")

    proc = subprocess.Popen(
        [sys.executable, "gofan_ms_worker.py", out_dir, input_csv, str(max(0, limit))],
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    JOBS[job_id] = {
        "status": "running",
        "started_at": time.time(),
        "error": None,
        "filename": file.filename,
        "limit": max(0, limit),
        "proc": proc,
        # Liveness bookkeeping for _stalled(); updated on every status poll.
        "last_beat": None,
        "last_beat_at": time.time(),
    }
    return {"job_id": job_id, "status": "running"}


@router.get("/gofan/{job_id}")
def gofan_status(job_id: str):
    job = _require(job_id)
    progress = _read_progress(job_id) or {}
    counts = None
    if job["status"] == "done":
        counts = {
            "schools": _count_rows(_csv_path(job_id, "schools")),
            "matched": progress.get("matched", 0),
            "events": _count_rows(_csv_path(job_id, "schedule")),
        }
    return {
        "job_id": job_id,
        "status": job["status"],
        "filename": job["filename"],
        "limit": job["limit"],
        "phase": progress.get("phase"),
        "progress": {
            "done": progress.get("done", 0),
            "total": progress.get("total", 0),
        },
        "counts": counts,
        "error": job["error"],
    }


@router.get("/gofan/{job_id}/results")
def gofan_results(job_id: str, type: str = Query("schools", pattern=RESULT_TYPES)):
    job = _require(job_id)
    if job["status"] != "done":
        raise HTTPException(409, f"job not done (status: {job['status']})")
    return _preview(_csv_path(job_id, type))


@router.get("/gofan/{job_id}/download")
def gofan_download(job_id: str, type: str = Query("schools", pattern=RESULT_TYPES)):
    job = _require(job_id)
    if job["status"] != "done":
        raise HTTPException(409, f"job not done (status: {job['status']})")
    path = _csv_path(job_id, type)
    if not os.path.exists(path):
        raise HTTPException(404, f"no {type} file for this job")
    return FileResponse(path, media_type="text/csv", filename=FILENAMES[type])


@router.delete("/gofan/{job_id}")
def gofan_delete(job_id: str):
    job = JOBS.pop(job_id, None)
    if job and job.get("proc") and job["proc"].poll() is None:
        job["proc"].terminate()
    shutil.rmtree(_job_dir(job_id), ignore_errors=True)
    return {"deleted": True}
