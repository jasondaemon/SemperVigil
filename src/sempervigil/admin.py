from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException
from datetime import datetime, timezone
from pydantic import BaseModel

from .config import ConfigError, load_config
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import enqueue_job, get_source_run_streaks, init_db, list_jobs
from .utils import log_event

app = FastAPI(title="SemperVigil Admin API")


class JobRequest(BaseModel):
    job_type: str
    source_id: str | None = None


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "SemperVigil Admin API"}


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "ok": True,
        "version": _get_version(),
        "time": datetime.now(tz=timezone.utc).isoformat(),
    }


@app.on_event("startup")
def _startup() -> None:
    try:
        config = load_config(None)
    except ConfigError:
        return
    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))


@app.post("/jobs/enqueue")
def enqueue(job: JobRequest) -> dict[str, str]:
    logger = logging.getLogger("sempervigil.admin")
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    conn = init_db(config.paths.state_db)
    payload = {"source_id": job.source_id} if job.source_id else None
    job_id = enqueue_job(conn, job.job_type, payload, debounce=True)
    log_event(
        logger,
        logging.INFO,
        "job_enqueued",
        job_id=job_id,
        job_type=job.job_type,
    )
    return {"job_id": job_id}


@app.get("/jobs")
def jobs(limit: int = 20) -> list[dict[str, str]]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    conn = init_db(config.paths.state_db)
    rows = []
    for job in list_jobs(conn, limit=limit):
        rows.append(
            {
                "id": job.id,
                "job_type": job.job_type,
                "status": job.status,
                "requested_at": job.requested_at,
                "started_at": job.started_at or "",
                "finished_at": job.finished_at or "",
                "error": job.error or "",
                "result": job.result or {},
            }
        )
    return rows


@app.get("/sources/health")
def sources_health() -> list[dict[str, object]]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    conn = init_db(config.paths.state_db)
    cursor = conn.execute(
        """
        SELECT s.id, s.name, s.enabled, s.pause_until, s.paused_reason,
               r.status, r.started_at, r.items_accepted, r.error
        FROM sources s
        LEFT JOIN (
            SELECT source_id, status, started_at, items_accepted, error
            FROM source_runs
            WHERE (source_id, started_at) IN (
                SELECT source_id, MAX(started_at) FROM source_runs GROUP BY source_id
            )
        ) r ON r.source_id = s.id
        ORDER BY s.id
        """
    )
    rows = []
    for (
        source_id,
        name,
        enabled,
        pause_until,
        paused_reason,
        status,
        started_at,
        items_accepted,
        error,
    ) in cursor.fetchall():
        streaks = get_source_run_streaks(conn, source_id)
        rows.append(
            {
                "id": source_id,
                "name": name,
                "enabled": bool(enabled),
                "pause_until": pause_until,
                "paused_reason": paused_reason,
                "last_status": status,
                "last_run_at": started_at,
                "last_items_accepted": items_accepted,
                "last_error": error,
                "consecutive_errors": streaks["consecutive_errors"],
                "consecutive_zero": streaks["consecutive_zero"],
            }
        )
    return rows


def _setup_logging() -> None:
    level_name = os.environ.get("SV_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


_setup_logging()


def _get_version() -> str:
    try:
        from importlib.metadata import version

        return version("sempervigil")
    except Exception:  # noqa: BLE001
        return "unknown"
