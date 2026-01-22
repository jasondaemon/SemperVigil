from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import ConfigError, load_config
from .storage import enqueue_job, init_db, list_jobs
from .utils import log_event

app = FastAPI(title="SemperVigil Admin API")


class JobRequest(BaseModel):
    job_type: str
    source_id: str | None = None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
