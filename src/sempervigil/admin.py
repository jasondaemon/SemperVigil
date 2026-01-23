from __future__ import annotations

import logging
import os

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime, timezone
from pydantic import BaseModel

from .config import ConfigError, load_config
from .admin_ui import TEMPLATES, ui_router
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import enqueue_job, get_source_run_streaks, init_db, list_jobs
from .ingest import process_source
from .services.sources_service import (
    create_source,
    delete_source,
    get_source,
    list_sources,
    record_test_result,
    update_source,
)
from .utils import log_event

app = FastAPI(title="SemperVigil Admin API")

ADMIN_COOKIE_NAME = "sv_admin_token"

app.mount(
    "/ui/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="ui-static",
)


@app.get("/ui")
def ui_redirect():
    return RedirectResponse("/ui/", status_code=307)


def _require_admin_token(request: Request) -> None:
    token = os.environ.get("SV_ADMIN_TOKEN")
    if not token:
        return
    if not _is_authorized(request, token):
        raise HTTPException(status_code=401, detail="unauthorized")


def _is_authorized(request: Request, token: str) -> bool:
    header = request.headers.get("X-Admin-Token")
    if header and header == token:
        return True
    cookie = request.cookies.get(ADMIN_COOKIE_NAME)
    return cookie == token


@app.middleware("http")
async def _admin_token_middleware(request: Request, call_next):
    if request.url.path.startswith("/ui"):
        if request.url.path.startswith("/ui/login"):
            return await call_next(request)
        if request.url.path.startswith("/ui/static"):
            return await call_next(request)
        token = os.environ.get("SV_ADMIN_TOKEN")
        if token and not _is_authorized(request, token):
            return RedirectResponse("/ui/login", status_code=303)
    return await call_next(request)


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


@app.get("/ui/login")
def ui_login(request: Request):
    token_enabled = bool(os.environ.get("SV_ADMIN_TOKEN"))
    return TEMPLATES.TemplateResponse(
        "admin/login.html",
        {"request": request, "token_enabled": token_enabled},
    )


@app.post("/ui/login")
async def ui_login_post(request: Request):
    token = os.environ.get("SV_ADMIN_TOKEN")
    if not token:
        response = RedirectResponse("/ui", status_code=303)
        return response
    payload = await request.json()
    candidate = str(payload.get("token") or "")
    if candidate != token:
        return JSONResponse({"ok": False, "error": "invalid_token"}, status_code=401)
    response = JSONResponse({"ok": True})
    response.set_cookie(
        ADMIN_COOKIE_NAME,
        token,
        httponly=True,
        samesite="lax",
        max_age=86400,
    )
    return response


@app.post("/ui/logout")
def ui_logout():
    response = RedirectResponse("/ui/login", status_code=303)
    response.delete_cookie(ADMIN_COOKIE_NAME)
    return response


@app.on_event("startup")
def _startup() -> None:
    try:
        config = load_config(None)
    except ConfigError:
        return
    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))


@app.post("/jobs/enqueue")
def enqueue(job: JobRequest, _: None = Depends(_require_admin_token)) -> dict[str, str]:
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


class SourceRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    kind: str | None = None
    url: str | None = None
    enabled: bool | None = None
    interval_minutes: int | None = None
    tags: list[str] | str | None = None


@app.get("/sources")
def sources_list() -> list[dict[str, object]]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    conn = init_db(config.paths.state_db)
    return list_sources(conn)


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


@app.post("/sources")
def sources_create(
    payload: SourceRequest, _: None = Depends(_require_admin_token)
) -> dict[str, object]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    conn = init_db(config.paths.state_db)
    try:
        return create_source(conn, payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/sources/{source_id}")
def sources_read(source_id: str) -> dict[str, object]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    conn = init_db(config.paths.state_db)
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    return source


@app.put("/sources/{source_id}")
@app.patch("/sources/{source_id}")
def sources_update(
    source_id: str,
    payload: SourceRequest,
    _: None = Depends(_require_admin_token),
) -> dict[str, object]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    conn = init_db(config.paths.state_db)
    try:
        return update_source(conn, source_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/sources/{source_id}")
def sources_delete(source_id: str, _: None = Depends(_require_admin_token)) -> dict[str, str]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    conn = init_db(config.paths.state_db)
    delete_source(conn, source_id)
    return {"status": "deleted"}


@app.post("/sources/{source_id}/test")
def sources_test(
    source_id: str, _: None = Depends(_require_admin_token)
) -> dict[str, object]:
    try:
        config = load_config(None)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    conn = init_db(config.paths.state_db)
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    result = process_source(
        source=source_to_model(source),
        config=config,
        logger=logging.getLogger("sempervigil.admin"),
        conn=conn,
        test_mode=True,
    )
    ok = result.status == "ok"
    record_test_result(conn, source_id, ok=ok, error=result.error)
    preview = []
    for decision in result.decisions[:5]:
        preview.append(
            {
                "title": decision.title,
                "url": decision.normalized_url,
                "published_at": decision.published_at,
                "decision": decision.decision,
                "reasons": decision.reasons,
            }
        )
    return {
        "status": result.status,
        "http_status": result.http_status,
        "error": result.error,
        "found_count": result.found_count,
        "accepted_count": result.accepted_count,
        "items": preview,
    }




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


def source_to_model(source: dict[str, object]):
    from .models import Source

    return Source(
        id=str(source.get("id")),
        name=str(source.get("name")),
        enabled=bool(source.get("enabled", True)),
        base_url=source.get("url") or source.get("base_url"),
        topic_key=None,
        default_frequency_minutes=int(source.get("interval_minutes", 60)),
        pause_until=source.get("pause_until"),
        paused_reason=source.get("paused_reason"),
        robots_notes=None,
    )


app.include_router(ui_router(_require_admin_token), prefix="/ui")
