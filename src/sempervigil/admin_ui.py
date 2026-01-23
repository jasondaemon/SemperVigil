from __future__ import annotations

import os
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import ConfigError, load_config
from .services.sources_service import list_sources
from .storage import get_source_run_streaks, init_db, list_jobs


BASE_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))
ADMIN_COOKIE_NAME = "sv_admin_token"


def ui_router(token_guard) -> APIRouter:
    router = APIRouter(dependencies=[Depends(token_guard)])

    @router.get("/", response_class=HTMLResponse)
    def dashboard(request: Request):
        config = load_config(None)
        conn = init_db(config.paths.state_db)
        sources = list_sources(conn)
        jobs = list_jobs(conn, limit=10)
        enabled_count = sum(1 for item in sources if item.get("enabled"))
        return TEMPLATES.TemplateResponse(
            "admin/dashboard.html",
            {
                "request": request,
                "sources_count": len(sources),
                "enabled_count": enabled_count,
                "jobs": jobs,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/sources", response_class=HTMLResponse)
    def sources(request: Request):
        config = load_config(None)
        conn = init_db(config.paths.state_db)
        items = list_sources(conn)
        return TEMPLATES.TemplateResponse(
            "admin/sources.html",
            {
                "request": request,
                "sources": items,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/jobs", response_class=HTMLResponse)
    def jobs(request: Request):
        config = load_config(None)
        conn = init_db(config.paths.state_db)
        items = list_jobs(conn, limit=50)
        return TEMPLATES.TemplateResponse(
            "admin/jobs.html",
            {
                "request": request,
                "jobs": items,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/health", response_class=HTMLResponse)
    def health(request: Request):
        config = load_config(None)
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
        return TEMPLATES.TemplateResponse(
            "admin/health.html",
            {
                "request": request,
                "health": rows,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    return router
