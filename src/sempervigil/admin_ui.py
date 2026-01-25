from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import bootstrap_runtime_config, get_runtime_config, get_state_db_path
from .services.sources_service import list_sources
from .services.ai_service import (
    list_models,
    list_pipeline_routing,
    list_profiles,
    list_prompts,
    list_providers,
    list_schemas,
)
from .storage import (
    count_articles_since,
    get_last_source_run,
    get_source_run_streaks,
    init_db,
    list_jobs,
)
from .utils import utc_now_iso_offset
from .llm import STAGE_NAMES


BASE_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))
ADMIN_COOKIE_NAME = "sv_admin_token"


def _get_conn():
    conn = init_db(get_state_db_path())
    bootstrap_runtime_config(conn)
    return conn


def ui_router(token_guard) -> APIRouter:
    router = APIRouter(dependencies=[Depends(token_guard)])

    @router.get("/", response_class=HTMLResponse)
    def dashboard(request: Request):
        conn = _get_conn()
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
        conn = _get_conn()
        items = list_sources(conn)
        since = utc_now_iso_offset(seconds=-24 * 3600)
        for item in items:
            item["articles_24h"] = count_articles_since(conn, item["id"], since)
            last_run = get_last_source_run(conn, item["id"])
            item["accepted_last_run"] = last_run["items_accepted"] if last_run else 0
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
        conn = _get_conn()
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
        conn = _get_conn()
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

    @router.get("/ai", response_class=HTMLResponse)
    def ai_config(request: Request):
        conn = _get_conn()
        return TEMPLATES.TemplateResponse(
            "admin/ai.html",
            {
                "request": request,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
                "providers": list_providers(conn),
                "models": list_models(conn),
                "prompts": list_prompts(conn),
                "schemas": list_schemas(conn),
                "profiles": list_profiles(conn),
                "routing": list_pipeline_routing(conn),
                "stages": STAGE_NAMES,
            },
        )

    @router.get("/analytics", response_class=HTMLResponse)
    def analytics(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/analytics.html",
            {
                "request": request,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/cves", response_class=HTMLResponse)
    def cves(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/cves.html",
            {
                "request": request,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/cves/settings", response_class=HTMLResponse)
    def cve_settings(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/cve_settings.html",
            {
                "request": request,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/cves/{cve_id}", response_class=HTMLResponse)
    def cve_detail(request: Request, cve_id: str):
        return TEMPLATES.TemplateResponse(
            "admin/cve_detail.html",
            {
                "request": request,
                "cve_id": cve_id,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/content", response_class=HTMLResponse)
    def content(request: Request):
        conn = _get_conn()
        sources = list_sources(conn)
        return TEMPLATES.TemplateResponse(
            "admin/content.html",
            {
                "request": request,
                "sources": sources,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/content/articles/{article_id}", response_class=HTMLResponse)
    def content_article(request: Request, article_id: int):
        return TEMPLATES.TemplateResponse(
            "admin/content_article.html",
            {
                "request": request,
                "article_id": article_id,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
            },
        )

    @router.get("/config", response_class=HTMLResponse)
    def runtime_config(request: Request):
        conn = _get_conn()
        cfg = get_runtime_config(conn)
        return TEMPLATES.TemplateResponse(
            "admin/config.html",
            {
                "request": request,
                "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
                "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
                "config_json": json.dumps(cfg, indent=2, sort_keys=True),
            },
        )

    return router
