from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import (
    bootstrap_events_settings,
    bootstrap_runtime_config,
    get_runtime_config,
)
from .services.sources_service import list_sources
from .services.ai_service import (
    list_models,
    list_pipeline_routing,
    list_profiles,
    list_prompts,
    list_providers,
    list_schemas,
    list_stage_statuses,
)
from .storage import (
    count_articles_since,
    count_articles_total,
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
    conn = init_db()
    bootstrap_runtime_config(conn)
    bootstrap_events_settings(conn)
    return conn


def _base_context(request: Request) -> dict[str, object]:
    conn = _get_conn()
    cfg = get_runtime_config(conn)
    publishing = cfg.get("publishing") or {}
    app_cfg = cfg.get("app") or {}
    personalization = cfg.get("personalization") or {}
    site_url = str(publishing.get("public_base_url") or "").strip()
    return {
        "request": request,
        "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
        "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
        "site_url": site_url or None,
        "timezone": str(app_cfg.get("timezone") or "").strip() or None,
        "watchlist_enabled": bool(personalization.get("watchlist_enabled")),
        "watchlist_exposure_mode": personalization.get("watchlist_exposure_mode") or "private_only",
    }


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
                **_base_context(request),
                "sources_count": len(sources),
                "enabled_count": enabled_count,
                "jobs": jobs,
            },
        )

    @router.get("/sources", response_class=HTMLResponse)
    def sources(request: Request):
        conn = _get_conn()
        items = list_sources(conn)
        since = utc_now_iso_offset(seconds=-24 * 3600)
        for item in items:
            item["articles_24h"] = count_articles_since(conn, item["id"], since)
            item["total_articles"] = count_articles_total(conn, item["id"])
        return TEMPLATES.TemplateResponse(
            "admin/sources.html",
            {
                **_base_context(request),
                "sources": items,
            },
        )

    @router.get("/watchlist", response_class=HTMLResponse)
    def watchlist(request: Request):
        base = _base_context(request)
        if not base.get("watchlist_enabled"):
            raise HTTPException(status_code=404, detail="watchlist_disabled")
        return TEMPLATES.TemplateResponse(
            "admin/watchlist.html",
            {
                **base,
                "nav_active": "system",
                "nav_subactive": "watchlist",
            },
        )

    @router.get("/personalization", response_class=HTMLResponse)
    def personalization(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/personalization.html",
            {
                **_base_context(request),
                "nav_active": "system",
                "nav_subactive": "personalization",
            },
        )

    @router.get("/jobs", response_class=HTMLResponse)
    def jobs(request: Request):
        conn = _get_conn()
        items = list_jobs(conn, limit=50)
        return TEMPLATES.TemplateResponse(
            "admin/jobs.html",
            {
                **_base_context(request),
                "jobs": items,
            },
        )

    @router.get("/logs", response_class=HTMLResponse)
    def logs(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/logs.html",
            {
                **_base_context(request),
                "nav_active": "system",
                "nav_subactive": "logs",
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
                **_base_context(request),
                "health": rows,
            },
        )

    @router.get("/debug", response_class=HTMLResponse)
    def debug(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/debug.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/ai", response_class=HTMLResponse)
    def ai_config(request: Request):
        conn = _get_conn()
        return TEMPLATES.TemplateResponse(
            "admin/ai.html",
            {
                **_base_context(request),
                "providers": list_providers(conn),
                "models": list_models(conn),
                "prompts": list_prompts(conn),
                "schemas": list_schemas(conn),
                "profiles": list_profiles(conn),
                "routing": list_pipeline_routing(conn),
                "stages": STAGE_NAMES,
                "stage_statuses": list_stage_statuses(conn, STAGE_NAMES),
            },
        )

    @router.get("/analytics", response_class=HTMLResponse)
    def analytics(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/analytics.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/cves", response_class=HTMLResponse)
    def cves(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/cves.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/cves/settings", response_class=HTMLResponse)
    def cve_settings(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/cve_settings.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/cves/{cve_id}", response_class=HTMLResponse)
    def cve_detail(request: Request, cve_id: str):
        return TEMPLATES.TemplateResponse(
            "admin/cve_detail.html",
            {
                **_base_context(request),
                "cve_id": cve_id,
            },
        )

    @router.get("/events", response_class=HTMLResponse)
    def events(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/events.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/events/{event_id}", response_class=HTMLResponse)
    def event_detail(request: Request, event_id: str):
        return TEMPLATES.TemplateResponse(
            "admin/event_detail.html",
            {
                **_base_context(request),
                "event_id": event_id,
            },
        )

    @router.get("/products", response_class=HTMLResponse)
    def products(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/products.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/products/{product_key}", response_class=HTMLResponse)
    def product_detail(request: Request, product_key: str):
        return TEMPLATES.TemplateResponse(
            "admin/product_detail.html",
            {
                **_base_context(request),
                "product_key": product_key,
            },
        )

    @router.get("/content", response_class=HTMLResponse)
    def content(request: Request):
        conn = _get_conn()
        sources = list_sources(conn)
        return TEMPLATES.TemplateResponse(
            "admin/content.html",
            {
                **_base_context(request),
                "sources": sources,
            },
        )

    @router.get("/content/articles/{article_id}", response_class=HTMLResponse)
    def content_article(request: Request, article_id: int):
        return TEMPLATES.TemplateResponse(
            "admin/content_article.html",
            {
                **_base_context(request),
                "article_id": article_id,
            },
        )

    @router.get("/config", response_class=HTMLResponse)
    def runtime_config(request: Request):
        conn = _get_conn()
        cfg = get_runtime_config(conn)
        return TEMPLATES.TemplateResponse(
            "admin/config.html",
            {
                **_base_context(request),
                "config_json": json.dumps(cfg, indent=2, sort_keys=True),
            },
        )

    @router.get("/system/danger", response_class=HTMLResponse)
    def danger_zone(request: Request):
        return TEMPLATES.TemplateResponse(
            "admin/danger.html",
            {
                **_base_context(request),
            },
        )

    return router
