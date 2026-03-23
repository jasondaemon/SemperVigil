from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import (
    bootstrap_events_settings,
    bootstrap_schedule_settings,
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
    bootstrap_schedule_settings(conn)
    return conn


def _base_context(request: Request) -> dict[str, object]:
    conn = _get_conn()
    cfg = get_runtime_config(conn)
    publishing = cfg.get("publishing") or {}
    app_cfg = cfg.get("app") or {}
    personalization = cfg.get("personalization") or {}
    site_url = str(publishing.get("public_base_url") or "").strip()
    admin_js = BASE_DIR / "static" / "admin" / "admin.js"
    ui_build = os.environ.get("SV_UI_BUILD")
    queue_stale_minutes = int(os.environ.get("SV_QUEUE_STALE_MINUTES", "30"))
    if not ui_build and admin_js.exists():
        ts = datetime.utcfromtimestamp(admin_js.stat().st_mtime)
        ui_build = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    return {
        "request": request,
        "token_enabled": bool(os.environ.get("SV_ADMIN_TOKEN")),
        "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
        "site_url": site_url or None,
        "grafana_logs_url": str(os.environ.get("SV_GRAFANA_LOGS_URL") or "").strip() or None,
        "timezone": str(app_cfg.get("timezone") or "").strip() or None,
        "watchlist_enabled": bool(personalization.get("watchlist_enabled")),
        "watchlist_exposure_mode": personalization.get("watchlist_exposure_mode") or "private_only",
        "ui_build": ui_build,
        "queue_stale_minutes": queue_stale_minutes,
    }


def _render(template_name: str, context: dict[str, object]) -> HTMLResponse:
    request = context["request"]
    return TEMPLATES.TemplateResponse(request, template_name, context)


def ui_router(token_guard) -> APIRouter:
    router = APIRouter(dependencies=[Depends(token_guard)])

    @router.get("/", response_class=HTMLResponse)
    def dashboard(request: Request):
        conn = _get_conn()
        sources = list_sources(conn)
        jobs = list_jobs(conn, limit=10)
        enabled_count = sum(1 for item in sources if item.get("enabled"))
        return _render(
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
        return _render(
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
        return _render(
            "admin/watchlist.html",
            {
                **base,
                "nav_active": "system",
                "nav_subactive": "watchlist",
            },
        )

    @router.get("/personalization", response_class=HTMLResponse)
    def personalization(request: Request):
        return _render(
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
        return _render(
            "admin/jobs.html",
            {
                **_base_context(request),
                "jobs": items,
            },
        )

    @router.get("/system/utilities", response_class=HTMLResponse)
    def utilities(request: Request):
        return _render(
            "admin/utilities.html",
            {
                **_base_context(request),
                "nav_active": "system",
                "nav_subactive": "utilities",
            },
        )

    @router.get("/system/schedules", response_class=HTMLResponse)
    def schedules(request: Request):
        return _render(
            "admin/schedules.html",
            {
                **_base_context(request),
                "nav_active": "system",
                "nav_subactive": "schedules",
            },
        )

    @router.get("/threats", response_class=HTMLResponse)
    def threats(request: Request):
        return _render(
            "admin/threats.html",
            {
                **_base_context(request),
                "nav_active": "content",
                "nav_subactive": "threats",
            },
        )

    @router.get("/threats/{actor_key}", response_class=HTMLResponse)
    def threat_detail(request: Request, actor_key: str):
        return _render(
            "admin/threat_detail.html",
            {
                **_base_context(request),
                "nav_active": "content",
                "nav_subactive": "threats",
                "actor_key": actor_key,
            },
        )

    @router.get("/briefs", response_class=HTMLResponse)
    def briefs(request: Request):
        return _render(
            "admin/briefs.html",
            {
                **_base_context(request),
                "nav_active": "content",
                "nav_subactive": "briefs",
            },
        )

    @router.get("/briefs/{day}", response_class=HTMLResponse)
    def brief_detail(request: Request, day: str):
        return _render(
            "admin/brief_detail.html",
            {
                **_base_context(request),
                "nav_active": "content",
                "nav_subactive": "briefs",
                "brief_day": day,
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
        return _render(
            "admin/health.html",
            {
                **_base_context(request),
                "health": rows,
            },
        )

    @router.get("/debug", response_class=HTMLResponse)
    def debug(request: Request):
        return _render(
            "admin/debug.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/ai", response_class=HTMLResponse)
    def ai_config(request: Request):
        conn = _get_conn()
        prompts_all = list_prompts(conn)
        schemas_all = list_schemas(conn)
        profiles = list_profiles(conn)

        def _version_key(value: str) -> tuple[int, str]:
            text = (value or "").strip().lower()
            digits = "".join(ch for ch in text if ch.isdigit())
            return (int(digits) if digits else -1, text)

        def _group_latest(items: list[dict[str, object]], key_fn=None):
            grouped: dict[str, list[dict[str, object]]] = {}
            for item in items:
                key = key_fn(item) if key_fn else item["name"]
                grouped.setdefault(str(key), []).append(item)
            latest = []
            history_map: dict[str, list[dict[str, object]]] = {}
            for name, rows in grouped.items():
                rows_sorted = sorted(rows, key=lambda r: _version_key(str(r.get("version") or "")))
                latest_item = rows_sorted[-1]
                latest.append(latest_item)
                history = rows_sorted[:-1]
                if history:
                    history_map[name] = history
            latest_sorted = sorted(latest, key=lambda r: (str(r.get("name") or ""), _version_key(str(r.get("version") or ""))))
            return latest_sorted, history_map

        def _prompt_group_name(item: dict[str, object]) -> str:
            name = str(item.get("name") or "").strip()
            version = str(item.get("version") or "").strip()
            if version:
                name = re.sub(
                    rf"(?i)\s*[\-–—:]?\s*\(?{re.escape(version)}\)?\s*$",
                    "",
                    name,
                )
            name = re.sub(r"(?i)\s*[\-–—:]?\s*\(?v?\d+(\.\d+)*\)?\s*$", "", name).strip()
            normalized = name.replace("_", " ").strip().lower()
            normalized = re.sub(r"\s+", " ", normalized).strip()
            if "daily_brief" in (str(item.get("name") or "")).lower() or "daily brief" in normalized:
                if "cluster" in normalized:
                    return "daily brief topic clustering"
                if "summarize" in normalized or "topic summaries" in normalized:
                    return "daily brief topic summaries"
                if "nist" in normalized:
                    return "daily brief nist mapping"
                if "overall" in normalized or "synthesis" in normalized:
                    return "daily brief overall synthesis"
                return "daily brief"
            return normalized or (name or str(item.get("name") or ""))

        prompt_groups: dict[str, list[dict[str, object]]] = {}
        for item in prompts_all:
            key = _prompt_group_name(item)
            prompt_groups.setdefault(key, []).append(item)
        prompts_grouped = []
        for key, rows in prompt_groups.items():
            rows_sorted = sorted(rows, key=lambda r: _version_key(str(r.get("version") or "")))
            latest_item = rows_sorted[-1]
            history = rows_sorted[:-1]
            prompts_grouped.append(
                {
                    "key": key,
                    "latest": latest_item,
                    "history": history,
                }
            )
        prompts_grouped = sorted(
            prompts_grouped,
            key=lambda g: (str(g["latest"].get("name") or ""), _version_key(str(g["latest"].get("version") or ""))),
        )
        prompts_latest = [g["latest"] for g in prompts_grouped]
        prompts_history = {g["key"]: g["history"] for g in prompts_grouped if g["history"]}
        schemas_latest, schemas_history = _group_latest(schemas_all)
        return _render(
            "admin/ai.html",
            {
                **_base_context(request),
                "providers": list_providers(conn),
                "models": list_models(conn),
                "prompts": prompts_latest,
                "prompts_grouped": prompts_grouped,
                "prompts_all": prompts_all,
                "prompts_history": prompts_history,
                "schemas": schemas_latest,
                "schemas_all": schemas_all,
                "schemas_history": schemas_history,
                "profiles": profiles,
                "routing": list_pipeline_routing(conn),
                "stages": STAGE_NAMES,
                "stage_statuses": list_stage_statuses(conn, STAGE_NAMES),
            },
        )

    @router.get("/analytics", response_class=HTMLResponse)
    def analytics(request: Request):
        return _render(
            "admin/analytics.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/cves", response_class=HTMLResponse)
    def cves(request: Request):
        return _render(
            "admin/cves.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/cves/settings", response_class=HTMLResponse)
    def cve_settings(request: Request):
        return _render(
            "admin/cve_settings.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/cves/{cve_id}", response_class=HTMLResponse)
    def cve_detail(request: Request, cve_id: str):
        return _render(
            "admin/cve_detail.html",
            {
                **_base_context(request),
                "cve_id": cve_id,
            },
        )

    @router.get("/events", response_class=HTMLResponse)
    def events(request: Request):
        return _render(
            "admin/events.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/events/{event_id}", response_class=HTMLResponse)
    def event_detail(request: Request, event_id: str):
        return _render(
            "admin/event_detail.html",
            {
                **_base_context(request),
                "event_id": event_id,
            },
        )

    @router.get("/products", response_class=HTMLResponse)
    def products(request: Request):
        return _render(
            "admin/products.html",
            {
                **_base_context(request),
            },
        )

    @router.get("/products/{product_key}", response_class=HTMLResponse)
    def product_detail(request: Request, product_key: str):
        return _render(
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
        return _render(
            "admin/content.html",
            {
                **_base_context(request),
                "sources": sources,
            },
        )

    @router.get("/content/articles/{article_id}", response_class=HTMLResponse)
    def content_article(request: Request, article_id: int):
        return _render(
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
        return _render(
            "admin/config.html",
            {
                **_base_context(request),
                "config_json": json.dumps(cfg, indent=2, sort_keys=True),
            },
        )

    @router.get("/system/danger", response_class=HTMLResponse)
    def danger_zone(request: Request):
        return _render(
            "admin/danger.html",
            {
                **_base_context(request),
            },
        )

    return router
