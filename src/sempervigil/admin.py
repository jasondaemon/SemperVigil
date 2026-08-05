from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
import hashlib
from typing import Any
from pathlib import Path

from fastapi import Body, APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
try:
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
except Exception:  # noqa: BLE001
    ProxyHeadersMiddleware = None
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pydantic import BaseModel

from .config import (
    ConfigError,
    bootstrap_cve_settings,
    bootstrap_events_settings,
    bootstrap_schedule_settings,
    bootstrap_runtime_config,
    apply_runtime_config_patch,
    get_cve_settings,
    get_events_settings,
    get_schedule_settings,
    get_runtime_config,
    is_article_markdown_enabled,
    load_runtime_config,
    set_cve_settings,
    set_schedule_settings,
    set_runtime_config,
)
from .admin_ui import TEMPLATES, ui_router
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .utils import parse_log_line, utc_now_iso
from .worker import (
    WORKER_JOB_TYPES,
    _refresh_feed_data_files,
)
from .http_fetch import fetch_prefix
from .source_overrides import (
    get_http_fetch_compressed,
    get_http_fetch_range_chunks,
    get_http_fetch_settings,
    get_http_fetch_version,
    normalize_source_overrides,
)
from .pipelines.content_fetch import fetch_article_content
from bs4 import BeautifulSoup
from .storage import (
    enqueue_job,
    get_source_run_streaks,
    init_db,
    list_jobs,
    list_jobs_filtered,
    list_queued_job_stats,
    cancel_job,
    cancel_all_jobs,
    cancel_jobs_by_type,
    count_articles_total,
    get_schema_version,
    count_table,
    get_last_job_by_type,
    get_job,
    has_pending_job,
    get_build_state,
    get_build_status,
    get_job_metrics,
    get_queue_stats,
    get_queue_worker_health,
    get_runner_stats,
    get_runner_health_stats,
    get_source_ingest_state_counts,
    get_stale_job_stats,
    get_public_metrics_daily_counts,
    mark_build_dirty,
    enqueue_build_site_if_needed,
)
from .cve_filters import CveSignals, matches_filters
from .cve_sync import CveSyncConfig, isoformat_utc, preview_cves
from .storage import (
    count_articles_since,
    delete_all_articles,
    delete_all_content,
    delete_all_cves,
    delete_all_events,
    purge_weak_events,
    get_dashboard_metrics,
    get_event,
    get_article_by_id,
    update_article_content,
    get_article_tags,
    get_cve,
    get_cve_last_seen,
    get_product,
    get_product_cves,
    get_product_facets,
    count_articles_for_product,
    list_articles_for_product,
    get_setting,
    set_setting,
    list_settings_with_prefix,
    get_source_stats,
    get_pending_article_job_id,
    get_pending_cve_job_id,
    get_pending_job_id_for_cve,
    list_tactics,
    list_article_tags,
    list_article_ids_missing_summary,
    list_article_ids_missing_content,
    list_article_ids_missing_content_all,
    list_article_ids_missing_products,
    list_article_ids_ready_for_summary,
    list_article_ids_ready_for_summary_all,
    list_article_ids_ready_for_context_all,
    list_article_ids_missing_context_pack,
    list_article_ids_with_content_error_all,
    list_articles_per_day,
    list_events,
    list_events_with_counts,
    list_events_for_product,
    list_source_health_events,
    list_event_articles,
    list_event_web_sources,
    list_llm_runs,
    insert_llm_run,
    list_products_for_article,
    get_article_threat_actors,
    list_article_ids_missing_threat_actors,
    list_article_ids_without_event,
    list_threat_actors,
    list_cve_ids_needing_kev_check,
    get_threat_actor_detail,
    list_jobs_by_types_since,
    query_products,
    backfill_products_from_cves,
    cve_data_completeness,
    list_watchlist_vendors,
    list_watchlist_products,
    add_watchlist_vendor,
    add_watchlist_product,
    update_watchlist_vendor,
    update_watchlist_product,
    delete_watchlist_vendor,
    delete_watchlist_product,
    list_watchlist_suggestions,
    compute_scope_for_cves,
    list_cve_ids,
    list_cve_ids_missing_description,
    list_cve_ids_missing_products,
    list_cve_ids_missing_threat_actors,
    list_cve_vendor_products,
    list_daily_briefs,
    get_daily_brief,
    search_articles,
    search_cves,
    create_event,
    update_event,
    delete_event,
    update_article_suppressed,
    upsert_event_by_key,
    link_event_article,
    update_event_summary_from_articles,
    normalize_cve_cluster_event_keys,
    mark_event_web_source_status,
    upsert_event_web_source,
    promote_event_web_source_to_article,
    rebuild_event_timeline_from_articles,
    event_publish_readiness,
    set_event_publish_state,
    has_pending_article_job,
)
from .ingest import process_source
from .pipelines.content_fetch import fetch_article_content
from .services.sources_service import (
    create_source,
    delete_source,
    get_source,
    list_sources,
    record_test_result,
    update_source,
)
from .services.ai_service import get_active_profile_for_stage, list_stage_statuses
from .normalize import normalize_name
from .services.ai_service import (
    clear_provider_secret,
    create_model,
    create_profile,
    create_prompt,
    create_provider,
    create_schema,
    delete_model,
    delete_profile,
    delete_prompt,
    delete_provider,
    delete_schema,
    get_model,
    get_profile,
    get_prompt,
    get_provider,
    get_schema,
    list_models,
    list_pipeline_routing,
    list_profiles,
    list_prompts,
    list_providers,
    list_schemas,
    set_pipeline_routing,
    set_provider_secret,
    update_model,
    update_profile,
    update_prompt,
    update_provider,
    update_provider_test_status,
    update_schema,
)
from .llm import STAGE_NAMES, test_model, test_profile, test_provider
from .utils import configure_logging, log_event, utc_now_iso, utc_now_iso_offset

_LOG_SERVICES = {
    "admin": "admin.log",
    "orchestrator": "orchestrator.log",
    "worker": "worker_fetch.log",
    "worker_llm": "worker_llm.log",
    "worker_fetch": "worker_fetch.log",
    "worker_openai": "worker_openai.log",
    "openai_prompts": "openai_http.log",
    "vpn_watchdog": "vpn-403-watchdog.log",
    "builder": "builder.log",
    "build_hugo": "",
}

_DASHBOARD_LLM_JOB_TYPES = [
    "summarize_article_llm",
    "summarize_article_context_llm",
    "derive_events_from_articles",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "cve_enrich_llm",
    "cve_enrich_threat_actors",
    "event_report_llm",
]
_DASHBOARD_FETCH_JOB_TYPES = [
    "fetch_article_content",
    "cve_sync",
    "cve_enrich_kev",
    "events_rebuild",
    "enrich_event_from_web",
    "validate_event_web_source",
    "promote_event_web_source_to_article",
    "source_acquire",
    "ingest_due_sources",
    "ingest_source",
    "rebuild_vendor_products",
]
_DASHBOARD_BUILD_JOB_TYPES = [
    "build_site",
]

_LOG_STREAM_SERVICES = {
    "all",
    "admin",
    "orchestrator",
    "worker",
    "worker_fetch",
    "worker_llm",
    "worker_openai",
    "openai_prompts",
    "vpn_watchdog",
    "builder",
    "build_hugo",
}

_DASHBOARD_STATUS_COLUMNS = {
    "need": None,
    "queued": "queued",
    "running": "running",
    "failed": "failed",
    "complete": "succeeded",
}


def _dashboard_visible_job_types() -> list[str]:
    return _DASHBOARD_FETCH_JOB_TYPES + _DASHBOARD_LLM_JOB_TYPES + _DASHBOARD_BUILD_JOB_TYPES


def _dashboard_job_group_id(job_type: str) -> str:
    if job_type in _DASHBOARD_LLM_JOB_TYPES:
        return "llm"
    if job_type in _DASHBOARD_FETCH_JOB_TYPES:
        return "fetch"
    if job_type in _DASHBOARD_BUILD_JOB_TYPES:
        return "build"
    return "all"


def _dashboard_display_rows(payload: dict[str, object]) -> list[dict[str, object]]:
    counts = payload.get("job_counts_by_type_status") or {}
    queueable = payload.get("queueable_by_job_type") or {}
    rows: list[dict[str, object]] = []
    for index, job_type in enumerate(_dashboard_visible_job_types()):
        status_map = counts.get(job_type) if isinstance(counts, dict) else {}
        if not isinstance(status_map, dict):
            status_map = {}
        row = {
            "worker_group": _dashboard_job_group_id(job_type),
            "job_type": job_type,
            "display_order": index,
            "need": int(queueable.get(job_type) or 0) if isinstance(queueable, dict) else 0,
            "queued": int(status_map.get("queued") or 0),
            "running": int(status_map.get("running") or 0),
            "failed": int(status_map.get("failed") or 0),
            "complete": int(status_map.get("succeeded") or 0),
        }
        rows.append(row)
    return rows


def _read_log_tail(path: str, max_lines: int, max_bytes: int) -> str:
    if max_lines <= 0:
        return ""
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            read_size = min(size, max_bytes)
            handle.seek(-read_size, os.SEEK_END)
            data = handle.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""
    lines = data.splitlines()
    return "\n".join(lines[-max_lines:])


def _latest_hugo_build_log_path() -> str | None:
    conn = _get_conn()
    config = load_runtime_config(conn)
    log_path = Path(config.paths.logs_dir) / "hugo-build.log"
    if not log_path.exists():
        return None
    return str(log_path)


def _log_paths_for_service(service_key: str, config) -> list[str]:
    service = str(service_key or "").strip().lower()
    paths: list[str] = []
    if service == "all":
        deduped: list[str] = []
        seen: set[str] = set()
        for child in _LOG_STREAM_SERVICES:
            if child == "all":
                continue
            for path in _log_paths_for_service(child, config):
                if not path or path in seen:
                    continue
                seen.add(path)
                deduped.append(path)
        return deduped
    if service == "build_hugo":
        latest = _latest_hugo_build_log_path()
        return [latest] if latest else []
    path = _LOG_SERVICES.get(service)
    if not path:
        return []
    if not os.path.isabs(path):
        path = str(Path(config.paths.logs_dir) / path)
    return [path]


def _read_log_lines(path: str, max_lines: int, max_bytes: int) -> list[str]:
    text = _read_log_tail(path, max_lines=max_lines, max_bytes=max_bytes)
    return [line for line in text.splitlines() if line.strip()]


def _normalize_log_service(path: str) -> str:
    name = Path(path).name
    mapping = {
        "admin.log": "admin",
        "orchestrator.log": "orchestrator",
        "worker_fetch.log": "worker_fetch",
        "worker_llm.log": "worker_llm",
        "worker_openai.log": "worker_openai",
        "builder.log": "builder",
        "hugo-build.log": "build_hugo",
    }
    if name in mapping:
        return mapping[name]
    if name.endswith(".stdout.log") or name.endswith(".stderr.log"):
        return "builder"
    return name


def _build_log_entry(path: str, raw_line: str) -> dict[str, object]:
    entry = parse_log_line(raw_line)
    if not isinstance(entry, dict):
        entry = {"message": raw_line, "raw": raw_line}
    entry.setdefault("service", _normalize_log_service(path))
    entry.setdefault("log_path", path)
    entry.setdefault("raw", raw_line.rstrip("\n"))
    digest = hashlib.sha1(f"{path}|{entry.get('raw','')}".encode("utf-8")).hexdigest()
    entry.setdefault("id", digest)
    return entry


def _matches_log_filters(
    entry: dict[str, object],
    *,
    runner: str | None = None,
    job_type: str | None = None,
    event: str | None = None,
    level: str | None = None,
) -> bool:
    if runner:
        runner_value = str(entry.get("runner_type") or entry.get("runner_id") or "").strip()
        if runner_value != runner:
            return False
    if job_type and str(entry.get("job_type") or "").strip() != job_type:
        return False
    if event and str(entry.get("event") or "").strip() != event:
        return False
    if level and str(entry.get("level") or "").strip().lower() != level.lower():
        return False
    return True


def _query_logs(
    config,
    *,
    service: str,
    limit: int,
    runner: str | None = None,
    job_type: str | None = None,
    event: str | None = None,
    level: str | None = None,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for path in _log_paths_for_service(service, config):
        max_bytes = 600_000 if service in {"all", "build_hugo"} else 250_000
        for raw_line in _read_log_lines(path, max_lines=max(limit * 4, 200), max_bytes=max_bytes):
            entry = _build_log_entry(path, raw_line)
            if _matches_log_filters(entry, runner=runner, job_type=job_type, event=event, level=level):
                entries.append(entry)
    entries.sort(key=lambda item: (str(item.get("ts") or ""), str(item.get("id") or "")))
    return entries[-limit:] if limit > 0 else entries


def _format_log_header(path: str | None) -> str:
    if not path:
        return ""
    try:
        p = Path(path)
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).isoformat()
    except Exception:
        mtime = "unknown"
    return f"# {path} (mtime={mtime})\n"


def _parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _wait_for_job(conn: Any, job_id: str, timeout_seconds: int) -> Any:
    start = time.monotonic()
    last_job = None
    while time.monotonic() - start < timeout_seconds:
        job = get_job(conn, job_id)
        if not job:
            return None
        last_job = job
        if job.status in {"running", "succeeded", "failed", "canceled"}:
            return job
        time.sleep(1)
    return last_job

app = FastAPI(title="SemperVigil Admin API")

ADMIN_COOKIE_NAME = "sv_admin_token"

if ProxyHeadersMiddleware:
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

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


def _is_secure_request(request: Request) -> bool:
    forwarded_proto = request.headers.get("x-forwarded-proto", "").lower()
    if forwarded_proto:
        return forwarded_proto == "https"
    return request.url.scheme == "https"


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


class RuntimeConfigRequest(BaseModel):
    config: dict


class CveSettingsRequest(BaseModel):
    settings: dict


class CveTestRequest(BaseModel):
    hours: int = 24
    limit: int = 5


class ArticleContentUpdate(BaseModel):
    content_text: str


class ClearRequest(BaseModel):
    confirm: str
    delete_files: bool = False


class SmokeRequest(BaseModel):
    sources_limit: int = 2
    per_source_limit: int = 10


class ProductsSmokeRequest(BaseModel):
    limit: int = 5
    timeout_seconds: int = 120


class SourceAcquireRequest(BaseModel):
    limit: int | None = None


class WatchVendorRequest(BaseModel):
    display_name: str
    enabled: bool = True


class WatchProductRequest(BaseModel):
    display_name: str
    vendor_norm: str | None = None
    match_mode: str = "exact"
    enabled: bool = True


class WatchToggleRequest(BaseModel):
    enabled: bool
    match_mode: str | None = None


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


def _prometheus_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _prometheus_label_block(labels: dict[str, object]) -> str:
    parts = []
    for key, value in sorted(labels.items()):
        parts.append(f'{key}="{_prometheus_escape(str(value))}"')
    return "{" + ",".join(parts) + "}" if parts else ""


def _prometheus_timestamp(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return _parse_iso(value).timestamp()
    except Exception:  # noqa: BLE001
        return None


def _dashboard_job_groups() -> list[dict[str, object]]:
    return [
        {"id": "fetch", "title": "Fetch Worker", "job_types": _DASHBOARD_FETCH_JOB_TYPES},
        {"id": "llm", "title": "LLM Worker", "job_types": _DASHBOARD_LLM_JOB_TYPES},
        {"id": "build", "title": "Build / Publish", "job_types": _DASHBOARD_BUILD_JOB_TYPES},
    ]


def _job_group_id_for_job_type(job_type: str) -> str:
    for group in _dashboard_job_groups():
        if job_type in set(group.get("job_types") or []):
            return str(group.get("id") or "other")
    return "other"


def _build_dashboard_metrics_payload(conn: Any) -> dict[str, object]:
    metrics = get_dashboard_metrics(conn)
    visible_job_types = _dashboard_visible_job_types()
    metrics["job_types"] = visible_job_types
    metrics["job_groups"] = _dashboard_job_groups()
    metrics["build_state"] = get_build_state(conn)
    metrics["build_status"] = get_build_status(conn)
    metrics["queue_stats"] = get_queue_stats(conn)
    metrics["runner_health"] = get_runner_health_stats(conn)
    metrics["queue_worker_health"] = get_queue_worker_health(conn)
    stage_statuses = list_stage_statuses(conn, STAGE_NAMES)
    metrics["llm_stage_active"] = sum(1 for item in stage_statuses if item["status"] == "active")
    metrics["llm_stage_total"] = len(stage_statuses)
    metrics["llm_configured"] = metrics["llm_stage_active"] > 0

    def _pending_article_ids(job_type: str) -> set[int]:
        rows = conn.execute(
            """
            SELECT payload_json
            FROM jobs
            WHERE job_type = %s
              AND status IN ('queued', 'running')
              AND payload_json IS NOT NULL
            """,
            (job_type,),
        ).fetchall()
        ids: set[int] = set()
        for row in rows:
            raw = row[0] if row else None
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            value = payload.get("article_id") if isinstance(payload, dict) else None
            if value is None:
                continue
            try:
                ids.add(int(value))
            except (TypeError, ValueError):
                continue
        return ids

    def _pending_cve_ids(job_type: str) -> set[str]:
        rows = conn.execute(
            """
            SELECT payload_json
            FROM jobs
            WHERE job_type = %s
              AND status IN ('queued', 'running')
              AND payload_json IS NOT NULL
            """,
            (job_type,),
        ).fetchall()
        ids: set[str] = set()
        for row in rows:
            raw = row[0] if row else None
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            value = payload.get("cve_id") if isinstance(payload, dict) else None
            if value is None:
                continue
            text = str(value).strip()
            if text:
                ids.add(text)
        return ids

    queueable: dict[str, int] = {}
    missing_content_ids = list_article_ids_missing_content_all(conn, limit=None)
    pending_fetch = _pending_article_ids("fetch_article_content")
    queueable["fetch_article_content"] = sum(1 for article_id in missing_content_ids if int(article_id) not in pending_fetch)
    profile, _reason = get_active_profile_for_stage(conn, "summarize_article")
    if profile:
        summary_ids = list_article_ids_ready_for_summary_all(conn)
        pending_summary = _pending_article_ids("summarize_article_llm")
        queueable["summarize_article_llm"] = sum(1 for article_id in summary_ids if int(article_id) not in pending_summary)
    else:
        queueable["summarize_article_llm"] = 0
    profile, _reason = get_active_profile_for_stage(conn, "article_context_pack")
    if profile:
        context_ids = list_article_ids_missing_context_pack(conn, limit=None)
        pending_context = _pending_article_ids("summarize_article_context_llm")
        queueable["summarize_article_context_llm"] = sum(1 for article_id in context_ids if int(article_id) not in pending_context)
    else:
        queueable["summarize_article_context_llm"] = 0
    product_ids = list_article_ids_missing_products(conn, limit=None)
    pending_products = _pending_article_ids("article_enrich_products")
    queueable["article_enrich_products"] = sum(1 for article_id in product_ids if int(article_id) not in pending_products)
    threat_article_ids = list_article_ids_missing_threat_actors(conn, limit=None)
    pending_article_threats = _pending_article_ids("article_enrich_threat_actors")
    queueable["article_enrich_threat_actors"] = sum(1 for article_id in threat_article_ids if int(article_id) not in pending_article_threats)
    event_candidate_ids = list_article_ids_without_event(conn, limit=None)
    pending_derive_events = _pending_article_ids("derive_events_from_articles")
    queueable["derive_events_from_articles"] = sum(1 for article_id in event_candidate_ids if int(article_id) not in pending_derive_events)
    profile, _reason = get_active_profile_for_stage(conn, "cve_enrich_products")
    if profile:
        cve_ids = list_cve_ids_missing_products(conn, limit=None)
        pending_cve_products = _pending_cve_ids("cve_enrich_llm")
        queueable["cve_enrich_llm"] = sum(1 for cve_id in cve_ids if str(cve_id) not in pending_cve_products)
    else:
        queueable["cve_enrich_llm"] = 0
    kev_ids = list_cve_ids_needing_kev_check(conn, limit=None)
    pending_kev = _pending_cve_ids("cve_enrich_kev")
    queueable["cve_enrich_kev"] = sum(1 for cve_id in kev_ids if str(cve_id) not in pending_kev)
    cve_threat_ids = list_cve_ids_missing_threat_actors(conn, limit=None)
    pending_cve_threats = _pending_cve_ids("cve_enrich_threat_actors")
    queueable["cve_enrich_threat_actors"] = sum(1 for cve_id in cve_threat_ids if str(cve_id) not in pending_cve_threats)
    metrics["queueable_by_job_type"] = queueable
    metrics["dashboard_display_rows"] = _dashboard_display_rows(metrics)
    return metrics


def _render_metrics_text(conn: Any) -> str:
    now = datetime.now(tz=timezone.utc)
    lines: list[str] = []

    def add_metric(
        name: str,
        help_text: str,
        metric_type: str,
        samples: list[tuple[dict[str, object], int | float]],
    ) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
        for labels, value in samples:
            lines.append(f"{name}{_prometheus_label_block(labels)} {value}")

    lines.append("# SemperVigil metrics")
    version = _prometheus_escape(_get_version())
    lines.append("# HELP sempervigil_info SemperVigil build and version metadata")
    lines.append("# TYPE sempervigil_info gauge")
    lines.append(f'sempervigil_info{{version="{version}"}} 1')

    dashboard_metrics = _build_dashboard_metrics_payload(conn)
    build_state = dashboard_metrics.get("build_state") or get_build_state(conn)
    build_status = dashboard_metrics.get("build_status") or get_build_status(conn)
    counts_since = _prometheus_timestamp(dashboard_metrics.get("job_counts_since"))
    add_metric(
        "sempervigil_build_dirty",
        "Whether SemperVigil has a pending build request",
        "gauge",
        [({}, 1 if build_state.get("dirty") else 0)],
    )
    add_metric(
        "sempervigil_build_status",
        "Current SemperVigil build status by state label",
        "gauge",
        [
            ({"status": status}, 1 if build_status.get("status") == status else 0)
            for status in ("idle", "building", "error")
        ],
    )
    add_metric(
        "sempervigil_build_status_code",
        "Current SemperVigil build status as a numeric code: 0=idle, 1=building, 2=error",
        "gauge",
        [({}, {"idle": 0, "building": 1, "error": 2}.get(str(build_status.get("status") or "idle"), 0))],
    )
    last_built_at = _prometheus_timestamp(build_state.get("last_built_at"))
    if last_built_at is not None:
        add_metric(
            "sempervigil_build_last_success_timestamp_seconds",
            "Unix timestamp of the last successful site build",
            "gauge",
            [({}, last_built_at)],
        )
    if counts_since is not None:
        add_metric(
            "sempervigil_dashboard_counts_since_timestamp_seconds",
            "Unix timestamp used for dashboard failed and completed counters",
            "gauge",
            [({}, counts_since)],
        )

    queue_samples: list[tuple[dict[str, object], int | float]] = []
    oldest_samples: list[tuple[dict[str, object], int | float]] = []
    for row in get_queue_stats(conn):
        queue_name = str(row.get("queue_name") or "default")
        queue_samples.append(({"queue_name": queue_name, "status": "queued"}, int(row.get("queued") or 0)))
        queue_samples.append(({"queue_name": queue_name, "status": "running"}, int(row.get("running") or 0)))
        oldest_at = row.get("oldest_requested_at")
        if isinstance(oldest_at, str):
            try:
                age_seconds = max(0.0, (now - _parse_iso(oldest_at)).total_seconds())
                oldest_samples.append(({"queue_name": queue_name}, age_seconds))
            except Exception:  # noqa: BLE001
                pass
    add_metric(
        "sempervigil_queue_jobs",
        "Queued and running job counts by logical queue",
        "gauge",
        queue_samples,
    )
    add_metric(
        "sempervigil_queue_oldest_age_seconds",
        "Age in seconds of the oldest queued job by logical queue",
        "gauge",
        oldest_samples,
    )

    add_metric(
        "sempervigil_jobs",
        "Job counts by logical queue, job type, and status",
        "gauge",
        [
            (
                {
                    "queue_name": str(row.get("queue_name") or "default"),
                    "job_type": str(row.get("job_type") or ""),
                    "status": str(row.get("status") or ""),
                },
                int(row.get("count") or 0),
            )
            for row in get_job_metrics(conn)
        ],
    )

    runner_samples: list[tuple[dict[str, object], int | float]] = []
    active_runner_samples: list[tuple[dict[str, object], int | float]] = []
    runner_rows = get_runner_stats(conn)
    for row in runner_rows:
        runner_type = str(row.get("runner_type") or "unknown")
        status = str(row.get("status") or "")
        count = int(row.get("count") or 0)
        runner_samples.append(({"runner_type": runner_type, "status": status}, count))
    active_by_type: dict[str, int] = {}
    for row in runner_rows:
        if str(row.get("status") or "") != "running":
            continue
        runner_type = str(row.get("runner_type") or "unknown")
        active_by_type[runner_type] = active_by_type.get(runner_type, 0) + int(row.get("count") or 0)
    for runner_type, count in sorted(active_by_type.items()):
        active_runner_samples.append(({"runner_type": runner_type}, count))
    add_metric(
        "sempervigil_runner_launch_jobs",
        "Launch job counts by runner type and status",
        "gauge",
        runner_samples,
    )
    add_metric(
        "sempervigil_runner_active",
        "Active runner launches by runner type",
        "gauge",
        active_runner_samples,
    )
    add_metric(
        "sempervigil_runner_health",
        "Runner health by runner type and health state",
        "gauge",
        [
            (
                {
                    "runner_type": str(row.get("runner_type") or "unknown"),
                    "health": str(row.get("health") or ""),
                },
                int(row.get("count") or 0),
            )
            for row in (dashboard_metrics.get("runner_health") or [])
        ],
    )
    add_metric(
        "sempervigil_queue_worker_health",
        "Queue and runner health metrics for each logical worker queue",
        "gauge",
        [
            (
                {
                    "queue_name": str(row.get("queue_name") or "default"),
                    "metric": str(row.get("metric") or ""),
                },
                int(row.get("count") or 0),
            )
            for row in (dashboard_metrics.get("queue_worker_health") or [])
        ],
    )

    add_metric(
        "sempervigil_stale_jobs",
        "Running jobs whose lease or lock has expired",
        "gauge",
        [
            ({"queue_name": str(row.get("queue_name") or "default")}, int(row.get("stale") or 0))
            for row in get_stale_job_stats(conn)
        ],
    )

    source_counts = get_source_ingest_state_counts(conn)
    add_metric(
        "sempervigil_sources_ingest_state",
        "Source ingest scheduling state counts",
        "gauge",
        [
            ({"state": "queued"}, int(source_counts.get("queued") or 0)),
            ({"state": "running"}, int(source_counts.get("running") or 0)),
        ],
    )

    legacy_daily = get_public_metrics_daily_counts(conn, days=14)
    add_metric(
        "sv_articles_daily_count",
        "Legacy SemperVigil public metrics: articles per day for the last 14 days",
        "gauge",
        [
            ({"day": str(row.get("day") or "")}, int(row.get("articles") or 0))
            for row in legacy_daily
        ],
    )
    add_metric(
        "sv_cves_high_daily_count",
        "Legacy SemperVigil public metrics: high-severity CVEs per day for the last 14 days",
        "gauge",
        [
            ({"day": str(row.get("day") or "")}, int(row.get("cves_high") or 0))
            for row in legacy_daily
        ],
    )
    add_metric(
        "sv_cves_critical_daily_count",
        "Legacy SemperVigil public metrics: critical-severity CVEs per day for the last 14 days",
        "gauge",
        [
            ({"day": str(row.get("day") or "")}, int(row.get("cves_critical") or 0))
            for row in legacy_daily
        ],
    )

    dashboard_job_counts = dashboard_metrics.get("job_counts_by_type_status") or {}
    dashboard_jobs_samples: list[tuple[dict[str, object], int | float]] = []
    for job_type, statuses in sorted(dashboard_job_counts.items()):
        if not isinstance(statuses, dict):
            continue
        worker_group = _job_group_id_for_job_type(str(job_type))
        for status_key, value in sorted(statuses.items()):
            dashboard_jobs_samples.append(
                (
                    {
                        "worker_group": worker_group,
                        "job_type": str(job_type),
                        "status": str(status_key),
                    },
                    int(value or 0),
                )
            )
    add_metric(
        "sempervigil_dashboard_jobs",
        "Admin-style current job counts by worker group, job type, and status",
        "gauge",
        dashboard_jobs_samples,
    )

    queueable = dashboard_metrics.get("queueable_by_job_type") or {}
    add_metric(
        "sempervigil_dashboard_need",
        "Admin-style queueable job counts by worker group and job type",
        "gauge",
        [
            (
                {
                    "worker_group": _job_group_id_for_job_type(str(job_type)),
                    "job_type": str(job_type),
                },
                int(value or 0),
            )
            for job_type, value in sorted(queueable.items())
        ],
    )
    display_rows = dashboard_metrics.get("dashboard_display_rows") or []
    add_metric(
        "sempervigil_dashboard_current",
        "Exact admin dashboard display values by worker group, job type, and column",
        "gauge",
        [
            (
                {
                    "worker_group": str(row.get("worker_group") or "all"),
                    "job_type": str(row.get("job_type") or ""),
                    "column": str(column),
                },
                int(row.get(column) or 0),
            )
            for row in display_rows
            for column in _DASHBOARD_STATUS_COLUMNS
        ],
    )
    add_metric(
        "sempervigil_dashboard_order",
        "Admin dashboard display order by worker group and job type",
        "gauge",
        [
            (
                {
                    "worker_group": str(row.get("worker_group") or "all"),
                    "job_type": str(row.get("job_type") or ""),
                },
                int(row.get("display_order") or 0),
            )
            for row in display_rows
        ],
    )

    return "\n".join(lines) + "\n"


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> PlainTextResponse:
    conn = _get_conn()
    return PlainTextResponse(
        _render_metrics_text(conn),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.get("/admin/config/runtime", dependencies=[Depends(_require_admin_token)])
def runtime_config_get() -> dict[str, object]:
    conn = _get_conn()
    try:
        cfg = get_runtime_config(conn)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"config": cfg}


@app.put("/admin/config/runtime", dependencies=[Depends(_require_admin_token)])
def runtime_config_set(payload: RuntimeConfigRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        set_runtime_config(conn, payload.config)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok"}


@app.put("/admin/api/config/patch", dependencies=[Depends(_require_admin_token)])
def runtime_config_patch(payload: RuntimeConfigRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        cfg = apply_runtime_config_patch(conn, payload.config)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok", "config": cfg}


@app.get("/admin/api/logs/tail", dependencies=[Depends(_require_admin_token)])
def logs_tail(service: str, lines: int = 200) -> dict[str, object]:
    service_key = str(service or "").strip().lower()
    if service_key not in _LOG_SERVICES:
        raise HTTPException(status_code=400, detail="invalid_service")
    line_limit = max(1, min(int(lines or 200), 500))
    if service_key == "build_hugo":
        log_path = _latest_hugo_build_log_path()
        text = _read_log_tail(log_path, line_limit, max_bytes=400_000) if log_path else ""
        header = _format_log_header(log_path)
        return {
            "service": service_key,
            "lines": line_limit,
            "text": f"{header}{text}" if text else header,
            "log_path": log_path,
        }
    conn = _get_conn()
    config = load_runtime_config(conn)
    path = _LOG_SERVICES[service_key]
    if path and not os.path.isabs(path):
        path = str(Path(config.paths.logs_dir) / path)
    text = _read_log_tail(path, line_limit, max_bytes=200_000)
    return {"service": service_key, "lines": line_limit, "text": text}


@app.get("/admin/api/logs/query", dependencies=[Depends(_require_admin_token)])
def logs_query(
    service: str = "all",
    lines: int = 200,
    runner: str | None = None,
    job_type: str | None = None,
    event: str | None = None,
    level: str | None = None,
) -> dict[str, object]:
    service_key = str(service or "all").strip().lower()
    if service_key not in _LOG_STREAM_SERVICES:
        raise HTTPException(status_code=400, detail="invalid_service")
    line_limit = max(1, min(int(lines or 200), 1000))
    conn = _get_conn()
    config = load_runtime_config(conn)
    entries = _query_logs(
        config,
        service=service_key,
        limit=line_limit,
        runner=(str(runner).strip() or None) if runner is not None else None,
        job_type=(str(job_type).strip() or None) if job_type is not None else None,
        event=(str(event).strip() or None) if event is not None else None,
        level=(str(level).strip() or None) if level is not None else None,
    )
    return {
        "service": service_key,
        "lines": line_limit,
        "runner": runner,
        "job_type": job_type,
        "event": event,
        "level": level,
        "entries": entries,
    }


@app.get("/admin/api/logs/stream", dependencies=[Depends(_require_admin_token)])
def logs_stream(
    request: Request,
    service: str = "all",
    lines: int = 200,
    runner: str | None = None,
    job_type: str | None = None,
    event: str | None = None,
    level: str | None = None,
) -> StreamingResponse:
    del request
    service_key = str(service or "all").strip().lower()
    if service_key not in _LOG_STREAM_SERVICES:
        raise HTTPException(status_code=400, detail="invalid_service")
    line_limit = max(1, min(int(lines or 200), 1000))
    conn = _get_conn()
    config = load_runtime_config(conn)

    def _event_stream():
        seen_ids: set[str] = set()
        while True:
            entries = _query_logs(
                config,
                service=service_key,
                limit=line_limit,
                runner=(str(runner).strip() or None) if runner is not None else None,
                job_type=(str(job_type).strip() or None) if job_type is not None else None,
                event=(str(event).strip() or None) if event is not None else None,
                level=(str(level).strip() or None) if level is not None else None,
            )
            new_entries = [entry for entry in entries if str(entry.get("id") or "") not in seen_ids]
            for entry in new_entries:
                seen_ids.add(str(entry.get("id") or ""))
                yield f"data: {json.dumps(entry, default=str)}\n\n"
            yield ": heartbeat\n\n"
            time.sleep(2)

    return StreamingResponse(_event_stream(), media_type="text/event-stream")


@app.get("/admin/api/logs/builds/latest", dependencies=[Depends(_require_admin_token)])
def logs_latest_build(stream: str = "stdout", lines: int = 200) -> dict[str, object]:
    service_key = str(stream or "stdout").strip().lower()
    if service_key not in {"stdout", "stderr"}:
        raise HTTPException(status_code=400, detail="invalid_stream")
    line_limit = max(1, min(int(lines or 200), 500))
    conn = _get_conn()
    config = load_runtime_config(conn)
    last_build = get_last_job_by_type(conn, "build_site")
    log_path = None
    if last_build and isinstance(last_build.result, dict):
        key = f"{service_key}_log_path"
        value = last_build.result.get(key)
        if isinstance(value, str) and value:
            log_path = value
    if not log_path:
        logs_dir = Path(config.paths.logs_dir) / "builds"
        if not logs_dir.exists():
            return {"stream": service_key, "lines": line_limit, "text": "", "log_path": None}
        suffix = f".{service_key}.log"
        candidates = sorted(
            logs_dir.glob(f"*{suffix}"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            log_path = str(candidates[0])
    if not log_path:
        return {"stream": service_key, "lines": line_limit, "text": "", "log_path": None}
    text = _read_log_tail(log_path, line_limit, max_bytes=400_000)
    return {"stream": service_key, "lines": line_limit, "text": text, "log_path": log_path}


@app.get("/admin/api/dashboard/metrics", dependencies=[Depends(_require_admin_token)])
def dashboard_metrics() -> dict[str, object]:
    conn = _get_conn()
    return _build_dashboard_metrics_payload(conn)


@app.get("/admin/api/health/vpn", dependencies=[Depends(_require_admin_token)])
def vpn_health() -> dict[str, object]:
    conn = _get_conn()
    recent = conn.execute(
        """
        SELECT COUNT(DISTINCT h.source_id), MAX(h.ts::timestamptz)
        FROM source_health_history h
        JOIN sources s ON s.id = h.source_id
        WHERE h.ok = 0
          AND h.ts::timestamptz >= NOW() - INTERVAL '60 minutes'
          AND COALESCE(h.last_error, '') ILIKE %s
          AND COALESCE(LOWER(s.overrides #>> '{fetch,use_vpn}') <> 'false', TRUE)
        """,
        ("%503%",),
    ).fetchone()
    paused = conn.execute(
        """
        SELECT COUNT(*)
        FROM sources
        WHERE enabled = 0
          AND COALESCE(paused_reason, '') LIKE 'auto_pause:error_streak:%'
          AND COALESCE(last_error, '') ILIKE %s
          AND COALESCE(LOWER(overrides #>> '{fetch,use_vpn}') <> 'false', TRUE)
        """,
        ("%503%",),
    ).fetchone()
    sample_rows = conn.execute(
        """
        SELECT DISTINCT s.name
        FROM source_health_history h
        JOIN sources s ON s.id = h.source_id
        WHERE h.ok = 0
          AND h.ts::timestamptz >= NOW() - INTERVAL '60 minutes'
          AND COALESCE(h.last_error, '') ILIKE %s
          AND COALESCE(LOWER(s.overrides #>> '{fetch,use_vpn}') <> 'false', TRUE)
        ORDER BY s.name
        LIMIT 8
        """,
        ("%503%",),
    ).fetchall()
    recent_failures = int(recent[0] or 0) if recent else 0
    paused_sources = int(paused[0] or 0) if paused else 0
    last_failure_at = recent[1].isoformat() if recent and recent[1] else None
    status = "ok"
    if paused_sources >= 5 or recent_failures >= 5:
        status = "down"
    elif paused_sources > 0 or recent_failures > 0:
        status = "degraded"
    return {
        "status": status,
        "recent_503_sources": recent_failures,
        "paused_503_sources": paused_sources,
        "last_failure_at": last_failure_at,
        "sample_sources": [row[0] for row in sample_rows],
    }


@app.get("/admin/api/public-metrics/daily", dependencies=[Depends(_require_admin_token)])
def public_metrics_daily(days: int = 14) -> dict[str, object]:
    conn = _get_conn()
    safe_days = max(1, min(int(days or 14), 90))
    rows = get_public_metrics_daily_counts(conn, days=safe_days)
    return {
        "days": safe_days,
        "generated_at": utc_now_iso(),
        "rows": rows,
    }

@app.post("/admin/api/dashboard/reset_failures", dependencies=[Depends(_require_admin_token)])
def dashboard_reset_failures() -> dict[str, object]:
    conn = _get_conn()
    now = utc_now_iso()
    set_setting(conn, "dashboard_failures_since", now)
    set_setting(conn, "dashboard_job_counts_since", now)
    conn.commit()
    return {
        "status": "ok",
        "failures_since": get_setting(conn, "dashboard_failures_since", None),
        "counts_since": get_setting(conn, "dashboard_job_counts_since", None),
    }


@app.post("/admin/api/dashboard/rebuild_vendor_products", dependencies=[Depends(_require_admin_token)])
def dashboard_rebuild_vendor_products() -> dict[str, object]:
    conn = _get_conn()
    job_id = enqueue_job(conn, "rebuild_vendor_products", {})
    return {"status": "queued", "job_id": job_id}


@app.post("/admin/api/threats/backfill/articles", dependencies=[Depends(_require_admin_token)])
def threats_backfill_articles(payload: dict | None = Body(None)) -> dict[str, object]:
    conn = _get_conn()
    limit = int(payload.get("limit") or 200) if payload else 200
    job_id = enqueue_job(conn, "article_threat_actors_backfill", {"limit": limit})
    return {"status": "queued", "job_id": job_id, "limit": limit}


@app.post("/admin/api/threats/backfill/cves", dependencies=[Depends(_require_admin_token)])
def threats_backfill_cves(payload: dict | None = Body(None)) -> dict[str, object]:
    conn = _get_conn()
    limit = int(payload.get("limit") or 200) if payload else 200
    job_id = enqueue_job(conn, "cve_threat_actors_backfill", {"limit": limit})
    return {"status": "queued", "job_id": job_id, "limit": limit}



@app.get("/admin/api/cves/settings", dependencies=[Depends(_require_admin_token)])
def cve_settings_get() -> dict[str, object]:
    conn = _get_conn()
    try:
        settings = get_cve_settings(conn)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    last_sync = get_setting(conn, "cve.last_successful_sync_at", None)
    settings = dict(settings)
    settings["last_run_at"] = last_sync
    return {"settings": settings}


@app.get("/admin/api/schedules/settings", dependencies=[Depends(_require_admin_token)])
def schedule_settings_get() -> dict[str, object]:
    conn = _get_conn()
    try:
        settings = get_schedule_settings(conn)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"settings": settings}


@app.put("/admin/api/schedules/settings", dependencies=[Depends(_require_admin_token)])
def schedule_settings_put(payload: dict[str, object]) -> dict[str, object]:
    conn = _get_conn()
    settings = payload.get("settings")
    if not isinstance(settings, dict):
        raise HTTPException(status_code=400, detail="settings_required")
    try:
        current = get_schedule_settings(conn)
    except ConfigError:
        current = {}
    current_tasks = (current.get("tasks") or {}) if isinstance(current, dict) else {}
    new_tasks = settings.get("tasks")
    if isinstance(new_tasks, dict):
        for key, task in new_tasks.items():
            if not isinstance(task, dict):
                continue
            if "last_run" not in task and isinstance(current_tasks.get(key), dict):
                last_run = current_tasks[key].get("last_run")
                if last_run:
                    task["last_run"] = last_run
    try:
        set_schedule_settings(conn, settings)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok"}

@app.post("/admin/api/dashboard/queue_missing", dependencies=[Depends(_require_admin_token)])
def dashboard_queue_missing(payload: dict[str, object]) -> dict[str, object]:
    conn = _get_conn()
    kind = str(payload.get("kind") or "").strip()
    limit = None
    if payload and isinstance(payload.get("limit"), int):
        limit = int(payload["limit"])
    queued = 0
    skipped = 0
    if kind == "missing_content":
        article_ids = list_article_ids_missing_content_all(conn, limit=limit)
        for article_id in article_ids:
            existing = get_pending_article_job_id(conn, "fetch_article_content", int(article_id))
            if existing:
                skipped += 1
                continue
            enqueue_job(conn, "fetch_article_content", {"article_id": int(article_id)}, dedupe=True)
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped}
    if kind == "content_error":
        article_ids = list_article_ids_with_content_error_all(conn)
        for article_id in article_ids:
            existing = get_pending_article_job_id(conn, "fetch_article_content", int(article_id))
            if existing:
                skipped += 1
                continue
            enqueue_job(conn, "fetch_article_content", {"article_id": int(article_id)}, dedupe=True)
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped}
    if kind == "missing_summary":
        profile, reason = get_active_profile_for_stage(conn, "summarize_article")
        if not profile:
            return {"status": "disabled", "message": f"Summarization disabled: {reason}"}
        article_ids = list_article_ids_ready_for_summary_all(conn)
        for article_id in article_ids:
            existing = get_pending_article_job_id(conn, "summarize_article_llm", int(article_id))
            if existing:
                skipped += 1
                continue
            payload = {"article_id": int(article_id)}
            payload["profile_id"] = profile.get("id")
            enqueue_job(conn, "summarize_article_llm", payload, dedupe=True)
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped}
    if kind == "missing_context":
        profile, reason = get_active_profile_for_stage(conn, "article_context_pack")
        if not profile:
            return {"status": "disabled", "message": f"Context pack disabled: {reason}"}
        limit = int(payload.get("limit") or 200)
        article_ids = list_article_ids_missing_context_pack(conn, limit=limit)
        for article_id in article_ids:
            existing = get_pending_article_job_id(conn, "summarize_article_context_llm", int(article_id))
            if existing:
                skipped += 1
                continue
            payload = {"article_id": int(article_id)}
            payload["profile_id"] = profile.get("id")
            enqueue_job(conn, "summarize_article_context_llm", payload, dedupe=True)
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(article_ids)}
    if kind == "cve_description":
        limit = int(payload.get("limit") or 500)
        cve_ids = list_cve_ids_missing_description(conn, limit=limit)
        for cve_id in cve_ids:
            existing = get_pending_cve_job_id(conn, cve_id)
            if existing:
                skipped += 1
                continue
            enqueue_job(conn, "cve_sync", {"cve_id": cve_id}, dedupe=True)
            queued += 1
        log_event(
            logging.getLogger("sempervigil.cve"),
            logging.INFO,
            "cve_missing_description_queued",
            queued=queued,
            skipped=skipped,
            total=len(cve_ids),
        )
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(cve_ids)}

    if kind == "cve_products":
        profile, reason = get_active_profile_for_stage(conn, "cve_enrich_products")
        if not profile:
            return {"status": "disabled", "message": f"CVE enrichment disabled: {reason}"}
        limit = int(payload.get("limit") or 500)
        cve_ids = list_cve_ids_missing_products(conn, limit=limit)
        for cve_id in cve_ids:
            existing = get_pending_job_id_for_cve(conn, "cve_enrich_llm", cve_id)
            if existing:
                skipped += 1
                continue
            enqueue_job(conn, "cve_enrich_llm", {"cve_id": cve_id, "profile_id": profile.get("id")}, dedupe=True)
            queued += 1
        log_event(
            logging.getLogger("sempervigil.cve"),
            logging.INFO,
            "cve_missing_products_queued",
            queued=queued,
            skipped=skipped,
            total=len(cve_ids),
        )
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(cve_ids)}
    if kind == "cve_kev":
        limit = int(payload.get("limit") or 500)
        cve_ids = list_cve_ids_needing_kev_check(conn, limit=limit)
        for cve_id in cve_ids:
            existing = get_pending_job_id_for_cve(conn, "cve_enrich_kev", cve_id)
            if existing:
                skipped += 1
                continue
            enqueue_job(conn, "cve_enrich_kev", {"cve_id": cve_id}, dedupe=True)
            queued += 1
        log_event(
            logging.getLogger("sempervigil.cve"),
            logging.INFO,
            "cve_kev_queued",
            queued=queued,
            skipped=skipped,
            total=len(cve_ids),
        )
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(cve_ids)}
    if kind == "article_products":
        limit = int(payload.get("limit") or 500)
        article_ids = list_article_ids_missing_products(conn, limit=limit)
        for article_id in article_ids:
            existing = get_pending_article_job_id(conn, "article_enrich_products", int(article_id))
            if existing:
                skipped += 1
                continue
            enqueue_job(conn, "article_enrich_products", {"article_id": int(article_id)}, dedupe=True)
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(article_ids)}
    if kind == "article_threats":
        limit = int(payload.get("limit") or 200)
        article_ids = list_article_ids_missing_threat_actors(conn, limit=limit)
        for article_id in article_ids:
            existing = get_pending_article_job_id(conn, "article_enrich_threat_actors", int(article_id))
            if existing:
                skipped += 1
                continue
            enqueue_job(
                conn,
                "article_enrich_threat_actors",
                {"article_id": int(article_id)},
                dedupe=True,
            )
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(article_ids)}
    if kind == "article_events":
        limit = int(payload.get("limit") or 200)
        article_ids = list_article_ids_without_event(conn, limit=limit)
        for article_id in article_ids:
            existing = get_pending_article_job_id(conn, "derive_events_from_articles", int(article_id))
            if existing:
                skipped += 1
                continue
            enqueue_job(
                conn,
                "derive_events_from_articles",
                {"article_id": int(article_id)},
                dedupe=True,
            )
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(article_ids)}
    if kind == "cve_threats":
        limit = int(payload.get("limit") or 200)
        cve_ids = list_cve_ids_missing_threat_actors(conn, limit=limit)
        for cve_id in cve_ids:
            existing = get_pending_job_id_for_cve(conn, "cve_enrich_threat_actors", cve_id)
            if existing:
                skipped += 1
                continue
            enqueue_job(conn, "cve_enrich_threat_actors", {"cve_id": cve_id}, dedupe=True)
            queued += 1
        return {"status": "queued", "queued": queued, "skipped": skipped, "total": len(cve_ids)}
    raise HTTPException(status_code=400, detail="unknown_kind")



@app.put("/admin/api/cves/settings", dependencies=[Depends(_require_admin_token)])
def cve_settings_set(payload: CveSettingsRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        set_cve_settings(conn, payload.settings)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok"}


@app.get("/admin/api/watchlist/vendors", dependencies=[Depends(_require_admin_token)])
def watchlist_vendors() -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    return {"items": list_watchlist_vendors(conn)}


@app.post("/admin/api/watchlist/vendors", dependencies=[Depends(_require_admin_token)])
def watchlist_vendor_add(payload: WatchVendorRequest) -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    item = add_watchlist_vendor(conn, payload.display_name)
    _recompute_scope(conn)
    return item


@app.patch("/admin/api/watchlist/vendors/{vendor_id}", dependencies=[Depends(_require_admin_token)])
def watchlist_vendor_toggle(vendor_id: str, payload: WatchToggleRequest) -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    update_watchlist_vendor(conn, vendor_id, payload.enabled)
    _recompute_scope(conn)
    return {"status": "ok"}


@app.delete("/admin/api/watchlist/vendors/{vendor_id}", dependencies=[Depends(_require_admin_token)])
def watchlist_vendor_delete(vendor_id: str) -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    delete_watchlist_vendor(conn, vendor_id)
    _recompute_scope(conn)
    return {"status": "deleted"}


@app.get("/admin/api/watchlist/products", dependencies=[Depends(_require_admin_token)])
def watchlist_products() -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    return {"items": list_watchlist_products(conn)}


@app.post("/admin/api/watchlist/products", dependencies=[Depends(_require_admin_token)])
def watchlist_product_add(payload: WatchProductRequest) -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    item = add_watchlist_product(
        conn,
        display_name=payload.display_name,
        vendor_norm=payload.vendor_norm,
        match_mode=payload.match_mode,
    )
    _recompute_scope(conn)
    return item


@app.patch("/admin/api/watchlist/products/{product_id}", dependencies=[Depends(_require_admin_token)])
def watchlist_product_toggle(product_id: str, payload: WatchToggleRequest) -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    update_watchlist_product(conn, product_id, payload.enabled, payload.match_mode)
    _recompute_scope(conn)
    return {"status": "ok"}


@app.delete("/admin/api/watchlist/products/{product_id}", dependencies=[Depends(_require_admin_token)])
def watchlist_product_delete(product_id: str) -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    delete_watchlist_product(conn, product_id)
    _recompute_scope(conn)
    return {"status": "deleted"}


@app.get("/admin/api/watchlist/suggestions", dependencies=[Depends(_require_admin_token)])
def watchlist_suggestions() -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    return list_watchlist_suggestions(conn)


@app.post("/admin/api/watchlist/recompute", dependencies=[Depends(_require_admin_token)])
def watchlist_recompute() -> dict[str, object]:
    conn = _get_conn()
    _ensure_watchlist_enabled(conn)
    stats = _recompute_scope(conn)
    return {"status": "ok", **stats}


@app.post("/admin/api/cves/run", dependencies=[Depends(_require_admin_token)])
def cve_settings_run() -> dict[str, object]:
    conn = _get_conn()
    job_id = enqueue_job(conn, "cve_sync", None, debounce=True)
    return {"job_id": job_id}


@app.post("/admin/api/cves/test", dependencies=[Depends(_require_admin_token)])
def cve_settings_test(payload: CveTestRequest) -> dict[str, object]:
    conn = _get_conn()
    settings = get_cve_settings(conn)
    try:
        cfg = load_runtime_config(conn)
        scope_min_cvss = cfg.scope.min_cvss
        watchlist_enabled = cfg.personalization.watchlist_enabled
    except Exception:  # noqa: BLE001
        scope_min_cvss = None
        watchlist_enabled = False
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(hours=max(1, int(payload.hours)))
    start_iso = isoformat_utc(start)
    end_iso = isoformat_utc(now)
    api_key = os.environ.get("NVD_API_KEY")
    nvd = settings.get("nvd") or {}
    result = preview_cves(
        CveSyncConfig(
            api_base=str(nvd.get("api_base") or "https://services.nvd.nist.gov/rest/json/cves/2.0"),
            results_per_page=int(nvd.get("results_per_page") or 2000),
            rate_limit_seconds=float(settings.get("rate_limit_seconds", 1.0)),
            backoff_seconds=float(settings.get("backoff_seconds", 2.0)),
            max_retries=int(settings.get("max_retries", 3)),
            prefer_v4=bool(settings.get("prefer_v4", True)),
            scope_min_cvss=scope_min_cvss,
            watchlist_enabled=watchlist_enabled,
            api_key=api_key,
            filters=settings.get("filters") or {},
        ),
        last_modified_start=start_iso,
        last_modified_end=end_iso,
        limit=payload.limit,
    )
    result["start"] = start_iso
    result["end"] = end_iso
    return result


@app.get("/admin/api/cves/completeness", dependencies=[Depends(_require_admin_token)])
def cve_completeness(limit: int = 20) -> dict[str, object]:
    conn = _get_conn()
    return cve_data_completeness(conn, limit=limit)


@app.get("/ui/login")
def ui_login(request: Request):
    token_enabled = bool(os.environ.get("SV_ADMIN_TOKEN"))
    return TEMPLATES.TemplateResponse(
        request,
        "admin/login.html",
        {
            "request": request,
            "token_enabled": token_enabled,
            "is_authenticated": bool(request.cookies.get(ADMIN_COOKIE_NAME)),
        },
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
        secure=_is_secure_request(request),
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
        conn = init_db()
        config = load_runtime_config(conn)
        bootstrap_cve_settings(conn)
        bootstrap_events_settings(conn)
        bootstrap_schedule_settings(conn)
    except ConfigError:
        return
    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir, config.paths.logs_dir))


@app.post("/jobs/enqueue")
def enqueue(job: JobRequest, _: None = Depends(_require_admin_token)) -> dict[str, str]:
    logger = logging.getLogger("sempervigil.admin")
    conn = _get_conn()
    allowed_job_types = set(WORKER_JOB_TYPES) | {"build_site"}
    if job.job_type not in allowed_job_types:
        raise HTTPException(status_code=400, detail="unsupported_job_type")
    payload = {"source_id": job.source_id} if job.source_id else None
    # Debounce is job_type-wide; when a source_id is provided we need payload-level dedupe instead.
    use_debounce = False if job.source_id else True
    if job.job_type == "build_site" and has_pending_job(conn, "build_site"):
        last = get_last_job_by_type(conn, "build_site")
        if last and last.status in {"queued", "running"}:
            return {"status": "already_queued", "job_id": last.id}
        return {"status": "already_queued"}
    job_id = enqueue_job(conn, job.job_type, payload, debounce=use_debounce, dedupe=True)
    log_event(
        logger,
        logging.INFO,
        "job_enqueued",
        job_id=job_id,
        job_type=job.job_type,
    )
    return {"job_id": job_id}


@app.post("/admin/api/build/request", dependencies=[Depends(_require_admin_token)])
def request_build() -> dict[str, object]:
    conn = _get_conn()
    state = get_build_state(conn)
    if state.get("dirty"):
        return {"status": "already_dirty", "build_state": state}
    mark_build_dirty(conn, reason="admin_requested")
    return {"status": "requested", "build_state": get_build_state(conn)}


@app.post("/jobs/{job_id}/cancel", dependencies=[Depends(_require_admin_token)])
def cancel_job_api(job_id: str, request: Request) -> dict[str, object]:
    conn = _get_conn()
    canceled = cancel_job(conn, job_id)
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.WARNING,
        "job_canceled",
        job_id=job_id,
        client=request.client.host if request.client else "unknown",
    )
    if not canceled:
        raise HTTPException(status_code=404, detail="job_not_cancelable")
    return {"status": "ok", "job_id": job_id}


@app.post("/jobs/{job_id}/rerun", dependencies=[Depends(_require_admin_token)])
def rerun_job_api(job_id: str) -> dict[str, object]:
    conn = _get_conn()
    job = get_job(conn, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job_not_found")
    if job.status in {"queued", "running"}:
        return {"status": "already_running", "job_id": job.id}
    new_id = enqueue_job(conn, job.job_type, job.payload or {})
    return {"status": "queued", "job_id": new_id}


@app.post("/jobs/cancel-all", dependencies=[Depends(_require_admin_token)])
def cancel_all_jobs_api(request: Request) -> dict[str, object]:
    conn = _get_conn()
    canceled = cancel_all_jobs(conn, reason="canceled_by_admin")
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.WARNING,
        "jobs_canceled_all",
        client=request.client.host if request.client else "unknown",
        canceled=canceled,
    )
    return {"status": "ok", "canceled": canceled}


@app.post("/admin/api/briefs/cancel-running", dependencies=[Depends(_require_admin_token)])
def cancel_running_daily_brief() -> dict[str, object]:
    conn = _get_conn()
    canceled = cancel_jobs_by_type(
        conn, "build_daily_brief", status=None, reason="canceled_by_admin"
    )
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.INFO,
        "daily_brief_cancel_running",
        canceled=canceled,
    )
    return {"status": "ok", "canceled": canceled}


@app.post("/admin/api/briefs/cancel-running-restart", dependencies=[Depends(_require_admin_token)])
def cancel_running_daily_brief_restart() -> dict[str, object]:
    conn = _get_conn()
    canceled = cancel_jobs_by_type(
        conn, "build_daily_brief", status=None, reason="canceled_by_admin"
    )
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.INFO,
        "daily_brief_cancel_running_restart",
        canceled=canceled,
    )
    return {
        "status": "ok",
        "canceled": canceled,
        "restart_command": "docker compose restart worker_llm",
    }


@app.get("/admin/api/debug/overview", dependencies=[Depends(_require_admin_token)])
def debug_overview() -> dict[str, object]:
    conn = _get_conn()
    pipeline_metrics = get_dashboard_metrics(conn)
    stage_statuses = list_stage_statuses(conn, STAGE_NAMES)
    llm_active = sum(1 for item in stage_statuses if item["status"] == "active")
    llm_total = len(stage_statuses)
    counts = {
        "articles": count_table(conn, "articles"),
        "article_tags": count_table(conn, "article_tags"),
        "cves": count_table(conn, "cves"),
        "vendors": count_table(conn, "vendors"),
        "products": count_table(conn, "products"),
        "cve_products": count_table(conn, "cve_products"),
        "cve_product_versions": count_table(conn, "cve_product_versions"),
        "events": count_table(conn, "events"),
        "event_items": count_table(conn, "event_items"),
        "jobs": count_table(conn, "jobs"),
        "source_health_history": count_table(conn, "source_health_history"),
        "llm_runs": count_table(conn, "llm_runs"),
    }
    last_jobs = [
        {
            "id": job.id,
            "job_type": job.job_type,
            "status": job.status,
            "requested_at": job.requested_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "error": job.error,
        }
        for job in list_jobs(conn, limit=10)
    ]
    last_build = get_last_job_by_type(conn, "build_site")
    last_build_job = None
    if last_build:
        last_build_job = {
            "id": last_build.id,
            "status": last_build.status,
            "requested_at": last_build.requested_at,
            "started_at": last_build.started_at,
            "finished_at": last_build.finished_at,
            "error": last_build.error,
            "result": last_build.result or {},
        }
    last_cve = get_last_job_by_type(conn, "cve_sync")
    last_article_ingest = get_last_job_by_type(conn, "ingest_source")
    return {
        "db_schema_version": get_schema_version(conn),
        "counts": counts,
        "status_metrics": {
            "articles_with_content_error_count": int(
                pipeline_metrics.get("articles_with_content_error_count") or 0
            ),
            "articles_404_count": int(pipeline_metrics.get("articles_404_count") or 0),
            "articles_stale_count": int(pipeline_metrics.get("articles_stale_count") or 0),
            "articles_max_retries_count": int(
                pipeline_metrics.get("articles_max_retries_count") or 0
            ),
            "articles_pending_publish": int(pipeline_metrics.get("articles_pending_publish") or 0),
            "cves_missing_description_count": int(
                pipeline_metrics.get("cves_missing_description_count") or 0
            ),
            "llm_configured": llm_active > 0,
            "llm_stage_active": llm_active,
            "llm_stage_total": llm_total,
        },
        "llm_parse_metrics": _build_llm_parse_metrics(conn, "cve_enrich_products"),
        "last_jobs": last_jobs,
        "last_build_job": last_build_job,
        "last_llm_runs": list_llm_runs(conn, limit=10),
        "last_cve_sync": {
            "job_id": last_cve.id,
            "status": last_cve.status,
            "finished_at": last_cve.finished_at,
            "result": last_cve.result or {},
        }
        if last_cve
        else None,
        "last_article_ingest": {
            "job_id": last_article_ingest.id,
            "status": last_article_ingest.status,
            "finished_at": last_article_ingest.finished_at,
            "result": last_article_ingest.result or {},
        }
        if last_article_ingest
        else None,
    }


def _build_llm_parse_metrics(conn, stage: str) -> list[dict[str, object]]:
    prefix = f"metrics.llm_parse.{stage}."
    data = list_settings_with_prefix(conn, prefix, limit=5000)
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    for key, value in data.items():
        if not isinstance(key, str):
            continue
        suffix = key[len(prefix):] if key.startswith(prefix) else key
        parts = suffix.split(".")
        # profile.<id>.model.<id>.<metric>
        if len(parts) < 5 or parts[0] != "profile" or parts[2] != "model":
            continue
        profile_id = parts[1] or "unknown"
        model_id = ".".join(parts[3:-1]) or "unknown"
        metric = parts[-1]
        try:
            count = int(value)
        except (TypeError, ValueError):
            continue
        bucket = grouped.setdefault(
            (profile_id, model_id),
            {"profile_id": profile_id, "model_id": model_id},
        )
        bucket[metric] = count
    rows = list(grouped.values())
    for row in rows:
        total = int(row.get("total") or 0)
        invalid = int(row.get("invalid_json") or 0)
        row["invalid_json_rate"] = (invalid / total) if total > 0 else 0.0
    rows.sort(key=lambda r: int(r.get("total") or 0), reverse=True)
    return rows


@app.get("/admin/api/diagnostics/queue", dependencies=[Depends(_require_admin_token)])
def queue_diagnostics() -> dict[str, object]:
    conn = _get_conn()
    now = datetime.now(tz=timezone.utc)
    items = []
    for row in list_queued_job_stats(conn):
        oldest_at = row.get("oldest_requested_at")
        age_minutes = None
        if isinstance(oldest_at, str):
            try:
                age_minutes = int((now - _parse_iso(oldest_at)).total_seconds() // 60)
            except Exception:  # noqa: BLE001
                age_minutes = None
        items.append(
            {
                "job_type": row.get("job_type"),
                "queued": row.get("queued"),
                "oldest_requested_at": oldest_at,
                "oldest_age_minutes": age_minutes,
            }
        )
    return {
        "now": now.isoformat(),
        "queue": items,
        "queue_stats": get_queue_stats(conn),
        "job_metrics": get_job_metrics(conn),
        "runner_stats": get_runner_stats(conn),
        "runner_health": get_runner_health_stats(conn),
        "queue_worker_health": get_queue_worker_health(conn),
        "stale_jobs": get_stale_job_stats(conn),
        "source_ingest_state": get_source_ingest_state_counts(conn),
        "build_state": get_build_state(conn),
    }


@app.post("/admin/api/debug/smoke", dependencies=[Depends(_require_admin_token)])
def debug_smoke(payload: SmokeRequest) -> dict[str, object]:
    conn = _get_conn()
    job_id = enqueue_job(
        conn,
        "smoke_test",
        {
            "sources_limit": int(payload.sources_limit),
            "per_source_limit": int(payload.per_source_limit),
        },
        debounce=True,
    )
    return {"job_id": job_id}


@app.post("/admin/api/debug/products-smoke", dependencies=[Depends(_require_admin_token)])
def debug_products_smoke(payload: ProductsSmokeRequest) -> dict[str, object]:
    conn = _get_conn()
    limit = max(1, int(payload.limit))
    timeout_seconds = max(10, int(payload.timeout_seconds))
    result: dict[str, object] = {
        "limit": limit,
        "timeout_seconds": timeout_seconds,
        "steps": [],
    }
    status = "ok"

    def add_step(step: str, status: str, **extra) -> None:
        entry = {"step": step, "status": status}
        if extra:
            entry.update(extra)
        result["steps"].append(entry)

    def matches_worker(value: str | None, candidates: list[str]) -> bool:
        if not value:
            return False
        lowered = value.lower()
        return any(candidate in lowered for candidate in candidates)

    start_marker = utc_now_iso()
    missing_ids = list_article_ids_missing_products(conn, limit=limit)
    add_step("scan_missing_products", "ok", missing_count=len(missing_ids))

    backfill_job_id = enqueue_job(conn, "article_products_backfill", {"limit": limit})
    add_step("enqueue_backfill", "ok", job_id=backfill_job_id)
    backfill_job = _wait_for_job(conn, backfill_job_id, timeout_seconds)
    if not backfill_job:
        add_step("backfill_claim", "timeout")
        result["status"] = "timeout"
        return result
    add_step(
        "backfill_claim",
        "ok",
        status=backfill_job.status,
        locked_by=backfill_job.locked_by,
    )
    if not matches_worker(backfill_job.locked_by, ["worker-fetch", "worker_fetch"]):
        add_step(
            "backfill_worker_check",
            "warning",
            expected="worker_fetch",
            locked_by=backfill_job.locked_by,
        )
        status = "warning"
    if backfill_job.status in {"queued", "running"}:
        backfill_job = _wait_for_job(conn, backfill_job_id, timeout_seconds)
    if backfill_job:
        add_step(
            "backfill_complete",
            "ok" if backfill_job.status in {"succeeded", "failed", "canceled"} else "timeout",
            status=backfill_job.status,
            result=backfill_job.result or {},
        )

    if missing_ids and not has_pending_article_job(
        conn, "article_enrich_products", int(missing_ids[0])
    ):
        direct_job_id = enqueue_job(
            conn,
            "article_enrich_products",
            {"article_id": int(missing_ids[0])},
        )
        add_step("enqueue_enrich_direct", "ok", job_id=direct_job_id, article_id=missing_ids[0])

    enrich_jobs = list_jobs_by_types_since(
        conn,
        types=["article_enrich_products"],
        since=start_marker,
    )
    if not enrich_jobs:
        add_step("enrich_claim", "skipped", reason="no_jobs_enqueued")
        result["status"] = "skipped"
        return result

    target_job = enrich_jobs[0]
    claimed = _wait_for_job(conn, target_job.id, timeout_seconds)
    if not claimed:
        add_step("enrich_claim", "timeout", job_id=target_job.id)
        result["status"] = "timeout"
        return result
    add_step(
        "enrich_claim",
        "ok",
        job_id=claimed.id,
        status=claimed.status,
        locked_by=claimed.locked_by,
    )
    if not matches_worker(
        claimed.locked_by, ["worker-llm", "worker_llm", "worker-openai", "worker_openai"]
    ):
        add_step(
            "enrich_worker_check",
            "warning",
            expected="worker_llm",
            locked_by=claimed.locked_by,
        )
        status = "warning"
    if claimed.status in {"queued", "running"}:
        claimed = _wait_for_job(conn, claimed.id, timeout_seconds)
    if claimed:
        add_step(
            "enrich_complete",
            "ok" if claimed.status in {"succeeded", "failed", "canceled"} else "timeout",
            status=claimed.status,
            result=claimed.result or {},
        )
    result["status"] = status
    return result


@app.get("/jobs")
def jobs(
    page: int = 1,
    page_size: int = 20,
    status: str | None = None,
    job_type: str | None = None,
) -> dict[str, object]:
    conn = _get_conn()
    items, total = list_jobs_filtered(
        conn, status=status, job_type=job_type, page=page, page_size=page_size
    )
    rows = []
    for job in items:
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
    return {
        "items": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "status": status,
        "job_type": job_type,
    }


class SourceRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    kind: str | None = None
    url: str | None = None
    enabled: bool | None = None
    interval_minutes: int | None = None
    tags: list[str] | str | None = None
    overrides: dict[str, object] | str | None = None


class SourceOverrideTestRequest(BaseModel):
    url: str


class ProviderRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    type: str | None = None
    base_url: str | None = None
    is_enabled: bool | None = None
    timeout_s: int | None = None
    retries: int | None = None


class ProviderSecretRequest(BaseModel):
    api_key: str


class ModelRequest(BaseModel):
    id: str | None = None
    provider_id: str | None = None
    model_name: str | None = None
    max_context: int | None = None
    default_params: dict[str, object] | None = None
    tags: list[str] | str | None = None
    is_enabled: bool | None = None


class PromptRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    version: str | None = None
    system_template: str | None = None
    user_template: str | None = None
    notes: str | None = None


class SchemaRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    version: str | None = None
    json_schema: dict[str, object] | None = None


class ProfileRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    primary_provider_id: str | None = None
    primary_model_id: str | None = None
    prompt_id: str | None = None
    schema_id: str | None = None
    params: dict[str, object] | None = None
    fallback: list[dict[str, object]] | None = None
    is_enabled: bool | None = None


class PipelineStageRequest(BaseModel):
    stage_name: str
    profile_id: str


class ProfileTestRequest(BaseModel):
    text: str


class DailyBriefRequest(BaseModel):
    date: str | None = None
    profile_id: str | None = None


class AiTestRequest(BaseModel):
    provider_id: str
    model_id: str
    prompt: str


class AnalyticsRequest(BaseModel):
    days: int = 30


@app.get("/sources")
def sources_list() -> list[dict[str, object]]:
    conn = _get_conn()
    sources = list_sources(conn)
    since = utc_now_iso_offset(seconds=-24 * 3600)
    for item in sources:
        item["articles_24h"] = count_articles_since(conn, item["id"], since)
        item["total_articles"] = count_articles_total(conn, item["id"])
    return sources


@app.get("/sources/health")
def sources_health() -> list[dict[str, object]]:
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
    return rows


@app.post("/sources")
def sources_create(
    payload: SourceRequest, _: None = Depends(_require_admin_token)
) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_source(conn, payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/sources/{source_id}")
def sources_read(source_id: str) -> dict[str, object]:
    conn = _get_conn()
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
    conn = _get_conn()
    try:
        return update_source(conn, source_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/sources/{source_id}")
def sources_delete(source_id: str, _: None = Depends(_require_admin_token)) -> dict[str, str]:
    conn = _get_conn()
    delete_source(conn, source_id)
    return {"status": "deleted"}


@app.post("/sources/{source_id}/test")
def sources_test(
    source_id: str, _: None = Depends(_require_admin_token)
) -> dict[str, object]:
    conn = _get_conn()
    try:
        config = load_runtime_config(conn)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    overrides = normalize_source_overrides(
        source.get("overrides"),
        logger=logging.getLogger("sempervigil.admin"),
        source_id=str(source.get("id") or ""),
        source_name=str(source.get("name") or ""),
    )
    discovery_cfg = overrides.get("discovery", {}) if isinstance(overrides, dict) else {}
    content_cfg = overrides.get("content", {}) if isinstance(overrides, dict) else {}
    fetcher, fetch_timeout_seconds, fetch_headers = get_http_fetch_settings(
        overrides, config.ingest.http.timeout_seconds
    )
    fetch_compressed = get_http_fetch_compressed(overrides)
    fetch_range_chunks = get_http_fetch_range_chunks(overrides)
    fetch_http_version = get_http_fetch_version(overrides)
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
    notes = result.notes or []
    used_tactic = None
    for note in notes:
        if note.get("status") == "ok":
            used_tactic = note.get("tactic_type")
            break
    tactics = list_tactics(conn, source_id)
    tactic_map = {t.tactic_type: t for t in tactics}
    feed_url = None
    if used_tactic and used_tactic in tactic_map:
        feed_url = tactic_map[used_tactic].config.get("feed_url")
    if source.get("kind") == "rss" and source.get("base_url"):
        if used_tactic != "rss" or not feed_url:
            feed_url = source.get("base_url")

    rss_warning = None
    rss_probe = None
    raw_mode = None
    raw_overrides = source.get("overrides")
    if isinstance(raw_overrides, dict):
        raw_disc = raw_overrides.get("discovery")
        if isinstance(raw_disc, dict):
            raw_mode = raw_disc.get("mode")
    elif isinstance(raw_overrides, str):
        try:
            parsed_raw = json.loads(raw_overrides)
        except json.JSONDecodeError:
            parsed_raw = None
        if isinstance(parsed_raw, dict):
            raw_disc = parsed_raw.get("discovery")
            if isinstance(raw_disc, dict):
                raw_mode = raw_disc.get("mode")
    if feed_url and (source.get("kind") == "rss" or raw_mode == "rss_only"):
        try:
            request_headers = {"User-Agent": config.ingest.http.user_agent}
            request_headers.update(fetch_headers)
            (
                status_code,
                final_url,
                headers_dict,
                prefix_bytes,
                fetcher_used,
            ) = fetch_prefix(
                feed_url,
                headers=request_headers,
                timeout_seconds=fetch_timeout_seconds,
                max_bytes=8192,
                fetcher=fetcher,
                compressed=fetch_compressed,
                range_chunks=fetch_range_chunks,
                http_version=fetch_http_version,
            )
            prefix_bytes = prefix_bytes.lstrip()
            prefix_text = prefix_bytes.decode("utf-8", errors="ignore")
            prefix_lc = prefix_text.lower()
            looks_like_rss = ("<rss" in prefix_lc) or ("<feed" in prefix_lc) or ("<rdf" in prefix_lc)
            looks_like_html = ("<!doctype html" in prefix_lc[:512]) or ("<html" in prefix_lc[:1024])
            if looks_like_rss:
                looks_like_html = False
            rss_probe = {
                "content_type": headers_dict.get("content-type", ""),
                "looks_like_rss": looks_like_rss,
                "looks_like_html": looks_like_html,
                "sniff_prefix": prefix_text[:240],
                "status_code": status_code,
                "final_url": final_url,
                "prefix_len": len(prefix_bytes),
                "fetcher_used": fetcher_used,
                "error": None,
            }
            if looks_like_html and not looks_like_rss:
                if raw_mode == "rss_only":
                    rss_warning = (
                        "rss_only enabled: feed URL appears to be HTML (not RSS). "
                        "No fallback performed."
                    )
                else:
                    rss_warning = "RSS source URL appears to be HTML (not RSS)."
        except Exception as exc:  # noqa: BLE001
            rss_warning = f"rss_only probe failed: {exc}"
            rss_probe = {
                "content_type": "",
                "looks_like_rss": False,
                "looks_like_html": False,
                "sniff_prefix": "",
                "status_code": None,
                "final_url": None,
                "prefix_len": 0,
                "fetcher_used": fetcher,
                "error": str(exc),
            }

    reason_counts: dict[str, int] = {}
    for decision in result.decisions:
        if decision.decision == "ACCEPT":
            continue
        for reason in decision.reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    top_reasons = sorted(reason_counts.items(), key=lambda item: item[1], reverse=True)[:8]
    reject_reasons = [{"reason": reason, "count": count} for reason, count in top_reasons]

    accepted_urls = [
        decision.normalized_url or decision.original_url
        for decision in result.decisions
        if decision.decision == "ACCEPT" and (decision.normalized_url or decision.original_url)
    ]
    samples = []
    min_chars = int(content_cfg.get("min_chars") or 800)
    for url in accepted_urls[:3]:
        sample = {"url": url}
        try:
            extracted = fetch_article_content(
                url,
                timeout_seconds=config.ingest.http.timeout_seconds,
                user_agent=config.ingest.http.user_agent,
                logger=logging.getLogger("sempervigil.admin"),
                source_id=str(source.get("id") or ""),
                source_name=str(source.get("name") or ""),
                overrides=source.get("overrides"),
            )
            content_text = str(extracted.get("content_text") or "")
            html = str(extracted.get("content_html") or "")
            title = ""
            if html:
                try:
                    soup = BeautifulSoup(html, "html.parser")
                    title = (soup.title.string or "").strip() if soup.title else ""
                except Exception:
                    title = ""
            sample.update(
                {
                    "title": title,
                    "method": extracted.get("method") or "default",
                    "char_count": len(content_text),
                    "min_chars": min_chars,
                    "passed_min_chars": len(content_text) >= min_chars,
                }
            )
        except Exception as exc:  # noqa: BLE001
            sample.update({"error": str(exc)})
        samples.append(sample)
    return {
        "status": result.status,
        "http_status": result.http_status,
        "error": result.error,
        "found_count": result.found_count,
        "accepted_count": result.accepted_count,
        "discovery": {
            "mode": discovery_cfg.get("mode") or "default",
            "used_tactic": used_tactic,
            "tactics": notes,
            "feed_url": feed_url,
            "rss_probe": rss_probe,
            "warning": rss_warning,
        },
        "reject_reasons": reject_reasons,
        "extraction_samples": samples,
        "items": preview,
    }


@app.post(
    "/admin/api/sources/{source_id}/test_override",
    dependencies=[Depends(_require_admin_token)],
)
def sources_test_override(source_id: str, payload: SourceOverrideTestRequest) -> dict[str, object]:
    conn = _get_conn()
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    url = str(payload.url or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="url_required")
    try:
        config = load_runtime_config(conn)
    except ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    result = fetch_article_content(
        url,
        timeout_seconds=config.ingest.http.timeout_seconds,
        user_agent=config.ingest.http.user_agent,
        logger=logging.getLogger("sempervigil.admin"),
        source_id=str(source.get("id") or ""),
        source_name=str(source.get("name") or ""),
        overrides=source.get("overrides"),
    )
    content_text = str(result.get("content_text") or "")
    return {
        "method": result.get("method") or "default",
        "char_count": len(content_text),
        "preview_first_400": content_text[:400],
    }


@app.post("/admin/api/sources/{source_id}/acquire", dependencies=[Depends(_require_admin_token)])
def sources_acquire(
    source_id: str, payload: SourceAcquireRequest | None = None
) -> dict[str, object]:
    conn = _get_conn()
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    job_payload: dict[str, object] = {"source_id": source_id}
    if payload and payload.limit is not None:
        job_payload["limit"] = int(payload.limit)
    job_id = enqueue_job(conn, "source_acquire", job_payload)
    return {"job_id": job_id}


@app.post(
    "/admin/api/sources/{source_id}/summarize_missing",
    dependencies=[Depends(_require_admin_token)],
)
def sources_summarize_missing(source_id: str) -> dict[str, object]:
    conn = _get_conn()
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    profile, reason = get_active_profile_for_stage(conn, "summarize_article")
    if not profile:
        return {"status": "disabled", "message": f"Summarization disabled: {reason}"}
    article_ids = list_article_ids_ready_for_summary(conn, source_id)
    queued = 0
    skipped = 0
    for article_id in article_ids:
        existing = get_pending_article_job_id(conn, "summarize_article_llm", int(article_id))
        if existing:
            skipped += 1
            continue
        payload = {"article_id": int(article_id), "source_id": source_id}
        payload["profile_id"] = profile.get("id")
        enqueue_job(conn, "summarize_article_llm", payload, dedupe=True)
        queued += 1
    return {"status": "queued", "queued": queued, "skipped": skipped}


@app.post(
    "/admin/api/sources/{source_id}/fetch_missing",
    dependencies=[Depends(_require_admin_token)],
)
def sources_fetch_missing(source_id: str) -> dict[str, object]:
    conn = _get_conn()
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    article_ids = list_article_ids_missing_content(conn, source_id)
    queued = 0
    skipped = 0
    for article_id in article_ids:
        existing = get_pending_article_job_id(conn, "fetch_article_content", int(article_id))
        if existing:
            skipped += 1
            continue
        article = get_article_by_id(conn, int(article_id))
        if not article:
            skipped += 1
            continue
        url = article.get("original_url") or article.get("normalized_url")
        if not url:
            skipped += 1
            continue
        payload = {"article_id": int(article_id), "source_id": source_id}
        payload["original_url"] = url
        enqueue_job(conn, "fetch_article_content", payload, dedupe=True)
        queued += 1
    return {"status": "queued", "queued": queued, "skipped": skipped}


@app.post(
    "/admin/api/sources/{source_id}/resume",
    dependencies=[Depends(_require_admin_token)],
)
def sources_resume(source_id: str) -> dict[str, object]:
    conn = _get_conn()
    source = get_source(conn, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="source_not_found")
    conn.execute(
        """
        UPDATE sources
        SET enabled = 1, pause_until = NULL, paused_reason = NULL, updated_at = %s
        WHERE id = %s
        """,
        (utc_now_iso(), source_id),
    )
    conn.commit()
    return {"status": "ok", "source_id": source_id}


@app.get("/sources/{source_id}/health")
def sources_health_history(source_id: str, limit: int = 50) -> list[dict[str, object]]:
    conn = _get_conn()
    return list_source_health_events(conn, source_id, limit=limit)


@app.get("/admin/analytics/articles_per_day", dependencies=[Depends(_require_admin_token)])
def analytics_articles_per_day(days: int = 30) -> dict[str, object]:
    conn = _get_conn()
    try:
        since_day = (datetime.now(tz=timezone.utc) - timedelta(days=days)).date().isoformat()
        return {"days": days, "data": list_articles_per_day(conn, since_day)}
    except Exception as exc:  # noqa: BLE001
        return {"days": days, "data": [], "error": str(exc)}


@app.get("/admin/analytics/source_stats", dependencies=[Depends(_require_admin_token)])
def analytics_source_stats(days: int = 7, runs: int = 20) -> dict[str, object]:
    conn = _get_conn()
    try:
        return {"days": days, "runs": runs, "data": get_source_stats(conn, days, runs)}
    except Exception as exc:  # noqa: BLE001
        return {"days": days, "runs": runs, "data": [], "error": str(exc)}


@app.get("/admin/api/cves", dependencies=[Depends(_require_admin_token)])
def api_cves(
    query: str | None = None,
    severity: str | None = None,
    min_cvss: float | None = None,
    missing_description: bool | None = None,
    missing_products: bool | None = None,
    kev: bool | None = None,
    after: str | None = None,
    before: str | None = None,
    vendor: str | None = None,
    product: str | None = None,
    in_scope: bool | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    watchlist_enabled = _watchlist_enabled(conn)
    settings = get_cve_settings(conn)
    severities = [item.strip().upper() for item in severity.split(",")] if severity else None
    vendor_keywords = [item.strip() for item in vendor.split(",")] if vendor else None
    product_keywords = [item.strip() for item in product.split(",")] if product else None
    items, total = search_cves(
        conn,
        query=query,
        severities=severities,
        min_cvss=min_cvss,
        missing_description=missing_description,
        missing_products=missing_products,
        kev=kev,
        after=after,
        before=before,
        vendor_keywords=vendor_keywords,
        product_keywords=product_keywords,
        in_scope=in_scope if watchlist_enabled else None,
        settings=settings,
        page=page,
        page_size=page_size,
    )
    for item in items:
        if not watchlist_enabled:
            item["in_scope"] = None
            item["scope_reasons"] = []
            continue
        if item.get("in_scope") is None:
            signals = CveSignals(
                vendors=[],
                vendor_norms=[],
                products=item.get("affected_products") or [],
                product_norms=[],
                cpes=item.get("affected_cpes") or [],
                reference_domains=item.get("reference_domains") or [],
                product_versions=item.get("product_versions") or [],
            )
            item["in_scope"] = matches_filters(
                preferred_score=item.get("preferred_base_score"),
                preferred_severity=item.get("preferred_base_severity"),
                description=item.get("summary"),
                signals=signals,
                filters=(settings.get("filters") or {}),
            )
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.get("/admin/api/cves/{cve_id}", dependencies=[Depends(_require_admin_token)])
def api_cve_detail(cve_id: str) -> dict[str, object]:
    conn = _get_conn()
    cve = get_cve(conn, cve_id)
    if not cve:
        raise HTTPException(status_code=404, detail="cve_not_found")
    cve["last_seen_at"] = get_cve_last_seen(conn, cve_id)
    if not _watchlist_enabled(conn):
        cve["in_scope"] = None
        cve["scope_reasons"] = []
    cve["watchlist_enabled"] = _watchlist_enabled(conn)
    return cve


@app.get("/admin/api/threats", dependencies=[Depends(_require_admin_token)])
def api_threats(
    query: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    items, total = list_threat_actors(conn, query=query, page=page, page_size=page_size)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.get("/admin/api/threats/{actor_key}", dependencies=[Depends(_require_admin_token)])
def api_threat_detail(actor_key: str) -> dict[str, object]:
    conn = _get_conn()
    detail = get_threat_actor_detail(conn, actor_key)
    if not detail:
        raise HTTPException(status_code=404, detail="threat_actor_not_found")
    return detail


@app.get("/admin/api/briefs", dependencies=[Depends(_require_admin_token)])
def api_briefs(
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    items, total = list_daily_briefs(conn, page=page, page_size=page_size)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.get("/admin/api/briefs/{day}", dependencies=[Depends(_require_admin_token)])
def api_brief_detail(day: str) -> dict[str, object]:
    conn = _get_conn()
    detail = get_daily_brief(conn, day)
    if not detail:
        raise HTTPException(status_code=404, detail="daily_brief_not_found")
    detail["pending_job"] = _get_pending_brief_job(conn, day)
    return detail


@app.get("/admin/api/briefs/{day}/status", dependencies=[Depends(_require_admin_token)])
def api_brief_status(day: str) -> dict[str, object]:
    conn = _get_conn()
    pending = _get_pending_brief_job(conn, day)
    return {"pending": bool(pending), "job": pending}


@app.get("/admin/api/events", dependencies=[Depends(_require_admin_token)])
def api_events(
    query: str | None = None,
    severity: str | None = None,
    kind: str | None = None,
    status: str | None = None,
    candidate: str | None = None,
    article_bucket: str | None = None,
    after: str | None = None,
    before: str | None = None,
    include_legacy: bool = False,
    include_suppressed: bool = False,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    items, total = list_events_with_counts(
        conn,
        status=status,
        candidate=candidate,
        article_bucket=article_bucket,
        kind=kind,
        severity=severity,
        query=query,
        after=after,
        before=before,
        include_legacy=include_legacy,
        include_suppressed=include_suppressed,
        page=page,
        page_size=page_size,
    )
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.get("/admin/api/events/{event_id}", dependencies=[Depends(_require_admin_token)])
def api_event_detail(event_id: str) -> dict[str, object]:
    conn = _get_conn()
    event = get_event(conn, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event_not_found")
    return event


class EventsRebuildRequest(BaseModel):
    limit: int | None = None


@app.post("/admin/api/events/rebuild", dependencies=[Depends(_require_admin_token)])
def api_events_rebuild(payload: EventsRebuildRequest | None = None) -> dict[str, object]:
    conn = _get_conn()
    limit = payload.limit if payload else None
    job_id = enqueue_job(
        conn,
        "events_rebuild",
        {"limit": limit} if limit is not None else None,
        debounce=True,
    )
    return {"status": "queued", "job_id": job_id}


class EventCreateRequest(BaseModel):
    title: str
    kind: str = "other"
    status: str = "confirmed"
    occurred_at: str | None = None
    summary: str | None = None
    event_key: str | None = None
    confidence: float | None = None
    manual: bool = True
    visibility: str = "active"
    confidence_tier: str = "watch"
    reasons: list[str] | None = None
    candidate: bool = False
    lifecycle: str | None = None
    entity: str | None = None
    incident_date: str | None = None
    evidence: list[str] | None = None
    tags: list[str] | None = None
    is_event: bool | None = None
    run_web_enrich: bool = False
    web_query: str | None = None
    web_max_results: int | None = None
    web_promote_on_enrich: bool = False

class EventUpdateRequest(BaseModel):
    title: str | None = None
    summary: str | None = None
    severity: str | None = None
    status: str | None = None
    kind: str | None = None
    visibility: str | None = None
    confidence: float | None = None
    confidence_tier: str | None = None
    candidate: bool | None = None
    lifecycle: str | None = None
    entity: str | None = None
    incident_date: str | None = None
    tags: list[str] | None = None
    is_event: bool | None = None
    publish_state: str | None = None
    published_at: str | None = None
    site_slug: str | None = None



@app.post("/admin/api/events", dependencies=[Depends(_require_admin_token)])
def api_event_create(payload: EventCreateRequest) -> dict[str, object]:
    conn = _get_conn()
    now = utc_now_iso()
    event_key = payload.event_key
    if not event_key:
        bucket = (payload.occurred_at or now)[:10]
        event_key = f"evt:{normalize_name(payload.title)}:{bucket}"
    lifecycle = (payload.lifecycle or "").strip() or ("candidate" if payload.candidate else payload.status)
    event_id, _ = upsert_event_by_key(
        conn,
        event_key=event_key,
        kind=payload.kind,
        title=payload.title,
        severity="UNKNOWN",
        first_seen_at=payload.occurred_at or now,
        last_seen_at=now,
        status=payload.status,
        summary=payload.summary,
        confidence=payload.confidence,
        manual=payload.manual,
        visibility=payload.visibility,
        confidence_tier=payload.confidence_tier,
        reasons=payload.reasons,
        candidate=payload.candidate,
        lifecycle=lifecycle,
        entity=payload.entity,
        incident_date=payload.incident_date,
        evidence=payload.evidence,
    )
    if payload.tags is not None or payload.is_event is not None:
        update_event(conn, event_id, tags=payload.tags, is_event=payload.is_event)
    enrich_job_id = None
    if payload.run_web_enrich:
        data: dict[str, object] = {"event_id": event_id}
        if payload.web_query:
            data["query"] = payload.web_query
        if payload.web_max_results:
            data["max_results"] = int(payload.web_max_results)
        if payload.web_promote_on_enrich:
            data["promote_on_enrich"] = True
        enrich_job_id = enqueue_job(conn, "enrich_event_from_web", data, debounce=True)
    else:
        try:
            event_cfg = get_events_settings(conn)
        except Exception:
            event_cfg = {}
        min_articles = int(event_cfg.get("enrich_min_articles", 0) or 0) if isinstance(event_cfg, dict) else 0
        if min_articles > 0:
            event = get_event(conn, event_id)
            article_count = len(((event or {}).get("items") or {}).get("articles") or [])
            if article_count < min_articles:
                max_results = int(event_cfg.get("enrich_min_articles_max_results", 12) or 12)
                enrich_job_id = enqueue_job(
                    conn,
                    "enrich_event_from_web",
                    {"event_id": event_id, "max_results": max_results, "replace_existing": False},
                    debounce=True,
                    dedupe=True,
                )
    event = get_event(conn, event_id)
    response = event or {"id": event_id}
    if enrich_job_id:
        response = dict(response)
        response["enrich_job_id"] = enrich_job_id
    return response


@app.put("/admin/api/events/{event_id}", dependencies=[Depends(_require_admin_token)])
def api_event_update(event_id: str, payload: EventUpdateRequest) -> dict[str, object]:
    conn = _get_conn()
    data = payload.model_dump(exclude_none=True)
    if not data:
        return get_event(conn, event_id) or {"id": event_id}
    update_event(conn, event_id, **data)
    event = get_event(conn, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event_not_found")
    return event


@app.delete("/admin/api/events/{event_id}", dependencies=[Depends(_require_admin_token)])
def api_event_delete(event_id: str) -> dict[str, object]:
    conn = _get_conn()
    ok = delete_event(conn, event_id)
    if not ok:
        raise HTTPException(status_code=404, detail="event_not_found")
    return {"status": "deleted", "event_id": event_id}


class EventAttachArticleRequest(BaseModel):
    article_id: int
    added_by: str | None = None


@app.post(
    "/admin/api/events/{event_id}/articles",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_attach_article(
    event_id: str, payload: EventAttachArticleRequest
) -> dict[str, object]:
    conn = _get_conn()
    article = get_article_by_id(conn, payload.article_id)
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    link_event_article(conn, event_id, payload.article_id, payload.added_by or "manual")
    update_event_summary_from_articles(conn, event_id)
    enqueue_job(conn, "event_report_llm", {"event_id": event_id}, dedupe=True)
    event = get_event(conn, event_id)
    return event or {"id": event_id}


@app.post(
    "/admin/api/events/{event_id}/summary",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_summary_rebuild(event_id: str) -> dict[str, object]:
    conn = _get_conn()
    summary = update_event_summary_from_articles(conn, event_id)
    enqueue_job(conn, "event_report_llm", {"event_id": event_id}, dedupe=True)
    event = get_event(conn, event_id)
    narrative = event.get("narrative") if isinstance(event, dict) else {}
    bullets = narrative.get("bullets") if isinstance(narrative, dict) else []
    timeline = event.get("timeline") if isinstance(event, dict) else []
    return {
        "event_id": event_id,
        "summary": summary,
        "narrative_bullet_count": len(bullets) if isinstance(bullets, list) else 0,
        "timeline_count": len(timeline) if isinstance(timeline, list) else 0,
    }


@app.post(
    "/admin/api/events/{event_id}/report",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_report_rebuild(event_id: str) -> dict[str, object]:
    conn = _get_conn()
    event = get_event(conn, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event_not_found")
    job_id = enqueue_job(conn, "event_report_llm", {"event_id": event_id}, dedupe=True)
    return {"event_id": event_id, "job_id": job_id}

@app.post(
    "/admin/api/events/{event_id}/articles/detach",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_detach_article(event_id: str, payload: EventAttachArticleRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        conn.execute(
            "DELETE FROM event_articles WHERE event_id = %s AND article_id = %s",
            (event_id, payload.article_id),
        )
        conn.commit()
        rebuild_event_timeline_from_articles(conn, event_id)
        update_event_summary_from_articles(conn, event_id)
    except Exception:
        pass
    event = get_event(conn, event_id)
    return event or {"id": event_id}


class EventPublishRequest(BaseModel):
    publish: bool = True
    force: bool = False
    site_slug: str | None = None


@app.post(
    "/admin/api/events/{event_id}/publish",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_publish(event_id: str, payload: EventPublishRequest | None = None) -> dict[str, object]:
    conn = _get_conn()
    event = get_event(conn, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event_not_found")
    data = payload or EventPublishRequest()
    if data.publish:
        rebuild_event_timeline_from_articles(conn, event_id)
        readiness = event_publish_readiness(conn, event_id)
        if not readiness.get("ready") and not data.force:
            return {
                "status": "blocked",
                "event_id": event_id,
                "readiness": readiness,
            }
        slug_value = (data.site_slug or "").strip()
        if not slug_value:
            slug_value = event.get("site_slug") or event_id
        ok = set_event_publish_state(
            conn,
            event_id,
            "published",
            site_slug=slug_value,
        )
        if not ok:
            raise HTTPException(status_code=409, detail="publish_state_not_supported")
    else:
        readiness = event_publish_readiness(conn, event_id)
        ok = set_event_publish_state(conn, event_id, "draft", site_slug=data.site_slug)
        if not ok:
            raise HTTPException(status_code=409, detail="publish_state_not_supported")
    enqueue_job(conn, "events_rebuild", None, debounce=True)
    mark_build_dirty(conn, reason="event_publish_state")
    updated = get_event(conn, event_id)
    return {
        "status": "ok",
        "event_id": event_id,
        "publish_state": updated.get("publish_state") if updated else None,
        "readiness": readiness,
    }

@app.post(
    "/admin/api/events/{event_id}/rederive",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_rederive(event_id: str) -> dict[str, object]:
    conn = _get_conn()
    articles = list_event_articles(conn, event_id)
    queued = 0
    for article in articles:
        article_id = article.get("id")
        if article_id is None:
            continue
        enqueue_job(conn, "derive_events_from_articles", {"article_id": int(article_id)})
        queued += 1
    return {"status": "queued", "queued": queued}




class EventsDeriveRequest(BaseModel):
    article_id: int | None = None


@app.post(
    "/admin/api/events/derive",
    dependencies=[Depends(_require_admin_token)],
)
def api_events_derive(payload: EventsDeriveRequest | None = None) -> dict[str, object]:
    conn = _get_conn()
    data = payload.model_dump() if payload else {}
    job_id = enqueue_job(
        conn,
        "derive_events_from_articles",
        data if data else None,
        debounce=False,
    )
    return {"status": "queued", "job_id": job_id}


class EventEnrichWebRequest(BaseModel):
    query: str | None = None
    max_results: int | None = None
    promote_on_enrich: bool = False
    keep_low: bool = False
    replace_existing: bool = True


class EventManualWebSourceRequest(BaseModel):
    url: str
    title: str | None = None
    snippet: str | None = None
    published_at: str | None = None
    score: int | None = None


@app.post(
    "/admin/api/events/{event_id}/enrich/web",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_enrich_web(
    event_id: str, payload: EventEnrichWebRequest | None = None
) -> dict[str, object]:
    if not os.getenv("SV_SEARXNG_URL", "").strip():
        raise HTTPException(
            status_code=400,
            detail="SV_SEARXNG_URL is not set. Configure it in .env before enrichment.",
        )
    conn = _get_conn()
    data = payload.model_dump() if payload else {}
    data["event_id"] = event_id
    job_id = enqueue_job(conn, "enrich_event_from_web", data, debounce=True)
    return {"status": "queued", "job_id": job_id}


@app.post(
    "/admin/api/events/{event_id}/web_sources/manual",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_web_source_manual_add(
    event_id: str, payload: EventManualWebSourceRequest
) -> dict[str, object]:
    conn = _get_conn()
    event = get_event(conn, event_id)
    if not event:
        raise HTTPException(status_code=404, detail="event_not_found")
    url = (payload.url or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="url_required")
    score = int(payload.score if payload.score is not None else 100)
    result = {
        "url": url,
        "title": payload.title,
        "snippet": payload.snippet,
        "published_at": payload.published_at,
        "engine": "manual",
        "category": "manual_add",
        "metadata": {"manual_add": True},
    }
    source_id = upsert_event_web_source(
        conn,
        event_id,
        result,
        score,
        {"manual_add": score},
    )
    if not source_id:
        raise HTTPException(status_code=400, detail="manual_source_rejected")
    job_id = enqueue_job(
        conn,
        "validate_event_web_source",
        {"event_id": event_id, "source_id": source_id},
        debounce=False,
        dedupe=True,
    )
    return {"status": "queued", "source_id": source_id, "job_id": job_id}


@app.get(
    "/admin/api/events/{event_id}/web_sources",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_web_sources(event_id: str, include_discarded: bool = False) -> dict[str, object]:
    conn = _get_conn()
    sources = list_event_web_sources(conn, event_id, include_discarded=include_discarded)
    return {"items": sources}


@app.post(
    "/admin/api/events/{event_id}/web_sources/{source_id}/promote",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_web_source_promote(event_id: str, source_id: str) -> dict[str, object]:
    conn = _get_conn()
    job_id = enqueue_job(
        conn,
        "promote_event_web_source_to_article",
        {"source_id": source_id},
        debounce=False,
    )
    return {"status": "queued", "job_id": job_id}


@app.post(
    "/admin/api/events/{event_id}/web_sources/{source_id}/discard",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_web_source_discard(event_id: str, source_id: str) -> dict[str, object]:
    conn = _get_conn()
    mark_event_web_source_status(conn, source_id, "discarded")
    return {"status": "ok", "source_id": source_id}


@app.post(
    "/admin/api/events/{event_id}/enrich/llm",
    dependencies=[Depends(_require_admin_token)],
)
def api_event_enrich_llm(event_id: str) -> dict[str, object]:
    conn = _get_conn()
    job_id = enqueue_job(
        conn,
        "enrich_event_summary_llm",
        {"event_id": event_id},
        debounce=True,
    )
    return {"status": "queued", "job_id": job_id}


class EventsPurgeRequest(BaseModel):
    dry_run: bool = True
    mode: str = "suppress"
    older_than_days: int | None = None
    kinds: list[str] | None = None
    require_no_victims: bool = False
    require_no_cves: bool = False
    require_no_sources: bool = False
    require_research: bool = False
    confidence_below: float | None = None
    only_empty_cve_clusters: bool = False


@app.post("/admin/api/events/purge", dependencies=[Depends(_require_admin_token)])
def api_events_purge(payload: EventsPurgeRequest | None = None) -> dict[str, object]:
    conn = _get_conn()
    data = payload.model_dump() if payload else {}
    logger = logging.getLogger("sempervigil.events")
    log_event(
        logger,
        logging.INFO,
        "events_purge_start",
        dry_run=bool(data.get("dry_run", True)),
        mode=data.get("mode", "suppress"),
        older_than_days=data.get("older_than_days"),
        kinds=data.get("kinds"),
        require_no_victims=bool(data.get("require_no_victims", False)),
        require_no_cves=bool(data.get("require_no_cves", False)),
        require_no_sources=bool(data.get("require_no_sources", False)),
        require_research=bool(data.get("require_research", False)),
        confidence_below=data.get("confidence_below"),
        only_empty_cve_clusters=bool(data.get("only_empty_cve_clusters", False)),
    )
    stats = purge_weak_events(
        conn,
        dry_run=bool(data.get("dry_run", True)),
        mode=str(data.get("mode") or "suppress"),
        older_than_days=data.get("older_than_days"),
        kinds=data.get("kinds"),
        require_no_victims=bool(data.get("require_no_victims", False)),
        require_no_cves=bool(data.get("require_no_cves", False)),
        require_no_sources=bool(data.get("require_no_sources", False)),
        require_research=bool(data.get("require_research", False)),
        confidence_below=data.get("confidence_below"),
        only_empty_cve_clusters=bool(data.get("only_empty_cve_clusters", False)),
    )
    log_event(
        logger,
        logging.INFO,
        "events_purge_done",
        scanned=stats.get("candidates", 0),
        purged=stats.get("deleted", 0),
        kept=stats.get("kept", 0),
    )
    return {"status": "ok", "stats": stats}


@app.post("/admin/api/events/normalize_cve_keys", dependencies=[Depends(_require_admin_token)])
def api_events_normalize_cve_keys(limit: int = 200) -> dict[str, object]:
    conn = _get_conn()
    stats = normalize_cve_cluster_event_keys(conn, limit=limit)
    return {"status": "ok", "stats": stats}


@app.get("/admin/api/products", dependencies=[Depends(_require_admin_token)])
def api_products(
    query: str | None = None,
    vendor: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    items, total = query_products(conn, query=query, vendor=vendor, page=page, page_size=page_size)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.get("/admin/api/products/{product_key}", dependencies=[Depends(_require_admin_token)])
def api_product_detail(product_key: str) -> dict[str, object]:
    conn = _get_conn()
    product = get_product(conn, product_key)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")
    facets = get_product_facets(conn, product["product_id"])
    facets["article_count"] = count_articles_for_product(conn, product["product_id"])
    return {"product": product, "facets": facets}


@app.get("/admin/api/products/{product_key}/cves", dependencies=[Depends(_require_admin_token)])
def api_product_cves(
    product_key: str,
    severity: str | None = None,
    min_cvss: float | None = None,
    missing_description: bool | None = None,
    missing_products: bool | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    product = get_product(conn, product_key)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")
    severities = [item.strip().upper() for item in severity.split(",")] if severity else None
    items, total = get_product_cves(
        conn,
        product["product_id"],
        severity_min=min_cvss,
        severities=severities,
        page=page,
        page_size=page_size,
    )
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.get("/admin/api/products/{product_key}/events", dependencies=[Depends(_require_admin_token)])
def api_product_events(
    product_key: str,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    items, total = list_events_for_product(conn, product_key, page, page_size)
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.get("/admin/api/products/{product_key}/articles", dependencies=[Depends(_require_admin_token)])
def api_product_articles(
    product_key: str,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    product = get_product(conn, product_key)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")
    items, total = list_articles_for_product(
        conn,
        product["product_id"],
        page=page,
        page_size=page_size,
    )
    return {"items": items, "total": total, "page": page, "page_size": page_size}




@app.post("/admin/api/products/backfill_articles", dependencies=[Depends(_require_admin_token)])
def api_products_backfill_articles(payload: dict[str, object] | None = None) -> dict[str, object]:
    conn = _get_conn()
    limit = None
    if payload and isinstance(payload.get("limit"), int):
        limit = int(payload["limit"])
    job_id = enqueue_job(conn, "article_products_backfill", {"limit": limit} if limit else {})
    return {"status": "ok", "job_id": job_id}
@app.post("/admin/api/products/backfill", dependencies=[Depends(_require_admin_token)])
def api_products_backfill(payload: dict[str, object] | None = None) -> dict[str, object]:
    conn = _get_conn()
    limit = None
    if payload and isinstance(payload.get("limit"), int):
        limit = int(payload["limit"])
    stats = backfill_products_from_cves(conn, limit=limit)
    return {"status": "ok", "stats": stats}


@app.get("/admin/api/content/search", dependencies=[Depends(_require_admin_token)])
def api_content_search(
    query: str | None = None,
    type: str | None = None,
    source_id: str | None = None,
    has_summary: bool | None = None,
    has_context: bool | None = None,
    missing: str | None = None,
    content_state: str | None = None,
    content_error: bool | None = None,
    content_error_kind: str | None = None,
    summary_error: bool | None = None,
    needs: str | None = None,
    watchlist_hit: bool | None = None,
    severity: str | None = None,
    min_cvss: float | None = None,
    missing_description: bool | None = None,
    missing_products: bool | None = None,
    after: str | None = None,
    before: str | None = None,
    tags: str | None = None,
    vendor: str | None = None,
    product: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, object]:
    conn = _get_conn()
    watchlist_enabled = _watchlist_enabled(conn)
    items: list[dict[str, object]] = []
    total = 0
    if type in (None, "all", "articles", "article"):
        tag_list = [item.strip() for item in tags.split(",")] if tags else None
        article_items, article_total = search_articles(
            conn,
            query=query,
            source_id=source_id,
            has_summary=has_summary,
            has_context=has_context,
            missing=missing,
            content_state=content_state,
            content_error=content_error,
            content_error_kind=content_error_kind,
            summary_error=summary_error,
            needs=needs,
            after=after,
            before=before,
            tags=tag_list,
            watchlist_enabled=watchlist_enabled,
            watchlist_hit=watchlist_hit if watchlist_enabled else None,
            page=page,
            page_size=page_size,
        )
        for item in article_items:
            items.append(
                {
                    "type": "article",
                    **item,
                }
            )
        total += article_total
    if type in ("cves", "cve") or (type in (None, "all")):
        settings = get_cve_settings(conn)
        severities = (
            [item.strip().upper() for item in severity.split(",")] if severity else None
        )
        vendor_keywords = [item.strip() for item in vendor.split(",")] if vendor else None
        product_keywords = [item.strip() for item in product.split(",")] if product else None
        md = missing_description if missing_description is not None else (missing == "description")
        mp = missing_products if missing_products is not None else (missing == "products")
        cve_items, cve_total = search_cves(
            conn,
            query=query,
            severities=severities,
            min_cvss=min_cvss,
            missing_description=md,
            missing_products=mp,
            kev=None,
            after=after,
            before=before,
            vendor_keywords=vendor_keywords,
            product_keywords=product_keywords,
            in_scope=None,
            settings=settings,
            page=page,
            page_size=page_size,
        )
        for item in cve_items:
            if not watchlist_enabled:
                item["in_scope"] = None
                item["scope_reasons"] = []
            items.append({"type": "cve", **item})
        total += cve_total
    return {"items": items, "total": total, "page": page, "page_size": page_size}


def _build_write_payload(conn, article: dict[str, object]) -> dict[str, object]:
    summary_text = article.get("summary") or ""
    summary_llm = article.get("summary_llm")
    if summary_llm:
        try:
            parsed = json.loads(summary_llm)
            if isinstance(parsed, dict) and parsed.get("summary"):
                summary_text = parsed.get("summary") or summary_text
        except json.JSONDecodeError:
            summary_text = summary_llm
    return {
        "article_id": article.get("id"),
        "stable_id": article.get("stable_id"),
        "title": article.get("title"),
        "source_id": article.get("source_id"),
        "published_at": article.get("published_at"),
        "published_at_source": article.get("published_at_source"),
        "ingested_at": article.get("ingested_at"),
        "summary": summary_text or None,
        "tags": get_article_tags(conn, int(article.get("id"))),
        "original_url": article.get("original_url"),
        "normalized_url": article.get("normalized_url"),
    }


@app.post("/admin/api/articles/{article_id}/fetch", dependencies=[Depends(_require_admin_token)])
def api_article_fetch(article_id: int) -> dict[str, object]:
    conn = _get_conn()
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    url = article.get("original_url") or article.get("normalized_url")
    if not url:
        raise HTTPException(status_code=400, detail="article_url_missing")
    existing = get_pending_article_job_id(conn, "fetch_article_content", int(article_id))
    if existing:
        return {"status": "already_queued", "job_id": existing}
    payload = {"article_id": int(article_id), "source_id": article.get("source_id")}
    payload["original_url"] = url
    job_id = enqueue_job(conn, "fetch_article_content", payload)
    return {"status": "queued", "job_id": job_id}


@app.post("/admin/api/articles/{article_id}/summarize", dependencies=[Depends(_require_admin_token)])
def api_article_summarize(article_id: int) -> dict[str, object]:
    conn = _get_conn()
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    profile, reason = get_active_profile_for_stage(conn, "summarize_article")
    if not profile:
        raise HTTPException(
            status_code=400,
            detail=f"Summarization disabled: {reason}",
        )
    existing = get_pending_article_job_id(conn, "summarize_article_llm", int(article_id))
    if existing:
        return {"status": "already_queued", "job_id": existing}
    payload = {"article_id": int(article_id), "source_id": article.get("source_id")}
    payload["profile_id"] = profile.get("id")
    job_id = enqueue_job(conn, "summarize_article_llm", payload)
    return {"status": "queued", "job_id": job_id}


@app.post("/admin/api/articles/{article_id}/context_pack", dependencies=[Depends(_require_admin_token)])
def api_article_context_pack(article_id: int) -> dict[str, object]:
    conn = _get_conn()
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    profile, reason = get_active_profile_for_stage(conn, "article_context_pack")
    if not profile:
        raise HTTPException(
            status_code=400,
            detail=f"Context pack disabled: {reason}",
        )
    existing = get_pending_article_job_id(conn, "summarize_article_context_llm", int(article_id))
    if existing:
        return {"status": "already_queued", "job_id": existing}
    payload = {"article_id": int(article_id), "source_id": article.get("source_id")}
    payload["profile_id"] = profile.get("id")
    job_id = enqueue_job(conn, "summarize_article_context_llm", payload)
    return {"status": "queued", "job_id": job_id}


@app.post("/admin/api/articles/{article_id}/publish", dependencies=[Depends(_require_admin_token)])
def api_article_publish(article_id: int) -> dict[str, object]:
    if not is_article_markdown_enabled():
        raise HTTPException(status_code=400, detail="article_markdown_disabled")
    conn = _get_conn()
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    existing = get_pending_article_job_id(conn, "write_article_markdown", int(article_id))
    if existing:
        return {"status": "already_queued", "job_id": existing}
    payload = _build_write_payload(conn, article)
    job_id = enqueue_job(conn, "write_article_markdown", payload)
    return {"status": "queued", "job_id": job_id}


@app.post("/admin/api/articles/{article_id}/pipeline", dependencies=[Depends(_require_admin_token)])
def api_article_pipeline(article_id: int) -> dict[str, object]:
    conn = _get_conn()
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    for job_type in ("fetch_article_content", "summarize_article_llm", "write_article_markdown"):
        existing = get_pending_article_job_id(conn, job_type, int(article_id))
        if existing:
            return {"status": "already_queued", "job_id": existing}
    url = article.get("original_url") or article.get("normalized_url")
    has_content = bool(article.get("has_full_content") or article.get("content_text"))
    has_summary = bool(article.get("summary_llm"))
    job_ids: list[str] = []
    if url and not has_content:
        payload = {"article_id": int(article_id), "source_id": article.get("source_id")}
        payload["original_url"] = url
        job_ids.append(enqueue_job(conn, "fetch_article_content", payload))
    elif not has_summary:
        profile, reason = get_active_profile_for_stage(conn, "summarize_article")
        if not profile:
            raise HTTPException(
                status_code=400,
                detail=f"Summarization disabled: {reason}",
            )
        payload = {"article_id": int(article_id), "source_id": article.get("source_id")}
        payload["profile_id"] = profile.get("id")
        job_ids.append(enqueue_job(conn, "summarize_article_llm", payload))
    else:
        if not is_article_markdown_enabled():
            return {"status": "skipped", "reason": "article_markdown_disabled"}
        job_ids.append(enqueue_job(conn, "write_article_markdown", _build_write_payload(conn, article)))
    return {"status": "queued", "job_ids": job_ids}

@app.post("/admin/api/articles/{article_id}/suppress", dependencies=[Depends(_require_admin_token)])
def api_article_suppress(article_id: int, payload: dict | None = Body(None)) -> dict[str, object]:
    conn = _get_conn()
    payload = payload or {}
    suppressed = payload.get("suppressed")
    reason = payload.get("reason") if isinstance(payload, dict) else None
    if suppressed is None:
        # toggle
        article = get_article_by_id(conn, article_id)
        meta_json = article.get("meta_json") if article else None
        current = False
        if meta_json:
            try:
                meta = json.loads(meta_json)
                if isinstance(meta, dict):
                    current = bool(meta.get("suppressed"))
            except Exception:
                current = False
        suppressed = not current
    result = update_article_suppressed(conn, article_id, bool(suppressed), reason if isinstance(reason, str) else None)
    config = load_runtime_config(conn)
    _refresh_feed_data_files(conn, config, logging.getLogger("admin"))
    mark_build_dirty(conn, reason="article_suppressed")
    return {"status": "ok", **result}

@app.delete("/admin/api/articles/{article_id}", dependencies=[Depends(_require_admin_token)])
def api_article_delete(article_id: int):
    conn = init_db()
    conn.execute("DELETE FROM event_articles WHERE article_id = %s", (article_id,))
    conn.execute("DELETE FROM article_tags WHERE article_id = %s", (article_id,))
    conn.execute("DELETE FROM article_products WHERE article_id = %s", (article_id,))
    conn.execute("DELETE FROM articles WHERE id = %s", (article_id,))
    conn.commit()
    config = load_runtime_config(conn)
    _refresh_feed_data_files(conn, config, logging.getLogger("admin"))
    mark_build_dirty(conn, reason="article_deleted")
    return {"status": "deleted", "article_id": article_id}



@app.post("/admin/api/cves/{cve_id}/refresh", dependencies=[Depends(_require_admin_token)])
def api_cve_refresh(cve_id: str) -> dict[str, object]:
    conn = _get_conn()
    existing = get_pending_cve_job_id(conn, cve_id)
    if existing:
        return {"status": "already_queued", "job_id": existing}
    job_id = enqueue_job(conn, "cve_sync", {"cve_id": cve_id})
    return {"status": "queued", "job_id": job_id}

@app.post("/admin/api/cves/{cve_id}/enrich_products", dependencies=[Depends(_require_admin_token)])
def api_cve_enrich_products(cve_id: str) -> dict[str, object]:
    conn = _get_conn()
    profile, reason = get_active_profile_for_stage(conn, "cve_enrich_products")
    if not profile:
        raise HTTPException(status_code=400, detail=f"CVE enrichment disabled: {reason}")
    existing = get_pending_job_id_for_cve(conn, "cve_enrich_llm", cve_id)
    if existing:
        return {"status": "already_queued", "job_id": existing}
    job_id = enqueue_job(conn, "cve_enrich_llm", {"cve_id": cve_id, "profile_id": profile.get("id")})
    return {"status": "queued", "job_id": job_id}



@app.get("/admin/api/content/articles/{article_id}", dependencies=[Depends(_require_admin_token)])
def api_article_detail(article_id: int) -> dict[str, object]:
    conn = _get_conn()
    article = get_article_by_id(conn, article_id)
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    products = list_products_for_article(conn, article_id)
    threat_actors = get_article_threat_actors(conn, article_id)
    article = dict(article)
    article["products"] = products
    article["threat_actors"] = threat_actors
    return article

@app.patch("/admin/api/articles/{article_id}/content", dependencies=[Depends(_require_admin_token)])
def api_article_update_content(article_id: int, payload: ArticleContentUpdate) -> dict[str, object]:
    conn = _get_conn()
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise HTTPException(status_code=404, detail="article_not_found")
    content_text = payload.content_text or ""
    try:
        min_len = int(os.environ.get("SV_CONTENT_MIN_LEN", "500"))
    except ValueError:
        min_len = 500
    has_full_content = len(content_text) >= min_len
    update_article_content(
        conn,
        int(article_id),
        content_text=content_text,
        content_html=article.get("content_html"),
        content_fetched_at=utc_now_iso(),
        content_error=None,
        has_full_content=has_full_content,
    )
    conn.commit()
    return {"status": "ok", "article_id": article_id, "content_len": len(content_text), "has_full_content": has_full_content}



@app.get("/admin/api/content/tags", dependencies=[Depends(_require_admin_token)])
def api_content_tags() -> dict[str, object]:
    conn = _get_conn()
    return {"tags": list_article_tags(conn)}


@app.post("/admin/api/admin/clear/articles", dependencies=[Depends(_require_admin_token)])
def api_clear_articles(payload: ClearRequest, request: Request) -> dict[str, object]:
    if payload.confirm != "DELETE_ALL_ARTICLES":
        raise HTTPException(status_code=400, detail="confirm_required")
    conn = _get_conn()
    stats = delete_all_articles(conn, delete_files=payload.delete_files)
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.WARNING,
        "admin_clear_articles",
        client=request.client.host if request.client else "unknown",
        delete_files=payload.delete_files,
    )
    return {"status": "ok", "stats": stats}


@app.post("/admin/api/admin/clear/cves", dependencies=[Depends(_require_admin_token)])
def api_clear_cves(payload: ClearRequest, request: Request) -> dict[str, object]:
    if payload.confirm != "DELETE_ALL_CVES":
        raise HTTPException(status_code=400, detail="confirm_required")
    conn = _get_conn()
    stats = delete_all_cves(conn)
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.WARNING,
        "admin_clear_cves",
        client=request.client.host if request.client else "unknown",
    )
    return {"status": "ok", "stats": stats}


@app.post("/admin/api/admin/clear/all", dependencies=[Depends(_require_admin_token)])
def api_clear_all(payload: ClearRequest, request: Request) -> dict[str, object]:
    if payload.confirm != "DELETE_ALL_CONTENT":
        raise HTTPException(status_code=400, detail="confirm_required")
    conn = _get_conn()
    cancel_all_jobs(conn, reason="canceled_by_admin:clear_all")
    stats = delete_all_content(conn, delete_files=payload.delete_files)
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.WARNING,
        "admin_clear_all",
        client=request.client.host if request.client else "unknown",
        delete_files=payload.delete_files,
    )
    return {"status": "ok", "stats": stats}


@app.post("/admin/api/admin/clear/events", dependencies=[Depends(_require_admin_token)])
def api_clear_events(payload: ClearRequest, request: Request) -> dict[str, object]:
    if payload.confirm != "DELETE_ALL_EVENTS":
        raise HTTPException(status_code=400, detail="confirm_required")
    conn = _get_conn()
    stats = delete_all_events(conn)
    logger = logging.getLogger("sempervigil.admin")
    log_event(
        logger,
        logging.WARNING,
        "admin_clear_events",
        client=request.client.host if request.client else "unknown",
    )
    return {"status": "ok", "stats": stats}

@app.post("/admin/api/admin/rebuild/site-data", dependencies=[Depends(_require_admin_token)])
def api_rebuild_site_data(payload: ClearRequest, request: Request) -> dict[str, object]:
    if payload.confirm != "REBUILD_SITE_DATA":
        raise HTTPException(status_code=400, detail="confirm_required")
    conn = _get_conn()
    logger = logging.getLogger("sempervigil.admin")
    mark_build_dirty(
        conn,
        reason="site_data_refresh",
        metadata={"site_data_refresh": {"requested_by": "admin"}},
    )
    build_job_id = enqueue_build_site_if_needed(conn, reason="site_data_refresh")
    log_event(
        logger,
        logging.WARNING,
        "admin_rebuild_site_data",
        client=request.client.host if request.client else "unknown",
        build_job_id=build_job_id,
    )
    return {
        "status": "ok",
        "build_job_id": build_job_id,
        "build_state": get_build_state(conn),
    }


@app.post("/admin/api/admin/rebuild/feed-days", dependencies=[Depends(_require_admin_token)])
def api_rebuild_feed_days(request: Request, payload: dict[str, object] | None = Body(None)) -> dict[str, object]:
    if not payload or str(payload.get("confirm") or "").strip() != "REBUILD_FEED_DAYS":
        raise HTTPException(status_code=400, detail="confirm_required")
    conn = _get_conn()
    logger = logging.getLogger("sempervigil.admin")
    mode = "missing_only"
    if payload and isinstance(payload.get("mode"), str) and payload["mode"].strip():
        mode = payload["mode"].strip().lower()
    if mode not in {"missing_only", "dirty_only", "full"}:
        raise HTTPException(status_code=400, detail="invalid_mode")
    mark_build_dirty(
        conn,
        reason="feed_archive_refresh",
        metadata={"feed_archive_refresh": {"mode": mode, "requested_by": "admin"}},
    )
    build_job_id = enqueue_build_site_if_needed(conn, reason="feed_archive_refresh")
    log_event(
        logger,
        logging.WARNING,
        "admin_rebuild_feed_days",
        client=request.client.host if request.client else "unknown",
        mode=mode,
        build_job_id=build_job_id,
    )
    return {
        "status": "ok",
        "build_job_id": build_job_id,
        "build_state": get_build_state(conn),
        "mode": mode,
    }



def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.admin")


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
        overrides=source.get("overrides"),
        kind=source.get("kind"),
        url=source.get("url"),
    )


def _get_conn() -> Any:
    conn = init_db()
    bootstrap_runtime_config(conn)
    return conn


def _get_pending_brief_job(conn: Any, day: str) -> dict[str, object] | None:
    if not day:
        return None
    pattern = f'%\"date\":\"{day}\"%'
    row = conn.execute(
        """
        SELECT id, status, requested_at, started_at
        FROM jobs
        WHERE job_type = 'build_daily_brief'
          AND status IN ('queued', 'running')
          AND payload_json LIKE %s
        ORDER BY requested_at DESC
        LIMIT 1
        """,
        (pattern,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "status": row[1],
        "requested_at": row[2],
        "started_at": row[3],
    }


def _watchlist_enabled(conn: Any) -> bool:
    try:
        cfg = get_runtime_config(conn)
    except Exception:  # noqa: BLE001
        return False
    personalization = cfg.get("personalization") or {}
    return bool(personalization.get("watchlist_enabled"))


def _ensure_watchlist_enabled(conn: Any) -> None:
    if not _watchlist_enabled(conn):
        raise HTTPException(status_code=403, detail="watchlist_disabled")


def _recompute_scope(conn: Any) -> dict[str, int]:
    try:
        cfg = load_runtime_config(conn)
        min_cvss = cfg.scope.min_cvss
    except Exception:  # noqa: BLE001
        min_cvss = None
    cve_ids = list_cve_ids(conn)
    return compute_scope_for_cves(conn, cve_ids, min_cvss=min_cvss)


ai_router = APIRouter(prefix="/admin/ai", dependencies=[Depends(_require_admin_token)])


@ai_router.get("/providers")
def ai_providers_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_providers(conn)


@ai_router.post("/providers")
def ai_providers_create(payload: ProviderRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_provider(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/providers/{provider_id}")
def ai_providers_get(provider_id: str) -> dict[str, object]:
    conn = _get_conn()
    provider = get_provider(conn, provider_id)
    if not provider:
        raise HTTPException(status_code=404, detail="provider_not_found")
    return provider


@ai_router.put("/providers/{provider_id}")
@ai_router.patch("/providers/{provider_id}")
def ai_providers_update(provider_id: str, payload: ProviderRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_provider(conn, provider_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/providers/{provider_id}")
def ai_providers_delete(provider_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_provider(conn, provider_id)
    return {"status": "deleted"}


@ai_router.post("/providers/{provider_id}/secret")
def ai_providers_set_secret(
    provider_id: str, payload: ProviderSecretRequest
) -> dict[str, object]:
    conn = _get_conn()
    try:
        return set_provider_secret(conn, provider_id, payload.api_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/providers/{provider_id}/secret")
def ai_providers_clear_secret(provider_id: str) -> dict[str, str]:
    conn = _get_conn()
    clear_provider_secret(conn, provider_id)
    return {"status": "cleared"}


@ai_router.post("/providers/{provider_id}/test")
def ai_providers_test(provider_id: str) -> dict[str, object]:
    conn = _get_conn()
    logger = logging.getLogger("sempervigil.admin")
    try:
        result = test_provider(conn, provider_id, logger)
        update_provider_test_status(conn, provider_id, "ok", None)
        return result
    except Exception as exc:  # noqa: BLE001
        update_provider_test_status(conn, provider_id, "error", str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/models")
def ai_models_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_models(conn)


@ai_router.post("/models")
def ai_models_create(payload: ModelRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_model(conn, _normalize_model_payload(payload))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/models/{model_id}")
def ai_models_get(model_id: str) -> dict[str, object]:
    conn = _get_conn()
    model = get_model(conn, model_id)
    if not model:
        raise HTTPException(status_code=404, detail="model_not_found")
    return model


@ai_router.put("/models/{model_id}")
@ai_router.patch("/models/{model_id}")
def ai_models_update(model_id: str, payload: ModelRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_model(conn, model_id, _normalize_model_payload(payload))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/models/{model_id}")
def ai_models_delete(model_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_model(conn, model_id)
    return {"status": "deleted"}


@ai_router.get("/prompts")
def ai_prompts_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_prompts(conn)


@ai_router.post("/prompts")
def ai_prompts_create(payload: PromptRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_prompt(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/prompts/{prompt_id}")
def ai_prompts_get(prompt_id: str) -> dict[str, object]:
    conn = _get_conn()
    prompt = get_prompt(conn, prompt_id)
    if not prompt:
        raise HTTPException(status_code=404, detail="prompt_not_found")
    return prompt


@ai_router.put("/prompts/{prompt_id}")
@ai_router.patch("/prompts/{prompt_id}")
def ai_prompts_update(prompt_id: str, payload: PromptRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_prompt(conn, prompt_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/prompts/{prompt_id}")
def ai_prompts_delete(prompt_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_prompt(conn, prompt_id)
    return {"status": "deleted"}


@ai_router.get("/schemas")
def ai_schemas_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_schemas(conn)


@ai_router.post("/schemas")
def ai_schemas_create(payload: SchemaRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_schema(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/schemas/{schema_id}")
def ai_schemas_get(schema_id: str) -> dict[str, object]:
    conn = _get_conn()
    schema = get_schema(conn, schema_id)
    if not schema:
        raise HTTPException(status_code=404, detail="schema_not_found")
    return schema


@ai_router.put("/schemas/{schema_id}")
@ai_router.patch("/schemas/{schema_id}")
def ai_schemas_update(schema_id: str, payload: SchemaRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_schema(conn, schema_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/schemas/{schema_id}")
def ai_schemas_delete(schema_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_schema(conn, schema_id)
    return {"status": "deleted"}


@ai_router.get("/profiles")
def ai_profiles_list() -> list[dict[str, object]]:
    conn = _get_conn()
    return list_profiles(conn)


@ai_router.post("/profiles")
def ai_profiles_create(payload: ProfileRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return create_profile(conn, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/profiles/{profile_id}")
def ai_profiles_get(profile_id: str) -> dict[str, object]:
    conn = _get_conn()
    profile = get_profile(conn, profile_id)
    if not profile:
        raise HTTPException(status_code=404, detail="profile_not_found")
    return profile


@ai_router.put("/profiles/{profile_id}")
@ai_router.patch("/profiles/{profile_id}")
def ai_profiles_update(profile_id: str, payload: ProfileRequest) -> dict[str, object]:
    conn = _get_conn()
    try:
        return update_profile(conn, profile_id, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.delete("/profiles/{profile_id}")
def ai_profiles_delete(profile_id: str) -> dict[str, str]:
    conn = _get_conn()
    delete_profile(conn, profile_id)
    return {"status": "deleted"}


@ai_router.post("/profiles/{profile_id}/test")
def ai_profiles_test(profile_id: str, payload: ProfileTestRequest) -> dict[str, object]:
    conn = _get_conn()
    logger = logging.getLogger("sempervigil.admin")
    try:
        result = test_profile(conn, profile_id, payload.text, logger)
        return {"ok": True, **result}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@ai_router.get("/pipeline-routing")
def ai_pipeline_list() -> dict[str, object]:
    conn = _get_conn()
    return {"stages": STAGE_NAMES, "routing": list_pipeline_routing(conn)}


@ai_router.post("/pipeline-routing")
def ai_pipeline_set(payload: PipelineStageRequest) -> dict[str, str]:
    conn = _get_conn()
    set_pipeline_routing(conn, payload.stage_name, payload.profile_id)
    return {"status": "ok"}


@ai_router.post("/clear-queued")
def ai_clear_queued() -> dict[str, object]:
    conn = _get_conn()
    stage_map = {
        "summarize_article": "summarize_article_llm",
        "article_enrich_products": "article_enrich_products",
        "cve_enrich_products": "cve_enrich_llm",
        "article_enrich_threat_actors": "article_enrich_threat_actors",
        "cve_enrich_threat_actors": "cve_enrich_threat_actors",
        "daily_brief_overall_synthesis": "build_daily_brief",
        "event_report_llm": "event_report_llm",
    }
    cleared = 0
    stage_results: dict[str, object] = {}
    for stage_name, job_type in stage_map.items():
        profile, reason = get_active_profile_for_stage(conn, stage_name)
        if profile:
            stage_results[stage_name] = {"canceled": 0, "reason": "active"}
            continue
        count = cancel_jobs_by_type(
            conn,
            job_type,
            status="queued",
            reason=f"llm_stage_{reason}",
        )
        cleared += count
        stage_results[stage_name] = {"canceled": count, "reason": reason}
    return {"cleared": cleared, "stages": stage_results}


def _normalize_model_payload(payload: ModelRequest) -> dict[str, object]:
    data = payload.model_dump(exclude_unset=True)
    tags = data.get("tags")
    if isinstance(tags, str):
        data["tags"] = [item.strip() for item in tags.split(",") if item.strip()]
    return data


app.include_router(ui_router(_require_admin_token), prefix="/ui")
app.include_router(ai_router)


def _enqueue_daily_brief(conn, payload: DailyBriefRequest) -> dict[str, str]:
    request_payload: dict[str, object] = {}
    date_value = payload.date
    if not date_value:
        try:
            config = load_runtime_config(conn)
            tz_name = config.app.timezone or "UTC"
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc
        date_value = datetime.now(tz).strftime("%Y-%m-%d")
    if date_value:
        try:
            datetime.strptime(date_value, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid_date") from exc
        request_payload["date"] = date_value
    if payload.profile_id:
        request_payload["profile_id"] = payload.profile_id
    job_id = enqueue_job(conn, "build_daily_brief", request_payload)
    return {"job_id": job_id, "date": date_value or ""}


@app.post("/admin/api/daily_brief/build", dependencies=[Depends(_require_admin_token)])
def build_daily_brief(payload: DailyBriefRequest) -> dict[str, str]:
    conn = _get_conn()
    return _enqueue_daily_brief(conn, payload)


@app.post("/admin/briefs/build", dependencies=[Depends(_require_admin_token)])
def build_brief(payload: DailyBriefRequest) -> dict[str, str]:
    conn = _get_conn()
    return _enqueue_daily_brief(conn, payload)


@app.post("/admin/api/ai/test", dependencies=[Depends(_require_admin_token)])
def api_ai_test(payload: AiTestRequest) -> dict[str, object]:
    conn = _get_conn()
    logger = logging.getLogger("sempervigil.admin")
    input_chars = len(payload.prompt or "")
    try:
        result = test_model(conn, payload.provider_id, payload.model_id, payload.prompt, logger)
        run_id = insert_llm_run(
            conn,
            job_id=None,
            provider_id=payload.provider_id,
            model_id=payload.model_id,
            prompt_name="ai_test",
            input_chars=input_chars,
            output_chars=len(result.get("output") or ""),
            latency_ms=int(result.get("latency_ms") or 0),
            ok=True,
            error=None,
        )
        return {**result, "run_id": run_id}
    except Exception as exc:  # noqa: BLE001
        run_id = insert_llm_run(
            conn,
            job_id=None,
            provider_id=payload.provider_id,
            model_id=payload.model_id,
            prompt_name="ai_test",
            input_chars=input_chars,
            output_chars=0,
            latency_ms=None,
            ok=False,
            error=str(exc),
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/admin/api/ai/runs", dependencies=[Depends(_require_admin_token)])
def api_ai_runs(limit: int = 10) -> dict[str, object]:
    conn = _get_conn()
    return {"items": list_llm_runs(conn, limit=limit)}
