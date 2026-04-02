from __future__ import annotations

import argparse
import json
import feedparser
import urllib.error
import re
import logging
import os
import time
import threading
import uuid
import socket
import urllib.error
from pathlib import Path
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from urllib.parse import urlparse, parse_qsl, urlencode, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from .config import (
    ConfigError,
    bootstrap_cve_settings,
    bootstrap_events_settings,
    get_cve_settings,
    get_events_settings,
    is_article_markdown_enabled,
    load_runtime_config,
)
from .ingest import process_source
from .models import Article, Job
from .cve_sync import CveSyncConfig, isoformat_utc, sync_cves, sync_cve_id
from .kev_sync import ensure_kev_cache
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .publish import write_article_markdown, write_events_index, write_events_markdown, write_json_index
from .signals import build_cve_evidence, extract_cve_ids
from .pipelines.content_fetch import fetch_article_content
from .llm.router import run_profile, run_pipeline_stage
from .services.ai_service import (
    get_active_profile_for_stage,
    get_profile,
    get_provider,
    get_model,
    list_providers,
)
from .normalize import normalize_name
from .searxng import searxng_search
from .enrichment.query import build_event_enrich_query
from .enrichment.scoring import score_web_result
from .storage import (
    list_articles_for_day,
    list_articles_per_day,
    list_cves_for_day,
    claim_next_job,
    complete_job,
    enqueue_job,
    fail_job,
    fail_job_force,
    get_source,
    list_sources,
    get_setting,
    set_setting,
    increment_setting_counter,
    get_article_id,
    get_article_by_id,
    get_article_tags,
    get_event,
    get_batch_job_counts,
    get_job,
    is_job_canceled,
    init_db,
    has_pending_article_job,
    count_failed_article_jobs,
    insert_articles,
    link_article_to_events,
    list_due_sources,
    list_events,
    list_jobs_by_types_since,
    requeue_job,
    has_pending_job,
    insert_llm_run,
    release_job,
    pause_source,
    record_health_alert,
    record_source_run,
    rebuild_events_from_cves,
    upsert_cve_links,
    upsert_event_by_key,
    upsert_event_item,
    list_product_keys_for_cve,
    list_article_cve_ids,
    list_event_ids_for_article,
    list_event_articles,
    list_article_ids_without_event,
    link_event_article,
    get_source_run_streaks,
    get_source_name,
    get_source_stats,
    insert_source_health_event,
    update_article_content,
    update_article_summary,
    update_article_context_pack,
    update_job_result,
    list_article_ids_missing_content,
    list_article_ids_missing_content_all,
    list_article_ids_missing_summary,
    list_article_ids_missing_context_pack,
    list_article_ids_ready_for_summary_all,
    list_products_for_article,
    list_article_ids_for_source_since,
    compute_watchlist_hits,
    try_acquire_lease,
    release_lease,
    update_event_summary_from_articles,
    update_event_report,
    update_event,
    list_event_web_sources,
    clear_event_web_sources,
    get_event_web_source,
    list_recent_articles,
    list_event_keys_for_articles,
    list_article_cve_tags,
    count_products_for_article,
    infer_article_products_from_cves,
    list_article_ids_missing_products,
    list_cve_ids_missing_products,
    list_cves_for_day,
    list_cve_ids_needing_kev_check,
    get_cve_kev,
    get_cve_kev_map,
    set_cve_kev_link,
    list_articles_for_product,
    list_products_with_article_counts,
    get_product,
    get_product_cves,
    get_cve,
    get_product_display_by_key,
    list_cve_vendor_products,
    upsert_event_web_source,
    update_event_web_source_status,
    update_event_web_source_published_at,
    mark_event_web_source_status,
    promote_event_web_source_to_article,
    link_cve_products_from_items,
    link_article_product,
    upsert_vendor,
    upsert_product,
    upsert_threat_actor,
    add_threat_actor_alias,
    link_article_threat_actor,
    link_cve_threat_actor,
    get_article_threat_actors,
    get_cve_threat_actors,
    get_vendor_id_by_name,
    get_product_id_by_vendor_name,
    get_threat_actor_id_by_key,
    list_article_ids_missing_threat_actors,
    list_cve_ids_missing_threat_actors,
    mark_article_products_checked,
    mark_article_events_checked,
    mark_article_threat_actors_checked,
    mark_cve_products_checked,
    mark_cve_threat_actors_checked,
    mark_build_dirty,
    mark_source_ingest_started,
    finalize_source_ingest_state,
    enqueue_source_ingest_job,
    get_pending_job_id_for_cve,
    list_queued_job_stats,
    search_cves,
    _table_exists,
    column_exists,
    delete_vendor_product_tags,
    upsert_daily_brief,
    touch_job_lock,
)
from .utils import (
    configure_logging,
    log_event,
    utc_now_iso,
    utc_now_iso_offset,
    slugify,
    atomic_write_json,
    atomic_write_text,
)

WORKER_JOB_TYPES = [
    "ingest_source",
    "ingest_due_sources",
    "test_source",
    "cve_sync",
    "cve_enrich_kev",
    "cve_enrich_llm",
    "cve_enrich_threat_actors",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "article_products_backfill",
    "article_threat_actors_backfill",
    "cve_threat_actors_backfill",
    "events_rebuild",
    "fetch_article_content",
    "summarize_article_llm",
    "summarize_article_context_llm",
    "build_daily_brief",
    "write_article_markdown",
    "derive_events_from_articles",
    "enrich_event_from_web",
    "validate_event_web_source",
    "promote_event_web_source_to_article",
    "enrich_event_summary_llm",
    "event_report_llm",
    "source_acquire",
    "rebuild_vendor_products",
    "smoke_test",
]

QUEUE_WORKER_TYPES = {
    "discovery": ["ingest_due_sources", "test_source", "cve_sync", "source_acquire", "smoke_test"],
    "fetch": [
        "ingest_source",
        "fetch_article_content",
        "cve_enrich_kev",
        "events_rebuild",
        "enrich_event_from_web",
        "validate_event_web_source",
        "promote_event_web_source_to_article",
        "rebuild_vendor_products",
    ],
    "llm_local": [
        "summarize_article_llm",
        "summarize_article_context_llm",
        "derive_events_from_articles",
        "article_enrich_products",
        "article_enrich_threat_actors",
        "cve_enrich_llm",
        "cve_enrich_threat_actors",
        "event_report_llm",
        "enrich_event_summary_llm",
        "article_products_backfill",
        "article_threat_actors_backfill",
        "cve_threat_actors_backfill",
    ],
    "openai": ["build_daily_brief"],
    "build": ["write_article_markdown"],
}

_AUTO_CATCHUP_JOB_TYPES = {
    "fetch_article_content",
    "summarize_article_llm",
    "summarize_article_context_llm",
    "derive_events_from_articles",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "cve_enrich_kev",
    "cve_enrich_llm",
    "cve_enrich_threat_actors",
}
_AUTO_CATCHUP_BATCH_LIMIT = 200
_AUTO_CATCHUP_LEASE = "auto_catchup_enqueue"
_RUN_ONCE_IDLE = 3
HANDLED_JOB_TYPES = {
    "ingest_source",
    "ingest_due_sources",
    "test_source",
    "cve_sync",
    "cve_enrich_kev",
    "cve_enrich_llm",
    "cve_enrich_threat_actors",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "article_products_backfill",
    "article_threat_actors_backfill",
    "cve_threat_actors_backfill",
    "events_rebuild",
    "fetch_article_content",
    "summarize_article_llm",
    "summarize_article_context_llm",
    "build_daily_brief",
    "write_article_markdown",
    "derive_events_from_articles",
    "enrich_event_from_web",
    "validate_event_web_source",
    "promote_event_web_source_to_article",
    "enrich_event_summary_llm",
    "event_report_llm",
    "source_acquire",
    "rebuild_vendor_products",
    "smoke_test",
    "cve_threat_actors_backfill",
}

_SCHEMA_LOGGED = False


def _log_vendor_product_schema(conn, logger: logging.Logger) -> None:
    global _SCHEMA_LOGGED
    if _SCHEMA_LOGGED:
        return
    _SCHEMA_LOGGED = True
    article_cols = {
        "id": column_exists(conn, "articles", "id"),
        "title": column_exists(conn, "articles", "title"),
        "published_at": column_exists(conn, "articles", "published_at"),
        "ingested_at": column_exists(conn, "articles", "ingested_at"),
        "source_id": column_exists(conn, "articles", "source_id"),
        "original_url": column_exists(conn, "articles", "original_url"),
        "summary_llm": column_exists(conn, "articles", "summary_llm"),
        "content_text": column_exists(conn, "articles", "content_text"),
    }
    cve_cols = {
        "cve_id": column_exists(conn, "cves", "cve_id"),
        "published_at": column_exists(conn, "cves", "published_at"),
        "last_modified_at": column_exists(conn, "cves", "last_modified_at"),
        "preferred_base_severity": column_exists(conn, "cves", "preferred_base_severity"),
        "description_text": column_exists(conn, "cves", "description_text"),
    }
    log_event(
        logger,
        logging.INFO,
        "vendor_product_schema",
        articles_table=_table_exists(conn, "articles"),
        article_tags_table=_table_exists(conn, "article_tags"),
        cves_table=_table_exists(conn, "cves"),
        products_table=_table_exists(conn, "products"),
        vendors_table=_table_exists(conn, "vendors"),
        articles_cols=article_cols,
        cves_cols=cve_cols,
    )


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.worker")


def _site_root_from_output_dir(output_dir: str) -> str:
    output_path = Path(output_dir)
    if output_path.name == "posts":
        return str(output_path.parent.parent)
    if output_path.name == "content":
        return str(output_path.parent)
    return str(output_path.parent)


def _cve_page_url(cve_id: str | None) -> str:
    cve = str(cve_id or "").strip()
    if not cve:
        return ""
    return f"https://nvd.nist.gov/vuln/detail/{cve}"


def _format_human_ts(value: str | None, tz_name: str) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    local = parsed.astimezone(tz)
    return local.strftime("%b %d, %Y · %H:%M")


def _is_article_suppressed(meta_json: object) -> bool:
    if not meta_json:
        return False
    try:
        parsed = json.loads(meta_json) if isinstance(meta_json, str) else meta_json
    except Exception:
        return False
    if isinstance(parsed, dict):
        return bool(parsed.get("suppressed"))
    return False


def _is_timeout_error(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    if isinstance(exc, urllib.error.URLError):
        if isinstance(exc.reason, (TimeoutError, socket.timeout)):
            return True
    message = str(exc).lower()
    return "timeout" in message or "timed out" in message


def _looks_like_thn_teaser(source_id: str | None, content_text: str | None) -> bool:
    if (source_id or "").strip().lower() != "the-hacker-news":
        return False
    text = (content_text or "").strip()
    if not text:
        return False
    lowered = text.lower()
    return "zero trust + ai: thrive in the ai era" in lowered and "zero trust everywhere" in lowered


def _dark_reading_rss_summary_fallback(url: str, logger: logging.Logger) -> str:
    try:
        parsed = feedparser.parse("https://www.darkreading.com/rss.xml")
    except Exception:  # noqa: BLE001
        return ""
    if getattr(parsed, "bozo", False):
        return ""
    target = _normalize_canonical_url(url).lower()
    for entry in getattr(parsed, "entries", []) or []:
        link = _normalize_canonical_url(str(getattr(entry, "link", "") or "")).lower()
        if not link or link != target:
            continue
        summary = str(getattr(entry, "summary", "") or "").strip()
        summary = re.sub(r"<[^>]+>", " ", summary)
        summary = re.sub(r"\s+", " ", summary).strip()
        if not summary:
            return ""
        return summary
    log_event(
        logger,
        logging.INFO,
        "dark_reading_rss_fallback_miss",
        url=url,
    )
    return ""


def _apply_dark_reading_403_fallback(
    conn: DB,
    config: RuntimeConfig,
    logger: logging.Logger,
    article_id: int,
    article: dict[str, Any],
    source_id: str | None,
    url: str,
) -> bool:
    if (source_id or "").strip().lower() != "dark-reading":
        return False
    fallback_summary = _dark_reading_rss_summary_fallback(url, logger)
    if not fallback_summary:
        return False
    update_article_content(
        conn,
        int(article_id),
        content_text=fallback_summary,
        content_html=None,
        content_fetched_at=utc_now_iso(),
        content_error="fallback:rss_summary_403",
        has_full_content=True,
    )
    log_event(
        logger,
        logging.INFO,
        "dark_reading_rss_fallback_applied",
        article_id=article_id,
        content_len=len(fallback_summary),
    )
    _maybe_enqueue_context_pack(conn, int(article_id), article["source_id"], logger)
    if not _maybe_enqueue_summarize(conn, int(article_id), article["source_id"], logger):
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
    _maybe_enqueue_article_product_enrich(conn, int(article_id), article["source_id"], logger)
    return True


_LLM_JOB_TYPES = {
    "summarize_article_llm",
    "summarize_article_context_llm",
    "cve_enrich_llm",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "cve_enrich_threat_actors",
    "derive_events_from_articles",
    "enrich_event_summary_llm",
    "event_report_llm",
    "build_daily_brief",
}


def _llm_lock_timeout_seconds(conn, config, allowed_types: list[str] | None) -> int:
    base = int(config.jobs.lock_timeout_seconds)
    if not allowed_types:
        return base
    if not any(job_type in _LLM_JOB_TYPES for job_type in allowed_types):
        return base
    timeout_s = 1200
    try:
        providers = list_providers(conn)
        enabled = [p for p in providers if p.get("is_enabled")]
        if enabled:
            timeout_s = max(int(p.get("timeout_s") or 0) for p in enabled) or timeout_s
    except Exception:  # noqa: BLE001
        timeout_s = 1200
    return max(base, timeout_s + 120)


def _is_openai_provider(provider: dict[str, object] | None) -> bool:
    if not provider:
        return False
    provider_type = str(provider.get("type") or "").lower()
    base_url = str(provider.get("base_url") or "").lower()
    if provider_type != "openai_compatible":
        return False
    if "api.openai.com" in base_url:
        return True
    return False


def _llm_profile_labels(conn, profile: dict[str, object] | None) -> dict[str, object]:
    if not profile:
        return {}
    provider_id = profile.get("primary_provider_id")
    model_id = profile.get("primary_model_id")
    provider = get_provider(conn, provider_id) if provider_id else None
    model = get_model(conn, model_id) if model_id else None
    return {
        "profile_id": profile.get("id"),
        "profile_name": profile.get("name") or "",
        "provider_id": provider_id,
        "provider_name": provider.get("name") if isinstance(provider, dict) else "",
        "model_id": model_id,
        "model_name": model.get("model_name") if isinstance(model, dict) else "",
    }


def _record_llm_parse_metric(
    conn,
    *,
    stage: str,
    profile: dict[str, object] | None,
    outcome: str,
) -> None:
    labels = _llm_profile_labels(conn, profile)
    profile_id = str(labels.get("profile_id") or "unknown")
    model_id = str(labels.get("model_id") or "unknown")
    prefix = f"metrics.llm_parse.{stage}.profile.{profile_id}.model.{model_id}"
    increment_setting_counter(conn, f"{prefix}.total", 1)
    increment_setting_counter(conn, f"{prefix}.{outcome}", 1)


def _coerce_profile(value: object) -> dict[str, object] | None:
    if isinstance(value, tuple):
        value = value[0] if value else None
    return value if isinstance(value, dict) else None


def _resolve_profile_ids_for_job(conn, job) -> list[str]:
    profile_ids: list[str] = []
    payload = job.payload or {}
    payload_profile = payload.get("profile_id")
    if isinstance(payload_profile, str) and payload_profile:
        profile_ids.append(payload_profile)
    stage_map = {
        "summarize_article_llm": ["summarize_article_llm"],
        "summarize_article_context_llm": ["article_context_pack"],
        "cve_enrich_llm": ["cve_enrich_products"],
        "article_enrich_products": ["article_enrich_products"],
        "article_enrich_threat_actors": ["article_enrich_threat_actors"],
        "cve_enrich_threat_actors": ["cve_enrich_threat_actors"],
        "derive_events_from_articles": ["derive_events_from_articles"],
        "enrich_event_summary_llm": ["enrich_event_summary_llm"],
        "event_report_llm": ["event_report_llm", "enrich_event_summary_llm", "summarize_article"],
        "build_daily_brief": ["daily_brief_overall_synthesis"],
    }
    stages = stage_map.get(job.job_type, [])
    for stage in stages:
        profile = _coerce_profile(get_active_profile_for_stage(conn, stage))
        profile_id = profile.get("id") if profile else None
        if isinstance(profile_id, str) and profile_id:
            profile_ids.append(profile_id)
    return list({pid for pid in profile_ids if pid})


def _job_uses_openai(conn, job) -> bool:
    for profile_id in _resolve_profile_ids_for_job(conn, job):
        profile = get_profile(conn, profile_id) or {}
        provider_id = profile.get("primary_provider_id")
        if not provider_id:
            continue
        provider = get_provider(conn, provider_id)
        if _is_openai_provider(provider):
            return True
    return False


def _collect_jsonld_headlines(payload: object) -> list[str]:
    headlines: list[str] = []
    if isinstance(payload, list):
        for item in payload:
            headlines.extend(_collect_jsonld_headlines(item))
        return headlines
    if isinstance(payload, dict):
        headline = payload.get("headline")
        if isinstance(headline, str) and headline.strip():
            headlines.append(headline.strip())
        graph = payload.get("@graph")
        if graph is not None:
            headlines.extend(_collect_jsonld_headlines(graph))
        for value in payload.values():
            if isinstance(value, (dict, list)):
                headlines.extend(_collect_jsonld_headlines(value))
    return headlines


def _extract_title_from_html(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type=re.compile(r"application/ld\\+json", re.I)):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        headlines = _collect_jsonld_headlines(parsed)
        if headlines:
            return headlines[0]
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        return og_title.get("content", "").strip()
    title_tag = soup.find("title")
    if title_tag and title_tag.get_text():
        return title_tag.get_text(strip=True)
    return ""


def _is_article_in_today_feed(
    conn, config, article_id: int, logger: logging.Logger
) -> bool:
    if not article_id:
        return False
    site_root = _site_root_from_output_dir(config.paths.output_dir)
    today_path = Path(site_root) / "data" / "articles" / "today.json"
    try:
        if today_path.exists():
            today_items = json.loads(today_path.read_text(encoding="utf-8") or "[]")
            if isinstance(today_items, list):
                for item in today_items:
                    try:
                        if int(item.get("id") or 0) == int(article_id):
                            return True
                    except Exception:
                        continue
    except Exception:
        log_event(
            logger,
            logging.WARNING,
            "today_feed_read_failed",
            article_id=article_id,
            path=str(today_path),
        )
    recent_rows = list_recent_articles(conn, limit=200)
    if not recent_rows:
        return False
    tz_name = config.app.timezone or "UTC"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    now_local = datetime.now(tz)
    today_date = now_local.date()

    def _parse_ts(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    items: list[dict[str, object]] = []
    for row in recent_rows:
        if _is_article_suppressed(row.get("meta_json")):
            continue
        published_at = row.get("published_at") or row.get("ingested_at")
        parsed = _parse_ts(published_at)
        local = parsed.astimezone(tz) if parsed else None
        items.append(
            {
                "id": row.get("id"),
                "published_at_iso": published_at or "",
                "_sort": local or parsed or datetime.min.replace(tzinfo=timezone.utc),
            }
        )
    items.sort(key=lambda item: item["_sort"], reverse=True)
    for item in items:
        item.pop("_sort", None)

    today_items: list[dict[str, object]] = []
    for item in items:
        if not item["published_at_iso"]:
            continue
        parsed = _parse_ts(str(item["published_at_iso"]))
        if not parsed:
            continue
        local = parsed.astimezone(tz)
        if local.date() == today_date:
            today_items.append(item)

    min_items = 20
    if len(today_items) < min_items:
        seen_ids = {item.get("id") for item in today_items}
        for item in items:
            if len(today_items) >= min_items:
                break
            if item.get("id") in seen_ids:
                continue
            today_items.append(item)
            seen_ids.add(item.get("id"))

    for item in today_items:
        try:
            if int(item.get("id") or 0) == int(article_id):
                return True
        except Exception:
            continue
    return False


def _refresh_feed_data_files(conn, config, logger: logging.Logger) -> dict[str, object]:
    site_root = _site_root_from_output_dir(config.paths.output_dir)
    data_root = getattr(config.paths, "data_dir", None) or ""
    if not data_root:
        data_root = str(Path(site_root) / "data")
    data_dir = Path(data_root) / "articles"
    data_dir.mkdir(parents=True, exist_ok=True)
    min_items = 20
    feed_recent_limit = max(200, int(os.environ.get("SV_FEED_RECENT_LIMIT", "2000") or 2000))
    recent_rows = list_recent_articles(conn, limit=feed_recent_limit)
    if not recent_rows:
        (data_dir / "today.json").write_text("[]", encoding="utf-8")
        (data_dir / "recent.json").write_text("[]", encoding="utf-8")
        return {"today": 0, "recent": 0}
    article_ids = [row["id"] for row in recent_rows if row.get("id") is not None]
    event_keys_map = list_event_keys_for_articles(conn, article_ids)
    cve_tags_map = list_article_cve_tags(conn, article_ids)
    tz_name = config.app.timezone or "UTC"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    now_local = datetime.now(tz)
    today_date = now_local.date()

    def _parse_ts(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed

    def _extract_summary_payload(value: object) -> dict[str, object]:
        if not value:
            return {}
        if isinstance(value, dict):
            return value
        raw = str(value).strip()
        # Strip common code-fence wrappers, even if the closing fence is missing.
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            raw = raw.strip()
        candidate = raw
        if "{" in raw and "}" in raw:
            candidate = raw[raw.find("{"): raw.rfind("}") + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
        return {}

    def _extract_summary_text(value: object) -> str:
        if not value:
            return ""
        payload = _extract_summary_payload(value)
        if payload:
            summary = str(payload.get("summary") or "").strip()
            if summary:
                return summary
        raw = str(value).strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            raw = raw.strip()
        return raw

    def _build_facets(
        vendors: list[dict[str, object]],
        products: list[dict[str, object]],
        threats: list[dict[str, object]],
    ) -> list[dict[str, str]]:
        facets: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()

        def _add(kind: str, slug_value: object, display_value: object) -> None:
            display_name = str(display_value or "").strip()
            slug_text = str(slug_value or "").strip()
            if not slug_text and display_name:
                slug_text = slugify(display_name)
            if not display_name and not slug_text:
                return
            key = (kind, (slug_text or display_name).lower())
            if key in seen:
                return
            seen.add(key)
            facets.append(
                {
                    "kind": kind,
                    "slug": slug_text or slugify(display_name),
                    "display_name": display_name or slug_text,
                }
            )

        for vendor in vendors or []:
            if isinstance(vendor, dict):
                _add("vendor", vendor.get("slug"), vendor.get("display_name"))
        for product in products or []:
            if isinstance(product, dict):
                _add("product", product.get("slug"), product.get("display_name"))
        for threat in threats or []:
            if isinstance(threat, dict):
                _add(
                    "threat",
                    threat.get("slug") or threat.get("actor_key"),
                    threat.get("display_name") or threat.get("actor_key"),
                )
        return facets

    items = []
    for row in recent_rows:
        if _is_article_suppressed(row.get("meta_json")):
            continue
        published_at = row.get("published_at") or row.get("ingested_at")
        parsed = _parse_ts(published_at)
        local = parsed.astimezone(tz) if parsed else None
        nist_family = None
        summary_text = ""
        summary_bullets: list[str] = []
        summary_llm = row.get("summary_llm")
        if summary_llm:
            parsed_summary = _extract_summary_payload(summary_llm)
            if parsed_summary:
                nist_family = parsed_summary.get("nist_family")
                summary_text = str(parsed_summary.get("summary") or "").strip()
                bullets = (
                    parsed_summary.get("bullets")
                    or parsed_summary.get("key_points")
                    or parsed_summary.get("tldr")
                    or []
                )
                if isinstance(bullets, list):
                    summary_bullets = [
                        str(item).strip() for item in bullets if str(item).strip()
                    ][:8]
        if not summary_text:
            summary_text = _extract_summary_text(summary_llm)
        article_id = int(row.get("id") or 0)
        product_links = list_products_for_article(conn, article_id) if article_id else []
        vendors: list[dict[str, str]] = []
        product_items: list[dict[str, str]] = []
        product_labels: list[str] = []
        seen_vendor: set[str] = set()
        seen_product: set[str] = set()
        for product in product_links:
            vendor_name = str(product.get("vendor_display") or product.get("vendor") or "").strip()
            product_name = str(product.get("product_display") or product.get("product") or "").strip()
            if vendor_name and vendor_name not in seen_vendor:
                vendors.append({"slug": slugify(vendor_name), "display_name": vendor_name})
                seen_vendor.add(vendor_name)
            if product_name and product_name not in seen_product:
                product_slug = slugify(f"{vendor_name} {product_name}".strip())
                product_items.append({"slug": product_slug, "display_name": product_name})
                seen_product.add(product_name)
            label = (
                f"{vendor_name} — {product_name}".strip(" —")
                if vendor_name or product_name
                else ""
            )
            if label:
                product_labels.append(label)
        threat_actors = (
            get_article_threat_actors(conn, article_id) if article_id else []
        )
        source_id = str(row.get("source_id") or "").strip()
        source_icon = "/img/source-default-news.svg"
        if source_id and (Path(site_root) / "static" / "img" / f"{source_id}.png").exists():
            source_icon = f"/img/{source_id}.png"
        published_epoch = int(parsed.timestamp()) if parsed else 0
        time_label = ""
        human_ts = _format_human_ts(published_at, tz_name)
        if " · " in human_ts:
            try:
                time_label = human_ts.split(" · ", 1)[1].strip()
            except Exception:
                time_label = ""
        items.append(
            {
                "article_id": article_id,
                "id": row.get("id"),
                "title": row.get("title") or "",
                "source": row.get("source_name") or "",
                "source_id": source_id,
                "source_icon": source_icon,
                "published_at_iso": published_at or "",
                "published_at_human": human_ts,
                "published_epoch": published_epoch,
                "time_label": time_label,
                "url": row.get("original_url") or "",
                "summary": summary_text,
                "summary_bullets": summary_bullets,
                "tags": sorted({t for t in (row.get("tags") or "").split(",") if t} | set(cve_tags_map.get(row.get("id"), []))),
                "vendor_products": product_links,
                "vendors": vendors,
                "product_items": product_items,
                "products": product_labels,
                "threat_actors": threat_actors,
                "facets": _build_facets(vendors, product_items, threat_actors),
                "event_keys": event_keys_map.get(row.get("id"), []),
                "nist_family": nist_family or "",
                "_sort": local or parsed or datetime.min.replace(tzinfo=timezone.utc),
            }
        )
    items.sort(key=lambda item: item["_sort"], reverse=True)
    for item in items:
        item.pop("_sort", None)

    today_items = []
    for item in items:
        if not item["published_at_iso"]:
            continue
        parsed = _parse_ts(item["published_at_iso"])
        if not parsed:
            continue
        local = parsed.astimezone(tz)
        if local.date() == today_date:
            today_items.append(item)

    if len(today_items) < min_items:
        seen_ids = {item.get("id") for item in today_items}
        for item in items:
            if len(today_items) >= min_items:
                break
            if item.get("id") in seen_ids:
                continue
            today_items.append(item)
            seen_ids.add(item.get("id"))

    recent_items = items[: max(min_items, len(today_items))]

    (data_dir / "today.json").write_text(json.dumps(today_items, indent=2), encoding="utf-8")
    (data_dir / "recent.json").write_text(json.dumps(recent_items, indent=2), encoding="utf-8")

    cve_dir = Path(data_root) / "cves"
    cve_dir.mkdir(parents=True, exist_ok=True)
    cves_today = list_cves_for_day(conn, today_date.isoformat(), limit=200)
    cve_items = []
    seen_cves: set[str] = set()

    def _cve_item(cve: dict[str, object]) -> dict[str, object]:
        cve_id = cve.get("cve_id")
        product_title = ""
        title_seed = ""
        product_labels: list[str] = []
        vendors: list[dict[str, str]] = []
        product_items: list[dict[str, str]] = []
        vendor_products = []
        if cve_id:
            vendor_products = list_cve_vendor_products(conn, str(cve_id))
            seen_vendors: set[str] = set()
            seen_products: set[str] = set()
            for entry in vendor_products:
                vendor = str(entry.get("vendor_display") or "").strip()
                product = str(entry.get("product_display") or "").strip()
                if vendor and vendor.lower() == "unknown":
                    vendor = ""
                label = f"{vendor} — {product}" if vendor and product else (product or vendor)
                if label:
                    product_labels.append(label)
                if vendor and product and not title_seed:
                    title_seed = f"{vendor} — {product}"
                if vendor and vendor not in seen_vendors:
                    vendors.append({"slug": slugify(vendor), "display_name": vendor})
                    seen_vendors.add(vendor)
                if product and product not in seen_products:
                    product_slug = slugify(f"{vendor} {product}".strip())
                    product_items.append({"slug": product_slug, "display_name": product})
                    seen_products.add(product)
            if title_seed:
                product_title = title_seed
        severity = (cve.get("preferred_base_severity") or "").strip()
        if not product_title:
            product_title = severity or "CVE"
        elif severity:
            product_title = f"{product_title} — {severity}"
        desc = (cve.get("description_text") or cve.get("summary") or "").strip()
        if len(desc) > 220:
            desc = desc[:217].rstrip() + "..."
        published_at = cve.get("published_at") or cve.get("last_modified_at") or ""
        parsed_published = _parse_ts(str(published_at)) if published_at else None
        published_epoch = int(parsed_published.timestamp()) if parsed_published else 0
        human_ts = _format_human_ts(published_at, tz_name)
        time_label = ""
        if " · " in human_ts:
            try:
                time_label = human_ts.split(" · ", 1)[1].strip()
            except Exception:
                time_label = ""
        threat_actors = get_cve_threat_actors(conn, str(cve_id)) if cve_id else []
        kev = get_cve_kev(conn, str(cve_id)) if cve_id else None
        kev_due_date = kev.get("due_date") if kev else ""
        cve_url = _cve_page_url(str(cve_id) if cve_id else "")
        return {
            "cve_id": cve_id,
            "product_title": product_title,
            "published_at_iso": published_at,
            "published_at_human": human_ts,
            "published_epoch": published_epoch,
            "time_label": time_label,
            "summary": desc,
            "base_score": cve.get("preferred_base_score"),
            "severity": severity or "",
            "url": cve_url,
            "nvd_url": f"https://nvd.nist.gov/vuln/detail/{cve_id}" if cve_id else "",
            "products": product_labels,
            "product_items": product_items,
            "vendors": vendors,
            "vendor_products": vendor_products,
            "threat_actors": threat_actors,
            "facets": _build_facets(vendors, product_items, threat_actors),
            "kev_due_date": kev_due_date,
            "kev_known_exploited": bool(kev),
        }

    for cve in cves_today:
        cve_id = cve.get("cve_id")
        if not cve_id:
            continue
        seen_cves.add(str(cve_id))
        cve_items.append(_cve_item(cve))

    recent_cves, _ = search_cves(
        conn,
        query=None,
        severities=None,
        min_cvss=None,
        missing_description=None,
        missing_products=None,
        kev=None,
        after=None,
        before=None,
        vendor_keywords=None,
        product_keywords=None,
        in_scope=None,
        settings=None,
        page=1,
        page_size=200,
    )
    def _cve_sort_key(cve: dict[str, object]) -> datetime:
        ts = cve.get("published_at") or cve.get("last_modified_at")
        parsed = _parse_ts(str(ts)) if ts else None
        return parsed or datetime.min.replace(tzinfo=timezone.utc)

    recent_cves.sort(key=_cve_sort_key, reverse=True)
    recent_cve_items = []
    seen_recent: set[str] = set()
    for cve in recent_cves:
        cve_id = cve.get("cve_id")
        if not cve_id or str(cve_id) in seen_recent:
            continue
        seen_recent.add(str(cve_id))
        recent_cve_items.append(_cve_item(cve))

    if len(cve_items) < min_items:
        for item in recent_cve_items:
            if len(cve_items) >= min_items:
                break
            cve_id = item.get("cve_id")
            if not cve_id or str(cve_id) in seen_cves:
                continue
            seen_cves.add(str(cve_id))
            cve_items.append(item)

    (cve_dir / "today.json").write_text(json.dumps(cve_items, indent=2), encoding="utf-8")
    (cve_dir / "recent.json").write_text(json.dumps(recent_cve_items[: max(min_items, len(cve_items))], indent=2), encoding="utf-8")

    # Build per-day front-page feed JSON for client-side day pagination.
    # Write under static so Hugo publishes it to /feed/...
    feed_dir = Path(site_root) / "static" / "feed"
    feed_days_dir = feed_dir / "days"
    feed_days_dir.mkdir(parents=True, exist_ok=True)
    for old in feed_days_dir.glob("*.json"):
        try:
            old.unlink()
        except Exception:
            pass

    feed_entries: list[dict[str, object]] = []
    for article in items:
        feed_entries.append(
            {
                "kind": "article",
                "article_id": article.get("article_id") or article.get("id") or "",
                "published_epoch": int(article.get("published_epoch") or 0),
                "published_at_iso": article.get("published_at_iso") or "",
                "published_at_human": article.get("published_at_human") or "",
                "time_label": article.get("time_label") or "",
                "title": article.get("title") or "",
                "url": article.get("url") or "",
                "source": article.get("source") or "",
                "source_id": article.get("source_id") or "",
                "source_icon": article.get("source_icon") or "/img/source-default-news.svg",
                "summary": article.get("summary") or "",
                "summary_bullets": article.get("summary_bullets") or [],
                "tags": article.get("tags") or [],
                "vendors": article.get("vendors") or [],
                "product_items": article.get("product_items") or [],
                "products": article.get("products") or [],
                "vendor_products": article.get("vendor_products") or [],
                "threat_actors": article.get("threat_actors") or [],
                "facets": article.get("facets") or [],
                "event_keys": article.get("event_keys") or [],
                "nist_family": article.get("nist_family") or "",
            }
        )
    for cve in recent_cve_items:
        feed_entries.append(
            {
                "kind": "cve",
                "published_epoch": int(cve.get("published_epoch") or 0),
                "published_at_iso": cve.get("published_at_iso") or "",
                "published_at_human": cve.get("published_at_human") or "",
                "time_label": cve.get("time_label") or "",
                "cve_id": cve.get("cve_id") or "",
                "url": cve.get("url") or "",
                "nvd_url": cve.get("nvd_url") or cve.get("url") or "",
                "severity": cve.get("severity") or "",
                "product_title": cve.get("product_title") or "",
                "summary": cve.get("summary") or "",
                "vendors": cve.get("vendors") or [],
                "product_items": cve.get("product_items") or [],
                "products": cve.get("products") or [],
                "vendor_products": cve.get("vendor_products") or [],
                "threat_actors": cve.get("threat_actors") or [],
                "facets": cve.get("facets") or [],
                "kev_due_date": cve.get("kev_due_date") or "",
                "kev_known_exploited": bool(cve.get("kev_known_exploited")),
            }
        )

    feed_entries = [e for e in feed_entries if int(e.get("published_epoch") or 0) > 0]
    feed_entries.sort(key=lambda e: int(e.get("published_epoch") or 0), reverse=True)
    day_buckets: dict[str, list[dict[str, object]]] = {}
    for entry in feed_entries:
        epoch = int(entry.get("published_epoch") or 0)
        if epoch <= 0:
            continue
        day_key = datetime.fromtimestamp(epoch, tz).date().isoformat()
        day_buckets.setdefault(day_key, []).append(entry)

    day_keys = sorted(day_buckets.keys(), reverse=True)
    for day_key in day_keys:
        day_items = day_buckets.get(day_key, [])
        payload = {
            "day": day_key,
            "items": day_items,
            "counts": {
                "article": sum(1 for i in day_items if str(i.get("kind")) == "article"),
                "cve": sum(1 for i in day_items if str(i.get("kind")) == "cve"),
            },
        }
        (feed_days_dir / f"{day_key}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    feed_index = {
        "days": day_keys,
        "latest_day": day_keys[0] if day_keys else "",
        "oldest_day": day_keys[-1] if day_keys else "",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    (feed_dir / "index.json").write_text(json.dumps(feed_index, indent=2), encoding="utf-8")
    _write_product_data_files(conn, site_root, tz_name, logger)
    _write_sources_data_files(conn, data_root, logger)
    _write_cve_pages(conn, site_root, tz_name, logger)
    log_event(
        logger,
        logging.INFO,
        "feed_data_refreshed",
        today=len(today_items),
        recent=len(recent_items),
        path=str(data_dir),
    )
    return {"today": len(today_items), "recent": len(recent_items)}


def _write_sources_data_files(conn, data_root: str, logger: logging.Logger) -> dict[str, int]:
    data_dir = Path(data_root)
    data_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    chart_days = 30
    chart_since = (now - timedelta(days=chart_days)).date().isoformat()
    chart_data = list_articles_per_day(conn, chart_since)
    stats_days = 7
    stats_runs = 20
    stats = get_source_stats(conn, stats_days, stats_runs)
    stats_by_id = {item.get("source_id"): item for item in stats if item.get("source_id")}
    sources = list_sources(conn, enabled_only=False)
    rows: list[dict[str, object]] = []
    for source in sources:
        status = "active"
        status_label = "Active"
        pause_until = source.pause_until or ""
        paused_reason = source.paused_reason or ""
        if not source.enabled:
            status = "disabled"
            status_label = "Disabled"
        elif pause_until:
            try:
                paused_until_dt = _parse_iso(pause_until)
                if paused_until_dt > now:
                    status = "paused"
                    status_label = f"Paused until {pause_until}"
            except Exception:
                status = "paused"
                status_label = "Paused"
        stats_row = stats_by_id.get(source.id) or {}
        rows.append(
            {
                "name": source.name,
                "url": source.base_url or "",
                "status": status,
                "status_label": status_label,
                "enabled": bool(source.enabled),
                "pause_until": pause_until,
                "paused_reason": paused_reason,
                "topic_key": source.topic_key or "",
                "articles_per_day_avg": stats_row.get("articles_per_day_avg"),
            }
        )
    rows.sort(key=lambda item: str(item.get("name") or "").lower())
    payload = {
        "sources": rows,
        "articles_per_day": chart_data,
        "articles_per_day_days": chart_days,
    }
    (data_dir / "sources.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if logger:
        log_event(logger, logging.INFO, "sources_data_written", count=len(rows))
    return {"count": len(rows)}






def _yaml_escape_title(value: str) -> str:
    cleaned = (value or "").replace("\\", "")
    cleaned = cleaned.replace("'", "''")
    return f"'{cleaned}'"


def _yaml_escape_value(value: object) -> str:
    if value is None:
        return "''"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    return _yaml_escape_title(text)


def _clean_entity_name(value: object) -> str:
    name = str(value or "").strip()
    if not name:
        return ""
    lowered = name.lower()
    if lowered in {"unknown", "n/a", "none", "undefined"}:
        return ""
    return name


def _summary_from_llm(raw: object) -> str:
    if not raw:
        return ""
    if isinstance(raw, dict):
        return str(raw.get("summary") or "").strip()
    text = str(raw).strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    candidate = text
    if "{" in text and "}" in text:
        candidate = text[text.find("{"): text.rfind("}") + 1]
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return str(parsed.get("summary") or "").strip() or text
    except Exception:
        pass
    return text


def _summary_payload_from_llm(raw: object) -> dict[str, object]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    text = str(raw).strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    candidate = text
    if "{" in text and "}" in text:
        candidate = text[text.find("{"): text.rfind("}") + 1]
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def _ensure_section_index(path: Path, title: str, type_name: str) -> None:
    payload = "\n".join(
        [
            "---",
            f"title: \"{title}\"",
            f"date: {utc_now_iso().split('T')[0]}",
            f"type: {type_name}",
            "---",
            "",
        ]
    )
    if not path.exists():
        atomic_write_text(path, payload, encoding="utf-8")
        return
    try:
        current = path.read_text(encoding="utf-8")
    except Exception:
        atomic_write_text(path, payload, encoding="utf-8")
        return
    for line in current.splitlines():
        if line.strip().lower().startswith("title:"):
            if f"\"{title}\"" in line:
                return
            break
    atomic_write_text(path, payload, encoding="utf-8")


def _purge_generated_entity_content(site_root: str, folders: tuple[str, ...]) -> None:
    content_dir = Path(site_root) / "content"
    for folder in folders:
        target_dir = content_dir / folder
        if not target_dir.exists():
            continue
        for md_path in target_dir.glob("*.md"):
            md_path.unlink()


def _write_product_data_files(conn, site_root: str, tz_name: str, logger: logging.Logger) -> dict[str, int]:
    data_dir = Path(site_root) / "data" / "products"
    data_dir.mkdir(parents=True, exist_ok=True)
    index_path = data_dir / "index.json"

    def _safe_key(product_key: str) -> str:
        return product_key.replace(":", "__")

    def _summary_text(raw: object) -> str:
        if not raw:
            return ""
        if isinstance(raw, dict):
            return str(raw.get("summary") or "").strip()
        text = str(raw).strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            text = text.strip()
        candidate = text
        if "{" in text and "}" in text:
            candidate = text[text.find("{"): text.rfind("}") + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return str(parsed.get("summary") or "").strip() or text
        except Exception:
            pass
        return text

    def _summary_bullets(raw: object) -> list[str]:
        parsed = _summary_payload_from_llm(raw)
        if not parsed:
            return []
        bullets = parsed.get("bullets") or parsed.get("key_points") or parsed.get("tldr") or []
        if not isinstance(bullets, list):
            return []
        return [str(item).strip() for item in bullets if str(item).strip()][:8]

    products = list_products_with_article_counts(conn, limit=200)
    index_items = []
    for product in products:
        product_key = product.get("product_key") or ""
        if not product_key:
            continue
        safe_key = _safe_key(str(product_key))
        index_items.append(
            {
                "product_key": product_key,
                "safe_key": safe_key,
                "product_name": product.get("product_name") or "",
                "vendor_name": product.get("vendor_name") or "",
                "article_count": int(product.get("article_count") or 0),
            }
        )
        article_items = []
        articles, _ = list_articles_for_product(conn, int(product["product_id"]), page=1, page_size=20)
        for item in articles:
            if _is_article_suppressed(item.get("meta_json")):
                continue
            published_at = item.get("published_at") or item.get("ingested_at")
            article_items.append(
                {
                    "id": item.get("id"),
                    "title": item.get("title") or "",
                    "source": item.get("source_name") or "",
                    "published_at_iso": published_at or "",
                    "published_at_human": _format_human_ts(published_at, tz_name),
                    "url": item.get("original_url") or "",
                    "summary": _summary_text(item.get("summary_llm")),
                    "summary_bullets": _summary_bullets(item.get("summary_llm")),
                    "tags": [t for t in (item.get("tags") or "").split(",") if t],
                }
            )
        cves, _ = get_product_cves(conn, int(product["product_id"]), severity_min=None, severities=None, page=1, page_size=20)
        cve_items = []
        for cve in cves:
            cve_items.append(
                {
                    "cve_id": cve.get("cve_id"),
                    "published_at": cve.get("published_at") or "",
                    "severity": cve.get("preferred_base_severity") or "",
                    "score": cve.get("preferred_base_score"),
                    "summary": cve.get("summary") or "",
                    "kev_due_date": cve.get("kev_due_date") or "",
                    "kev_known_exploited": bool(cve.get("kev_known_exploited")),
                }
            )
        data_path = data_dir / f"{safe_key}.json"
        data_path.write_text(
            json.dumps(
                {
                    "product_key": product_key,
                    "safe_key": safe_key,
                    "product_name": product.get("product_name") or "",
                    "vendor_name": product.get("vendor_name") or "",
                    "articles": article_items,
                    "cves": cve_items,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    index_path.write_text(json.dumps(index_items, indent=2), encoding="utf-8")
    _write_vendor_product_indexes(conn, site_root, tz_name, logger)
    _write_threat_indexes(conn, site_root, tz_name, logger)
    return {"products": len(index_items)}


def _article_vendor_product_select_cols(columns: set[str]) -> list[str]:
    select_cols = [
        "a.id",
        "a.title",
        "s.name AS source_name",
        "a.original_url",
        "a.published_at",
        "a.ingested_at",
    ]
    if "summary_llm" in columns:
        select_cols.append("a.summary_llm")
    return select_cols


def _cve_vendor_product_select_cols(columns: set[str]) -> list[str]:
    select_cols = [
        "cve_id",
        "published_at",
        "last_modified_at",
        "preferred_base_severity",
    ]
    if "description_text" in columns:
        select_cols.append("description_text")
    return select_cols


def _write_vendor_product_indexes(
    conn, site_root: str, tz_name: str, logger: logging.Logger
) -> dict[str, int]:
    data_root = Path(site_root) / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    vendors_path = data_root / "vendors.json"
    products_path = data_root / "products.json"
    vendor_map_path = data_root / "vendor_map.json"
    product_map_path = data_root / "product_map.json"

    if not (_table_exists(conn, "products") and _table_exists(conn, "vendors")):
        atomic_write_json(vendors_path, [], indent=2)
        atomic_write_json(products_path, [], indent=2)
        atomic_write_json(vendor_map_path, {}, indent=2)
        atomic_write_json(product_map_path, {}, indent=2)
        return {"vendors": 0, "products": 0}

    cursor = conn.execute(
        """
        SELECT p.id, p.product_key, p.display_name, v.display_name
        FROM products p
        JOIN vendors v ON v.id = p.vendor_id
        """
    )
    products_rows = cursor.fetchall()

    article_counts: dict[int, int] = {}
    if _table_exists(conn, "article_products"):
        cursor = conn.execute(
            "SELECT product_id, COUNT(DISTINCT article_id) FROM article_products GROUP BY product_id"
        )
        article_counts = {int(row[0]): int(row[1] or 0) for row in cursor.fetchall()}

    cve_counts: dict[int, int] = {}
    if _table_exists(conn, "cve_products"):
        cursor = conn.execute(
            "SELECT product_id, COUNT(DISTINCT cve_id) FROM cve_products GROUP BY product_id"
        )
        cve_counts = {int(row[0]): int(row[1] or 0) for row in cursor.fetchall()}

    product_by_id: dict[int, dict[str, str]] = {}
    product_id_by_slug: dict[str, int] = {}
    vendor_display_by_slug: dict[str, str] = {}
    products_index: list[dict[str, object]] = []

    for row in products_rows:
        product_id = int(row[0])
        product_key = str(row[1] or "")
        product_name = _clean_entity_name(row[2])
        vendor_name = _clean_entity_name(row[3])
        if not product_name or not vendor_name:
            continue
        vendor_slug = slugify(vendor_name)
        product_slug = slugify(f"{vendor_name} {product_name}")
        product_by_id[product_id] = {
            "product_key": product_key,
            "product_name": product_name,
            "vendor_name": vendor_name,
            "vendor_slug": vendor_slug,
            "product_slug": product_slug,
        }
        product_id_by_slug[product_slug] = product_id
        vendor_display_by_slug.setdefault(vendor_slug, vendor_name)
        article_count = int(article_counts.get(product_id, 0))
        cve_count = int(cve_counts.get(product_id, 0))
        products_index.append(
            {
                "slug": product_slug,
                "display_name": product_name,
                "vendor_name": vendor_name,
                "vendor_slug": vendor_slug,
                "article_count": article_count,
                "cve_count": cve_count,
                "total_count": article_count + cve_count,
            }
        )

    product_article_ids: dict[int, set[int]] = {}
    vendor_article_ids: dict[str, set[int]] = {}
    article_product_ids: dict[int, set[int]] = {}
    if _table_exists(conn, "article_products"):
        cursor = conn.execute(
            """
            SELECT ap.article_id, ap.product_id, v.display_name, p.display_name
            FROM article_products ap
            JOIN products p ON p.id = ap.product_id
            JOIN vendors v ON v.id = p.vendor_id
            """
        )
        for row in cursor.fetchall():
            article_id = int(row[0])
            product_id = int(row[1])
            vendor_name = _clean_entity_name(row[2])
            product_name = _clean_entity_name(row[3])
            if product_id not in product_by_id or not vendor_name or not product_name:
                continue
            vendor_slug = product_by_id[product_id]["vendor_slug"]
            product_article_ids.setdefault(product_id, set()).add(article_id)
            vendor_article_ids.setdefault(vendor_slug, set()).add(article_id)
            article_product_ids.setdefault(article_id, set()).add(product_id)

    product_cve_ids: dict[int, set[str]] = {}
    vendor_cve_ids: dict[str, set[str]] = {}
    cve_product_ids: dict[str, set[int]] = {}
    if _table_exists(conn, "cve_products"):
        cursor = conn.execute(
            """
            SELECT cp.cve_id, cp.product_id, v.display_name, p.display_name
            FROM cve_products cp
            JOIN products p ON p.id = cp.product_id
            JOIN vendors v ON v.id = p.vendor_id
            """
        )
        for row in cursor.fetchall():
            cve_id = str(row[0] or "")
            product_id = int(row[1])
            vendor_name = _clean_entity_name(row[2])
            product_name = _clean_entity_name(row[3])
            if not cve_id or product_id not in product_by_id or not vendor_name or not product_name:
                continue
            vendor_slug = product_by_id[product_id]["vendor_slug"]
            product_cve_ids.setdefault(product_id, set()).add(cve_id)
            vendor_cve_ids.setdefault(vendor_slug, set()).add(cve_id)
            cve_product_ids.setdefault(cve_id, set()).add(product_id)

    all_article_ids = sorted({aid for ids in vendor_article_ids.values() for aid in ids})
    article_meta: dict[int, dict[str, object]] = {}
    if all_article_ids:
        article_columns: set[str] = set()
        if column_exists(conn, "articles", "summary_llm"):
            article_columns.add("summary_llm")
        chunk_size = 500
        for i in range(0, len(all_article_ids), chunk_size):
            chunk = all_article_ids[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            select_cols = _article_vendor_product_select_cols(article_columns)
            cursor = conn.execute(
                f"""
                SELECT {", ".join(select_cols)}
                FROM articles a
                LEFT JOIN sources s ON s.id = a.source_id
                WHERE a.id IN ({placeholders})
                """,
                tuple(chunk),
            )
            for row in cursor.fetchall():
                article_id = int(row[0])
                published_at = row[4] or row[5] or ""
                summary_payload = _summary_payload_from_llm(row[6]) if "summary_llm" in article_columns else {}
                summary = (
                    str(summary_payload.get("summary") or "").strip()
                    if summary_payload
                    else _summary_from_llm(row[6]) if "summary_llm" in article_columns else ""
                )
                summary_bullets = []
                if summary_payload:
                    bullets = (
                        summary_payload.get("bullets")
                        or summary_payload.get("key_points")
                        or summary_payload.get("tldr")
                        or []
                    )
                    if isinstance(bullets, list):
                        summary_bullets = [
                            str(item).strip() for item in bullets if str(item).strip()
                        ][:8]
                if len(summary) > 240:
                    summary = summary[:237].rstrip() + "..."
                product_ids = article_product_ids.get(article_id, set())
                vendors = []
                products = []
                seen_vendor = set()
                seen_product = set()
                for pid in product_ids:
                    meta = product_by_id.get(pid)
                    if not meta:
                        continue
                    vendor_slug = meta["vendor_slug"]
                    product_slug = meta["product_slug"]
                    if vendor_slug not in seen_vendor:
                        vendors.append(
                            {"slug": vendor_slug, "display_name": meta["vendor_name"]}
                        )
                        seen_vendor.add(vendor_slug)
                    if product_slug not in seen_product:
                        products.append(
                            {"slug": product_slug, "display_name": meta["product_name"]}
                        )
                        seen_product.add(product_slug)
                article_meta[article_id] = {
                    "id": article_id,
                    "title": row[1] or "",
                    "source": row[2] or "",
                    "published_at_iso": published_at,
                    "published_at_human": _format_human_ts(published_at, tz_name),
                    "url": row[3] or "",
                    "summary": summary,
                    "summary_bullets": summary_bullets,
                    "tags": [],
                    "vendors": vendors,
                    "product_items": products,
                    "threat_actors": get_article_threat_actors(conn, article_id),
                }

    all_cve_ids = sorted({cid for ids in vendor_cve_ids.values() for cid in ids})
    cve_meta: dict[str, dict[str, object]] = {}
    if all_cve_ids:
        cve_columns: set[str] = set()
        if column_exists(conn, "cves", "description_text"):
            cve_columns.add("description_text")
        versions_by_cve: dict[str, list[str]] = {}
        if _table_exists(conn, "cve_product_versions"):
            cursor = conn.execute(
                """
                SELECT cve_id, version
                FROM cve_product_versions
                WHERE cve_id = ANY(%s)
                """,
                (all_cve_ids,),
            )
            for cve_id, version in cursor.fetchall():
                if not cve_id or not version:
                    continue
                versions_by_cve.setdefault(str(cve_id), []).append(str(version))
        chunk_size = 500
        for i in range(0, len(all_cve_ids), chunk_size):
            chunk = all_cve_ids[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            select_cols = _cve_vendor_product_select_cols(cve_columns)
            cursor = conn.execute(
                f"""
                SELECT {", ".join(select_cols)}
                FROM cves
                WHERE cve_id IN ({placeholders})
                """,
                tuple(chunk),
            )
            for row in cursor.fetchall():
                cve_id = str(row[0] or "")
                if not cve_id:
                    continue
                published_at = row[1] or row[2] or ""
                summary = ""
                if "description_text" in cve_columns and len(row) >= 5:
                    summary = (row[4] or "").strip()
                if len(summary) > 240:
                    summary = summary[:237].rstrip() + "..."
                product_ids = cve_product_ids.get(cve_id, set())
                vendors = []
                products = []
                seen_vendor = set()
                seen_product = set()
                for pid in product_ids:
                    meta = product_by_id.get(pid)
                    if not meta:
                        continue
                    vendor_slug = meta["vendor_slug"]
                    product_slug = meta["product_slug"]
                    if vendor_slug not in seen_vendor:
                        vendors.append(
                            {"slug": vendor_slug, "display_name": meta["vendor_name"]}
                        )
                        seen_vendor.add(vendor_slug)
                    if product_slug not in seen_product:
                        products.append(
                            {"slug": product_slug, "display_name": meta["product_name"]}
                        )
                        seen_product.add(product_slug)
                kev = get_cve_kev(conn, cve_id)
                product_title = ""
                if vendors and products:
                    product_title = f"{vendors[0]['display_name']} — {products[0]['display_name']}"
                elif products:
                    product_title = products[0]['display_name']
                elif vendors:
                    product_title = vendors[0]['display_name']
                severity = (row[3] or "").strip()
                if product_title and severity:
                    product_title = f"{product_title} — {severity}"
                elif severity:
                    product_title = severity
                cve_meta[cve_id] = {
                    "cve_id": cve_id,
                    "severity": severity,
                    "published_at_iso": published_at,
                    "published_at_human": _format_human_ts(published_at, tz_name),
                    "summary": summary,
                    "product_title": product_title,
                    "title": product_title or cve_id,
                    "url": _cve_page_url(cve_id),
                    "nvd_url": f"https://nvd.nist.gov/vuln/detail/{cve_id}",
                    "vendors": vendors,
                    "product_items": products,
                    "versions": versions_by_cve.get(cve_id, []),
                    "threat_actors": get_cve_threat_actors(conn, cve_id),
                    "kev_due_date": kev.get("due_date") if kev else "",
                    "kev_known_exploited": bool(kev),
                }

    vendors_index: list[dict[str, object]] = []
    vendor_map: dict[str, dict[str, object]] = {}
    for vendor_slug, vendor_name in vendor_display_by_slug.items():
        article_ids = sorted(vendor_article_ids.get(vendor_slug, set()))
        cve_ids = sorted(vendor_cve_ids.get(vendor_slug, set()))
        article_count = len(article_ids)
        cve_count = len(cve_ids)
        total_count = article_count + cve_count
        if total_count == 0:
            continue
        products_for_vendor = sorted(
            {
                meta["product_slug"]
                for meta in product_by_id.values()
                if meta["vendor_slug"] == vendor_slug
            }
        )
        article_items = [article_meta.get(aid) for aid in article_ids if aid in article_meta]
        cve_items = [cve_meta.get(cid) for cid in cve_ids if cid in cve_meta]
        vendors_index.append(
            {
                "slug": vendor_slug,
                "display_name": vendor_name,
                "article_count": article_count,
                "cve_count": cve_count,
                "total_count": total_count,
            }
        )
        vendor_map[vendor_slug] = {
            "slug": vendor_slug,
            "display_name": vendor_name,
            "article_count": article_count,
            "cve_count": cve_count,
            "total_count": total_count,
            "products": products_for_vendor,
            "articles": article_ids,
            "cves": cve_ids,
            "article_items": article_items,
            "cve_items": cve_items,
        }

    filtered_products = [p for p in products_index if int(p.get("total_count") or 0) > 0]
    product_map: dict[str, dict[str, object]] = {}
    for product in filtered_products:
        product_slug = str(product.get("slug") or "")
        if not product_slug:
            continue
        product_id = product_id_by_slug.get(product_slug)
        if product_id is None:
            continue
        article_ids = sorted(product_article_ids.get(product_id, set()))
        cve_ids = sorted(product_cve_ids.get(product_id, set()))
        article_items = [article_meta.get(aid) for aid in article_ids if aid in article_meta]
        cve_items = [cve_meta.get(cid) for cid in cve_ids if cid in cve_meta]
        product_map[product_slug] = {
            "slug": product_slug,
            "display_name": product.get("display_name") or "",
            "vendor_name": product.get("vendor_name") or "",
            "vendor_slug": product.get("vendor_slug") or "",
            "article_count": int(product.get("article_count") or 0),
            "cve_count": int(product.get("cve_count") or 0),
            "total_count": int(product.get("total_count") or 0),
            "vendors": [product.get("vendor_slug") or ""],
            "articles": article_ids,
            "cves": cve_ids,
            "article_items": article_items,
            "cve_items": cve_items,
        }

    vendors_index.sort(key=lambda item: item.get("total_count", 0), reverse=True)
    filtered_products.sort(key=lambda item: item.get("total_count", 0), reverse=True)

    atomic_write_json(vendors_path, vendors_index, indent=2)
    atomic_write_json(products_path, filtered_products, indent=2)
    atomic_write_json(vendor_map_path, vendor_map, indent=2)
    atomic_write_json(product_map_path, product_map, indent=2)

    # Entity pages are deprecated in favor of /entities search. Keep only JSON data.
    _purge_generated_entity_content(site_root, ("vendor", "vendors", "product", "products"))

    return {"vendors": len(vendors_index), "products": len(products_index)}



def _write_cve_pages(conn, site_root: str, tz_name: str, logger: logging.Logger) -> dict[str, int]:
    if not _table_exists(conn, "cves"):
        return {"cves": 0}
    select_cols = [
        "cve_id",
        "published_at",
        "last_modified_at",
        "preferred_cvss_version",
        "preferred_base_score",
        "preferred_base_severity",
        "preferred_vector",
        "description_text",
        "affected_products_json",
        "affected_cpes_json",
        "reference_domains_json",
        "cvss_v31_json",
        "cvss_v40_json",
        "cvss_v31_list_json",
        "cvss_v40_list_json",
        "updated_at",
    ]
    select_cols = [col for col in select_cols if column_exists(conn, "cves", col)]
    if "cve_id" not in select_cols:
        return {"cves": 0}

    cursor = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM cves ORDER BY last_modified_at DESC"
    )
    rows = cursor.fetchall()
    data_rows = [dict(zip(select_cols, row)) for row in rows]
    cve_ids = [str(row.get("cve_id") or "") for row in data_rows if row.get("cve_id")]
    if not cve_ids:
        return {"cves": 0}

    vendor_map: dict[str, list[dict[str, str]]] = {}
    product_map: dict[str, list[dict[str, str]]] = {}
    vendor_product_map: dict[str, list[dict[str, str]]] = {}
    if _table_exists(conn, "cve_products") and _table_exists(conn, "products") and _table_exists(conn, "vendors"):
        cursor = conn.execute(
            """
            SELECT cp.cve_id, v.display_name, p.display_name
            FROM cve_products cp
            JOIN products p ON p.id = cp.product_id
            JOIN vendors v ON v.id = p.vendor_id
            """
        )
        for cve_id, vendor_name, product_name in cursor.fetchall():
            cve_id = str(cve_id or "")
            vendor_name = _clean_entity_name(vendor_name)
            product_name = _clean_entity_name(product_name)
            if not cve_id or not vendor_name or not product_name:
                continue
            vendor_slug = slugify(vendor_name)
            product_slug = slugify(f"{vendor_name} {product_name}")
            vendor_product_map.setdefault(cve_id, []).append(
                {
                    "vendor": vendor_name,
                    "product": product_name,
                    "vendor_slug": vendor_slug,
                    "product_slug": product_slug,
                }
            )
            vendor_list = vendor_map.setdefault(cve_id, [])
            if not any(item["slug"] == vendor_slug for item in vendor_list):
                vendor_list.append({"slug": vendor_slug, "display_name": vendor_name})
            product_list = product_map.setdefault(cve_id, [])
            if not any(item["slug"] == product_slug for item in product_list):
                product_list.append({"slug": product_slug, "display_name": product_name})

    version_map: dict[str, list[str]] = {}
    if _table_exists(conn, "cve_product_versions") and _table_exists(conn, "products") and _table_exists(conn, "vendors"):
        cursor = conn.execute(
            """
            SELECT cpv.cve_id, v.display_name, p.display_name, cpv.version
            FROM cve_product_versions cpv
            JOIN products p ON p.id = cpv.product_id
            JOIN vendors v ON v.id = p.vendor_id
            """
        )
        for cve_id, vendor_name, product_name, version in cursor.fetchall():
            cve_id = str(cve_id or "")
            vendor_name = _clean_entity_name(vendor_name)
            product_name = _clean_entity_name(product_name)
            if not cve_id or not vendor_name or not product_name or not version:
                continue
            version_map.setdefault(cve_id, []).append(f"{vendor_name}:{product_name}:{version}")

    article_map: dict[str, list[dict[str, object]]] = {}
    if _table_exists(conn, "article_cves") and _table_exists(conn, "articles"):
        article_columns: set[str] = set()
        if column_exists(conn, "articles", "summary_llm"):
            article_columns.add("summary_llm")
        cursor = conn.execute(
            """
            SELECT ac.cve_id, a.id, a.title, s.name, a.original_url, a.published_at, a.ingested_at, a.summary_llm
            FROM article_cves ac
            JOIN articles a ON a.id = ac.article_id
            LEFT JOIN sources s ON s.id = a.source_id
            ORDER BY a.published_at DESC NULLS LAST, a.ingested_at DESC NULLS LAST
            """
        )
        for row in cursor.fetchall():
            cve_id = str(row[0] or "")
            if not cve_id:
                continue
            items = article_map.setdefault(cve_id, [])
            if len(items) >= 50:
                continue
            published_at = row[5] or row[6] or ""
            summary_payload = _summary_payload_from_llm(row[7]) if "summary_llm" in article_columns else {}
            summary = (
                str(summary_payload.get("summary") or "").strip()
                if summary_payload
                else _summary_from_llm(row[7]) if "summary_llm" in article_columns else ""
            )
            if len(summary) > 240:
                summary = summary[:237].rstrip() + "..."
            items.append(
                {
                    "id": int(row[1]),
                    "title": row[2] or "",
                    "source": row[3] or "",
                    "url": row[4] or "",
                    "published_at": published_at,
                    "published_at_human": _format_human_ts(published_at, tz_name),
                    "summary": summary,
                }
            )

    kev_map = get_cve_kev_map(conn, cve_ids) if cve_ids else {}

    cve_records: list[dict[str, object]] = []
    for row in data_rows:
        cve_id = str(row.get("cve_id") or "")
        if not cve_id:
            continue
        published_at = row.get("published_at") or ""
        last_modified_at = row.get("last_modified_at") or ""
        updated_at = row.get("updated_at") or ""
        preferred_base_score = row.get("preferred_base_score")
        preferred_base_severity = (row.get("preferred_base_severity") or "").strip()
        preferred_vector = (row.get("preferred_vector") or "").strip()
        description = (row.get("description_text") or "").strip()
        summary = description
        if len(summary) > 320:
            summary = summary[:317].rstrip() + "..."
        affected_products = json.loads(row.get("affected_products_json") or "[]")
        affected_cpes = json.loads(row.get("affected_cpes_json") or "[]")
        reference_domains = json.loads(row.get("reference_domains_json") or "[]")
        cvss_v31 = json.loads(row.get("cvss_v31_json") or "null")
        cvss_v40 = json.loads(row.get("cvss_v40_json") or "null")
        cvss_v31_list = json.loads(row.get("cvss_v31_list_json") or "[]")
        cvss_v40_list = json.loads(row.get("cvss_v40_list_json") or "[]")

        date_value = published_at or last_modified_at or updated_at or utc_now_iso()
        parsed_date = _parse_ts(str(date_value)) if date_value else None
        date_string = (parsed_date or datetime.now(timezone.utc)).date().isoformat()
        published_epoch = int(parsed_date.timestamp()) if parsed_date else 0

        vendors = vendor_map.get(cve_id, [])
        products = product_map.get(cve_id, [])
        vendor_products = vendor_product_map.get(cve_id, [])
        versions = version_map.get(cve_id, [])
        articles = article_map.get(cve_id, [])
        kev = kev_map.get(cve_id)
        nvd_url = f"https://nvd.nist.gov/vuln/detail/{cve_id}"
        product_title = ""
        if vendor_products:
            first_vp = vendor_products[0]
            vendor_name = str(first_vp.get("vendor") or "").strip()
            product_name = str(first_vp.get("product") or "").strip()
            if vendor_name and product_name:
                product_title = f"{vendor_name} — {product_name}"
            else:
                product_title = product_name or vendor_name
        elif vendors and products:
            product_title = f"{vendors[0]['display_name']} — {products[0]['display_name']}"
        elif products:
            product_title = products[0]['display_name']
        elif vendors:
            product_title = vendors[0]['display_name']
        if product_title and preferred_base_severity:
            product_title = f"{product_title} — {preferred_base_severity}"
        elif preferred_base_severity:
            product_title = preferred_base_severity

        cve_records.append(
            {
                "cve_id": cve_id,
                "title": product_title or cve_id,
                "product_title": product_title,
                "description": description,
                "summary": summary,
                "date": date_string,
                "published_at": published_at,
                "last_modified_at": last_modified_at,
                "published_epoch": published_epoch,
                "published_at_human": _format_human_ts(published_at or last_modified_at or updated_at or "", tz_name),
                "severity": preferred_base_severity,
                "base_score": preferred_base_score,
                "vector": preferred_vector,
                "nvd_url": nvd_url,
                "url": _cve_page_url(cve_id),
                "vendors": vendors,
                "products": products,
                "vendor_products": vendor_products,
                "product_versions": versions,
                "articles": articles,
                "threat_actors": get_cve_threat_actors(conn, cve_id),
                "kev": kev if kev else {},
                "kev_known_exploited": bool(kev),
                "kev_due_date": kev.get("due_date") if kev else "",
                "affected_products": affected_products,
                "affected_cpes": affected_cpes,
                "reference_domains": reference_domains,
                "preferred_cvss_version": row.get("preferred_cvss_version") or "",
                "cvss_v31": cvss_v31,
                "cvss_v40": cvss_v40,
                "cvss_v31_list": cvss_v31_list,
                "cvss_v40_list": cvss_v40_list,
            }
        )

    cve_records.sort(
        key=lambda item: (
            int(item.get("published_epoch") or 0),
            str(item.get("last_modified_at") or ""),
            str(item.get("cve_id") or ""),
        ),
        reverse=True,
    )

    data_root = Path(site_root) / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    atomic_write_json(data_root / "cves.json", cve_records, indent=2)
    _purge_generated_entity_content(site_root, ("cves",))

    return {"cves": len(cve_ids)}

def _write_threat_indexes(
    conn, site_root: str, tz_name: str, logger: logging.Logger
) -> dict[str, int]:
    data_root = Path(site_root) / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    threats_path = data_root / "threats.json"
    threat_map_path = data_root / "threat_map.json"

    if not _table_exists(conn, "threat_actors"):
        atomic_write_json(threats_path, [], indent=2)
        atomic_write_json(threat_map_path, {}, indent=2)
        return {"threats": 0}

    cursor = conn.execute(
        """
        SELECT ta.id,
               ta.actor_key,
               ta.display_name,
               ta.actor_type,
               COUNT(DISTINCT ata.article_id) AS article_count,
               COUNT(DISTINCT cta.cve_id) AS cve_count
        FROM threat_actors ta
        LEFT JOIN article_threat_actors ata ON ata.actor_id = ta.id
        LEFT JOIN cve_threat_actors cta ON cta.actor_id = ta.id
        GROUP BY ta.id
        ORDER BY (COUNT(DISTINCT ata.article_id) + COUNT(DISTINCT cta.cve_id)) DESC, ta.display_name
        """
    )
    threat_rows = cursor.fetchall()

    threat_index: list[dict[str, object]] = []
    actor_ids: list[int] = []
    actor_key_by_id: dict[int, str] = {}
    actor_meta_by_id: dict[int, dict[str, object]] = {}
    for row in threat_rows:
        actor_id = int(row[0])
        actor_key = str(row[1] or "")
        display_name = str(row[2] or "").strip()
        if not actor_key or not display_name:
            continue
        article_count = int(row[4] or 0)
        cve_count = int(row[5] or 0)
        threat_index.append(
            {
                "slug": actor_key,
                "display_name": display_name,
                "actor_type": row[3] or "",
                "article_count": article_count,
                "cve_count": cve_count,
                "total_count": article_count + cve_count,
            }
        )
        actor_ids.append(actor_id)
        actor_key_by_id[actor_id] = actor_key
        actor_meta_by_id[actor_id] = {
            "slug": actor_key,
            "display_name": display_name,
            "actor_type": row[3] or "",
        }

    threat_article_ids: dict[int, set[int]] = {}
    if _table_exists(conn, "article_threat_actors") and actor_ids:
        cursor = conn.execute(
            """
            SELECT actor_id, article_id
            FROM article_threat_actors
            WHERE actor_id = ANY(%s)
            """,
            (actor_ids,),
        )
        for actor_id, article_id in cursor.fetchall():
            if actor_id is None or article_id is None:
                continue
            threat_article_ids.setdefault(int(actor_id), set()).add(int(article_id))

    threat_cve_ids: dict[int, set[str]] = {}
    if _table_exists(conn, "cve_threat_actors") and actor_ids:
        cursor = conn.execute(
            """
            SELECT actor_id, cve_id
            FROM cve_threat_actors
            WHERE actor_id = ANY(%s)
            """,
            (actor_ids,),
        )
        for actor_id, cve_id in cursor.fetchall():
            if actor_id is None or not cve_id:
                continue
            threat_cve_ids.setdefault(int(actor_id), set()).add(str(cve_id))

    all_article_ids = sorted({aid for ids in threat_article_ids.values() for aid in ids})
    article_meta: dict[int, dict[str, object]] = {}
    if all_article_ids:
        article_columns: set[str] = set()
        if column_exists(conn, "articles", "summary_llm"):
            article_columns.add("summary_llm")
        chunk_size = 500
        for i in range(0, len(all_article_ids), chunk_size):
            chunk = all_article_ids[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            select_cols = _article_vendor_product_select_cols(article_columns)
            cursor = conn.execute(
                f"""
                SELECT {", ".join(select_cols)}
                FROM articles a
                LEFT JOIN sources s ON s.id = a.source_id
                WHERE a.id IN ({placeholders})
                """,
                tuple(chunk),
            )
            for row in cursor.fetchall():
                article_id = int(row[0])
                published_at = row[4] or row[5] or ""
                summary = _summary_from_llm(row[6]) if "summary_llm" in article_columns else ""
                if len(summary) > 240:
                    summary = summary[:237].rstrip() + "..."
                article_meta[article_id] = {
                    "id": article_id,
                    "title": row[1] or "",
                    "source": row[2] or "",
                    "published_at_iso": published_at,
                    "published_at_human": _format_human_ts(published_at, tz_name),
                    "url": row[3] or "",
                    "summary": summary,
                }

    all_cve_ids = sorted({cid for ids in threat_cve_ids.values() for cid in ids})
    cve_meta: dict[str, dict[str, object]] = {}
    if all_cve_ids:
        cve_columns: set[str] = set()
        if column_exists(conn, "cves", "description_text"):
            cve_columns.add("description_text")
        chunk_size = 500
        for i in range(0, len(all_cve_ids), chunk_size):
            chunk = all_cve_ids[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            select_cols = _cve_vendor_product_select_cols(cve_columns)
            cursor = conn.execute(
                f"""
                SELECT {", ".join(select_cols)}
                FROM cves
                WHERE cve_id IN ({placeholders})
                """,
                tuple(chunk),
            )
            for row in cursor.fetchall():
                cve_id = str(row[0] or "")
                if not cve_id:
                    continue
                published_at = row[1] or row[2] or ""
                summary = ""
                if "description_text" in cve_columns and len(row) >= 5:
                    summary = (row[4] or "").strip()
                if len(summary) > 240:
                    summary = summary[:237].rstrip() + "..."
                cve_meta[cve_id] = {
                    "cve_id": cve_id,
                    "severity": (row[3] or "").strip(),
                    "published_at_iso": published_at,
                    "published_at_human": _format_human_ts(published_at, tz_name),
                    "summary": summary,
                    "url": f"https://nvd.nist.gov/vuln/detail/{cve_id}",
                }

    threat_map: dict[str, dict[str, object]] = {}
    for actor_id, meta in actor_meta_by_id.items():
        actor_key = actor_key_by_id.get(actor_id)
        if not actor_key:
            continue
        article_ids = sorted(threat_article_ids.get(actor_id, set()))
        cve_ids = sorted(threat_cve_ids.get(actor_id, set()))
        article_items = [article_meta.get(aid) for aid in article_ids if aid in article_meta]
        cve_items = [cve_meta.get(cid) for cid in cve_ids if cid in cve_meta]
        threat_map[actor_key] = {
            **meta,
            "article_count": len(article_ids),
            "cve_count": len(cve_ids),
            "total_count": len(article_ids) + len(cve_ids),
            "articles": article_ids,
            "cves": cve_ids,
            "article_items": article_items,
            "cve_items": cve_items,
        }

    atomic_write_json(threats_path, threat_index, indent=2)
    atomic_write_json(threat_map_path, threat_map, indent=2)

    # Threat pages are deprecated in favor of /entities search. Keep only JSON data.
    _purge_generated_entity_content(site_root, ("threat", "threats"))

    return {"threats": len(threat_index)}


def _get_today_articles_for_brief(
    conn,
    day: str,
    logger: logging.Logger,
) -> list[dict[str, object]]:
    rows = list_articles_for_day(conn, day)
    rows = [row for row in rows if not _is_article_suppressed(row.get("meta_json"))]
    if not rows:
        return []
    articles: list[dict[str, object]] = []
    for row in rows:
        article_id = row.get("id")
        if article_id is None:
            continue
        article = get_article_by_id(conn, int(article_id)) or {}
        source_name = get_source_name(conn, str(row.get("source_id") or "")) or ""
        title = str(row.get("title") or article.get("title") or "").strip()
        if not title:
            title = str(row.get("original_url") or article.get("original_url") or "").strip()
        summary_llm = row.get("summary_llm") or article.get("summary_llm")
        context_llm = row.get("context_llm") or article.get("context_llm")
        summary_text = ""
        summary_bullets: list[str] = []
        if summary_llm:
            try:
                parsed = json.loads(summary_llm)
                if isinstance(parsed, dict):
                    summary_text = str(parsed.get("summary") or "").strip()
                    bullets = parsed.get("bullets")
                    if isinstance(bullets, list):
                        summary_bullets = [
                            str(item).strip() for item in bullets if str(item).strip()
                        ][:8]
            except Exception:
                summary_text = _summary_from_llm(summary_llm)
        if not summary_text:
            summary_text = _summary_from_llm(summary_llm)
        if len(summary_text) > 800:
            summary_text = summary_text[:800].rstrip() + "..."
        content_text = article.get("content_text") or ""
        if len(content_text) > 1200:
            content_text = content_text[:1200].rstrip() + "..."
        cve_ids = list_article_cve_ids(conn, int(article_id))
        tags = get_article_tags(conn, int(article_id))
        articles.append(
            {
                "id": int(article_id),
                "source_name": source_name,
                "published_at": row.get("published_at") or row.get("ingested_at"),
                "canonical_url": row.get("original_url")
                or article.get("original_url")
                or article.get("normalized_url")
                or "",
                "original_url": row.get("original_url") or article.get("original_url") or "",
                "normalized_url": article.get("normalized_url") or "",
                "title": title,
                "summary_llm": summary_text,
                "summary_bullets": summary_bullets,
                "context_llm": context_llm,
                "content_excerpt": content_text,
                "tags": tags,
                "cves": cve_ids,
            }
        )
    return articles


def _get_daily_cves_for_brief(
    conn,
    day: str,
    *,
    limit: int = 60,
) -> list[dict[str, object]]:
    rows = list_cves_for_day(conn, day, limit=limit)
    if not rows:
        return []
    items: list[dict[str, object]] = []
    for row in rows:
        cve_id = str(row.get("cve_id") or "").strip()
        if not cve_id:
            continue
        vendor = ""
        product = ""
        vp_items = list_cve_vendor_products(conn, cve_id)
        if vp_items:
            vendor = str(vp_items[0].get("vendor_display") or "").strip()
            product = str(vp_items[0].get("product_display") or "").strip()
        kev = get_cve_kev(conn, cve_id) or None
        kev_status = bool(kev)
        kev_url = "https://www.cisa.gov/known-exploited-vulnerabilities-catalog" if kev_status else ""
        if not vendor and kev:
            vendor = str(kev.get("vendor_project") or "").strip()
        if not product and kev:
            product = str(kev.get("product") or "").strip()
        items.append(
            {
                "cve_id": cve_id,
                "vendor": vendor,
                "product": product,
                "url": _cve_page_url(cve_id),
                "nvd_url": f"https://nvd.nist.gov/vuln/detail/{cve_id}",
                "kev": kev_status,
                "kev_url": kev_url,
                "kev_due_date": kev.get("due_date") if kev else "",
            }
        )
    return items


def _ensure_articles_mapped_to_topics(
    articles: list[dict[str, object]],
    topic_payload: dict[str, object],
    logger: logging.Logger,
) -> list[dict[str, object]]:
    topics = topic_payload.get("topics") if isinstance(topic_payload, dict) else None
    article_topics = (
        topic_payload.get("article_topics") if isinstance(topic_payload, dict) else None
    )
    if not isinstance(topics, list):
        topics = []
    if not isinstance(article_topics, list):
        article_topics = []
    id_to_topic: dict[int, dict[str, object]] = {}
    for item in article_topics:
        if not isinstance(item, dict):
            continue
        article_id = item.get("id")
        topic_key = item.get("topic_key")
        if article_id is None or not topic_key:
            continue
        try:
            id_to_topic[int(article_id)] = item
        except Exception:
            continue
    topic_index: dict[str, dict[str, object]] = {}
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        key = str(topic.get("topic_key") or "").strip()
        if key:
            topic_index[key] = topic
    if not topics:
        topics = []
    unmapped: list[int] = []
    for article in articles:
        article_id = article.get("id")
        if article_id is None:
            continue
        if int(article_id) in id_to_topic:
            continue
        unmapped.append(int(article_id))
    if unmapped:
        logger.warning("daily_brief_unmapped_articles count=%s", len(unmapped))
        noise_key = "contextual:noise"
        if noise_key not in topic_index:
            noise_topic = {
                "topic_key": noise_key,
                "label": "Low-signal or contextual items",
                "importance": 0.1,
                "confidence": 0.2,
                "why": "auto_bucket_unmapped",
                "topic_type": "contextual",
            }
            topics.append(noise_topic)
            topic_index[noise_key] = noise_topic
        for article_id in unmapped:
            id_to_topic[int(article_id)] = {
                "id": article_id,
                "topic_key": noise_key,
                "confidence": 0.2,
            }
    return topics, list(id_to_topic.values())


def _filter_daily_brief_topics(
    topics: list[dict[str, object]],
    article_topics: list[dict[str, object]],
    logger: logging.Logger,
) -> tuple[list[dict[str, object]], list[dict[str, object]], int]:
    if not topics:
        return topics, article_topics, 0
    cleaned: list[dict[str, object]] = []
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        topic_type = str(topic.get("topic_type") or "").strip().lower()
        try:
            importance = float(topic.get("importance") or 0)
        except Exception:
            importance = 0.0
        if topic_type == "contextual" and importance < 0.3:
            continue
        cleaned.append(topic)
    cleaned.sort(key=lambda item: float(item.get("importance") or 0), reverse=True)
    if len(cleaned) > 15:
        cleaned = cleaned[:15]
    keep_keys = {str(item.get("topic_key") or "").strip() for item in cleaned if item.get("topic_key")}
    remapped = []
    noise_key = "contextual:noise"
    for mapping in article_topics:
        if not isinstance(mapping, dict):
            continue
        key = str(mapping.get("topic_key") or "").strip()
        if key and key in keep_keys:
            remapped.append(mapping)
            continue
        remapped.append({**mapping, "topic_key": noise_key})
    if any(item.get("topic_key") == noise_key for item in remapped):
        if noise_key not in keep_keys:
            cleaned.append(
                {
                    "topic_key": noise_key,
                    "label": "Low-signal or contextual items",
                    "importance": 0.1,
                    "confidence": 0.2,
                    "why": "auto_bucket_unmapped",
                    "topic_type": "contextual",
                }
            )
            keep_keys.add(noise_key)
    dropped = max(len(topics) - len(cleaned), 0)
    if dropped:
        logger.info("daily_brief_cluster_pruned dropped=%s kept=%s", dropped, len(cleaned))
    return cleaned, remapped, dropped


def _normalize_topic_summaries(
    summaries: object,
    logger: logging.Logger,
) -> dict[str, dict[str, object]]:
    normalized: dict[str, dict[str, object]] = {}
    if isinstance(summaries, dict):
        candidates = summaries.get("topics")
        if isinstance(candidates, list):
            summaries = candidates
    if not isinstance(summaries, list):
        return normalized
    for entry in summaries:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("topic_key") or "").strip()
        if not key:
            continue
        normalized[key] = entry
    return normalized


def _normalize_topic_families(
    mapping: object,
    logger: logging.Logger,
) -> dict[str, list[dict[str, object]]]:
    normalized: dict[str, list[dict[str, object]]] = {}
    if isinstance(mapping, dict):
        candidates = mapping.get("topics")
        if isinstance(candidates, list):
            mapping = candidates
    if not isinstance(mapping, list):
        return normalized
    for entry in mapping:
        if not isinstance(entry, dict):
            continue
        topic_key = str(entry.get("topic_key") or "").strip()
        if not topic_key:
            continue
        families = entry.get("families")
        if not isinstance(families, list):
            continue
        cleaned: list[dict[str, object]] = []
        for fam in families:
            if not isinstance(fam, dict):
                continue
            code = str(fam.get("family") or "").strip().upper()
            if not code or code not in NIST_FAMILY_TITLES:
                if code:
                    logger.warning("daily_brief_invalid_family topic_key=%s family=%s", topic_key, code)
                continue
            cleaned.append(
                {
                    "family": code,
                    "title": NIST_FAMILY_TITLES[code],
                }
            )
        if cleaned:
            normalized[topic_key] = cleaned
    return normalized


def _normalize_nist_breakdown(
    payload: object,
    logger: logging.Logger,
) -> list[dict[str, object]]:
    families: list[dict[str, object]] = []
    seen: dict[str, dict[str, object]] = {}
    if isinstance(payload, dict):
        candidates = payload.get("families") or payload.get("topics")
        if isinstance(candidates, list):
            payload = candidates
    if not isinstance(payload, list):
        return families
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        raw_family = str(entry.get("family_id") or entry.get("family") or "").strip().upper()
        family_id = NIST_FAMILY_REMAP.get(raw_family, raw_family)
        if not family_id or family_id not in NIST_FAMILY_TITLES:
            title_candidate = str(
                entry.get("family_title")
                or entry.get("title")
                or entry.get("family_name")
                or ""
            ).strip()
            family_id = _family_id_from_title(title_candidate)
        if not family_id or family_id not in NIST_FAMILY_TITLES:
            continue
        family_title = NIST_FAMILY_TITLES[family_id]
        summary = str(entry.get("summary") or entry.get("narrative") or "").strip()
        subtopics = []
        for sub in entry.get("subtopics") or []:
            if not isinstance(sub, dict):
                continue
            subtopics.append(
                {
                    "subtopic_id": str(sub.get("subtopic_id") or sub.get("id") or ""),
                    "title": str(sub.get("title") or "").strip(),
                    "severity": str(sub.get("severity") or "").strip(),
                    "narrative": str(sub.get("narrative") or "").strip(),
                    "citations": _normalize_list_field(sub.get("citations")),
                }
            )
        if subtopics:
            deduped = []
            seen_keys: set[tuple[str, str, tuple[str, ...]]] = set()
            for sub in subtopics:
                key = (
                    str(sub.get("title") or "").strip().lower(),
                    str(sub.get("narrative") or "").strip().lower(),
                    tuple(sub.get("citations") or []),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                deduped.append(sub)
            subtopics = deduped
        existing = seen.get(family_id)
        if existing:
            if summary and not existing.get("summary"):
                existing["summary"] = summary
            existing["subtopics"].extend(subtopics)
            continue
        family_entry = {
            "family_id": family_id,
            "family_title": family_title,
            "summary": summary,
            "subtopics": subtopics,
        }
        seen[family_id] = family_entry
        families.append(family_entry)
    return families


def _clean_summary_text(raw: object) -> str:
    if raw is None:
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
        if text.endswith("```"):
            text = text[: -len("```")].strip()
    candidate = text.strip()
    if candidate.startswith("{") or candidate.startswith("["):
        try:
            parsed = json.loads(candidate)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            summary = parsed.get("summary") or parsed.get("tldr") or parsed.get("highlights")
            if isinstance(summary, list):
                return " ".join(str(item).strip() for item in summary if str(item).strip())
            if isinstance(summary, str):
                return summary.strip()
            bullets = parsed.get("bullets") or parsed.get("key_points") or []
            if isinstance(bullets, list):
                return " ".join(str(item).strip() for item in bullets if str(item).strip())
        if isinstance(parsed, list):
            return " ".join(str(item).strip() for item in parsed if str(item).strip())
    return re.sub(r"\s+", " ", text).strip()


def _normalize_family_title_key(title: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
    return " ".join(cleaned.split())


FAMILY_TITLE_REMAP: dict[str, str] = {
    "access management": "AC",
    "access control": "AC",
    "incident response": "IR",
    "configuration management": "CM",
    "system and information integrity": "SI",
    "system and communications protection": "SC",
    "assessment authorization and monitoring": "CA",
    "personally identifiable information processing and transparency": "PT",
    "supply chain risk management": "SR",
    "awareness and training": "AT",
    "audit and accountability": "AU",
    "contingency planning": "CP",
    "identification and authentication": "IA",
    "maintenance": "MA",
    "media protection": "MP",
    "physical and environmental protection": "PE",
    "planning": "PL",
    "program management": "PM",
    "personnel security": "PS",
    "risk assessment": "RA",
    "system and services acquisition": "SA",
}


def _family_id_from_title(title: str) -> str:
    if not title:
        return ""
    key = _normalize_family_title_key(title)
    if key in FAMILY_TITLE_REMAP:
        return FAMILY_TITLE_REMAP[key]
    for code, fam_title in NIST_FAMILY_TITLES.items():
        if _normalize_family_title_key(fam_title) == key:
            return code
    return ""


NIST_FAMILY_TITLES = {
    "AC": "Access Control",
    "AT": "Awareness and Training",
    "AU": "Audit and Accountability",
    "CA": "Assessment, Authorization, and Monitoring",
    "CM": "Configuration Management",
    "CP": "Contingency Planning",
    "IA": "Identification and Authentication",
    "IR": "Incident Response",
    "MA": "Maintenance",
    "MP": "Media Protection",
    "PE": "Physical and Environmental Protection",
    "PL": "Planning",
    "PM": "Program Management",
    "PS": "Personnel Security",
    "PT": "Personally Identifiable Information Processing and Transparency",
    "RA": "Risk Assessment",
    "SA": "System and Services Acquisition",
    "SC": "System and Communications Protection",
    "SI": "System and Information Integrity",
    "SR": "Supply Chain Risk Management",
}

NIST_FAMILY_DETAILS = {
    "AC": "Access control policies, account management, privilege boundaries, and authorization enforcement.",
    "AT": "Security awareness and training requirements and execution.",
    "AU": "Audit logging, monitoring, and review of security-relevant events.",
    "CA": "Control assessments, continuous monitoring, and authorization decisions.",
    "CM": "Configuration baselines, change control, and system hardening.",
    "CP": "Contingency planning, backup, and recovery readiness.",
    "IA": "Identification, authentication, credential management, and identity assurance.",
    "IR": "Incident response readiness, detection, and handling.",
    "MA": "System maintenance and controlled servicing.",
    "MP": "Media protection, handling, and data at-rest safeguards.",
    "PE": "Physical and environmental protection controls.",
    "PL": "Security planning and governance artifacts.",
    "PM": "Program management controls and oversight.",
    "PS": "Personnel security and insider risk controls.",
    "PT": "Privacy, PII processing, and transparency controls.",
    "RA": "Risk assessment and threat modeling.",
    "SA": "System and service acquisition, supply chain, and third-party risk.",
    "SC": "System and communications protection, network boundaries, and cryptography.",
    "SI": "System and information integrity, vulnerability management, and malware protection.",
    "SR": "Supply chain risk management.",
}

NIST_FAMILY_REMAP: dict[str, str] = {}


def _normalize_list_field(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip().lower()
        if not text or text in {"none", "none provided", "n/a"}:
            return []
    return []


def _remap_citation_ids(
    values: object,
    article_to_citation: dict[int, int],
    max_citation_id: int,
) -> list[int]:
    if not isinstance(values, list):
        return []
    remapped: list[int] = []
    for item in values:
        try:
            cid = int(item)
        except Exception:
            continue
        if cid > max_citation_id and cid in article_to_citation:
            cid = article_to_citation[cid]
        if 1 <= cid <= max_citation_id:
            remapped.append(cid)
    # preserve order while deduping
    seen: set[int] = set()
    output: list[int] = []
    for cid in remapped:
        if cid in seen:
            continue
        seen.add(cid)
        output.append(cid)
    return output


def _remap_citations_in_text(
    text: str,
    article_to_citation: dict[int, int],
    max_citation_id: int,
) -> str:
    if not text:
        return text

    def _replace(match: re.Match[str]) -> str:
        try:
            cid = int(match.group(1))
        except Exception:
            return match.group(0)
        if 1 <= cid <= max_citation_id:
            return match.group(0)
        if cid in article_to_citation:
            return f"({article_to_citation[cid]})"
        return match.group(0)

    return re.sub(r"\((\d+)\)", _replace, text)


def _strip_tldr_prefix(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        return cleaned
    cleaned = re.sub(
        r"^\s*read this because[:\-\u2014]?\s+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned[:1].upper() + cleaned[1:]


def _sanitize_llm_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", text)
        if text.endswith("```"):
            text = text[: -len("```")].strip()
    candidate = text.strip()
    if candidate.startswith("{") or candidate.startswith("["):
        try:
            parsed = json.loads(candidate)
        except Exception:
            return re.sub(r"\s+", " ", text).strip()
        if isinstance(parsed, dict):
            for key in ("tldr_narrative", "technical_synthesis", "summary", "highlights", "text"):
                val = parsed.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
            return ""
        if isinstance(parsed, list):
            return " ".join(str(item).strip() for item in parsed if str(item).strip())
    return re.sub(r"\s+", " ", text).strip()


def _split_paragraphs(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"\n{2,}", text) if part.strip()]


def _split_sentences(text: str) -> list[str]:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        return []
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    return [part.strip() for part in parts if part.strip()]


def _format_technical_synthesis(technical: dict[str, object]) -> None:
    text = str(technical.get("text") or "").strip()
    if not text:
        return
    sentences = _split_sentences(text)
    if not sentences:
        return
    max_paragraphs = 5
    min_paragraphs = 3
    sentences_per_paragraph = 4
    paragraphs: list[str] = []
    cursor = 0
    while cursor < len(sentences) and len(paragraphs) < max_paragraphs:
        chunk = sentences[cursor : cursor + sentences_per_paragraph]
        cursor += sentences_per_paragraph
        paragraphs.append(" ".join(chunk))
    if len(paragraphs) < min_paragraphs and len(paragraphs) > 1:
        # redistribute to reach minimum paragraphs
        flat = " ".join(sentences)
        sentences = _split_sentences(flat)
        if sentences:
            paragraphs = []
            cursor = 0
            per_para = max(3, len(sentences) // min_paragraphs)
            while cursor < len(sentences) and len(paragraphs) < max_paragraphs:
                chunk = sentences[cursor : cursor + per_para]
                cursor += per_para
                paragraphs.append(" ".join(chunk))
    technical["text"] = "\n\n".join(paragraphs[:max_paragraphs])
    technical["citations"] = sorted(set(_collect_citations_from_text(technical["text"])))


def _ensure_min_technical_synthesis(
    technical: dict[str, object],
    families: list[dict[str, object]],
) -> None:
    text = str(technical.get("text") or "").strip()
    if not text:
        return
    paragraphs = _split_paragraphs(text)
    if len(paragraphs) >= 3 and len(text) >= 900:
        return
    fallback_parts: list[str] = []
    for family in families:
        if not isinstance(family, dict):
            continue
        summary = str(family.get("summary") or "").strip()
        narrative = ""
        for sub in family.get("subtopics") or []:
            if not isinstance(sub, dict):
                continue
            sub_text = str(sub.get("narrative") or "").strip()
            if sub_text:
                narrative = sub_text
                break
        if summary and narrative:
            fallback_parts.append(f"{summary} {narrative}")
        elif summary:
            fallback_parts.append(summary)
        elif narrative:
            fallback_parts.append(narrative)
        if len(fallback_parts) >= 6:
            break
    if not fallback_parts:
        return
    new_paragraphs: list[str] = []
    buffer: list[str] = []
    for part in fallback_parts:
        buffer.append(part)
        if len(buffer) == 2:
            new_paragraphs.append(" ".join(buffer))
            buffer = []
        if len(new_paragraphs) >= 4:
            break
    if buffer and len(new_paragraphs) < 4:
        new_paragraphs.append(" ".join(buffer))
    if not new_paragraphs:
        return
    technical["text"] = "\n\n".join(new_paragraphs[:5])
    technical["citations"] = sorted(set(_collect_citations_from_text(technical["text"])))


def _clean_url_for_display(url: str) -> str:
    if not url:
        return ""
    return url.split("#")[0]


def _normalize_canonical_url(url: str) -> str:
    if not url:
        return ""
    cleaned = _clean_url_for_display(url)
    try:
        parts = urlsplit(cleaned)
    except Exception:
        return cleaned
    if not parts.query:
        return cleaned
    params = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_l = key.lower()
        if key_l.startswith("utm_"):
            continue
        if key_l in {"intcid", "icid", "cmpid", "ref", "source", "fbclid", "gclid"}:
            continue
        params.append((key, value))
    query = urlencode(params, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _build_evidence_pack(
    topic: dict[str, object],
    articles: list[dict[str, object]],
    max_articles: int = 8,
    max_chars: int = 9000,
) -> str:
    lines = [
        f"TOPIC: {topic.get('label') or ''}",
        f"WHY: {topic.get('why') or ''}",
        "",
    ]
    total = 0
    count = 0
    for article in articles:
        if count >= max_articles:
            break
        title = str(article.get("title") or "").strip()
        source = str(article.get("source_name") or "").strip()
        url = str(article.get("canonical_url") or "").strip()
        summary = str(article.get("summary_llm") or "").strip()
        bullets = article.get("summary_bullets") or []
        excerpt = str(article.get("content_excerpt") or "").strip()
        cves = article.get("cves") or []
        tags = article.get("tags") or []
        bullet_text = "; ".join(str(item).strip() for item in bullets if str(item).strip())
        entry = [
            f"- [{article.get('id')}] {source} — {title}",
            f"  URL: {url}",
            f"  CVEs: {', '.join(cves) if cves else 'none'} | Tags: {', '.join(tags) if tags else 'none'}",
        ]
        if summary:
            entry.append(f"  Summary: {summary}")
        if bullet_text:
            entry.append(f"  Bullets: {bullet_text}")
        if excerpt:
            entry.append(f"  Excerpt: {excerpt}")
        entry.append("")
        block = "\n".join(entry)
        if total + len(block) > max_chars:
            break
        lines.append(block)
        total += len(block)
        count += 1
    return "\n".join(lines).strip()


def _build_daily_brief_citations(
    articles: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[int, int]]:
    sorted_articles = sorted(
        articles,
        key=lambda item: (
            str(item.get("source_name") or "").lower(),
            str(item.get("title") or "").lower(),
            int(item.get("id") or 0),
        ),
    )
    citations: list[dict[str, object]] = []
    article_to_cite: dict[int, int] = {}
    for idx, article in enumerate(sorted_articles, start=1):
        try:
            article_id = int(article.get("id") or 0)
        except Exception:
            continue
        url = _clean_url_for_display(str(article.get("canonical_url") or "").strip())
        if not url:
            url = _clean_url_for_display(str(article.get("normalized_url") or "").strip())
        if not url:
            url = _clean_url_for_display(str(article.get("original_url") or "").strip())
        title = str(article.get("title") or "").strip() or url
        source_name = str(article.get("source_name") or "").strip()
        summary_text = _clean_summary_text(article.get("summary_llm"))
        citations.append(
            {
                "id": idx,
                "article_id": article_id,
                "title": title,
                "source_name": source_name,
                "url": url,
                "summary": summary_text,
            }
        )
        article_to_cite[article_id] = idx
    return citations, article_to_cite


def _force_daily_brief_citations(
    articles: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[int, int]]:
    citations: list[dict[str, object]] = []
    article_to_cite: dict[int, int] = {}
    for idx, article in enumerate(articles, start=1):
        try:
            article_id = int(article.get("id") or 0)
        except Exception:
            continue
        url = _clean_url_for_display(str(article.get("canonical_url") or "").strip())
        if not url:
            url = _clean_url_for_display(str(article.get("normalized_url") or "").strip())
        if not url:
            url = _clean_url_for_display(str(article.get("original_url") or "").strip())
        title = str(article.get("title") or "").strip() or url
        source_name = str(article.get("source_name") or "").strip()
        summary_text = _clean_summary_text(article.get("summary_llm"))
        citations.append(
            {
                "id": idx,
                "article_id": article_id,
                "title": title,
                "source_name": source_name,
                "url": url,
                "summary": summary_text,
            }
        )
        article_to_cite[article_id] = idx
    return citations, article_to_cite


def _low_value_reason(article: object) -> str | None:
    if not isinstance(article, dict):
        return None
    title = str(article.get("title") or "").lower()
    summary = str(article.get("summary_llm") or "").lower()
    source = str(article.get("source_name") or "").lower()
    cues = [
        "webinar",
        "sponsored",
        "press release",
        "promo",
        "advertisement",
        "marketing",
        "whitepaper",
        "ebook",
        "roundup",
        "opinion",
        "survey",
        "podcast",
    ]
    for cue in cues:
        if cue in title or cue in summary:
            return cue
    if title.startswith("http"):
        return "url_only"
    if "sponsored" in source:
        return "sponsored"
    return None


def _assign_family_for_article(article: object) -> str:
    if not isinstance(article, dict):
        return "RA"
    context_text = ""
    context_raw = article.get("context_llm")
    if context_raw:
        if isinstance(context_raw, (dict, list)):
            context_text = _clean_summary_text(context_raw)
        else:
            context_text = _clean_summary_text(context_raw)
    text = " ".join(
        str(part or "").lower()
        for part in (
            article.get("title"),
            article.get("summary_llm"),
            context_text,
            " ".join(article.get("tags") or []),
            " ".join(article.get("cves") or []),
        )
    )
    if "ransomware" in text or "extortion" in text or "breach" in text:
        return "IR"
    if "identity" in text or "authentication" in text or "credential" in text:
        return "IA"
    if "misconfig" in text or "configuration" in text:
        return "CM"
    if "supply chain" in text or "third party" in text:
        return "SR"
    if "privacy" in text or "pii" in text:
        return "PT"
    if "audit" in text or "monitoring" in text or "authorization" in text:
        return "CA"
    if "vulnerability" in text or "cve" in text or "exploited" in text:
        return "SI"
    if "network" in text or "tls" in text or "segmentation" in text:
        return "SC"
    return "RA"


def _first_sentence(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", cleaned, maxsplit=1)
    return parts[0].strip()


def _article_sentence(article: dict[str, object], citation: dict[str, object]) -> str:
    context_raw = article.get("context_llm")
    summary = ""
    if context_raw:
        summary = _clean_summary_text(context_raw)
    if not summary:
        summary = _clean_summary_text(article.get("summary_llm"))
    if not summary:
        summary = _clean_summary_text(citation.get("summary"))
    if not summary:
        summary = str(article.get("title") or citation.get("title") or "").strip()
    return _first_sentence(summary) or str(article.get("title") or "").strip()


def _build_family_summary_from_titles(family_title: str, titles: list[str]) -> str:
    titles = [t for t in titles if t]
    if not titles:
        return f"Today’s {family_title} coverage centered on multiple security updates and incidents."
    if len(titles) == 1:
        return f"Today’s {family_title} coverage centered on {titles[0]}."
    return f"Today’s {family_title} coverage centered on {titles[0]} and {titles[1]}."


def _build_fallback_families_from_articles(
    *,
    articles: list[dict[str, object]],
    citations: list[dict[str, object]],
    article_to_citation: dict[int, int],
    low_value_ids: set[int],
) -> list[dict[str, object]]:
    citation_by_id = {c.get("id"): c for c in citations if isinstance(c, dict)}
    family_buckets: dict[str, list[dict[str, object]]] = {}
    for article in articles:
        if not isinstance(article, dict):
            continue
        try:
            article_id = int(article.get("id") or 0)
        except Exception:
            continue
        citation_id = article_to_citation.get(article_id)
        if not citation_id or citation_id in low_value_ids:
            continue
        family_id = _assign_family_for_article(article)
        if family_id not in NIST_FAMILY_TITLES:
            family_id = "RA"
        citation = citation_by_id.get(citation_id, {})
        family_buckets.setdefault(family_id, []).append(
            {
                "citation_id": citation_id,
                "title": str(citation.get("title") or article.get("title") or "").strip(),
                "sentence": _article_sentence(article, citation),
            }
        )
    families: list[dict[str, object]] = []
    for family_id, items in family_buckets.items():
        if not items:
            continue
        items.sort(key=lambda item: int(item.get("citation_id") or 0))
        family_title = NIST_FAMILY_TITLES.get(family_id, family_id)
        summary = _build_family_summary_from_titles(
            family_title,
            [item.get("title") or "" for item in items[:2]],
        )
        group_count = min(3, max(1, (len(items) + 11) // 12))
        group_size = max(1, (len(items) + group_count - 1) // group_count)
        subtopics: list[dict[str, object]] = []
        for idx in range(group_count):
            chunk = items[idx * group_size : (idx + 1) * group_size]
            if not chunk:
                continue
            max_sentences = 6
            clauses_per_sentence = max(1, (len(chunk) + max_sentences - 1) // max_sentences)
            sentences: list[str] = []
            cursor = 0
            while cursor < len(chunk):
                segment = chunk[cursor : cursor + clauses_per_sentence]
                cursor += clauses_per_sentence
                clauses = [str(item.get("sentence") or "").strip() for item in segment if str(item.get("sentence") or "").strip()]
                if not clauses:
                    continue
                sentence = "; ".join(clauses)
                citation_marks = "".join(f"({item.get('citation_id')})" for item in segment)
                sentences.append(f"{sentence} {citation_marks}".strip())
            narrative = " ".join(sentences[:max_sentences]).strip()
            subtopics.append(
                {
                    "subtopic_id": f"{family_id.lower()}_{idx+1}",
                    "title": str(chunk[0].get("title") or family_title),
                    "severity": "High" if idx == 0 else "Medium" if idx == 1 else "Low",
                    "narrative": narrative,
                    "citations": [int(item.get("citation_id") or 0) for item in chunk if item.get("citation_id")],
                }
            )
        families.append(
            {
                "family_id": family_id,
                "family_title": family_title,
                "summary": summary,
                "subtopics": subtopics,
            }
        )
    return families


def _detect_low_value_entries(
    articles: list[dict[str, object]],
    article_to_citation: dict[int, int],
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for article in articles:
        if not isinstance(article, dict):
            continue
        reason = _low_value_reason(article)
        if not reason:
            continue
        try:
            article_id = int(article.get("id") or 0)
        except Exception:
            continue
        cid = article_to_citation.get(article_id)
        if cid:
            entries.append({"citation_id": cid, "reason": reason})
    # dedupe by citation_id
    seen: set[int] = set()
    deduped: list[dict[str, object]] = []
    for item in entries:
        try:
            cid = int(item.get("citation_id"))
        except Exception:
            continue
        if cid in seen:
            continue
        seen.add(cid)
        deduped.append(item)
    return deduped


def _collect_citations_from_text(text: str) -> list[int]:
    if not text:
        return []
    return [int(match) for match in re.findall(r"\((\d+)\)", text)]


def _ensure_daily_brief_coverage(
    brief_payload: dict[str, object],
    citations: list[dict[str, object]],
    article_by_citation: dict[int, dict[str, object]],
) -> None:
    cited_citation_ids: set[int] = set()
    for item in brief_payload.get("tldr") or []:
        if isinstance(item, dict):
            for cid in item.get("citations") or []:
                try:
                    cited_citation_ids.add(int(cid))
                except Exception:
                    continue
            cited_citation_ids.update(_collect_citations_from_text(str(item.get("text") or "")))
    technical = brief_payload.get("technical_synthesis") or {}
    if isinstance(technical, dict):
        for cid in technical.get("citations") or []:
            try:
                cited_citation_ids.add(int(cid))
            except Exception:
                continue
        cited_citation_ids.update(_collect_citations_from_text(str(technical.get("text") or "")))
    for action in brief_payload.get("actions") or []:
        if not isinstance(action, dict):
            continue
        for cid in action.get("citations") or []:
            try:
                cited_citation_ids.add(int(cid))
            except Exception:
                continue
        cited_citation_ids.update(_collect_citations_from_text(str(action.get("action") or "")))
        cited_citation_ids.update(_collect_citations_from_text(str(action.get("why") or "")))
    family_citations: set[int] = set()
    for family in brief_payload.get("families") or []:
        if not isinstance(family, dict):
            continue
        cited_citation_ids.update(_collect_citations_from_text(str(family.get("summary") or "")))
        for sub in family.get("subtopics") or []:
            if not isinstance(sub, dict):
                continue
            for cid in sub.get("citations") or []:
                try:
                    cited_citation_ids.add(int(cid))
                    family_citations.add(int(cid))
                except Exception:
                    continue
            cited_citation_ids.update(_collect_citations_from_text(str(sub.get("narrative") or "")))
    for entry in brief_payload.get("low_value") or []:
        if not isinstance(entry, dict):
            continue
        try:
            cited_citation_ids.add(int(entry.get("citation_id")))
        except Exception:
            continue

    citation_ids = {
        int(item.get("id"))
        for item in citations
        if isinstance(item, dict) and item.get("id")
    }
    uncited = sorted(cid for cid in citation_ids if cid not in cited_citation_ids)
    if not uncited:
        brief_payload["meta"]["coverage"]["cited_article_ids"] = sorted(citation_ids)
        brief_payload["meta"]["coverage"]["uncited_article_ids"] = []
        brief_payload["meta"]["coverage"]["low_value_article_ids"] = [
            int(item.get("citation_id"))
            for item in brief_payload.get("low_value") or []
            if isinstance(item, dict) and item.get("citation_id")
        ]
        return

    low_value_entries = list(brief_payload.get("low_value") or [])
    families = list(brief_payload.get("families") or [])
    family_index = {fam.get("family_id"): fam for fam in families if isinstance(fam, dict)}

    for cid in uncited:
        article = article_by_citation.get(cid)
        if not article:
            continue
        reason = _low_value_reason(article)
        if reason:
            low_value_entries.append({"citation_id": cid, "reason": reason})
            cited_citation_ids.add(cid)
        # ensure every citation appears in exactly one family
        if cid in family_citations:
            continue
        family_code = _assign_family_for_article(article)
        family_entry = family_index.get(family_code)
        if not family_entry:
            family_entry = {
                "family_id": family_code,
                "family_title": NIST_FAMILY_TITLES.get(family_code, family_code),
                "summary": "",
                "subtopics": [],
            }
            families.append(family_entry)
            family_index[family_code] = family_entry
        subtopics = family_entry.get("subtopics") or []
        subtopic = next((s for s in subtopics if s.get("subtopic_id") == "auto_coverage"), None)
        if not subtopic:
            subtopic = {
                "subtopic_id": "auto_coverage",
                "title": "Additional items",
                "severity": "Low",
                "narrative": "",
                "citations": [],
            }
            subtopics.append(subtopic)
            family_entry["subtopics"] = subtopics
        subtopic["citations"] = sorted(set(subtopic.get("citations") or []).union({cid}))
        cited_citation_ids.add(cid)
        family_citations.add(cid)

    brief_payload["families"] = families
    brief_payload["families"] = _dedupe_nist_citation_assignments(brief_payload["families"])
    brief_payload["low_value"] = low_value_entries
    brief_payload["meta"]["coverage"]["low_value_article_ids"] = sorted(
        int(item.get("citation_id"))
        for item in low_value_entries
        if isinstance(item, dict) and item.get("citation_id")
    )
    brief_payload["meta"]["coverage"]["cited_article_ids"] = sorted(cited_citation_ids)
    brief_payload["meta"]["coverage"]["uncited_article_ids"] = sorted(
        cid for cid in citation_ids if cid not in cited_citation_ids
    )


def _dedupe_nist_citation_assignments(
    families: list[dict[str, object]],
) -> list[dict[str, object]]:
    seen: set[int] = set()
    for family in families:
        if not isinstance(family, dict):
            continue
        for sub in family.get("subtopics") or []:
            if not isinstance(sub, dict):
                continue
            unique = []
            for cid in sub.get("citations") or []:
                try:
                    cid_int = int(cid)
                except Exception:
                    continue
                if cid_int in seen:
                    continue
                seen.add(cid_int)
                unique.append(cid_int)
            sub["citations"] = unique
    return families
def _importance_label(value: object) -> str:
    if value is None:
        return "Medium"
    try:
        score = float(value)
    except Exception:
        return "Medium"
    if score <= 1.0:
        if score >= 0.66:
            return "High"
        if score >= 0.33:
            return "Medium"
        return "Low"
    if score >= 8:
        return "High"
    if score >= 5:
        return "Medium"
    return "Low"


def _infer_topic_type(topic: dict[str, object]) -> str:
    text = " ".join(
        str(part or "")
        for part in (
            topic.get("label"),
            topic.get("topic_summary"),
            " ".join(topic.get("topic_tldr") or []),
            topic.get("why"),
        )
    ).lower()
    operational_markers = [
        "cve-",
        "exploit",
        "ransomware",
        "breach",
        "intrusion",
        "malware",
        "phishing",
        "ddos",
        "extortion",
        "zero-day",
    ]
    contextual_markers = [
        "platform",
        "release",
        "launch",
        "update",
        "feature",
        "roadmap",
        "partnership",
        "acquisition",
        "policy",
        "guidance",
    ]
    if any(marker in text for marker in operational_markers):
        return "operational"
    if any(marker in text for marker in contextual_markers):
        return "contextual"
    return "operational"


def run_once(
    worker_id: str,
    allowed_types: list[str] | None = None,
    *,
    queue_name: str | None = None,
    lease_seconds: int | None = None,
) -> int:
    logger = _setup_logging()
    try:
        conn = init_db()
        config = load_runtime_config(conn)
        bootstrap_cve_settings(conn)
        bootstrap_events_settings(conn)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1
    _log_vendor_product_schema(conn, logger)

    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir, config.paths.logs_dir))
    if _scheduler_hooks_enabled():
        if _should_tick_ingest_due(allowed_types):
            _maybe_enqueue_ingest_due_sources(conn, logger)
        _maybe_enqueue_cve_sync(conn, logger)
        _maybe_enqueue_auto_catchup(conn, config, logger, worker_id, allowed_types)
    claim_types = WORKER_JOB_TYPES if allowed_types is None else allowed_types
    if not is_article_markdown_enabled() and allowed_types:
        if "write_article_markdown" not in claim_types:
            claim_types = claim_types + ["write_article_markdown"]
    lock_timeout = _llm_lock_timeout_seconds(conn, config, claim_types)
    provider_scope = os.environ.get("SV_WORKER_PROVIDER_SCOPE", "any").lower().strip()
    for _ in range(10):
        job = claim_next_job(
            conn,
            worker_id,
            allowed_types=claim_types,
            allowed_queues=[queue_name] if queue_name else None,
            lock_timeout_seconds=lock_timeout,
            lease_seconds=lease_seconds,
        )
        if not job:
            return _RUN_ONCE_IDLE
        if not queue_name and provider_scope in {"openai", "non_openai"} and job.job_type in _LLM_JOB_TYPES:
            uses_openai = _job_uses_openai(conn, job)
            if provider_scope == "openai" and not uses_openai:
                release_job(
                    conn, job.id, delay_seconds=15, reason="provider_scope_non_openai"
                )
                continue
            if provider_scope == "non_openai" and uses_openai:
                release_job(
                    conn, job.id, delay_seconds=15, reason="provider_scope_openai_only"
                )
                continue
        return _process_claimed_job(conn, config, job, logger)
    return 0


def _process_claimed_job(conn, config, job, logger: logging.Logger) -> int:
    if is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "job_canceled", job_id=job.id)
        if job.job_type == "ingest_source":
            source_id = str((job.payload or {}).get("source_id") or "")
            source = get_source(conn, source_id) if source_id else None
            if source is not None:
                finalize_source_ingest_state(
                    conn,
                    source_id=source.id,
                    job_id=job.id,
                    finished_at=utc_now_iso(),
                    next_due_at=utc_now_iso_offset(
                        seconds=source.default_frequency_minutes * 60
                    ),
                )
        return 0

    llm_job_types = _LLM_JOB_TYPES
    try:
        result = run_claimed_job(conn, config, job, logger)
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        if is_job_canceled(conn, job.id):
            log_event(logger, logging.INFO, "job_canceled", job_id=job.id)
            return 0
        if job.job_type in llm_job_types and _is_timeout_error(exc):
            timeout_retries = int(config.llm.get("max_timeout_retries", 0) or 0)
            if job.job_type == "build_daily_brief":
                timeout_retries = 0
            attempts = int((job.payload or {}).get("timeout_attempt", 0))
            failed = fail_job(conn, job.id, f"timeout: {exc}")
            if not failed:
                fail_job_force(conn, job.id, f"timeout: {exc}")
                log_event(
                    logger,
                    logging.WARNING,
                    "job_timeout_force_failed",
                    job_id=job.id,
                    error=str(exc),
                    **_job_context_fields(conn, job),
                )
            fields = _job_context_fields(conn, job)
            log_event(
                logger,
                logging.ERROR,
                "job_timeout_failed",
                job_id=job.id,
                error=str(exc),
                attempts=attempts,
                **fields,
            )
            if attempts < timeout_retries:
                next_payload = dict(job.payload or {})
                next_payload["timeout_attempt"] = attempts + 1
                next_payload["not_before"] = utc_now_iso_offset(seconds=600 * (attempts + 1))
                next_job_id = enqueue_job(conn, job.job_type, next_payload)
                log_event(
                    logger,
                    logging.INFO,
                    "job_requeued",
                    job_id=next_job_id,
                    reason="llm_timeout_retry",
                    attempt=attempts + 1,
                    previous_job_id=job.id,
                    **fields,
                )
            else:
                log_event(
                    logger,
                    logging.ERROR,
                    "job_final_failed",
                    job_id=job.id,
                    reason="llm_timeout_max_attempts",
                    attempts=attempts,
                    **fields,
                )
            return 1
        if job.job_type in {"rebuild_vendor_products", "build_daily_brief"}:
            attempts = int((job.payload or {}).get("attempt", 0))
            backoff = [300, 900]
            max_attempts = len(backoff)
            failed = fail_job(conn, job.id, str(exc))
            if not failed:
                fail_job_force(conn, job.id, str(exc))
            fields = _job_context_fields(conn, job)
            log_event(
                logger,
                logging.ERROR,
                "job_failed",
                job_id=job.id,
                error=str(exc),
                **fields,
            )
            if attempts < max_attempts and job.job_type != "build_daily_brief":
                next_payload = dict(job.payload or {})
                next_payload["attempt"] = attempts + 1
                next_payload["not_before"] = utc_now_iso_offset(seconds=backoff[attempts])
                next_job_id = enqueue_job(conn, job.job_type, next_payload)
                log_event(
                    logger,
                    logging.INFO,
                    "job_requeued",
                    job_id=next_job_id,
                    reason=f"{job.job_type}_retry",
                    attempt=attempts + 1,
                    previous_job_id=job.id,
                    **fields,
                )
            else:
                log_event(
                    logger,
                    logging.ERROR,
                    "job_final_failed",
                    job_id=job.id,
                    reason=f"{job.job_type}_max_attempts",
                    attempts=attempts,
                    **fields,
                )
            return 1
        failed = fail_job(conn, job.id, str(exc))
        if not failed:
            fail_job_force(conn, job.id, str(exc))
        if job.job_type == "ingest_source":
            source_id = str((job.payload or {}).get("source_id") or "")
            source = get_source(conn, source_id) if source_id else None
            if source is not None:
                finalize_source_ingest_state(
                    conn,
                    source_id=source.id,
                    job_id=job.id,
                    finished_at=utc_now_iso(),
                    next_due_at=utc_now_iso_offset(
                        seconds=source.default_frequency_minutes * 60
                    ),
                )
        fields = _job_context_fields(conn, job)
        log_event(
            logger,
            logging.ERROR,
            "job_failed",
            job_id=job.id,
            error=str(exc),
            **fields,
        )
        return 1

    if result.get("requeued"):
        fields = _job_context_fields(conn, job)
        log_event(
            logger,
            logging.INFO,
            "job_requeued",
            job_id=job.id,
            reason=result.get("reason"),
            attempt=result.get("attempt"),
            **fields,
        )
        return 0

    if is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "job_canceled", job_id=job.id)
        return 0

    if complete_job(conn, job.id, result=result):
        fields = _job_context_fields(conn, job)
        log_event(logger, logging.INFO, "job_succeeded", job_id=job.id, **fields)
    else:
        log_event(logger, logging.ERROR, "job_complete_failed", job_id=job.id)
    return 0


def _process_claimed_job_thread(worker_id: str, job: Job) -> int:
    logger = _setup_logging()
    try:
        conn = init_db()
        config = load_runtime_config(conn)
        bootstrap_cve_settings(conn)
        bootstrap_events_settings(conn)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1
    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir, config.paths.logs_dir))
    return _process_claimed_job(conn, config, job, logger)


def run_loop(
    worker_id: str,
    sleep_seconds: int,
    allowed_types: list[str] | None = None,
    concurrency: int = 1,
    *,
    queue_name: str | None = None,
    max_jobs: int | None = None,
    max_runtime_seconds: int | None = None,
    lease_seconds: int | None = None,
) -> int:
    started = time.monotonic()
    claimed_jobs = 0
    if concurrency <= 1:
        while True:
            result = run_once(
                worker_id,
                allowed_types,
                queue_name=queue_name,
                lease_seconds=lease_seconds,
            )
            if result != _RUN_ONCE_IDLE:
                claimed_jobs += 1
            elif claimed_jobs == 0 and max_jobs is None and max_runtime_seconds is None:
                time.sleep(sleep_seconds)
                continue
            elif result == _RUN_ONCE_IDLE:
                return 0
            if max_jobs is not None and claimed_jobs >= max_jobs:
                return 0
            if max_runtime_seconds is not None and (time.monotonic() - started) >= max_runtime_seconds:
                return 0
            time.sleep(sleep_seconds)
        return 0

    logger = _setup_logging()
    max_workers = max(1, concurrency)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = set()
        while True:
            while len(futures) < max_workers:
                try:
                    conn = init_db()
                    config = load_runtime_config(conn)
                    bootstrap_cve_settings(conn)
                    bootstrap_events_settings(conn)
                except ConfigError as exc:
                    log_event(logger, logging.ERROR, "config_error", error=str(exc))
                    break
                _log_vendor_product_schema(conn, logger)
                set_umask_from_env()
                ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir, config.paths.logs_dir))
                if _scheduler_hooks_enabled():
                    if _should_tick_ingest_due(allowed_types):
                        _maybe_enqueue_ingest_due_sources(conn, logger)
                    _maybe_enqueue_cve_sync(conn, logger)
                    _maybe_enqueue_auto_catchup(conn, config, logger, worker_id, allowed_types)
                lock_timeout = _llm_lock_timeout_seconds(conn, config, WORKER_JOB_TYPES if allowed_types is None else allowed_types)
                job = claim_next_job(
                    conn,
                    worker_id,
                    allowed_types=WORKER_JOB_TYPES if allowed_types is None else allowed_types,
                    allowed_queues=[queue_name] if queue_name else None,
                    lock_timeout_seconds=lock_timeout,
                    lease_seconds=lease_seconds,
                )
                conn.close()
                if not job:
                    break
                futures.add(executor.submit(_process_claimed_job_thread, worker_id, job))
                claimed_jobs += 1
                if max_jobs is not None and claimed_jobs >= max_jobs:
                    break
                if max_runtime_seconds is not None and (time.monotonic() - started) >= max_runtime_seconds:
                    break
            if futures:
                done, futures = wait(futures, timeout=sleep_seconds, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        future.result()
                    except Exception as exc:  # noqa: BLE001
                        log_event(logger, logging.ERROR, "job_thread_error", error=str(exc))
                if max_jobs is not None and claimed_jobs >= max_jobs and not futures:
                    return 0
                if max_runtime_seconds is not None and (time.monotonic() - started) >= max_runtime_seconds and not futures:
                    return 0
            else:
                if max_jobs is not None and claimed_jobs >= max_jobs:
                    return 0
                if max_runtime_seconds is not None and (time.monotonic() - started) >= max_runtime_seconds:
                    return 0
                time.sleep(sleep_seconds)


def _handle_ingest_source(
    conn,
    config,
    payload: dict[str, object],
    logger: logging.Logger,
    job_id: str | None = None,
) -> dict[str, object]:
    # Pipeline order: ingest creates article stubs first, then enqueues
    # fetch_article_content ( if enabled + URL present). Summarization runs
    # after fetch if configured, and publish runs after summarize or fetch.
    source_id = payload.get("source_id") if payload else None
    if not source_id:
        raise ValueError("ingest_source requires source_id")
    source = get_source(conn, str(source_id))
    if source is None:
        raise ValueError(f"Source not found: {source_id}")

    started_at = utc_now_iso()
    if job_id:
        mark_source_ingest_started(conn, source.id, job_id, started_at)
    now_dt = _parse_iso(started_at)
    next_due_at = utc_now_iso_offset(seconds=source.default_frequency_minutes * 60)
    if not source.enabled or ( source.pause_until and _parse_iso(source.pause_until) > now_dt):
        record_source_run(
            conn,
            source_id=source.id,
            started_at=started_at,
            finished_at=utc_now_iso(),
            status="paused" if source.pause_until else "skipped",
            http_status=None,
            items_found=0,
            items_accepted=0,
            skipped_duplicates=0,
            skipped_filters=0,
            skipped_missing_url=0,
            error=source.paused_reason or "source_disabled",
            notes=None,
        )
        finalize_source_ingest_state(
            conn,
            source_id=source.id,
            job_id=job_id,
            finished_at=utc_now_iso(),
            next_due_at=next_due_at,
        )
        return {
            "source_id": source.id,
            "status": "paused" if source.pause_until else "skipped",
            "error": source.paused_reason or "source_disabled",
            "found_count": 0,
            "accepted_count": 0,
        }

    result = process_source(source, config, logger, conn)
    limit = payload.get("limit")
    if isinstance(limit, int) and limit > 0 and len(result.articles) > limit:
        result = replace(result, accepted_count=limit, articles=result.articles[:limit])
    finished_at = utc_now_iso()
    duration_ms = int(
        ( datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)).total_seconds()
        * 1000
    )
    seen_count = result.skipped_duplicates
    filtered_count = result.skipped_filters
    error_count = result.skipped_missing_url

    log_event(
        logger,
        logging.INFO,
        "ingest_counts",
        source_id=source.id,
        source_name=source.name,
        found_count=result.found_count,
        accepted_count=result.accepted_count,
        seen_count=seen_count,
        filtered_count=filtered_count,
        error_count=error_count,
    )
    _log_decision_samples(logger, result)

    record_source_run(
        conn,
        source_id=source.id,
        started_at=started_at,
        finished_at=finished_at,
        status=result.status,
        http_status=result.http_status,
        items_found=result.found_count,
        items_accepted=result.accepted_count,
        skipped_duplicates=result.skipped_duplicates,
        skipped_filters=result.skipped_filters,
        skipped_missing_url=result.skipped_missing_url,
        error=result.error,
        notes={"tactics": result.notes} if result.notes else None,
    )
    insert_source_health_event(
        conn,
        source_id=source.id,
        ts=finished_at,
        ok=result.status == "ok",
        found_count=result.found_count,
        accepted_count=result.accepted_count,
        seen_count=result.skipped_duplicates,
        filtered_count=result.skipped_filters,
        error_count=result.skipped_missing_url,
        last_error=result.error,
        duration_ms=duration_ms,
    )

    if result.status != "ok":
        _maybe_pause_source(conn, source.id, logger)
        finalize_source_ingest_state(
            conn,
            source_id=source.id,
            job_id=job_id,
            finished_at=finished_at,
            next_due_at=next_due_at,
        )
        return {
            "source_id": source.id,
            "status": result.status,
            "error": result.error,
            "found_count": result.found_count,
            "accepted_count": result.accepted_count,
            "seen_count": seen_count,
            "filtered_count": filtered_count,
            "error_count": error_count,
        }

    if job_id and is_job_canceled(conn, job_id):
        finalize_source_ingest_state(
            conn,
            source_id=source.id,
            job_id=job_id,
            finished_at=finished_at,
            next_due_at=next_due_at,
        )
        return {"canceled": True}

    # When no new articles were accepted, the ingest work is complete once the
    # source run and health event have been recorded. Returning here avoids
    # unnecessary downstream work for duplicate-only feeds.
    if result.accepted_count <= 0:
        _maybe_pause_source(conn, source.id, logger)
        finalize_source_ingest_state(
            conn,
            source_id=source.id,
            job_id=job_id,
            finished_at=finished_at,
            next_due_at=next_due_at,
        )
        return {
            "source_id": source.id,
            "status": result.status,
            "found_count": result.found_count,
            "accepted_count": result.accepted_count,
            "skipped_duplicates": result.skipped_duplicates,
            "skipped_filters": result.skipped_filters,
            "skipped_missing_url": result.skipped_missing_url,
            "seen_count": seen_count,
            "filtered_count": filtered_count,
            "error_count": error_count,
        }

    insert_articles(conn, result.articles)
    for article in result.articles:
        if job_id and is_job_canceled(conn, job_id):
            finalize_source_ingest_state(
                conn,
                source_id=source.id,
                job_id=job_id,
                finished_at=finished_at,
                next_due_at=next_due_at,
            )
            return {"canceled": True}
        cve_ids = extract_cve_ids(
            [article.title, article.summary or "", article.original_url]
        )
        if not cve_ids:
            article_id = None
        else:
            article_id = get_article_id(conn, article.source_id, article.stable_id)
            if article_id is not None:
                evidence = build_cve_evidence(article, cve_ids)
                upsert_cve_links(conn, article_id, cve_ids, evidence)
                infer_article_products_from_cves(conn, article_id, cve_ids)
        if article_id is None:
            article_id = get_article_id(conn, article.source_id, article.stable_id)
        if article_id is not None:
            _maybe_enqueue_fetch(conn, config, article_id, article.source_id, logger)
        events_settings = get_events_settings(conn)
        if events_settings.get("enabled", True) and cve_ids and article_id is not None:
            link_article_to_events(
                conn,
                article_id=article_id,
                cve_ids=cve_ids,
                published_at=article.published_at or article.ingested_at,
            )
    if config.publishing.write_json_index:
        extra_by_stable: dict[str, dict[str, object]] | None = None
        if ( 
            config.personalization.watchlist_enabled
            and config.personalization.watchlist_exposure_mode == "public_highlights"
        ):
            extra_by_stable = {}
            for article in result.articles:
                article_id = get_article_id(conn, article.source_id, article.stable_id)
                if article_id is None:
                    continue
                hit = compute_watchlist_hits(
                    conn,
                    item_type="article",
                    item_key=article_id,
                    min_cvss=config.scope.min_cvss,
                )
                if hit.get("hit"):
                    extra_by_stable[article.stable_id] = {"watchlist_hit": True}
        write_json_index(result.articles, config.publishing.json_index_path, extra_by_stable)
    if result.accepted_count > 0:
        _refresh_feed_data_files(conn, config, logger)
    else:
        # Avoid expensive full data/CVE page rewrites when ingest found no new articles.
        log_event(
            logger,
            logging.INFO,
            "feed_data_refresh_skipped",
            source_id=source.id,
            source_name=source.name,
            reason="no_new_articles",
        )
    _maybe_pause_source(conn, source.id, logger)
    finalize_source_ingest_state(
        conn,
        source_id=source.id,
        job_id=job_id,
        finished_at=finished_at,
        next_due_at=next_due_at,
    )
    return {
        "source_id": source.id,
        "status": result.status,
        "found_count": result.found_count,
        "accepted_count": result.accepted_count,
        "skipped_duplicates": result.skipped_duplicates,
        "skipped_filters": result.skipped_filters,
        "skipped_missing_url": result.skipped_missing_url,
        "seen_count": seen_count,
        "filtered_count": filtered_count,
        "error_count": error_count,
    }


def _handle_test_source(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    source_id = payload.get("source_id") if payload else None
    if not source_id:
        raise ValueError("test_source requires source_id")
    source = get_source(conn, str(source_id))
    if source is None:
        raise ValueError(f"Source not found: {source_id}")
    result = process_source(source, config, logger, conn, test_mode=True)
    preview = []
    for decision in result.decisions[:5]:
        preview.append(
            {
                "decision": decision.decision,
                "reasons": decision.reasons,
                "title": decision.title,
                "url": decision.normalized_url,
            }
        )
    return {
        "source_id": source.id,
        "status": result.status,
        "http_status": result.http_status,
        "error": result.error,
        "found_count": result.found_count,
        "accepted_count": result.accepted_count,
        "skipped_duplicates": result.skipped_duplicates,
        "skipped_filters": result.skipped_filters,
        "skipped_missing_url": result.skipped_missing_url,
        "preview": preview,
    }


def _log_decision_samples(logger: logging.Logger, result: object) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    decisions = getattr(result, "decisions", [])
    buckets = {
        "accepted": [],
        "seen": [],
        "filtered": [],
        "error": [],
    }
    for decision in decisions:
        entry = {
            "title": getattr(decision, "title", None),
            "url": getattr(decision, "normalized_url", None),
        }
        if getattr(decision, "decision", "") == "ACCEPT":
            buckets["accepted"].append(entry)
            continue
        reasons = getattr(decision, "reasons", []) or []
        if "duplicate" in reasons:
            buckets["seen"].append(entry)
        elif "missing_url" in reasons:
            buckets["error"].append(entry)
        elif any(reason.startswith("deny_keywords") for reason in reasons) or "allow_keywords:miss" in reasons:
            buckets["filtered"].append(entry)
    for bucket, samples in buckets.items():
        if not samples:
            continue
        log_event(
            logger,
            logging.DEBUG,
            "ingest_samples",
            bucket=bucket,
            samples=samples[:3],
        )


def _handle_write_article_markdown(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    if not payload:
        raise ValueError("write_article_markdown requires payload")
    if not is_article_markdown_enabled():
        log_event(
            logger,
            logging.INFO,
            "article_markdown_skipped",
            reason="article_markdown_disabled",
        )
        return {"status": "skipped", "reason": "article_markdown_disabled"}
    source_id = str(payload.get("source_id"))
    source_name = get_source_name(conn, source_id) or ""
    batch_id = str(payload.get("batch_id") or "")
    batch_total = int(payload.get("batch_total") or 0)
    batch_index = int(payload.get("batch_index") or 0)
    article = Article(
        id=None,
        stable_id=str(payload.get("stable_id")),
        original_url=str(payload.get("original_url")),
        normalized_url=str(payload.get("normalized_url")),
        title=str(payload.get("title")),
        source_id=source_id,
        published_at=payload.get("published_at") or None,
        published_at_source=payload.get("published_at_source") or None,
        ingested_at=str(payload.get("ingested_at")),
        summary=payload.get("summary") or None,
        tags=list(payload.get("tags") or []),
    )
    extra_frontmatter = None
    if ( 
        config.personalization.watchlist_enabled
        and config.personalization.watchlist_exposure_mode == "public_highlights"
        and payload.get("watchlist_hit") is True
    ):
        extra_frontmatter = {"watchlist_hit": True}
    path = write_article_markdown(article, config.paths.output_dir, extra_frontmatter=extra_frontmatter)
    progress = ""
    if batch_total and batch_index:
        progress = f"{batch_index}/{batch_total}"
        log_event(
            logger,
            logging.INFO,
            "batch_progress",
            source_id=source_id,
            source_name=source_name,
            i=batch_index,
            total=batch_total,
        )
    log_event(
        logger,
        logging.INFO,
        "article_markdown_written",
        path=path,
        source_id=source_id,
        source_name=source_name,
        article_id=payload.get("article_id"),
        article_url=article.original_url,
        progress=progress,
    )
    _refresh_feed_data_files(conn, config, logger)
    if batch_id and batch_total:
        counts = get_batch_job_counts(conn, batch_id)
        remaining = counts["queued"] + counts["running"] - 1
        if remaining == 0:
            log_event(
                logger,
                logging.INFO,
                "batch_complete",
                batch_id=batch_id,
                total=counts["total"],
                succeeded=counts.get("succeeded", 0) + 1,
                failed=counts.get("failed", 0),
                source_id=source_id,
                source_name=source_name,
            )
    return {
        "path": path,
        "batch_id": batch_id,
        "batch_total": batch_total,
        "batch_index": batch_index,
    }


def _handle_fetch_article_content(
    conn, config, job, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    article_id = payload.get("article_id") if payload else None
    if not article_id:
        raise ValueError("fetch_article_content requires article_id")
    article = get_article_by_id(conn, int(article_id))
    if not article:
        log_event(
            logger,
            logging.WARNING,
            "fetch_article_missing",
            article_id=article_id,
        )
        raise ValueError("article_missing")
    url = article.get("original_url") or article.get("normalized_url") or ""
    if not url:
        attempts = int(payload.get("attempt", 0) if payload else 0)
        backoff = [10, 30, 60, 120, 300]
        if attempts < len(backoff):
            delay = backoff[attempts]
            next_payload = dict(payload or {})
            next_payload["attempt"] = attempts + 1
            next_payload["not_before"] = utc_now_iso_offset(seconds=delay)
            requeue_job(conn, job.id, next_payload, next_payload["not_before"])
            log_event(
                logger,
                logging.INFO,
                "fetch_article_url_missing",
                article_id=article_id,
                attempt=attempts + 1,
                next_in=delay,
            )
            return {"requeued": True, "reason": "article_url_missing", "attempt": attempts + 1}
        log_event(
            logger,
            logging.WARNING,
            "fetch_article_url_gave_up",
            article_id=article_id,
            attempts=attempts,
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        raise ValueError("article_not_ready")
    try:
        source = get_source(conn, str(article.get("source_id") or ""))
        overrides = source.overrides if source else None
        source_id = None
        source_name = None
        if source:
            source_id = getattr(source, "id", None) or (
                source.get("id") if isinstance(source, dict) else None
            )
            source_name = getattr(source, "name", None) or (
                source.get("name") if isinstance(source, dict) else None
            )
        result = fetch_article_content(
            url,
            timeout_seconds=config.ingest.http.timeout_seconds,
            user_agent=config.ingest.http.user_agent,
            logger=logger,
            source_id=str(source_id) if source_id else None,
            source_name=str(source_name) if source_name else None,
            overrides=overrides,
        )
        content_text = result["content_text"]
        if _looks_like_thn_teaser(str(source_id) if source_id else None, content_text):
            update_article_content(
                conn,
                int(article_id),
                content_text=None,
                content_html=None,
                content_fetched_at=utc_now_iso(),
                content_error="content_guard:the_hacker_news_teaser",
                has_full_content=False,
            )
            _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
            log_event(
                logger,
                logging.WARNING,
                "fetch_article_guard_blocked",
                article_id=article_id,
                source_id=source_id,
                source_name=source_name,
                reason="the_hacker_news_teaser",
            )
            return {"article_id": article_id, "has_full_content": False, "permanent_error": True}
        fallback_title = ""
        existing_title = str(article.get("title") or "").strip()
        if not existing_title:
            fallback_title = _extract_title_from_html(result.get("content_html") or "")
            if fallback_title:
                conn.execute(
                    "UPDATE articles SET title = %s, updated_at = %s WHERE id = %s",
                    (fallback_title, utc_now_iso(), int(article_id)),
                )
                conn.commit()
        store_html = os.environ.get("SV_STORE_ARTICLE_HTML", "0") == "1"
        content_html = result["content_html"] if store_html else None
        try:
            min_len = int(os.environ.get("SV_CONTENT_MIN_LEN", "500"))
        except ValueError:
            min_len = 500
        has_full_content = len(content_text or "") >= min_len
        update_article_content(
            conn,
            int(article_id),
            content_text=content_text,
            content_html=content_html,
            content_fetched_at=utc_now_iso(),
            content_error=None,
            has_full_content=has_full_content,
        )
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        if status_code == 403 and _apply_dark_reading_403_fallback(
            conn,
            config,
            logger,
            int(article_id),
            article,
            str(source_id) if source_id else None,
            url,
        ):
            return {"article_id": article_id, "has_full_content": True}
        if status_code in (404, 410):
            update_article_content(
                conn,
                int(article_id),
                content_text=None,
                content_html=None,
                content_fetched_at=utc_now_iso(),
                content_error=f"http_{status_code}",
                has_full_content=False,
            )
            _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
            log_event(
                logger,
                logging.INFO,
                "fetch_article_permanent_error",
                article_id=article_id,
                status_code=status_code,
            )
            return {"article_id": article_id, "has_full_content": False, "permanent_error": True}
        update_article_content(
            conn,
            int(article_id),
            content_text=None,
            content_html=None,
            content_fetched_at=utc_now_iso(),
            content_error=f"fetch_failed:{exc}",
            has_full_content=False,
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        raise
    except Exception as exc:  # noqa: BLE001
        err_text = str(exc)
        if ("403" in err_text or "Forbidden" in err_text) and _apply_dark_reading_403_fallback(
            conn,
            config,
            logger,
            int(article_id),
            article,
            str(source_id) if source_id else None,
            url,
        ):
            return {"article_id": article_id, "has_full_content": True}
        previous_failures = count_failed_article_jobs(conn, "fetch_article_content", int(article_id))
        is_timeout = _is_timeout_error(exc)
        is_terminal_timeout = is_timeout and (previous_failures + 1 >= 3)
        update_article_content(
            conn,
            int(article_id),
            content_text=None,
            content_html=None,
            content_fetched_at=utc_now_iso(),
            content_error="max_retries_exceeded" if is_terminal_timeout else f"fetch_failed:{exc}",
            has_full_content=False,
        )
        if is_terminal_timeout:
            log_event(
                logger,
                logging.WARNING,
                "fetch_article_timeout_max_attempts",
                article_id=article_id,
                attempts=previous_failures + 1,
                error=str(exc),
            )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        raise
    _maybe_enqueue_context_pack(conn, int(article_id), article["source_id"], logger)
    if not _maybe_enqueue_summarize(conn, int(article_id), article["source_id"], logger):
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        if has_full_content and _is_article_in_today_feed(conn, config, int(article_id), logger):
            _refresh_feed_data_files(conn, config, logger)
            mark_build_dirty(conn, reason="article_content_ready_today")
    _maybe_enqueue_article_product_enrich(conn, int(article_id), article["source_id"], logger)
    return {"article_id": article_id, "has_full_content": has_full_content}


def _handle_summarize_article_llm(
    conn, config, job: Job, logger: logging.Logger
) -> dict[str, object]:
    payload = job.payload or {}
    article_id = payload.get("article_id")
    if not article_id:
        raise ValueError("summarize_article_llm requires article_id")
    article = get_article_by_id(conn, int(article_id))
    if not article:
        attempts = int(payload.get("attempt", 0))
        backoff = [10, 30, 60]
        if attempts < len(backoff):
            delay = backoff[attempts]
            next_payload = dict(payload)
            next_payload["attempt"] = attempts + 1
            next_payload["not_before"] = utc_now_iso_offset(seconds=delay)
            requeue_job(conn, job.id, next_payload, next_payload["not_before"])
            log_event(
                logger,
                logging.INFO,
                "summarize_article_not_found_requeued",
                article_id=article_id,
                attempt=attempts + 1,
                next_in=delay,
            )
            return {"requeued": True, "reason": "article_not_found", "attempt": attempts + 1}
        raise ValueError("article_not_found")
    profile = None
    reason = "missing_profile"
    payload_profile_id = payload.get("profile_id")
    if isinstance(payload_profile_id, str) and payload_profile_id:
        profile = get_profile(conn, payload_profile_id)
        if not profile:
            log_event(
                logger,
                logging.WARNING,
                "llm_profile_missing",
                job_id=job.id,
                job_type=job.job_type,
                profile_id=payload_profile_id,
            )
    if not profile:
        profile, reason = get_active_profile_for_stage(conn, "summarize_article")
    if not profile:
        update_article_summary(
            conn,
            int(article_id),
            summary_llm=None,
            summary_model=None,
            summary_generated_at=utc_now_iso(),
            summary_error=f"llm_stage_{reason}",
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        log_event(
            logger,
            logging.INFO,
            "llm_stage_skipped",
            stage="summarize_article",
            reason=reason,
            article_id=article_id,
            source_id=article["source_id"],
        )
        raise ValueError(f"llm_stage_{reason}")
    source_name = get_source_name(conn, article["source_id"]) or ""
    content = article.get("content_text") or article.get("summary") or article.get("title") or ""
    if not content.strip():
        update_article_summary(
            conn,
            int(article_id),
            summary_llm=None,
            summary_model=None,
            summary_generated_at=utc_now_iso(),
            summary_error="missing_content",
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        raise ValueError("missing_content")
    input_chars = len(content or "")
    lease_holder = f"{job.id}:{article_id}"
    max_inflight = int(os.environ.get("SV_LLM_MAX_INFLIGHT", "1") or "1")
    max_inflight = max(1, max_inflight)
    lease_names = ( 
        ["summarize_article_llm"]
        if max_inflight == 1
        else [f"summarize_article_llm:{idx}" for idx in range(max_inflight)]
    )
    lease_name = None
    for attempt, delay in enumerate([2, 3, 5], start=1):
        for candidate in lease_names:
            if try_acquire_lease(conn, candidate, lease_holder, ttl_seconds=600):
                lease_name = candidate
                break
        if lease_name:
            break
        time.sleep(delay)
    if not lease_name:
        next_payload = dict(payload)
        next_payload["not_before"] = utc_now_iso_offset(seconds=30)
        requeue_job(conn, job.id, next_payload, next_payload["not_before"])
        return {"requeued": True, "reason": "llm_lease_unavailable"}
    start = time.time()
    try:
        input_text = ( 
            f"Title: {article.get('title')}\n"
            f"Source: {source_name}\n"
            f"Published: {article.get('published_at') or 'unknown'}\n"
            f"URL: {article.get('original_url') or article.get('normalized_url')}\n\n"
            f"Content:\n{content}\n"
        )
        result = run_pipeline_stage(
            conn,
            "summarize_article",
            input_text,
            logger,
            profile_id=profile["id"],
            context={"stage": "summarize_article", "job_type": job.job_type},
        )
        latency_ms = int((time.time() - start) * 1000)
        parsed = result.get("parsed")
        raw = result.get("raw") if isinstance(result, dict) else None
        if isinstance(parsed, ( dict, list)):
            summary_payload = json.dumps(parsed)
            summary_text = parsed.get("summary") if isinstance(parsed, dict) else None
        elif isinstance(raw, str):
            summary_payload = json.dumps({"summary": raw})
            summary_text = raw
        else:
            raise ValueError("llm_empty_output")
        update_article_summary(
            conn,
            int(article_id),
            summary_llm=summary_payload,
            summary_model=profile.get("model_name") or profile.get("primary_model_id"),
            summary_generated_at=utc_now_iso(),
            summary_error=None,
        )
        mark_build_dirty(conn, reason="summarize_article_llm")
        insert_llm_run(
            conn,
            job_id=None,
            provider_id=profile.get("primary_provider_id"),
            model_id=profile.get("primary_model_id"),
            prompt_name=profile.get("name") or "summarize_article",
            input_chars=input_chars,
            output_chars=len(summary_text or ""),
            latency_ms=latency_ms,
            ok=True,
            error=None,
        )
        cve_ids = list_article_cve_ids(conn, int(article_id))
        if cve_ids:
            infer_article_products_from_cves(conn, int(article_id), cve_ids)
        for event_id in list_event_ids_for_article(conn, int(article_id)):
            event_id_text = str(event_id)
            update_event_summary_from_articles(conn, event_id_text)
            enqueue_job(conn, "event_report_llm", {"event_id": event_id_text}, dedupe=True)
        _maybe_enqueue_article_product_enrich(conn, int(article_id), article["source_id"], logger)
        _maybe_enqueue_context_pack(conn, int(article_id), article["source_id"], logger)
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        return {"ok": True, "summary": summary_text, "profile_id": profile.get("id")}
    except Exception as exc:  # noqa: BLE001
        insert_llm_run(
            conn,
            job_id=None,
            provider_id=profile.get("primary_provider_id") if profile else None,
            model_id=profile.get("primary_model_id") if profile else None,
            prompt_name=profile.get("name") if profile else "summarize_article",
            input_chars=input_chars,
            output_chars=0,
            latency_ms=int((time.time() - start) * 1000),
            ok=False,
            error=str(exc),
        )
        update_article_summary(
            conn,
            int(article_id),
            summary_llm=None,
            summary_model=None,
            summary_generated_at=utc_now_iso(),
            summary_error=str(exc),
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        raise
    finally:
        if lease_name:
            release_lease(conn, lease_name, lease_holder)


def _handle_summarize_article_context_llm(
    conn, config, job: Job, logger: logging.Logger
) -> dict[str, object]:
    payload = job.payload or {}
    article_id = payload.get("article_id")
    if not article_id:
        raise ValueError("summarize_article_context_llm requires article_id")
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise ValueError("article_not_found")
    profile = None
    reason = "missing_profile"
    payload_profile_id = payload.get("profile_id")
    if isinstance(payload_profile_id, str) and payload_profile_id:
        profile = get_profile(conn, payload_profile_id)
        if not profile:
            log_event(
                logger,
                logging.WARNING,
                "llm_profile_missing",
                job_id=job.id,
                job_type=job.job_type,
                profile_id=payload_profile_id,
            )
    if not profile:
        profile, reason = get_active_profile_for_stage(conn, "article_context_pack")
    if not profile:
        update_article_context_pack(
            conn,
            int(article_id),
            context_llm=None,
            context_model=None,
            context_generated_at=utc_now_iso(),
            context_error=f"llm_stage_{reason}",
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        log_event(
            logger,
            logging.INFO,
            "llm_stage_skipped",
            stage="article_context_pack",
            reason=reason,
            article_id=article_id,
            source_id=article["source_id"],
        )
        raise ValueError(f"llm_stage_{reason}")
    source_name = get_source_name(conn, article["source_id"]) or ""
    content = article.get("content_text") or article.get("summary") or article.get("title") or ""
    if not content.strip():
        update_article_context_pack(
            conn,
            int(article_id),
            context_llm=None,
            context_model=None,
            context_generated_at=utc_now_iso(),
            context_error="missing_content",
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        raise ValueError("missing_content")
    input_chars = len(content or "")
    lease_holder = f"{job.id}:{article_id}"
    max_inflight = int(os.environ.get("SV_LLM_MAX_INFLIGHT", "1") or "1")
    max_inflight = max(1, max_inflight)
    lease_names = (
        ["article_context_pack_llm"]
        if max_inflight == 1
        else [f"article_context_pack_llm:{idx}" for idx in range(max_inflight)]
    )
    lease_name = None
    for attempt, delay in enumerate([2, 3, 5], start=1):
        for candidate in lease_names:
            if try_acquire_lease(conn, candidate, lease_holder, ttl_seconds=600):
                lease_name = candidate
                break
        if lease_name:
            break
        time.sleep(delay)
    if not lease_name:
        next_payload = dict(payload)
        next_payload["not_before"] = utc_now_iso_offset(seconds=30)
        requeue_job(conn, job.id, next_payload, next_payload["not_before"])
        return {"requeued": True, "reason": "llm_lease_unavailable"}
    start = time.time()
    try:
        input_text = (
            f"Title: {article.get('title')}\n"
            f"Source: {source_name}\n"
            f"Published: {article.get('published_at') or 'unknown'}\n"
            f"URL: {article.get('original_url') or article.get('normalized_url')}\n\n"
            f"Content:\n{content}\n"
        )
        result = run_pipeline_stage(
            conn,
            "article_context_pack",
            input_text,
            logger,
            profile_id=profile["id"],
            context={"stage": "article_context_pack", "job_type": job.job_type},
        )
        latency_ms = int((time.time() - start) * 1000)
        parsed = result.get("parsed")
        raw = result.get("raw") if isinstance(result, dict) else None
        if isinstance(parsed, (dict, list)):
            context_payload = json.dumps(parsed)
            output_text = json.dumps(parsed)
        elif isinstance(raw, str):
            context_payload = json.dumps({"context_pack": raw})
            output_text = raw
        else:
            raise ValueError("llm_empty_output")
        update_article_context_pack(
            conn,
            int(article_id),
            context_llm=context_payload,
            context_model=profile.get("model_name") or profile.get("primary_model_id"),
            context_generated_at=utc_now_iso(),
            context_error=None,
        )
        insert_llm_run(
            conn,
            job_id=None,
            provider_id=profile.get("primary_provider_id"),
            model_id=profile.get("primary_model_id"),
            prompt_name=profile.get("name") or "article_context_pack",
            input_chars=input_chars,
            output_chars=len(output_text or ""),
            latency_ms=latency_ms,
            ok=True,
            error=None,
        )
        for event_id in list_event_ids_for_article(conn, int(article_id)):
            event_id_text = str(event_id)
            update_event_summary_from_articles(conn, event_id_text)
            enqueue_job(conn, "event_report_llm", {"event_id": event_id_text}, dedupe=True)
        # Event derivation now runs after context pack completion so classification can use
        # article context rather than markdown write timing.
        if not list_event_ids_for_article(conn, int(article_id)) and not has_pending_article_job(
            conn, "derive_events_from_articles", int(article_id)
        ):
            enqueue_job(conn, "derive_events_from_articles", {"article_id": int(article_id)})
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        return {"ok": True, "article_id": article_id, "profile_id": profile.get("id")}
    except Exception as exc:  # noqa: BLE001
        insert_llm_run(
            conn,
            job_id=None,
            provider_id=profile.get("primary_provider_id") if profile else None,
            model_id=profile.get("primary_model_id") if profile else None,
            prompt_name=profile.get("name") if profile else "article_context_pack",
            input_chars=input_chars,
            output_chars=0,
            latency_ms=int((time.time() - start) * 1000),
            ok=False,
            error=str(exc),
        )
        update_article_context_pack(
            conn,
            int(article_id),
            context_llm=None,
            context_model=None,
            context_generated_at=utc_now_iso(),
            context_error=str(exc),
        )
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
        raise
    finally:
        if lease_name:
            release_lease(conn, lease_name, lease_holder)


def _handle_build_daily_brief(
    conn, config, job, logger: logging.Logger
) -> dict[str, object]:
    tz_name = config.app.timezone or "UTC"
    payload: dict[str, object] = job.payload or {}
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    day = str(payload.get("date") or "")
    if not day:
        day = datetime.now(tz).strftime("%Y-%m-%d")
    profile_id = payload.get("profile_id")

    def _abort_if_canceled(stage: str) -> bool:
        if is_job_canceled(conn, job.id):
            log_event(
                logger,
                logging.INFO,
                "job_canceled",
                job_id=job.id,
                stage=stage,
                job_type=job.job_type,
            )
            return True
        return False

    if _abort_if_canceled("start"):
        return {"canceled": True, "day": day}

    def _run_stage_with_heartbeat(stage: str, payload_text: str, profile_id: str) -> dict[str, object]:
        stop = threading.Event()

        def _heartbeat() -> None:
            while not stop.wait(30):
                try:
                    beat_conn = init_db()
                    touch_job_lock(beat_conn, job.id)
                    beat_conn.close()
                except Exception:
                    pass

        thread = threading.Thread(target=_heartbeat, daemon=True)
        thread.start()
        try:
            return run_pipeline_stage(
                conn,
                stage,
                payload_text,
                logger,
                profile_id=profile_id,
                context={"stage": stage, "job_type": job.job_type},
            )
        finally:
            stop.set()
            thread.join(timeout=1)

    log_event(
        logger,
        logging.INFO,
        "daily_brief_stage_start",
        job_id=job.id,
        stage="gather_articles",
        day=day,
    )
    articles = _get_today_articles_for_brief(conn, day, logger)
    log_event(
        logger,
        logging.INFO,
        "daily_brief_stage_done",
        job_id=job.id,
        stage="gather_articles",
        day=day,
        count=len(articles),
    )
    if articles:
        sample = []
        for article in articles[:3]:
            if not isinstance(article, dict):
                continue
            sample.append(
                {
                    "id": article.get("id"),
                    "source_name": article.get("source_name"),
                    "title": article.get("title"),
                    "canonical_url": article.get("canonical_url"),
                    "original_url": article.get("original_url"),
                    "normalized_url": article.get("normalized_url"),
                }
            )
        log_event(
            logger,
            logging.INFO,
            "daily_brief_articles_sample",
            job_id=job.id,
            day=day,
            sample=json.dumps(sample, ensure_ascii=True),
        )
    if not articles:
        return {"status": "no_articles", "day": day}
    if _abort_if_canceled("after_gather_articles"):
        return {"canceled": True, "day": day}

    citations, article_to_citation = _build_daily_brief_citations(articles)
    if len(citations) != len(articles):
        log_event(
            logger,
            logging.WARNING,
            "daily_brief_citations_mismatch",
            job_id=job.id,
            day=day,
            article_count=len(articles),
            citation_count=len(citations),
        )
        citations, article_to_citation = _force_daily_brief_citations(articles)
    if len(citations) != len(articles):
        raise ValueError("daily_brief_citations_mismatch")
    if not citations:
        raise ValueError("daily_brief_citations_empty")
    log_event(
        logger,
        logging.INFO,
        "daily_brief_citations_ready",
        job_id=job.id,
        day=day,
        citation_count=len(citations),
        article_count=len(articles),
    )
    citations_by_id = {item["id"]: item for item in citations if isinstance(item, dict)}

    cluster_dropped = 0
    topics: list[dict[str, object]] = []
    articles_for_llm = []
    for article in articles:
        try:
            article_id = int(article.get("id") or 0)
        except Exception:
            article_id = 0
        context_raw = article.get("context_llm")
        context_pack: object = ""
        if context_raw:
            if isinstance(context_raw, (dict, list)):
                context_pack = context_raw
            else:
                try:
                    context_pack = json.loads(str(context_raw))
                except Exception:
                    context_pack = _clean_summary_text(context_raw)
        if not context_pack:
            context_pack = _clean_summary_text(article.get("summary_llm")) or ""
        articles_for_llm.append(
            {
                "id": article.get("id"),
                "citation_id": article_to_citation.get(article_id),
                "title": article.get("title"),
                "source": article.get("source_name"),
                "url": _normalize_canonical_url(str(article.get("canonical_url") or "")),
                "published_at": article.get("published_at"),
                "cves": article.get("cves") or [],
                "tags": article.get("tags") or [],
                "context_pack": context_pack,
            }
        )
    overall_input = {
        "day": day,
        "citations": citations,
        "articles": articles_for_llm,
        "nist_families": [
            {
                "code": code,
                "title": title,
                "description": NIST_FAMILY_DETAILS.get(code, ""),
            }
            for code, title in NIST_FAMILY_TITLES.items()
        ],
    }
    overall_profile_id = profile_id if isinstance(profile_id, str) else None
    if not overall_profile_id:
        overall_profile, overall_reason = get_active_profile_for_stage(
            conn, "daily_brief_overall_synthesis"
        )
        if not overall_profile:
            raise ValueError(f"llm_stage_{overall_reason}")
        overall_profile_id = overall_profile["id"]
    log_event(
        logger,
        logging.INFO,
        "daily_brief_stage_start",
        job_id=job.id,
        stage="daily_brief_overall_synthesis",
        day=day,
    )
    overall_result = _run_stage_with_heartbeat(
        "daily_brief_overall_synthesis",
        json.dumps(overall_input, indent=2),
        overall_profile_id,
    )
    log_event(
        logger,
        logging.INFO,
        "daily_brief_stage_done",
        job_id=job.id,
        stage="daily_brief_overall_synthesis",
        day=day,
    )
    overall_payload = overall_result.get("parsed") if isinstance(overall_result, dict) else {}
    raw_overall = overall_result.get("raw") if isinstance(overall_result, dict) else ""
    if not isinstance(overall_payload, dict):
        raise ValueError("daily_brief_overall_invalid")
    if raw_overall:
        overall_payload["raw"] = raw_overall
    nist_breakdown = _normalize_nist_breakdown(overall_payload.get("families"), logger)
    if not nist_breakdown:
        raise ValueError("daily_brief_nist_empty")
    nist_breakdown = _dedupe_nist_citation_assignments(nist_breakdown)
    max_citation_id = len(citations)
    raw_tldr = overall_payload.get("tldr")
    if not isinstance(raw_tldr, list):
        raise ValueError("daily_brief_tldr_invalid")
    tldr_items: list[dict[str, object]] = []
    for item in raw_tldr:
        if isinstance(item, dict):
            text = _remap_citations_in_text(
                str(item.get("text") or ""), article_to_citation, max_citation_id
            )
            text = _strip_tldr_prefix(text)
            item_citations = _remap_citation_ids(
                item.get("citations"), article_to_citation, max_citation_id
            )
            if text.strip():
                tldr_items.append({"text": text.strip(), "citations": item_citations})
            continue
        if isinstance(item, str):
            text = _strip_tldr_prefix(
                _remap_citations_in_text(item, article_to_citation, max_citation_id)
            )
            if text.strip():
                tldr_items.append(
                    {
                        "text": text.strip(),
                        "citations": _collect_citations_from_text(text),
                    }
                )
    if not tldr_items:
        raise ValueError("daily_brief_tldr_empty")
    technical_synthesis = overall_payload.get("technical_synthesis")
    if not isinstance(technical_synthesis, dict):
        raise ValueError("daily_brief_technical_synthesis_invalid")
    technical_synthesis["citations"] = _remap_citation_ids(
        technical_synthesis.get("citations"), article_to_citation, max_citation_id
    )
    technical_synthesis["text"] = _remap_citations_in_text(
        str(technical_synthesis.get("text") or ""), article_to_citation, max_citation_id
    )
    if not str(technical_synthesis.get("text") or "").strip():
        raise ValueError("daily_brief_technical_synthesis_empty")
    actions = overall_payload.get("actions")
    if not isinstance(actions, list):
        raise ValueError("daily_brief_actions_invalid")
    actions = [
        item
        for item in actions
        if isinstance(item, dict) and str(item.get("action") or "").strip()
    ]
    for item in actions:
        item["citations"] = _remap_citation_ids(
            item.get("citations"), article_to_citation, max_citation_id
        )
        item["action"] = _remap_citations_in_text(
            str(item.get("action") or ""), article_to_citation, max_citation_id
        )
        item["why"] = _remap_citations_in_text(
            str(item.get("why") or ""), article_to_citation, max_citation_id
        )
    if not actions:
        raise ValueError("daily_brief_actions_empty")
    low_value = overall_payload.get("low_value") if isinstance(overall_payload.get("low_value"), list) else []
    for item in low_value:
        if not isinstance(item, dict):
            continue
        item["citation_id"] = _remap_citation_ids(
            [item.get("citation_id")], article_to_citation, max_citation_id
        )[:1][0] if item.get("citation_id") is not None else item.get("citation_id")
    detected_low_value = _detect_low_value_entries(articles, article_to_citation)
    if detected_low_value:
        low_value = detected_low_value
    podcast_script = (
        str(overall_payload.get("podcast_script") or "").strip()
        if isinstance(overall_payload.get("podcast_script"), str)
        else ""
    )

    for family in nist_breakdown:
        if not isinstance(family, dict):
            continue
        family["summary"] = _remap_citations_in_text(
            str(family.get("summary") or ""), article_to_citation, max_citation_id
        )
        for sub in family.get("subtopics") or []:
            if not isinstance(sub, dict):
                continue
            sub["citations"] = _remap_citation_ids(
                sub.get("citations"), article_to_citation, max_citation_id
            )
            sub["narrative"] = _remap_citations_in_text(
                str(sub.get("narrative") or ""), article_to_citation, max_citation_id
            )
    low_value_ids: set[int] = set()
    for item in low_value:
        if not isinstance(item, dict):
            continue
        try:
            low_value_ids.add(int(item.get("citation_id")))
        except Exception:
            continue
    family_citations: set[int] = set()
    for family in nist_breakdown:
        if not isinstance(family, dict):
            continue
        for sub in family.get("subtopics") or []:
            if not isinstance(sub, dict):
                continue
            for cid in sub.get("citations") or []:
                try:
                    family_citations.add(int(cid))
                except Exception:
                    continue
    non_low_value = {cid for cid in range(1, max_citation_id + 1) if cid not in low_value_ids}
    if not non_low_value:
        non_low_value = set(range(1, max_citation_id + 1))
    if len(family_citations) < len(non_low_value):
        nist_breakdown = _build_fallback_families_from_articles(
            articles=articles,
            citations=citations,
            article_to_citation=article_to_citation,
            low_value_ids=low_value_ids,
        )
        nist_breakdown = _dedupe_nist_citation_assignments(nist_breakdown)
    _format_technical_synthesis(technical_synthesis)
    _ensure_min_technical_synthesis(technical_synthesis, nist_breakdown)
    daily_cves = _get_daily_cves_for_brief(conn, day, limit=60)
    brief_payload = {
        "meta": {
            "brief_day": day,
            "generated_at": datetime.now(tz).isoformat(),
            "article_count": len(articles),
            "citation_count": len(citations),
            "topic_count": len(topics),
            "family_count": len(nist_breakdown),
            "coverage": {
                "cited_article_ids": [],
                "uncited_article_ids": [],
                "low_value_article_ids": [],
            },
            "dropped_topics": cluster_dropped,
            "cluster_profile_id": None,
            "summarize_profile_id": None,
            "nist_profile_id": None,
            "overall_profile_id": overall_result.get("profile_id"),
            "cluster_raw": None,
            "summarize_raw": None,
            "nist_raw": None,
            "overall_raw": raw_overall,
        },
        "tldr": tldr_items,
        "technical_synthesis": technical_synthesis,
        "actions": actions,
        "families": nist_breakdown,
        "low_value": low_value,
        "daily_cves": daily_cves,
        "podcast_script": podcast_script,
        "citations": citations,
    }
    log_event(
        logger,
        logging.INFO,
        "daily_brief_payload_ready",
        job_id=job.id,
        day=day,
        citation_count=len(brief_payload.get("citations") or []),
        article_count=len(articles),
    )
    if len(brief_payload.get("citations") or []) != len(articles):
        raise ValueError("daily_brief_payload_citations_mismatch")

    article_by_citation = {
        cid: next(
            (article for article in articles if int(article.get("id") or 0) == citations_by_id[cid].get("article_id")),
            {},
        )
        for cid in citations_by_id
    }
    _ensure_daily_brief_coverage(brief_payload, citations, article_by_citation)

    if isinstance(profile_id, str):
        brief_payload["meta"]["profile_id"] = profile_id
    upsert_daily_brief(conn, brief_payload)
    log_event(
        logger,
        logging.INFO,
        "daily_brief_persisted",
        day=day,
        count=len(articles),
    )
    mark_build_dirty(conn, reason="build_daily_brief")
    return {"day": day, "count": len(articles), "status": "db_persisted"}


def _handle_smoke_test(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    payload = job.payload or {}
    sources_limit = int(payload.get("sources_limit") or 2)
    per_source_limit = int(payload.get("per_source_limit") or 10)
    timeout_seconds = int(payload.get("timeout_seconds") or 300)
    skip_ingest = bool(payload.get("skip_ingest"))
    skip_cve_sync = bool(payload.get("skip_cve_sync"))
    skip_events = bool(payload.get("skip_events"))
    skip_build = bool(payload.get("skip_build"))
    result: dict[str, object] = {"steps": []}

    def update_step(step: str, status: str, **extra) -> None:
        entry = {"step": step, "status": status}
        if extra:
            entry.update(extra)
        result["steps"].append(entry)
        update_job_result(conn, job.id, result)

    if is_job_canceled(conn, job.id):
        return {"canceled": True}

    start_marker = utc_now_iso()
    ingest_job_ids: list[str] = []
    if skip_ingest:
        update_step("ingest_sources", "skipped", reason="skip_ingest")
    else:
        sources = list_sources(conn, enabled_only=True)[: max(0, sources_limit)]
        if not sources:
            update_step("ingest_sources", "skipped", reason="no_sources")
        else:
            for source in sources:
                if is_job_canceled(conn, job.id):
                    return {"canceled": True}
                job_id = enqueue_job(
                    conn,
                    "ingest_source",
                    {"source_id": source.id, "limit": per_source_limit},
                )
                ingest_job_ids.append(job_id)
            update_step("ingest_sources", "enqueued", jobs=ingest_job_ids)

            _run_jobs_inline(
                conn,
                config,
                logger,
                allowed_types=["ingest_source"],
                timeout_seconds=timeout_seconds,
            )
            done = [get_job(conn, job_id) for job_id in ingest_job_ids]
            article_count = sum(
                int((job.result or {}).get("accepted_count") or 0) for job in done if job
            )
            update_step("ingest_sources", "completed", article_count_ingested=article_count)

            post_types = ["fetch_article_content", "summarize_article_llm"]
            if is_article_markdown_enabled():
                post_types.append("write_article_markdown")
            _run_jobs_inline(
                conn,
                config,
                logger,
                allowed_types=post_types,
                timeout_seconds=timeout_seconds,
            )
            jobs = list_jobs_by_types_since(
                conn,
                types=post_types,
                since=start_marker,
            )
            result["article_count_ingested"] = article_count
            result["content_fetch_ok"] = sum(1 for j in jobs if j.job_type == "fetch_article_content" and j.status == "succeeded")
            result["content_fetch_failed"] = sum(1 for j in jobs if j.job_type == "fetch_article_content" and j.status == "failed")
            result["summarize_ok"] = sum(1 for j in jobs if j.job_type == "summarize_article_llm" and j.status == "succeeded")
            result["summarize_failed"] = sum(1 for j in jobs if j.job_type == "summarize_article_llm" and j.status == "failed")
            result["markdown_ok"] = sum(1 for j in jobs if j.job_type == "write_article_markdown" and j.status == "succeeded")
            result["markdown_failed"] = sum(1 for j in jobs if j.job_type == "write_article_markdown" and j.status == "failed")
            update_job_result(conn, job.id, result)

    if is_job_canceled(conn, job.id):
        return {"canceled": True}

    if skip_cve_sync:
        update_step("cve_sync", "skipped", reason="skip_cve_sync")
    else:
        try:
            cve_result = _handle_cve_sync(conn, config, logger)
            update_step("cve_sync", "ok", **cve_result)
        except Exception as exc:  # noqa: BLE001
            update_step("cve_sync", "error", error=str(exc))

    if is_job_canceled(conn, job.id):
        return {"canceled": True}

    if skip_events:
        update_step("events_rebuild", "skipped", reason="skip_events")
    else:
        try:
            events_result = _handle_events_rebuild(conn, config, {"limit": 200}, logger)
            update_step("events_rebuild", "ok", **events_result)
        except Exception as exc:  # noqa: BLE001
            update_step("events_rebuild", "error", error=str(exc))

    if is_job_canceled(conn, job.id):
        return {"canceled": True}

    if skip_build:
        update_step("build_site", "skipped", reason="skip_build")
    else:
        build_job_id = enqueue_job(conn, "build_site", None, debounce=True)
        update_step("build_site", "enqueued", job_id=build_job_id)
        _wait_for_job(conn, build_job_id, timeout_seconds)
        build_job = get_job(conn, build_job_id)
        if build_job:
            result["build_ok"] = build_job.status == "succeeded"
            result["build_exit_code"] = ( build_job.result or {}).get("exit_code")
            update_job_result(conn, job.id, result)
    return result


def _extract_event_entity(title: str) -> str:
    if not title:
        return ""
    for sep in ( ":", " - ", " – ", " — "):
        if sep in title:
            return title.split(sep, 1)[0].strip()
    match = re.search(r"([A-Z][A-Za-z0-9&.\-]*(?:\s+[A-Z][A-Za-z0-9&.\-]*){0,3})", title)
    if match:
        return match.group(1).strip()
    return title.strip().split(" ")[0]


def _normalize_entity(value: str) -> str:
    if not value:
        return ""
    lowered = value.strip().lower()
    generic = {
        "security", "cybersecurity", "research", "report", "analysis", "study",
        "survey", "update", "advisory", "guidance", "warning", "alert", "newsletter",
        "roundup", "weekly", "monthly", "daily", "podcast", "webinar", "trend",
    }
    if lowered in generic:
        return ""
    return value.strip()


_EVENT_TYPE_MAP = {
    "breach": "breach",
    "data_breach": "breach",
    "data_leak": "breach",
    "data_exposure": "breach",
    "ransomware": "ransomware",
    "compromise": "compromise",
    "intrusion": "compromise",
    "service_compromise": "compromise",
    "active_exploitation": "exploit_in_the_wild",
    "exploit_in_the_wild": "exploit_in_the_wild",
    "ddos": "outage",
    "outage": "outage",
    "major_outage_security_related": "outage",
}

_STRICT_EVENT_TYPES = {"breach", "ransomware", "compromise", "exploit_in_the_wild", "outage"}


def _normalize_event_type(value: str) -> str:
    if not value:
        return ""
    normalized = normalize_name(value)
    return _EVENT_TYPE_MAP.get(normalized, "")


def _extract_compromise_scope(text: str) -> str | None:
    lowered = text.lower()
    scope_cues = [
        "data stolen",
        "customer data",
        "pii",
        "credentials",
        "accessed",
        "exfiltrated",
        "encrypted",
        "systems impacted",
        "service disruption",
        "outage",
        "ransomware",
    ]
    for cue in scope_cues:
        if cue in lowered:
            return cue
    return None


def _is_primary_source(article: dict[str, object]) -> bool:
    source_name = str(article.get("source_name") or "").lower()
    url_value = str(article.get("original_url") or article.get("normalized_url") or "").lower()
    primary_name_cues = (
        "cisa",
        "fbi",
        "cert",
        "nvd",
        "us-cert",
        "ics-cert",
        "security advisory",
        "incident disclosure",
    )
    if any(cue in source_name for cue in primary_name_cues):
        return True
    # Only treat clearly authoritative disclosure domains as primary.
    primary_domain_cues = (
        "nvd.nist.gov",
        "cisa.gov",
        "fbi.gov",
        "cert.org",
        "msrc.microsoft.com",
    )
    return any(cue in url_value for cue in primary_domain_cues)


def _maybe_promote_event_lifecycle(
    conn,
    event_id: str,
    article: dict[str, object],
    *,
    min_confirm_confidence: float = 0.8,
) -> str:
    linked = list_event_articles(conn, event_id)
    distinct_sources = {
        str(item.get("source_id") or "").strip()
        for item in linked
        if str(item.get("source_id") or "").strip()
    }
    # Auto-confirm only after three distinct sources have reported the event.
    if len(distinct_sources) >= 3:
        update_event(conn, event_id, candidate=False, lifecycle="confirmed", status="confirmed")
        return "confirmed"
    update_event(conn, event_id, candidate=True, lifecycle="candidate", status="candidate")
    return "candidate"


def _is_generic_event_entity(entity: str) -> bool:
    value = normalize_name(entity or "").replace("_", " ").strip()
    if not value:
        return True
    if value.startswith(("multiple ", "various ", "many ", "several ", "unknown ")):
        return True
    generic_terms = {
        "multiple", "various", "many", "several", "users", "customers", "victims",
        "organizations", "companies", "devices", "systems", "endpoints", "people",
        "individuals", "accounts", "applications", "apps", "servers", "networks",
    }
    tokens = set(value.split())
    return bool(tokens & generic_terms)


def _non_event_reason(text: str) -> str | None:
    lowered = text.lower()
    non_event = [
        "survey", "report", "research", "study", "analysis", "trends", "insights",
        "guide", "how to", "best practices", "prevention", "tips", "webinar",
        "podcast", "weekly", "monthly", "roundup", "forecast", "prediction",
        "statistics", "benchmark", "whitepaper", "proof of concept", "poc",
        "new malware", "malware found", "researchers discovered",
    ]
    for cue in non_event:
        if cue in lowered:
            return cue
    return None


def _has_event_qualifier(text: str, entity: str) -> tuple[bool, list[str]]:
    lowered = text.lower()
    reasons: list[str] = []
    incident_cues = [
        "breach", "data leak", "data exposure", "exposed", "stolen", "exfiltrated",
        "ransomware", "extortion", "compromise", "intrusion", "incident",
        "outage", "disruption",
    ]
    exploit_cues = ["exploited in the wild", "actively exploited", "in the wild", "weaponized"]
    law_cues = ["arrested", "charged", "indicted", "law enforcement", "seized", "takedown"]
    victim_cues = [
        "victim", "victims", "targeted", "hit by", "breach at", "breached",
        "ransomware attack on", "compromised", "suffered a breach",
    ]
    for cue in incident_cues:
        if cue in lowered:
            reasons.append(f"incident:{cue}")
    for cue in exploit_cues:
        if cue in lowered:
            reasons.append(f"exploit:{cue}")
    for cue in law_cues:
        if cue in lowered:
            reasons.append(f"law:{cue}")
    if entity:
        for cue in victim_cues:
            if cue in lowered:
                reasons.append(f"victim:{cue}")
                break
    return bool(reasons), reasons


def _derive_event_kind(text: str) -> str:
    lowered = text.lower()
    if any(word in lowered for word in ( "ransomware", "extortion")):
        return "ransomware"
    if any(word in lowered for word in ( "breach", "data leak", "leak")):
        return "breach"
    if any(word in lowered for word in ( "compromise", "intrusion")):
        return "compromise"
    if any(word in lowered for word in ( "exploited in the wild", "actively exploited", "in the wild")):
        return "exploit_in_the_wild"
    if any(word in lowered for word in ( "outage", "service disruption", "service unavailable", "denial of service")):
        return "outage"
    return "other"


def _derive_confidence_tier(text: str) -> str:
    lowered = text.lower()
    confirmed = ("confirmed", "official", "cisa", "fbi", "disclosed", "filing")
    likely = ( "likely", "suspected", "reportedly", "investigating", "possible")
    if any(word in lowered for word in confirmed):
        return "confirmed"
    if any(word in lowered for word in likely):
        return "likely"
    return "watch"

def _slugify(value: str) -> str:
    return normalize_name(value).replace("_", "-")


def _extract_incident_date(text: str) -> str | None:
    if not text:
        return None
    for match in re.finditer(r"\b(20\d{2}-\d{2}-\d{2})\b", text):
        return match.group(1)
    return None


def _derive_confidence(text: str) -> tuple[float, bool, list[str]]:
    lowered = text.lower()
    confirmed_cues = [
        "breach confirmed", "data stolen", "ransomware attack", "filing", "disclosed",
        "victims", "ioc", "attributed", "took responsibility", "compromised", "intrusion",
        "exploited in the wild", "actively exploited",
    ]
    speculative_cues = [
        "may have", "potential", "alleged", "reportedly", "possible", "suspected"
    ]
    evidence = []
    score = 0.5
    for cue in confirmed_cues:
        if cue in lowered:
            score += 0.1
            evidence.append(f"confirmed:{cue}")
    for cue in speculative_cues:
        if cue in lowered:
            score -= 0.1
            evidence.append(f"speculative:{cue}")
    score = max(0.0, min(1.0, score))
    candidate = score < 0.7
    return score, candidate, evidence



def _event_admission_policy(conn) -> dict[str, object]:
    settings = get_events_settings(conn)
    min_create_conf = float(settings.get("min_create_confidence", 0.55) or 0.55)
    min_confirm_conf = float(settings.get("min_confirm_confidence", 0.8) or 0.8)
    min_signal_reasons = int(settings.get("min_signal_reasons", 1) or 1)
    allow_cve_only_create = bool(settings.get("allow_cve_only_create", False))
    min_create_conf = max(0.0, min(1.0, min_create_conf))
    min_confirm_conf = max(0.0, min(1.0, min_confirm_conf))
    min_signal_reasons = max(0, min_signal_reasons)
    return {
        "min_create_confidence": min_create_conf,
        "min_confirm_confidence": min_confirm_conf,
        "min_signal_reasons": min_signal_reasons,
        "allow_cve_only_create": allow_cve_only_create,
    }


def _event_kind_label(kind: str) -> str:
    labels = {
        "breach": "Breach disclosed",
        "compromise": "Compromise",
        "ransomware": "Ransomware attack",
        "intrusion": "Intrusion",
        "malware_campaign": "Campaign",
        "campaign": "Campaign",
        "exploit_in_the_wild": "Exploit in the wild",
        "exploit": "Exploit",
        "advisory": "Advisory",
        "vuln_disclosure": "Vulnerability disclosure",
        "outage": "Outage",
    }
    return labels.get(kind, kind.title() if kind else "Event")


def _maybe_queue_event_research(conn, event_id: str) -> bool:
    try:
        settings = get_events_settings(conn)
    except Exception:
        return False
    min_articles = int(settings.get("enrich_min_articles", 0) or 0)
    if min_articles <= 0:
        return False
    article_count = len(list_event_articles(conn, event_id))
    if article_count >= min_articles:
        return False
    max_results = int(settings.get("enrich_min_articles_max_results", 12) or 12)
    enqueue_job(
        conn,
        "enrich_event_from_web",
        {"event_id": event_id, "max_results": max_results, "replace_existing": False},
        debounce=True,
        dedupe=True,
    )
    return True


def _handle_derive_events_from_articles(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    article_id = payload.get("article_id")
    if article_id is None:
        # Keep each claimed derive job small and predictable: process one article.
        # This avoids long-running "batch" claims that make queue progress look stalled.
        article_ids = list_article_ids_without_event(conn, limit=1)
        if not article_ids:
            return {"status": "skipped", "reason": "no_candidates"}
        item_id = int(article_ids[0])
        result = _handle_derive_events_from_articles(
            conn, config, {"article_id": item_id}, logger
        )
        return {"status": "single", "article_id": item_id, "result": result}
    article_id = int(article_id)

    def _done(result: dict[str, object]) -> dict[str, object]:
        # Mark this article as derive-events checked so queueable "need" can drain.
        mark_article_events_checked(conn, article_id, commit=False)
        return result

    if list_event_ids_for_article(conn, article_id):
        return _done({"status": "skipped", "reason": "already_linked"})
    article = get_article_by_id(conn, article_id)
    if not article:
        return _done({"status": "skipped", "reason": "article_missing"})
    title = str(article.get("title") or "")
    summary = str(article.get("summary") or "")
    content = str(article.get("content_text") or "")
    combined = " ".join(part for part in ( title, summary, content) if part).strip()
    if not combined:
        return _done({"status": "skipped", "reason": "no_content"})
    policy = _event_admission_policy(conn)
    cve_ids = list_article_cve_ids(conn, article_id)
    is_primary_source = _is_primary_source(article)
    profile, reason = get_active_profile_for_stage(conn, "derive_events_from_articles")
    if profile:
        if not content:
            return _done({"status": "skipped", "reason": "no_full_content"})
        excerpt = content.strip()
        if len(excerpt) > 20000:
            excerpt = excerpt[:20000] + "\n[TRUNCATED]"
        input_lines = [
            f"Title: {title}",
            f"Published: {article.get('published_at') or article.get('ingested_at') or ''}",
            f"URL: {article.get('original_url') or article.get('normalized_url') or ''}",
            "",
            "Content:",
            excerpt,
        ]
        result = run_pipeline_stage(
            conn,
            "derive_events_from_articles",
            "\n".join(input_lines).strip(),
            logger,
            profile_id=profile["id"],
            context={"stage": "derive_events_from_articles", "job_type": "derive_events_from_articles"},
        )
        parsed, error_reason = _parse_event_classification(result if isinstance(result, dict) else {})
        if error_reason:
            log_event(
                logger,
                logging.WARNING,
                "event_classify_failed",
                article_id=article_id,
                reason=error_reason,
            )
        else:
            if not parsed.get("is_event"):
                return _done({"status": "skipped", "reason": "llm_non_event"})
            event_type_raw = str(parsed.get("event_type") or "").strip()
            victim_raw = str(parsed.get("victim") or "").strip()
            kind = _normalize_event_type(event_type_raw)
            entity = _normalize_entity(victim_raw)
            headline = str(parsed.get("headline") or "").strip()
            summary_text = str(parsed.get("summary") or "").strip() or None
            llm_scope = str(parsed.get("what_compromised") or "").strip()
            scope = llm_scope or _extract_compromise_scope(" ".join([headline, summary_text or "", combined]))
            if not kind or kind not in _STRICT_EVENT_TYPES:
                return _done({"status": "skipped", "reason": "llm_non_incident_type"})
            if not entity:
                return _done({"status": "skipped", "reason": "llm_missing_entity"})
            if not scope:
                return _done({"status": "skipped", "reason": "llm_missing_scope"})
            parsed_incident_date = str(parsed.get("incident_date") or "").strip()
            if parsed_incident_date.lower() in {"unknown", "n/a", "none", "null"}:
                parsed_incident_date = ""
            bucket = parsed_incident_date or (article.get("published_at") or article.get("ingested_at") or "")[:10] or utc_now_iso()[:10]
            kind_label = _event_kind_label(kind)
            event_title = headline or f"{entity} — {kind_label} — {bucket}"
            event_key = f"event:{kind}:{_slugify(str(entity))}"
            confidence = float(parsed.get("confidence") or 0) / 100.0
            confidence_tier = _derive_confidence_tier(" ".join([headline, summary_text or "", combined]))
            _, qualifier_reasons = _has_event_qualifier(
                " ".join([headline, summary_text or "", combined]),
                entity,
            )
            if len(qualifier_reasons) < int(policy["min_signal_reasons"]):
                return _done({"status": "skipped", "reason": "llm_insufficient_signals"})
            if _is_generic_event_entity(entity):
                return _done({"status": "skipped", "reason": "llm_generic_entity"})
            if kind in {"breach", "compromise", "ransomware"} and not any(
                reason.startswith("victim:") for reason in qualifier_reasons
            ):
                return _done({"status": "skipped", "reason": "llm_missing_victim_signal"})
            llm_non_event = _non_event_reason(" ".join([headline, summary_text or "", combined]))
            if llm_non_event and not any(reason.startswith(("victim:", "law:")) for reason in qualifier_reasons):
                return _done({"status": "skipped", "reason": "llm_non_incident", "detail": llm_non_event})
            if (
                confidence < float(policy["min_create_confidence"])
                and not is_primary_source
                and not (bool(policy["allow_cve_only_create"]) and bool(cve_ids))
            ):
                return _done({"status": "skipped", "reason": "llm_low_confidence"})
            candidate = True
            status = "candidate"
            event_id, _ = upsert_event_by_key(
                conn,
                event_key=event_key,
                kind=kind,
                title=event_title,
                severity="UNKNOWN",
                first_seen_at=article.get("published_at") or article.get("ingested_at") or utc_now_iso(),
                last_seen_at=utc_now_iso(),
                summary=summary_text,
                meta={"seed_article_id": article_id},
                manual=False,
                visibility="active",
                confidence=confidence,
                confidence_tier=confidence_tier,
                candidate=candidate,
                lifecycle=status,
                entity=entity,
                incident_date=bucket,
                evidence=["llm:derive_events_from_articles", f"scope:{scope}"],
                reasons=["llm:derive_events_from_articles", f"scope:{scope}"] + qualifier_reasons,
                status=status,
            )
            log_event(
                logger,
                logging.INFO,
                "event_created_from_article",
                event_id=event_id,
                article_id=article_id,
                kind=kind,
                confidence=confidence,
                entity=entity,
                reasons=["llm"],
            )
            link_event_article(conn, event_id, article_id, "llm")
            lifecycle = _maybe_promote_event_lifecycle(
                conn,
                event_id,
                article,
                min_confirm_confidence=float(policy["min_confirm_confidence"]),
            )
            for cve_id in cve_ids:
                upsert_event_item(conn, event_id, "cve", cve_id)
                for product_key in list_product_keys_for_cve(conn, cve_id):
                    upsert_event_item(conn, event_id, "product", product_key)
            update_event_summary_from_articles(conn, event_id)
            enqueue_job(conn, "event_report_llm", {"event_id": event_id}, dedupe=True)
            _maybe_queue_event_research(conn, event_id)
            return _done({
                "status": "linked",
                "event_id": event_id,
                "cves": len(cve_ids),
                "source": "llm",
                "lifecycle": lifecycle,
            })
    kind = _derive_event_kind(combined)
    confidence, _, evidence = _derive_confidence(combined)
    if kind not in _STRICT_EVENT_TYPES:
        return _done({"status": "skipped", "reason": "non_incident_kind"})
    incident_date = _extract_incident_date(combined) or ( article.get("published_at") or "")[:10] or None
    entity = _normalize_entity(_extract_event_entity(title))
    if not entity and kind == "exploit_in_the_wild":
        for cve_id in cve_ids:
            product_keys = list_product_keys_for_cve(conn, cve_id)
            if product_keys:
                display = get_product_display_by_key(conn, product_keys[0])
                if display:
                    entity = _normalize_entity(f"{display['vendor']} {display['product']}")
                    break
    non_event = _non_event_reason(combined)
    has_qualifier, qualifier_reasons = _has_event_qualifier(combined, entity)
    strong_signal = any(
        reason.startswith(("incident:", "exploit:", "law:", "victim:"))
        for reason in qualifier_reasons
    )
    if len(qualifier_reasons) < int(policy["min_signal_reasons"]):
        return _done({"status": "skipped", "reason": "insufficient_signals"})
    if non_event and not strong_signal:
        return _done({"status": "skipped", "reason": "non_incident", "detail": non_event})
    if not strong_signal:
        return _done({"status": "skipped", "reason": "no_incident_signal"})
    if not entity:
        return _done({"status": "skipped", "reason": "entity_missing"})
    if _is_generic_event_entity(entity):
        return _done({"status": "skipped", "reason": "entity_generic"})
    if kind in {"breach", "compromise", "ransomware"} and not any(
        reason.startswith("victim:") for reason in qualifier_reasons
    ):
        return _done({"status": "skipped", "reason": "missing_victim_signal"})
    scope = _extract_compromise_scope(combined)
    if not scope:
        return _done({"status": "skipped", "reason": "scope_missing"})
    if (
        confidence < float(policy["min_create_confidence"])
        and not is_primary_source
        and not (bool(policy["allow_cve_only_create"]) and bool(cve_ids))
    ):
        return _done({"status": "skipped", "reason": "low_confidence"})
    bucket = incident_date or ( article.get("published_at") or article.get("ingested_at") or "")[:10]
    bucket = bucket or utc_now_iso()[:10]
    kind_label = _event_kind_label(kind)
    event_title = f"{entity} — {kind_label} — {bucket}"
    event_key = f"event:{kind}:{_slugify(str(entity))}"
    confidence_tier = _derive_confidence_tier(combined)
    candidate = True
    lifecycle = "candidate"
    status = "candidate"
    event_id, _ = upsert_event_by_key(
        conn,
        event_key=event_key,
        kind=kind,
        title=event_title,
        severity="UNKNOWN",
        first_seen_at=article.get("published_at") or article.get("ingested_at") or utc_now_iso(),
        last_seen_at=utc_now_iso(),
        meta={"seed_article_id": article_id},
        manual=False,
        visibility="active",
        confidence=confidence,
        confidence_tier=confidence_tier,
        candidate=candidate,
        lifecycle=lifecycle,
        entity=entity,
        incident_date=bucket,
        evidence=evidence + qualifier_reasons + [f"scope:{scope}"],
        reasons=["derived:article"] + qualifier_reasons + [f"scope:{scope}"],
        status=status,
    )
    log_event(
        logger,
        logging.INFO,
        "event_created_from_article",
        event_id=event_id,
        article_id=article_id,
        kind=kind,
        candidate=candidate,
        confidence=confidence,
        entity=entity,
        reasons=evidence + qualifier_reasons,
    )
    link_event_article(conn, event_id, article_id, "auto")
    lifecycle = _maybe_promote_event_lifecycle(
        conn,
        event_id,
        article,
        min_confirm_confidence=float(policy["min_confirm_confidence"]),
    )
    for cve_id in cve_ids:
        upsert_event_item(conn, event_id, "cve", cve_id)
        for product_key in list_product_keys_for_cve(conn, cve_id):
            upsert_event_item(conn, event_id, "product", product_key)
    update_event_summary_from_articles(conn, event_id)
    enqueue_job(conn, "event_report_llm", {"event_id": event_id}, dedupe=True)
    return _done({
        "status": "linked",
        "event_id": event_id,
        "cves": len(cve_ids),
        "lifecycle": lifecycle,
    })


def _handle_enrich_event_from_web(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    event_id = str(payload.get("event_id") or "")
    if not event_id:
        raise ValueError("event_id is required")
    event = get_event(conn, event_id)
    if not event:
        raise ValueError("event_not_found")
    query = str(payload.get("query") or "").strip() or build_event_enrich_query(event)
    searx_url = os.getenv("SV_SEARXNG_URL", "").strip()
    timeout_s = int(os.getenv("SV_SEARXNG_TIMEOUT_S", "20"))
    max_results = int(payload.get("max_results") or os.getenv("SV_SEARXNG_MAX_RESULTS", "10"))
    categories = os.getenv("SV_SEARXNG_CATEGORIES")
    engines = os.getenv("SV_SEARXNG_ENGINES")
    keep_low = bool(payload.get("keep_low", False))
    promote_on_enrich = bool(payload.get("promote_on_enrich", False))
    replace_existing = bool(payload.get("replace_existing", False))
    min_score = int(os.getenv("SV_ENRICH_MIN_SCORE", "20"))
    auto_fetch_enabled = os.getenv("SV_EVENT_ENRICH_AUTO_FETCH", "0").strip().lower() in {"1", "true", "yes"}
    auto_fetch_min_score = int(os.getenv("SV_EVENT_ENRICH_AUTO_FETCH_MIN_SCORE", "25"))
    auto_fetch_max = max(1, int(os.getenv("SV_EVENT_ENRICH_AUTO_FETCH_MAX_PER_RUN", "5")))
    try:
        results = searxng_search(
            query,
            url=searx_url,
            timeout_s=timeout_s,
            categories=categories,
            engines=engines,
            language=None,
            max_results=max_results,
        )
    except Exception as exc:
        log_event(
            logger,
            logging.ERROR,
            "event_enrich_failed",
            event_id=event_id,
            error=str(exc),
        )
        raise
    saved = 0
    promoted = 0
    promoted_queued = 0
    auto_queued = 0
    scored_results: list[tuple[int, dict[str, object], dict[str, int]]] = []
    for item in results:
        url_value = str(item.get("url") or "").strip()
        if not url_value:
            continue
        item["domain"] = urlparse(url_value).netloc.lower()
        score, reasons = score_web_result(event, item)
        scored_results.append((score, item, reasons))
    replaced = 0
    if replace_existing:
        replaced = clear_event_web_sources(conn, event_id, keep_promoted=True)
    for score, item, reasons in scored_results:
        if score < min_score and not keep_low:
            continue
        source_id = upsert_event_web_source(conn, event_id, item, score, reasons)
        if source_id:
            saved += 1
            if promote_on_enrich:
                # Route through validation/fetch instead of direct promote so event reports
                # are built from fetched + summarized article content.
                enqueue_job(
                    conn,
                    "validate_event_web_source",
                    {"source_id": source_id, "event_id": event_id},
                    dedupe=True,
                )
                promoted_queued += 1
            elif auto_fetch_enabled and score >= auto_fetch_min_score and auto_queued < auto_fetch_max:
                enqueue_job(
                    conn,
                    "validate_event_web_source",
                    {"source_id": source_id, "event_id": event_id},
                    dedupe=True,
                )
                auto_queued += 1
    return {
        "event_id": event_id,
        "query": query,
        "results": len(results),
        "saved": saved,
        "promoted": promoted,
        "promoted_queued": promoted_queued,
        "auto_queued": auto_queued,
        "replaced": replaced,
    }


def _handle_validate_event_web_source(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    source_id = str(payload.get("source_id") or "").strip()
    if not source_id:
        raise ValueError("source_id is required")
    source = get_event_web_source(conn, source_id)
    if not source:
        raise ValueError("event_web_source_not_found")
    if str(source.get("status") or "") in {"promoted", "discarded"}:
        return {"status": "skipped", "reason": "terminal_status", "source_id": source_id}
    event_id = str(source.get("event_id") or payload.get("event_id") or "")
    event = get_event(conn, event_id)
    if not event:
        raise ValueError("event_not_found")

    update_event_web_source_status(conn, source_id, "fetching")
    url = str(source.get("url") or "").strip()
    if not url:
        update_event_web_source_status(
            conn,
            source_id,
            "error",
            metadata_patch={"validation_error": "missing_url"},
        )
        raise ValueError("missing_url")

    try:
        fetch_result = fetch_article_content(
            url,
            timeout_seconds=config.ingest.http.timeout_seconds,
            user_agent=config.ingest.http.user_agent,
            logger=logger,
            source_id="event_web_enrich",
            source_name="Event Web Enrich",
            overrides=None,
        )
        content_text = str(fetch_result.get("content_text") or "")
        fetched_published_at = str(fetch_result.get("published_at") or "").strip()
        if fetched_published_at:
            update_event_web_source_published_at(conn, source_id, fetched_published_at)
    except Exception as exc:
        reject_status = (
            "discarded"
            if os.getenv("SV_EVENT_ENRICH_VALIDATE_AUTO_DISCARD", "1").strip().lower()
            in {"1", "true", "yes"}
            else "rejected"
        )
        update_event_web_source_status(
            conn,
            source_id,
            reject_status,
            metadata_patch={"validation_error": f"fetch_failed:{exc}"},
        )
        return {
            "source_id": source_id,
            "event_id": event_id,
            "status": reject_status,
            "reason": "fetch_failed",
            "error": str(exc),
        }

    update_event_web_source_status(conn, source_id, "validating")
    try:
        validation, fallback_reason = _validate_event_source_with_llm(
            conn,
            logger,
            event=event,
            source=source,
            content=content_text,
        )
    except Exception as exc:
        validation = _validate_event_source_fallback(event, source, content_text)
        fallback_reason = f"fallback_llm_error:{exc}"
    min_confidence = float(os.getenv("SV_EVENT_ENRICH_VALIDATE_MIN_CONFIDENCE", "0.70"))
    require_llm = os.getenv("SV_EVENT_ENRICH_VALIDATE_REQUIRE_LLM", "0").strip().lower() in {"1", "true", "yes"}
    validator = str(validation.get("validator") or "")
    related = bool(validation.get("related"))
    confidence = float(validation.get("confidence") or 0.0)
    passes = related and confidence >= min_confidence and (not require_llm or validator == "llm")
    metadata_patch = {
        "validation": {
            "related": related,
            "confidence": confidence,
            "validator": validator,
            "matched_facts": validation.get("matched_facts") or [],
            "contradictions": validation.get("contradictions") or [],
            "rationale": validation.get("rationale") or "",
            "fallback_reason": fallback_reason,
            "min_confidence": min_confidence,
            "require_llm": require_llm,
        }
    }
    if not passes:
        reject_status = "discarded" if os.getenv("SV_EVENT_ENRICH_VALIDATE_AUTO_DISCARD", "1").strip().lower() in {"1", "true", "yes"} else "rejected"
        update_event_web_source_status(
            conn,
            source_id,
            reject_status,
            metadata_patch=metadata_patch,
        )
        return {
            "source_id": source_id,
            "event_id": event_id,
            "status": reject_status,
            "related": related,
            "confidence": confidence,
            "validator": validator,
        }

    article_id = promote_event_web_source_to_article(conn, source_id)
    if not article_id:
        update_event_web_source_status(
            conn,
            source_id,
            "error",
            metadata_patch={"validation_error": "promotion_failed", **metadata_patch},
        )
        raise ValueError("promotion_failed")
    update_article_content(
        conn,
        int(article_id),
        content_text=content_text,
        content_html=None,
        content_fetched_at=utc_now_iso(),
        content_error=None,
        has_full_content=bool(content_text.strip()),
    )
    # Push promoted web articles through normal article pipelines so event synthesis has
    # structured summaries/context instead of raw snippets.
    enqueue_job(
        conn,
        "summarize_article_llm",
        {"article_id": int(article_id)},
        dedupe=True,
    )
    enqueue_job(
        conn,
        "summarize_article_context_llm",
        {"article_id": int(article_id)},
        dedupe=True,
    )
    update_event_web_source_status(
        conn,
        source_id,
        "promoted",
        metadata_patch=metadata_patch,
    )
    return {
        "source_id": source_id,
        "event_id": event_id,
        "article_id": article_id,
        "status": "promoted",
        "validator": validator,
        "confidence": confidence,
    }


def _handle_promote_event_web_source(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    source_id = str(payload.get("source_id") or "")
    if not source_id:
        raise ValueError("source_id is required")
    article_id = promote_event_web_source_to_article(conn, source_id)
    if not article_id:
        raise ValueError("promotion_failed")
    enqueue_job(
        conn,
        "fetch_article_content",
        {"article_id": int(article_id)},
        dedupe=True,
    )
    enqueue_job(
        conn,
        "summarize_article_llm",
        {"article_id": int(article_id)},
        dedupe=True,
    )
    enqueue_job(
        conn,
        "summarize_article_context_llm",
        {"article_id": int(article_id)},
        dedupe=True,
    )
    return {"source_id": source_id, "article_id": article_id}


def _handle_enrich_event_summary_llm(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    if os.getenv("SV_ENRICH_ENABLE_LLM", "0") not in {"1", "true", "yes"}:
        return {"status": "skipped", "reason": "llm_disabled"}
    event_id = str(payload.get("event_id") or "")
    if not event_id:
        raise ValueError("event_id is required")
    summary = update_event_summary_from_articles(conn, event_id)
    enqueue_job(conn, "event_report_llm", {"event_id": event_id}, dedupe=True)
    return {"event_id": event_id, "summary": summary}


def _event_report_profile(conn) -> tuple[dict[str, object] | None, str]:
    profile = None
    reason = "missing_profile"
    profile, reason = get_active_profile_for_stage(conn, "event_report_llm")
    if profile:
        return profile, ""
    profile, reason = get_active_profile_for_stage(conn, "enrich_event_summary_llm")
    if profile:
        return profile, ""
    profile, reason = get_active_profile_for_stage(conn, "summarize_article")
    if profile:
        return profile, ""
    return None, reason or "missing_profile"


def _event_article_summary_parts(summary_llm_raw: object) -> tuple[str, list[str]]:
    raw = str(summary_llm_raw or "").strip()
    if not raw:
        return "", []
    parsed = _decode_json_from_raw(raw)
    if not isinstance(parsed, dict):
        return raw, []
    summary = str(parsed.get("summary") or "").strip()
    bullets_raw = parsed.get("bullets") or parsed.get("key_points") or parsed.get("tldr") or []
    bullets: list[str] = []
    if isinstance(bullets_raw, list):
        bullets = [str(item).strip() for item in bullets_raw if str(item).strip()][:8]
    return summary or raw, bullets


def _event_article_context_parts(context_raw: object) -> tuple[list[str], list[str]]:
    raw = str(context_raw or "").strip()
    if not raw:
        return [], []
    parsed = _decode_json_from_raw(raw)
    if not isinstance(parsed, dict):
        return [], []
    facts_raw = parsed.get("facts") or []
    timeline_raw = parsed.get("timeline") or []
    facts = [str(item).strip() for item in facts_raw if str(item).strip()] if isinstance(facts_raw, list) else []
    timeline = [str(item).strip() for item in timeline_raw if str(item).strip()] if isinstance(timeline_raw, list) else []
    return facts[:10], timeline[:10]


def _build_event_report_input(
    event: dict[str, object],
    article_rows: list[dict[str, object]],
    web_rows: list[dict[str, object]],
) -> str:
    event_core = {
        "id": event.get("id"),
        "title": event.get("title"),
        "kind": event.get("kind"),
        "entity": event.get("entity"),
        "lifecycle": event.get("lifecycle"),
        "incident_date": event.get("incident_date"),
        "first_seen_at": event.get("first_seen_at"),
        "last_seen_at": event.get("last_seen_at"),
        "summary": event.get("summary"),
        "timeline": event.get("timeline") or [],
        "narrative": event.get("narrative") or {},
    }
    expected_schema = {
        "overview": "string",
        "attribution": {
            "responsible_actor": "string",
            "actor_type": "criminal|nation_state|insider|unknown",
            "confidence": "high|medium|low|unknown",
            "rationale": ["string"],
            "disputed_claims": ["string"],
        },
        "timeline": [{"date": "string", "event": "string", "evidence": ["string"]}],
        "impact": ["string"],
        "compromise_path": ["string"],
        "investigation_findings": ["string"],
        "legal_regulatory_outcomes": ["string"],
        "response_recovery": ["string"],
        "lessons_learned": ["string"],
        "confidence_notes": ["string"],
    }
    return (
        "You are producing a factual incident report for a cybersecurity event. "
        "Use only supplied evidence. Avoid speculation and avoid filler narrative.\n\n"
        "Return JSON only with keys exactly:\n"
        f"{json.dumps(expected_schema, ensure_ascii=True)}\n\n"
        "Rules:\n"
        "- Overview: 4-8 sentences with concrete facts and caveats when uncertain.\n"
        "- Timeline: 5-20 items if evidence allows, sorted oldest to newest.\n"
        "- Timeline must be incident milestones, not article chronology.\n"
        "- Do NOT use article publish dates unless the article explicitly states that date as incident activity.\n"
        "- Each timeline item should describe one concrete action by attacker, victim/org response, investigation step, legal/regulatory step, or recovery outcome.\n"
        "- Timeline event text must state what happened; avoid phrasing like 'article reported' or 'news said'.\n"
        "- Each timeline item must include date, event, and evidence list.\n"
        "- Evidence items should be short fact snippets (not bare URLs).\n"
        "- Attribution must separate confirmed vs alleged claims.\n"
        "- If actor identity is unproven, set responsible_actor='unknown' and explain why.\n"
        "- Prefer actor_type='unknown' over guessing.\n"
        "- Include investigation findings and legal/regulatory outcomes when supported by evidence.\n"
        "- Bulleted sections should be fact statements, not recommendations.\n"
        "- If data is missing, state uncertainty explicitly in confidence_notes.\n\n"
        f"EVENT:\n{json.dumps(event_core, ensure_ascii=True)}\n\n"
        f"ARTICLES_EVIDENCE:\n{json.dumps(article_rows, ensure_ascii=True)}\n\n"
        f"WEB_EVIDENCE:\n{json.dumps(web_rows, ensure_ascii=True)}"
    )


def _normalize_event_timeline_date(value: str) -> tuple[int, str]:
    text = str(value or "").strip()
    if not text:
        return (99991231, "Unknown date")
    digits = re.findall(r"\d+", text)
    if len(digits) >= 3:
        year = digits[0]
        month = digits[1]
        day = digits[2]
        if len(year) == 4 and len(month) <= 2 and len(day) <= 2:
            y = int(year)
            m = max(1, min(int(month), 12))
            d = max(1, min(int(day), 31))
            return (y * 10000 + m * 100 + d, f"{y:04d}-{m:02d}-{d:02d}")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return (
            parsed.year * 10000 + parsed.month * 100 + parsed.day,
            f"{parsed.year:04d}-{parsed.month:02d}-{parsed.day:02d}",
        )
    except ValueError:
        return (99991231, text[:32])


def _is_incident_timeline_event(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    low = value.lower()
    if "http://" in low or "https://" in low:
        return False
    article_markers = ("article", "news", "blog", "coverage", "reported by", "according to")
    incident_verbs = (
        "compromis",
        "breach",
        "stolen",
        "exfiltrat",
        "leak",
        "ransom",
        "extort",
        "investigat",
        "announc",
        "confirm",
        "notify",
        "disclos",
        "filed",
        "sued",
        "settl",
        "fine",
        "penalt",
        "recover",
        "restor",
        "patch",
        "contain",
        "eradica",
        "remediat",
    )
    has_article_marker = any(marker in low for marker in article_markers)
    has_incident_verb = any(verb in low for verb in incident_verbs)
    if has_article_marker and not has_incident_verb:
        return False
    return has_incident_verb or not has_article_marker


def _parse_event_report_output(result: dict[str, object], incident_date: str = "") -> dict[str, object]:
    parsed = result.get("parsed")
    report: dict[str, object] | None = parsed if isinstance(parsed, dict) else None
    if report is None:
        raw = str(result.get("raw") or "").strip()
        decoded = _decode_json_from_raw(raw) if raw else None
        if isinstance(decoded, dict):
            report = decoded
    if report is None:
        raw = str(result.get("raw") or "").strip()
        if not raw:
            return {}
        return {
            "overview": raw[:2000],
            "attribution": {
                "responsible_actor": "unknown",
                "actor_type": "unknown",
                "confidence": "unknown",
                "rationale": ["model_output_unstructured"],
                "disputed_claims": [],
            },
            "timeline": [],
            "impact": [],
            "compromise_path": [],
            "investigation_findings": [],
            "legal_regulatory_outcomes": [],
            "response_recovery": [],
            "lessons_learned": [],
            "confidence_notes": ["model_output_unstructured"],
        }
    cleaned: dict[str, object] = {
        "overview": str(report.get("overview") or "").strip(),
        "attribution": {
            "responsible_actor": "unknown",
            "actor_type": "unknown",
            "confidence": "unknown",
            "rationale": [],
            "disputed_claims": [],
        },
        "timeline": [],
        "impact": [],
        "compromise_path": [],
        "investigation_findings": [],
        "legal_regulatory_outcomes": [],
        "response_recovery": [],
        "lessons_learned": [],
        "confidence_notes": [],
    }
    attribution = report.get("attribution")
    if isinstance(attribution, dict):
        actor_name = str(attribution.get("responsible_actor") or "").strip()
        actor_type = str(attribution.get("actor_type") or "").strip().lower()
        actor_confidence = str(attribution.get("confidence") or "").strip().lower()
        if actor_name:
            cleaned["attribution"]["responsible_actor"] = actor_name
        if actor_type in {"criminal", "nation_state", "insider", "unknown"}:
            cleaned["attribution"]["actor_type"] = actor_type
        if actor_confidence in {"high", "medium", "low", "unknown"}:
            cleaned["attribution"]["confidence"] = actor_confidence
        rationale = attribution.get("rationale")
        if isinstance(rationale, list):
            cleaned["attribution"]["rationale"] = [
                str(x).strip() for x in rationale if str(x).strip()
            ][:8]
        disputed = attribution.get("disputed_claims")
        if isinstance(disputed, list):
            cleaned["attribution"]["disputed_claims"] = [
                str(x).strip() for x in disputed if str(x).strip()
            ][:8]
    timeline = report.get("timeline")
    if isinstance(timeline, list):
        timeline_rows: list[dict[str, object]] = []
        seen_events: set[tuple[str, str]] = set()
        for item in timeline[:30]:
            if not isinstance(item, dict):
                continue
            event_text = str(item.get("event") or item.get("title") or "").strip()
            if not _is_incident_timeline_event(event_text):
                continue
            sort_key, normalized_date = _normalize_event_timeline_date(str(item.get("date") or ""))
            dedupe_key = (normalized_date.lower(), event_text.lower())
            if dedupe_key in seen_events:
                continue
            seen_events.add(dedupe_key)
            row = {
                "date": normalized_date,
                "event": event_text,
                "evidence": [],
                "_sort_key": sort_key,
            }
            evidence = item.get("evidence")
            if isinstance(evidence, list):
                row["evidence"] = [
                    str(x).strip()
                    for x in evidence
                    if str(x).strip() and "http://" not in str(x).lower() and "https://" not in str(x).lower()
                ][:6]
            if row["event"]:
                timeline_rows.append(row)
        timeline_rows.sort(key=lambda x: int(x.get("_sort_key", 99991231)))
        cleaned["timeline"] = [
            {"date": str(x.get("date") or ""), "event": str(x.get("event") or ""), "evidence": x.get("evidence") or []}
            for x in timeline_rows
        ]
    for key in (
        "impact",
        "compromise_path",
        "investigation_findings",
        "legal_regulatory_outcomes",
        "response_recovery",
        "lessons_learned",
        "confidence_notes",
    ):
        value = report.get(key)
        if isinstance(value, list):
            cleaned[key] = [str(x).strip() for x in value if str(x).strip()][:20]
    if not cleaned["overview"]:
        tl = cleaned.get("timeline") or []
        if isinstance(tl, list) and tl:
            first = tl[0] if isinstance(tl[0], dict) else {}
            cleaned["overview"] = str(first.get("event") or "").strip()
    timeline_rows = cleaned.get("timeline")
    if isinstance(timeline_rows, list) and len(timeline_rows) < 3:
        incident_sort, incident_norm = _normalize_event_timeline_date(incident_date)
        if incident_sort >= 99991231:
            incident_norm = "Unknown date"
        existing = {
            str(row.get("event") or "").strip().lower()
            for row in timeline_rows
            if isinstance(row, dict)
        }
        backfill_rows: list[dict[str, object]] = []
        section_order = (
            "compromise_path",
            "investigation_findings",
            "response_recovery",
            "legal_regulatory_outcomes",
            "lessons_learned",
        )
        for key in section_order:
            values = cleaned.get(key)
            if not isinstance(values, list):
                continue
            for item in values[:3]:
                text = str(item or "").strip()
                if not text:
                    continue
                if not _is_incident_timeline_event(text):
                    continue
                lower = text.lower()
                if lower in existing:
                    continue
                existing.add(lower)
                backfill_rows.append({"date": incident_norm, "event": text, "evidence": []})
                if len(backfill_rows) >= 8:
                    break
            if len(backfill_rows) >= 8:
                break
        if backfill_rows:
            cleaned["timeline"] = (timeline_rows + backfill_rows)[:12]
    return cleaned


def _handle_event_report_llm(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    event_id = str(payload.get("event_id") or "").strip()
    if not event_id:
        raise ValueError("event_id is required")
    event = get_event(conn, event_id)
    if not event:
        raise ValueError("event_not_found")
    articles = ((event.get("items") or {}).get("articles") or [])[:20]
    article_rows: list[dict[str, object]] = []
    for item in articles:
        article_id = item.get("article_id")
        if not isinstance(article_id, int):
            continue
        article = get_article_by_id(conn, article_id)
        if not article:
            continue
        summary_text, bullets = _event_article_summary_parts(article.get("summary_llm"))
        context_facts, context_timeline = _event_article_context_parts(article.get("context_llm"))
        article_rows.append(
            {
                "article_id": article_id,
                "title": item.get("title"),
                "published_at": item.get("published_at"),
                "url": item.get("url"),
                "summary": summary_text,
                "bullets": bullets,
                "facts": context_facts,
                "timeline_facts": context_timeline,
            }
        )
    web_rows = list_event_web_sources(conn, event_id, include_discarded=False)
    web_rows = [
        {
            "title": row.get("title"),
            "domain": row.get("domain"),
            "published_at": row.get("published_at"),
            "url": row.get("url"),
            "snippet": row.get("snippet"),
            "score": row.get("score"),
            "status": row.get("status"),
        }
        for row in web_rows[:30]
        if str(row.get("status") or "") == "promoted"
    ]
    profile, reason = _event_report_profile(conn)
    if not profile:
        return {"status": "skipped", "reason": f"no_profile_routed:{reason}"}
    input_text = _build_event_report_input(event, article_rows, web_rows)
    result = run_profile(
        conn,
        str(profile["id"]),
        input_text,
        logger,
        context={"stage": "event_report_llm", "job_type": "event_report_llm"},
    )
    report = _parse_event_report_output(
        result if isinstance(result, dict) else {},
        incident_date=str(event.get("incident_date") or ""),
    )
    if not report:
        return {"status": "skipped", "reason": "empty_report"}
    report["source_articles"] = len(article_rows)
    report["source_web_promoted"] = len(web_rows)
    report["generated_at"] = utc_now_iso()
    updated = update_event_report(
        conn,
        event_id,
        report,
        profile_id=str(profile.get("id") or ""),
        profile_name=str(profile.get("name") or ""),
        model_id=str(profile.get("primary_model_id") or ""),
        model_name=str(profile.get("model_name") or ""),
    )
    if updated:
        lifecycle = str(event.get("lifecycle") or event.get("status") or "").lower()
        publish_state = str(event.get("publish_state") or "").lower()
        if publish_state == "published" and lifecycle == "confirmed":
            mark_build_dirty(conn, reason="event_report_update")
    return {
        "status": "ok" if updated else "skipped",
        "event_id": event_id,
        "source_articles": len(article_rows),
        "source_web_promoted": len(web_rows),
    }


def _handle_source_acquire(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    payload = job.payload or {}
    source_id = payload.get("source_id")
    if not source_id:
        raise ValueError("source_acquire requires source_id")
    source = get_source(conn, str(source_id))
    if source is None:
        raise ValueError(f"Source not found: {source_id}")
    limit = payload.get("limit")
    also_build = bool(payload.get("also_build"))
    also_events = bool(payload.get("also_events_rebuild"))
    timeout_seconds = int(payload.get("timeout_seconds") or 300)

    started = time.time()
    result: dict[str, object] = {"source_id": source.id, "counts": {}, "errors": []}
    start_marker = utc_now_iso()
    worker_types = _parse_only_types(os.environ.get("SV_WORKER_ONLY_TYPES", ""))

    def _claims(job_type: str) -> bool:
        if worker_types is None:
            return True
        return job_type in worker_types

    ingest_job_id = enqueue_source_ingest_job(
        conn,
        source.id,
        limit=limit if isinstance(limit, int) else None,
        manual=True,
    )
    if not ingest_job_id:
        raise ValueError(f"Unable to enqueue ingest_source for {source.id}")
    result["ingest_job_id"] = ingest_job_id
    _run_jobs_inline_if_allowed(
        conn,
        config,
        logger,
        required_type="ingest_source",
        allowed_types=["ingest_source"],
        timeout_seconds=timeout_seconds,
    )
    ingest_job = get_job(conn, ingest_job_id)
    result["counts"]["ingested"] = int((ingest_job.result or {}).get("accepted_count") or 0) if ingest_job else 0

    missing_content_ids = list_article_ids_missing_content(conn, source.id)
    for article_id in missing_content_ids:
        _maybe_enqueue_fetch(conn, config, article_id, source.id, logger)
    _run_jobs_inline_if_allowed(
        conn,
        config,
        logger,
        required_type="fetch_article_content",
        allowed_types=["fetch_article_content"],
        timeout_seconds=timeout_seconds,
    )

    missing_summary_ids: list[int] = []
    if _claims("summarize_article_llm") or _claims("summarize_article_context_llm"):
        missing_summary_ids = list_article_ids_missing_summary(conn, source.id)
        for article_id in missing_summary_ids:
            if _claims("summarize_article_llm"):
                _maybe_enqueue_summarize(conn, article_id, source.id, logger)
            if _claims("summarize_article_context_llm"):
                _maybe_enqueue_context_pack(conn, article_id, source.id, logger)
        _run_jobs_inline_if_allowed(
            conn,
            config,
            logger,
            required_type="summarize_article_llm",
            allowed_types=["summarize_article_llm"],
            timeout_seconds=timeout_seconds,
        )
    else:
        log_event(
            logger,
            logging.INFO,
            "source_acquire_stage_skipped",
            stage="summarize",
            reason="worker_scope_excludes_stage",
            source_name=source.name,
        )

    if _claims("write_article_markdown"):
        new_article_ids = list_article_ids_for_source_since(conn, source.id, start_marker)
        publish_ids = sorted(set(new_article_ids + missing_content_ids + missing_summary_ids))
        for article_id in publish_ids:
            _enqueue_write_from_article(conn, config, article_id, source.id)
        _run_jobs_inline_if_allowed(
            conn,
            config,
            logger,
            required_type="write_article_markdown",
            allowed_types=["write_article_markdown"],
            timeout_seconds=timeout_seconds,
        )
    else:
        log_event(
            logger,
            logging.INFO,
            "source_acquire_stage_skipped",
            stage="publish_markdown",
            reason="worker_scope_excludes_stage",
            source_name=source.name,
        )

    jobs = list_jobs_by_types_since(
        conn,
        types=[
            "fetch_article_content",
            "summarize_article_llm",
            "summarize_article_context_llm",
            "write_article_markdown",
        ],
        since=start_marker,
    )
    for job_row in jobs:
        if job_row.status == "failed" and job_row.error:
            result["errors"].append(
                {"job_type": job_row.job_type, "job_id": job_row.id, "error": job_row.error}
            )
    result["counts"]["fetched_ok"] = sum(
        1 for j in jobs if j.job_type == "fetch_article_content" and j.status == "succeeded"
    )
    result["counts"]["fetched_failed"] = sum(
        1 for j in jobs if j.job_type == "fetch_article_content" and j.status == "failed"
    )
    result["counts"]["summarized_ok"] = sum(
        1 for j in jobs if j.job_type == "summarize_article_llm" and j.status == "succeeded"
    )
    result["counts"]["summarized_failed"] = sum(
        1 for j in jobs if j.job_type == "summarize_article_llm" and j.status == "failed"
    )
    result["counts"]["context_ok"] = sum(
        1
        for j in jobs
        if j.job_type == "summarize_article_context_llm" and j.status == "succeeded"
    )
    result["counts"]["context_failed"] = sum(
        1
        for j in jobs
        if j.job_type == "summarize_article_context_llm" and j.status == "failed"
    )
    result["counts"]["markdown_ok"] = sum(
        1 for j in jobs if j.job_type == "write_article_markdown" and j.status == "succeeded"
    )
    result["counts"]["markdown_failed"] = sum(
        1 for j in jobs if j.job_type == "write_article_markdown" and j.status == "failed"
    )

    if also_events:
        events_job_id = enqueue_job(conn, "events_rebuild", None)
        _run_jobs_inline_if_allowed(
            conn,
            config,
            logger,
            required_type="events_rebuild",
            allowed_types=["events_rebuild"],
            timeout_seconds=timeout_seconds,
        )
        result["events_job_id"] = events_job_id

    if also_build:
        build_job_id = enqueue_job(conn, "build_site", None, debounce=True)
        result["build_job_id"] = build_job_id
        _wait_for_job(conn, build_job_id, timeout_seconds)
        build_job = get_job(conn, build_job_id)
        if build_job:
            result["build_ok"] = build_job.status == "succeeded"
            result["build_exit_code"] = ( build_job.result or {}).get("exit_code")
    result["duration_s"] = round(time.time() - started, 2)
    return result


def _run_jobs_inline(
    conn,
    config,
    logger: logging.Logger,
    *,
    allowed_types: list[str],
    timeout_seconds: int,
) -> None:
    start = time.monotonic()
    worker_id = f"smoke_inline_{uuid.uuid4().hex}"
    while time.monotonic() - start < timeout_seconds:
        job = claim_next_job(
            conn,
            worker_id,
            allowed_types=allowed_types,
            lock_timeout_seconds=config.jobs.lock_timeout_seconds,
        )
        if not job:
            return
        if is_job_canceled(conn, job.id):
            log_event(logger, logging.INFO, "job_canceled", job_id=job.id)
            continue
        try:
            result = run_claimed_job(conn, config, job, logger)
        except Exception as exc:  # noqa: BLE001
            fail_job(conn, job.id, str(exc))
            fields = _job_context_fields(conn, job)
            log_event(
                logger,
                logging.ERROR,
                "job_failed",
                job_id=job.id,
                error=str(exc),
                **fields,
            )
            continue
        if result.get("requeued"):
            fields = _job_context_fields(conn, job)
            log_event(
                logger,
                logging.INFO,
                "job_requeued",
                job_id=job.id,
                reason=result.get("reason"),
                attempt=result.get("attempt"),
                **fields,
            )
    log_event(logger, logging.WARNING, "smoke_inline_timeout", timeout_seconds=timeout_seconds)


def _run_jobs_inline_if_allowed(
    conn,
    config,
    logger: logging.Logger,
    *,
    required_type: str,
    allowed_types: list[str],
    timeout_seconds: int,
) -> None:
    worker_types = _parse_only_types(os.environ.get("SV_WORKER_ONLY_TYPES", ""))
    if worker_types is not None and required_type not in worker_types:
        log_event(
            logger,
            logging.INFO,
            "smoke_inline_skipped",
            required_type=required_type,
            reason="not_claimed_by_worker",
        )
        return
    _run_jobs_inline(
        conn,
        config,
        logger,
        allowed_types=allowed_types,
        timeout_seconds=timeout_seconds,
    )


def _wait_for_job(conn, job_id: str, timeout_seconds: int) -> Job | None:
    start = time.monotonic()
    while time.monotonic() - start < timeout_seconds:
        job = get_job(conn, job_id)
        if not job:
            return None
        if job.status in {"succeeded", "failed", "canceled"}:
            return job
        time.sleep(1)
    return get_job(conn, job_id)


def _handle_ingest_due_sources(conn, logger: logging.Logger) -> dict[str, object]:
    # Enqueue ingest_source jobs for due sources; downstream steps are queued
    # by ingest_source after article stubs are inserted.
    now = utc_now_iso()
    sources = list_due_sources(conn, now)
    enqueued: list[str] = []
    for source in sources:
        if enqueue_source_ingest_job(conn, source.id, now_iso=now):
            enqueued.append(source.id)
    log_event(
        logger,
        logging.INFO,
        "ingest_due_sources_enqueued",
        count=len(enqueued),
    )
    return {"enqueued_count": len(enqueued), "source_ids": enqueued}


def _handle_cve_sync(
    conn, config, logger: logging.Logger, payload: dict[str, object] | None = None
) -> dict[str, object]:
    settings = get_cve_settings(conn)
    if not settings.get("enabled", True):
        return {"status": "disabled"}
    now = datetime.now(tz=timezone.utc)
    last_sync = get_setting(conn, "cve.last_successful_sync_at", None)
    start = _parse_iso(last_sync) if isinstance(last_sync, str) else None
    if not start:
        start = now - timedelta(minutes=int(settings.get("schedule_minutes", 60)))
    start_iso = isoformat_utc(start)
    end_iso = isoformat_utc(now)
    api_key = os.environ.get("NVD_API_KEY")
    nvd = settings.get("nvd") or {}
    cve_id = None
    mode = payload.get("mode") if payload else None
    if payload and payload.get("cve_id"):
        cve_id = str(payload.get("cve_id"))
    if cve_id:
        log_event(logger, logging.INFO, "cve_enrich_start", cve_id=cve_id)
        updated = sync_cve_id(conn, api_key, cve_id)
        enqueue_job(conn, "cve_enrich_kev", {"cve_id": cve_id}, dedupe=True)
        return {"status": "ok", "cve_id": cve_id, "updated": bool(updated)}
    if mode == "cve_description":
        return _handle_cve_description_fill(conn, api_key, logger)
    result = sync_cves(
        conn,
        CveSyncConfig(
            api_base=str(nvd.get("api_base") or "https://services.nvd.nist.gov/rest/json/cves/2.0"),
            results_per_page=int(nvd.get("results_per_page") or 2000),
            rate_limit_seconds=float(settings.get("rate_limit_seconds", 1.0)),
            backoff_seconds=float(settings.get("backoff_seconds", 2.0)),
            max_retries=int(settings.get("max_retries", 3)),
            prefer_v4=bool(settings.get("prefer_v4", True)),
            scope_min_cvss=config.scope.min_cvss,
            watchlist_enabled=config.personalization.watchlist_enabled,
            api_key=api_key,
            filters=settings.get("filters") or {},
        ),
        last_modified_start=start_iso,
        last_modified_end=end_iso,
        cve_id=cve_id,
    )
    result["start"] = start_iso
    result["end"] = end_iso
    if cve_id:
        result["cve_id"] = cve_id
        log_event(
            logger,
            logging.INFO,
            "cve_enrich_done",
            cve_id=cve_id,
            processed=result.get("processed"),
            changes=result.get("changes"),
            errors=result.get("errors"),
        )
    events_settings = get_events_settings(conn)
    if events_settings.get("enabled", True):
        _publish_events(conn, config, logger)
    if (result.get("processed") or 0) > 0 or (result.get("changes") or 0) > 0:
        mark_build_dirty(conn, reason="cve_sync_updated")
        kev_limit = int(result.get("processed") or 0)
        kev_limit = min(kev_limit, 2000) if kev_limit else 500
        enqueue_job(
            conn,
            "cve_enrich_kev",
            {"since": start_iso, "limit": kev_limit},
            dedupe=True,
        )
    return result


def _handle_cve_enrich_kev(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    payload = job.payload or {}
    cve_id = str(payload.get("cve_id") or "").strip()
    since = str(payload.get("since") or "").strip() or None
    limit = int(payload.get("limit") or 500)
    max_age_minutes = int(payload.get("max_age_minutes") or 360)
    kev_sync = ensure_kev_cache(conn, logger, max_age_minutes=max_age_minutes)
    if cve_id:
        cve_ids = [cve_id]
    else:
        cve_ids = list_cve_ids_needing_kev_check(conn, limit=limit, since=since)
    if not cve_ids:
        return {"status": "skipped", "reason": "no_targets", "kev_sync": kev_sync}
    kev_map = get_cve_kev_map(conn, cve_ids)
    now = utc_now_iso()
    matched = 0
    for target_id in cve_ids:
        kev_entry = kev_map.get(target_id)
        set_cve_kev_link(conn, target_id, target_id if kev_entry else None, now, commit=False)
        if kev_entry:
            matched += 1
    conn.commit()
    return {
        "status": "ok",
        "checked": len(cve_ids),
        "matched": matched,
        "missing": len(cve_ids) - matched,
        "kev_sync": kev_sync,
    }




def _handle_cve_enrich_llm(
    conn, config, job, logger: logging.Logger
) -> dict[str, object]:
    payload = job.payload or {}
    cve_id = str(payload.get("cve_id") or "").strip()
    force = bool(payload.get("force"))
    if not cve_id:
        raise ValueError("cve_id is required")
    cve = get_cve(conn, cve_id)
    if not cve:
        return {"status": "skipped", "reason": "cve_not_found"}
    existing_products = cve.get("affected_products") or []
    existing_versions = cve.get("product_versions") or []
    if not force and existing_products and existing_versions:
        mark_cve_products_checked(conn, cve_id)
        return {"status": "skipped", "reason": "already_enriched"}
    profile, reason = get_active_profile_for_stage(conn, "cve_enrich_products")
    if not profile:
        return {"status": "skipped", "reason": f"no_profile_routed:{reason}"}
    description = cve.get("description_text") or ""
    references = cve.get("reference_domains") or []
    prompt_lines = [
        f"CVE: {cve_id}",
        "Description:",
        description,
        "",
    ]
    if references:
        prompt_lines.append("Reference domains:")
        prompt_lines.extend([f"- {ref}" for ref in references])
    input_text = "\n".join(prompt_lines).strip()
    if not input_text:
        return {"status": "skipped", "reason": "no_input"}
    result = run_pipeline_stage(
        conn,
        "cve_enrich_products",
        input_text,
        logger,
        profile_id=profile["id"],
        context={"stage": "cve_enrich_products", "job_type": job.job_type},
    )
    result_dict = result if isinstance(result, dict) else {}
    cleaned, error_reason = _parse_product_items(result_dict, allow_versions=True)
    if error_reason:
        _record_llm_parse_metric(
            conn,
            stage="cve_enrich_products",
            profile=profile,
            outcome=str(error_reason),
        )
        raw = result_dict.get("raw")
        preview = (raw or "").strip()
        if len(preview) > 800:
            preview = preview[:800] + "\n[TRUNCATED]"
        labels = _llm_profile_labels(conn, profile)
        # Treat parse outcomes as terminal for "need" accounting.
        mark_cve_products_checked(conn, cve_id)
        if error_reason in {"no_items", "no_valid_items"}:
            log_event(
                logger,
                logging.INFO,
                "llm_no_items",
                stage="cve_enrich_products",
                cve_id=cve_id,
                reason=error_reason,
                **labels,
            )
        else:
            log_event(
                logger,
                logging.WARNING,
                "llm_parse_failed",
                stage="cve_enrich_products",
                cve_id=cve_id,
                reason=error_reason,
                raw_preview=preview,
                **labels,
            )
        return {"status": "skipped", "reason": error_reason, "raw_preview": preview}
    stats = link_cve_products_from_items(conn, cve_id=cve_id, items=cleaned, source="llm")
    _record_llm_parse_metric(
        conn,
        stage="cve_enrich_products",
        profile=profile,
        outcome="success",
    )
    mark_cve_products_checked(conn, cve_id)
    log_event(
        logger,
        logging.INFO,
        "cve_enrich_products",
        cve_id=cve_id,
        items=len(cleaned),
        vendors_created=stats.get("vendors_created"),
        products_created=stats.get("products_created"),
        links_created=stats.get("links_created"),
        versions_created=stats.get("versions_created"),
    )
    return {"status": "ok", "cve_id": cve_id, "items": len(cleaned), **stats}


def _parse_threat_actor_items(
    result: dict[str, object],
) -> tuple[list[dict[str, object]], str | None]:
    parsed = result.get("parsed")
    raw = result.get("raw")
    items: list[dict[str, object]] = []
    if isinstance(parsed, list):
        items = [item for item in parsed if isinstance(item, dict)]
    if not items and isinstance(parsed, dict):
        if isinstance(parsed.get("items"), list):
            items = [item for item in parsed.get("items") if isinstance(item, dict)]
        elif parsed.get("name"):
            items = [parsed]
    if not items and isinstance(raw, str):
        stripped = raw.strip()
        if stripped.startswith("```"):
            stripped = re.sub(r"^```[a-zA-Z]*\s*", "", stripped)
            stripped = re.sub(r"\s*```$", "", stripped)
        try:
            parsed_raw = json.loads(stripped)
            if isinstance(parsed_raw, list):
                items = [item for item in parsed_raw if isinstance(item, dict)]
            elif isinstance(parsed_raw, dict):
                if isinstance(parsed_raw.get("items"), list):
                    items = [item for item in parsed_raw.get("items") if isinstance(item, dict)]
                elif parsed_raw.get("name"):
                    items = [parsed_raw]
        except Exception:
            extracted = _extract_json_payload(stripped)
            if extracted:
                try:
                    parsed_raw = json.loads(extracted)
                    if isinstance(parsed_raw, list):
                        items = [item for item in parsed_raw if isinstance(item, dict)]
                    elif isinstance(parsed_raw, dict):
                        if isinstance(parsed_raw.get("items"), list):
                            items = [item for item in parsed_raw.get("items") if isinstance(item, dict)]
                        elif parsed_raw.get("name"):
                            items = [parsed_raw]
                except Exception:
                    return [], "invalid_json"
            else:
                return [], "invalid_json"
    if not items:
        return [], "no_items"
    allowed = {"name", "type", "country", "aliases", "confidence"}
    cleaned: list[dict[str, object]] = []
    for item in items:
        item = {key: item.get(key) for key in allowed}
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        cleaned.append(item)
    if not cleaned:
        return [], "no_valid_items"
    return cleaned, None


def _extract_json_payload(raw: str) -> str | None:
    if not raw:
        return None
    start_obj = raw.find("{")
    start_arr = raw.find("[")
    starts = [s for s in (start_obj, start_arr) if s != -1]
    if not starts:
        return None
    start = min(starts)
    end_obj = raw.rfind("}")
    end_arr = raw.rfind("]")
    end = max(end_obj, end_arr)
    if end == -1 or end <= start:
        return None
    return raw[start : end + 1].strip()


def _extract_balanced_json(raw: str) -> str | None:
    if not raw:
        return None
    for i, ch in enumerate(raw):
        if ch not in "{[":
            continue
        stack = [ch]
        in_string = False
        escape = False
        for j in range(i + 1, len(raw)):
            c = raw[j]
            if escape:
                escape = False
                continue
            if c == "\\" and in_string:
                escape = True
                continue
            if c == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if c in "{[":
                stack.append(c)
                continue
            if c == "}":
                if not stack or stack[-1] != "{":
                    break
                stack.pop()
            elif c == "]":
                if not stack or stack[-1] != "[":
                    break
                stack.pop()
            if not stack:
                return raw[i : j + 1].strip()
    return None


def _decode_json_from_raw(raw: str) -> object | None:
    stripped = raw.strip()
    if not stripped:
        return None
    candidates: list[str] = [stripped]
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
        candidates.append(stripped.strip())
    for block in re.findall(r"```(?:json)?\s*([\s\S]*?)```", raw, flags=re.IGNORECASE):
        block = block.strip()
        if block:
            candidates.append(block)
    extracted = _extract_json_payload(stripped)
    if extracted:
        candidates.append(extracted)
    balanced = _extract_balanced_json(stripped)
    if balanced:
        candidates.append(balanced)
    seen: set[str] = set()
    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None


def _parse_product_items(
    result: dict[str, object],
    *,
    allow_versions: bool,
) -> tuple[list[dict[str, object]], str | None]:
    parsed = result.get("parsed")
    raw = result.get("raw")
    items: list[dict[str, object]] = []
    if isinstance(parsed, list):
        items = [item for item in parsed if isinstance(item, dict)]
    if not items and isinstance(parsed, dict):
        if isinstance(parsed.get("items"), list):
            items = [item for item in parsed.get("items") if isinstance(item, dict)]
        elif parsed.get("product"):
            items = [parsed]
    if not items and isinstance(raw, str):
        parsed_raw = _decode_json_from_raw(raw)
        if parsed_raw is None:
            return [], "invalid_json"
        if isinstance(parsed_raw, list):
            items = [item for item in parsed_raw if isinstance(item, dict)]
        elif isinstance(parsed_raw, dict):
            if isinstance(parsed_raw.get("items"), list):
                items = [item for item in parsed_raw.get("items") if isinstance(item, dict)]
            elif parsed_raw.get("product"):
                items = [parsed_raw]
    if not items:
        return [], "no_items"
    allowed = {"vendor", "product", "versions"} if allow_versions else {"vendor", "product"}
    cleaned: list[dict[str, object]] = []
    for item in items:
        unknown_keys = set(item.keys()) - allowed
        if unknown_keys:
            # Ignore unknown keys but keep output strict to expected fields.
            item = {key: item.get(key) for key in allowed}
        vendor = item.get("vendor")
        product = item.get("product")
        if isinstance(vendor, str):
            vendor = vendor.strip()
        if isinstance(product, str):
            product = product.strip()
        if not product:
            continue
        if not vendor:
            continue
        if str(vendor).strip().lower() in {"unknown", "n/a", "none", "null"}:
            continue
        record: dict[str, object] = {"vendor": vendor, "product": product}
        if allow_versions:
            versions = item.get("versions") or []
            if isinstance(versions, str):
                versions = [versions]
            record["versions"] = [str(v).strip() for v in versions if str(v).strip()]
        cleaned.append(record)
    if not cleaned:
        return [], "no_valid_items"
    return cleaned, None


def _parse_event_classification(
    result: dict[str, object],
) -> tuple[dict[str, object] | None, str | None]:
    parsed = result.get("parsed")
    raw = result.get("raw")
    data: dict[str, object] | None = None
    if isinstance(parsed, dict):
        data = parsed
    if data is None and isinstance(raw, str):
        stripped = raw.strip()
        if stripped.startswith("```"):
            stripped = re.sub(r"^```[a-zA-Z]*\s*", "", stripped)
            stripped = re.sub(r"\s*```$", "", stripped)
        try:
            parsed_raw = json.loads(stripped)
            if isinstance(parsed_raw, dict):
                data = parsed_raw
        except Exception:
            return None, "invalid_json"
    if data is None:
        return None, "no_items"
    allowed = {
        "is_event",
        "event_type",
        "victim",
        "headline",
        "summary",
        "confidence",
        "what_compromised",
        "incident_date",
    }
    data = {key: data.get(key) for key in allowed}
    if "is_event" not in data or data.get("is_event") is None:
        return None, "missing_is_event"
    is_event = data.get("is_event")
    if isinstance(is_event, str):
        is_event = is_event.strip().lower() in {"true", "yes", "1"}
    is_event = bool(is_event)
    event_type = str(data.get("event_type") or "").strip()
    victim = str(data.get("victim") or "").strip()
    headline = str(data.get("headline") or "").strip()
    summary = str(data.get("summary") or "").strip()
    what_compromised = str(data.get("what_compromised") or "").strip()
    incident_date = str(data.get("incident_date") or "").strip()
    confidence = data.get("confidence")
    if isinstance(confidence, str) and confidence.strip().isdigit():
        confidence = int(confidence.strip())
    if isinstance(confidence, float):
        confidence = int(confidence)
    if not isinstance(confidence, int):
        confidence = 0
    if not is_event:
        return {
            "is_event": False,
            "event_type": "",
            "victim": "",
            "headline": "",
            "summary": "",
            "what_compromised": "",
            "incident_date": "",
            "confidence": confidence,
        }, None
    if not event_type or event_type.lower() in {"unknown", "n/a", "none", "null"}:
        return None, "missing_event_type"
    if not victim or victim.lower() in {"unknown", "n/a", "none", "null"}:
        return None, "missing_victim"
    if not what_compromised or what_compromised.lower() in {"unknown", "n/a", "none", "null"}:
        return None, "missing_what_compromised"
    return {
        "is_event": True,
        "event_type": event_type,
        "victim": victim,
        "headline": headline,
        "summary": summary,
        "what_compromised": what_compromised,
        "incident_date": incident_date,
        "confidence": confidence,
    }, None


def _parse_event_web_validation(
    result: dict[str, object],
) -> tuple[dict[str, object] | None, str | None]:
    parsed = result.get("parsed")
    raw = result.get("raw")
    data: dict[str, object] | None = None
    if isinstance(parsed, dict):
        data = parsed
    if data is None and isinstance(raw, str):
        parsed_raw = _decode_json_from_raw(raw)
        if isinstance(parsed_raw, dict):
            data = parsed_raw
    if data is None:
        return None, "invalid_json"
    related = data.get("related")
    if isinstance(related, str):
        related = related.strip().lower() in {"1", "true", "yes"}
    related = bool(related)
    confidence = data.get("confidence")
    try:
        confidence = float(confidence)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    matched_facts = data.get("matched_facts") if isinstance(data.get("matched_facts"), list) else []
    contradictions = data.get("contradictions") if isinstance(data.get("contradictions"), list) else []
    rationale = str(data.get("rationale") or "").strip()
    return {
        "related": related,
        "confidence": confidence,
        "matched_facts": [str(x).strip() for x in matched_facts if str(x).strip()],
        "contradictions": [str(x).strip() for x in contradictions if str(x).strip()],
        "rationale": rationale,
    }, None


def _contains_word(text: str, token: str) -> bool:
    if not text or not token:
        return False
    pattern = re.compile(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", re.IGNORECASE)
    return bool(pattern.search(text))


def _validate_event_source_fallback(
    event: dict[str, object],
    source: dict[str, object],
    content: str,
) -> dict[str, object]:
    haystack = " ".join(
        [
            str(source.get("title") or ""),
            str(source.get("snippet") or ""),
            content or "",
        ]
    ).lower()
    entity = str(event.get("entity") or "").strip()
    title = str(event.get("title") or "").strip()
    entity_token = (entity or title).split()[0].strip().lower() if (entity or title) else ""
    kind = str(event.get("kind") or "").strip().lower()
    keyword_sets = {
        "breach": {"breach", "compromised", "intrusion", "incident", "stolen"},
        "ransomware": {"ransomware", "extortion", "leak", "encrypt"},
        "campaign": {"campaign", "apt", "espionage"},
        "exploit": {"exploit", "vulnerability", "poc"},
        "vuln": {"vulnerability", "advisory", "patch"},
    }
    kind_keywords = keyword_sets.get(kind, {"breach", "incident", "compromised"})
    entity_hit = _contains_word(haystack, entity_token) if entity_token else False
    keyword_hits = [kw for kw in kind_keywords if kw in haystack]
    related = entity_hit and bool(keyword_hits)
    confidence = 0.2
    if entity_hit:
        confidence += 0.35
    if keyword_hits:
        confidence += min(0.35, 0.1 * len(keyword_hits))
    incident_year = str(event.get("incident_date") or "")[:4]
    if incident_year and incident_year in haystack:
        confidence += 0.1
    confidence = max(0.0, min(1.0, confidence))
    matched = []
    if entity_hit and entity_token:
        matched.append(f"entity:{entity_token}")
    matched.extend([f"keyword:{kw}" for kw in keyword_hits[:4]])
    return {
        "related": related,
        "confidence": confidence,
        "matched_facts": matched,
        "contradictions": [],
        "rationale": "fallback_lexical_validation",
        "validator": "fallback",
    }


def _validate_event_source_with_llm(
    conn,
    logger: logging.Logger,
    *,
    event: dict[str, object],
    source: dict[str, object],
    content: str,
) -> tuple[dict[str, object], str]:
    profile = None
    profile_id = str(os.getenv("SV_EVENT_ENRICH_VALIDATION_PROFILE_ID", "")).strip()
    if profile_id:
        profile = get_profile(conn, profile_id)
    if not profile:
        profile, _reason = get_active_profile_for_stage(conn, "event_web_validate")
    if not profile:
        profile, _reason = get_active_profile_for_stage(conn, "summarize_article")
    if not profile:
        return _validate_event_source_fallback(event, source, content), "fallback_no_profile"

    prompt_input = (
        "Validate whether the candidate article is truly about the given security event.\n"
        "Return JSON only with keys: related (bool), confidence (0..1), matched_facts (array), "
        "contradictions (array), rationale (string).\n\n"
        f"Event title: {event.get('title')}\n"
        f"Event entity: {event.get('entity')}\n"
        f"Event kind: {event.get('kind')}\n"
        f"Event incident_date: {event.get('incident_date')}\n"
        f"Event summary: {event.get('summary')}\n\n"
        f"Candidate source title: {source.get('title')}\n"
        f"Candidate source domain: {source.get('domain')}\n"
        f"Candidate snippet: {source.get('snippet')}\n\n"
        "Candidate article content:\n"
        f"{content[:12000]}"
    )
    result = run_profile(
        conn,
        str(profile["id"]),
        prompt_input,
        logger,
        context={
            "stage": "event_web_validate",
            "job_type": "validate_event_web_source",
            "profile_name": profile.get("name") or "",
        },
    )
    parsed, reason = _parse_event_web_validation(result if isinstance(result, dict) else {})
    if parsed:
        parsed["validator"] = "llm"
        return parsed, ""
    return _validate_event_source_fallback(event, source, content), f"fallback_parse:{reason or 'unknown'}"


def _normalize_threat_actor_item(item: dict[str, object]) -> dict[str, object] | None:
    name = str(item.get("name") or item.get("actor") or item.get("group") or "").strip()
    if not name:
        return None
    if name.lower() in {"unknown", "n/a", "none", "null"}:
        return None
    actor_type = str(item.get("type") or item.get("actor_type") or "").strip() or "unknown"
    country = str(item.get("country") or "").strip() or None
    aliases = item.get("aliases") or []
    if isinstance(aliases, str):
        aliases = [aliases]
    cleaned_aliases = []
    for alias in aliases:
        alias_clean = str(alias or "").strip()
        if alias_clean and alias_clean.lower() != name.lower():
            cleaned_aliases.append(alias_clean)
    confidence = item.get("confidence")
    confidence_value = None
    if confidence is not None:
        try:
            confidence_value = int(confidence)
        except (TypeError, ValueError):
            confidence_value = None
        if confidence_value is not None:
            confidence_value = max(0, min(100, confidence_value))
    return {
        "name": name,
        "actor_type": actor_type,
        "country": country,
        "aliases": cleaned_aliases,
        "confidence": confidence_value,
    }


def _handle_article_enrich_threat_actors(
    conn, config, job, logger: logging.Logger
) -> dict[str, object]:
    payload = job.payload or {}
    article_id = payload.get("article_id")
    if not article_id:
        raise ValueError("article_id is required")
    article = get_article_by_id(conn, int(article_id))
    if not article:
        return {"status": "skipped", "reason": "article_not_found"}
    if get_article_threat_actors(conn, int(article_id)):
        mark_article_threat_actors_checked(conn, int(article_id))
        return {"status": "skipped", "reason": "already_linked"}
    profile, reason = get_active_profile_for_stage(conn, "article_enrich_threat_actors")
    if not profile:
        return {"status": "skipped", "reason": f"no_profile_routed:{reason}"}
    content_text = article.get("content_text") or ""
    if not content_text:
        return {"status": "skipped", "reason": "no_full_content"}
    excerpt = content_text.strip()
    if len(excerpt) > 20000:
        excerpt = excerpt[:20000] + "\n[TRUNCATED]"
    input_text = "\n".join(
        [
            f"Title: {article.get('title')}",
            f"Source: {get_source_name(conn, article.get('source_id') or '')}",
            f"Published: {article.get('published_at') or 'unknown'}",
            f"URL: {article.get('original_url') or article.get('normalized_url')}",
            "",
            "Full Content:",
            excerpt,
        ]
    ).strip()
    if not input_text:
        return {"status": "skipped", "reason": "no_input"}
    result = run_pipeline_stage(
        conn,
        "article_enrich_threat_actors",
        input_text,
        logger,
        profile_id=profile["id"],
        context={"stage": "article_enrich_threat_actors", "job_type": job.job_type},
    )
    result_dict = result if isinstance(result, dict) else {}
    items, error_reason = _parse_threat_actor_items(result_dict)
    if error_reason:
        raw = result_dict.get("raw")
        preview = (raw or "").strip()
        if len(preview) > 800:
            preview = preview[:800] + "\n[TRUNCATED]"
        labels = _llm_profile_labels(conn, profile)
        # Treat parse outcomes as terminal for "need" accounting.
        mark_article_threat_actors_checked(conn, int(article_id))
        if error_reason in {"no_items", "no_valid_items"}:
            log_event(
                logger,
                logging.INFO,
                "llm_no_items",
                stage="article_enrich_threat_actors",
                article_id=article_id,
                reason=error_reason,
                **labels,
            )
        else:
            log_event(
                logger,
                logging.WARNING,
                "llm_parse_failed",
                stage="article_enrich_threat_actors",
                article_id=article_id,
                reason=error_reason,
                **labels,
            )
        return {"status": "skipped", "reason": error_reason, "raw_preview": preview}
    cleaned = []
    for item in items:
        normalized = _normalize_threat_actor_item(item)
        if normalized:
            cleaned.append(normalized)
    if not cleaned:
        mark_article_threat_actors_checked(conn, int(article_id))
        return {"status": "skipped", "reason": "no_valid_items"}
    threat_actors_created = 0
    threat_links_created = 0
    for actor in cleaned:
        actor_key = slugify(actor["name"])
        existing_actor_id = get_threat_actor_id_by_key(conn, actor_key)
        actor_id = upsert_threat_actor(
            conn,
            actor_key,
            actor["name"],
            actor["actor_type"],
            country=actor.get("country"),
            confidence=actor.get("confidence"),
        )
        if existing_actor_id is None:
            threat_actors_created += 1
        for alias in actor.get("aliases") or []:
            add_threat_actor_alias(conn, actor_id, str(alias))
        link_article_threat_actor(conn, int(article_id), actor_id)
        threat_links_created += 1
    mark_article_threat_actors_checked(conn, int(article_id))
    log_event(
        logger,
        logging.INFO,
        "article_enrich_threat_actors",
        article_id=article_id,
        items=len(cleaned),
        threat_actors_created=threat_actors_created,
        threat_links_created=threat_links_created,
    )
    return {
        "status": "ok",
        "article_id": article_id,
        "items": len(cleaned),
        "threat_actors_created": threat_actors_created,
        "threat_links_created": threat_links_created,
    }


def _handle_cve_enrich_threat_actors(
    conn, config, job, logger: logging.Logger
) -> dict[str, object]:
    payload = job.payload or {}
    cve_id = str(payload.get("cve_id") or "").strip()
    if not cve_id:
        raise ValueError("cve_id is required")
    cve = get_cve(conn, cve_id)
    if not cve:
        return {"status": "skipped", "reason": "cve_not_found"}
    if get_cve_threat_actors(conn, cve_id):
        mark_cve_threat_actors_checked(conn, cve_id)
        return {"status": "skipped", "reason": "already_linked"}
    profile, reason = get_active_profile_for_stage(conn, "cve_enrich_threat_actors")
    if not profile:
        return {"status": "skipped", "reason": f"no_profile_routed:{reason}"}
    description = cve.get("description_text") or ""
    references = cve.get("reference_domains") or []
    if not description and not references:
        return {"status": "skipped", "reason": "no_input"}
    prompt_lines = [
        f"CVE: {cve_id}",
        "Description:",
        description,
        "",
    ]
    if references:
        prompt_lines.append("Reference domains:")
        prompt_lines.extend([f"- {ref}" for ref in references])
    input_text = "\n".join(prompt_lines).strip()
    result = run_pipeline_stage(
        conn,
        "cve_enrich_threat_actors",
        input_text,
        logger,
        profile_id=profile["id"],
        context={"stage": "cve_enrich_threat_actors", "job_type": job.job_type},
    )
    result_dict = result if isinstance(result, dict) else {}
    items, error_reason = _parse_threat_actor_items(result_dict)
    if error_reason:
        raw = result_dict.get("raw")
        preview = (raw or "").strip()
        if len(preview) > 800:
            preview = preview[:800] + "\n[TRUNCATED]"
        labels = _llm_profile_labels(conn, profile)
        # Treat parse outcomes as terminal for "need" accounting.
        mark_cve_threat_actors_checked(conn, cve_id)
        if error_reason in {"no_items", "no_valid_items"}:
            log_event(
                logger,
                logging.INFO,
                "llm_no_items",
                stage="cve_enrich_threat_actors",
                cve_id=cve_id,
                reason=error_reason,
                **labels,
            )
        else:
            log_event(
                logger,
                logging.WARNING,
                "llm_parse_failed",
                stage="cve_enrich_threat_actors",
                cve_id=cve_id,
                reason=error_reason,
                **labels,
            )
        return {"status": "skipped", "reason": error_reason, "raw_preview": preview}
    cleaned = []
    for item in items:
        normalized = _normalize_threat_actor_item(item)
        if normalized:
            cleaned.append(normalized)
    if not cleaned:
        mark_cve_threat_actors_checked(conn, cve_id)
        return {"status": "skipped", "reason": "no_valid_items"}
    threat_actors_created = 0
    threat_links_created = 0
    for actor in cleaned:
        actor_key = slugify(actor["name"])
        existing_actor_id = get_threat_actor_id_by_key(conn, actor_key)
        actor_id = upsert_threat_actor(
            conn,
            actor_key,
            actor["name"],
            actor["actor_type"],
            country=actor.get("country"),
            confidence=actor.get("confidence"),
        )
        if existing_actor_id is None:
            threat_actors_created += 1
        for alias in actor.get("aliases") or []:
            add_threat_actor_alias(conn, actor_id, str(alias))
        link_cve_threat_actor(conn, cve_id, actor_id)
        threat_links_created += 1
    mark_cve_threat_actors_checked(conn, cve_id)
    log_event(
        logger,
        logging.INFO,
        "cve_enrich_threat_actors",
        cve_id=cve_id,
        items=len(cleaned),
        threat_actors_created=threat_actors_created,
        threat_links_created=threat_links_created,
    )
    return {
        "status": "ok",
        "cve_id": cve_id,
        "items": len(cleaned),
        "threat_actors_created": threat_actors_created,
        "threat_links_created": threat_links_created,
    }


def _handle_article_enrich_products(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    payload = job.payload or {}
    article_id = payload.get("article_id")
    force = bool(payload.get("force"))
    if not article_id:
        raise ValueError("article_id is required")
    article = get_article_by_id(conn, int(article_id))
    if not article:
        return {"status": "skipped", "reason": "article_not_found"}
    if not force and count_products_for_article(conn, int(article_id)) > 0:
        mark_article_products_checked(conn, int(article_id))
        return {"status": "skipped", "reason": "already_linked"}
    profile, reason = get_active_profile_for_stage(conn, "article_enrich_products")
    if not profile:
        return {"status": "skipped", "reason": f"no_profile_routed:{reason}"}
    source_name = get_source_name(conn, article["source_id"]) or ""
    content_text = article.get("content_text") or ""
    if not content_text:
        return {"status": "skipped", "reason": "no_full_content"}
    excerpt = content_text.strip()
    if len(excerpt) > 20000:
        excerpt = excerpt[:20000] + "\n[TRUNCATED]"
    lines = [
        f"Title: {article.get('title')}",
        f"Source: {source_name}",
        f"Published: {article.get('published_at') or 'unknown'}",
        f"URL: {article.get('original_url') or article.get('normalized_url')}",
    ]
    if excerpt:
        lines.append("Content excerpt:")
        lines.append(excerpt)
    input_text = "\n".join(lines).strip()
    if not input_text:
        return {"status": "skipped", "reason": "no_input"}
    result = run_pipeline_stage(
        conn,
        "article_enrich_products",
        input_text,
        logger,
        profile_id=profile["id"],
        context={"stage": "article_enrich_products", "job_type": job.job_type},
    )
    result_dict = result if isinstance(result, dict) else {}
    items, error_reason = _parse_product_items(result_dict, allow_versions=False)
    if error_reason:
        raw = result_dict.get("raw")
        preview = (raw or "").strip()
        if len(preview) > 800:
            preview = preview[:800] + "\n[TRUNCATED]"
        labels = _llm_profile_labels(conn, profile)
        # Treat parse outcomes as terminal for "need" accounting.
        mark_article_products_checked(conn, int(article_id))
        if error_reason in {"no_items", "no_valid_items"}:
            log_event(
                logger,
                logging.INFO,
                "llm_no_items",
                stage="article_enrich_products",
                article_id=article_id,
                reason=error_reason,
                **labels,
            )
        else:
            log_event(
                logger,
                logging.WARNING,
                "llm_parse_failed",
                stage="article_enrich_products",
                article_id=article_id,
                reason=error_reason,
                **labels,
            )
        return {"status": "skipped", "reason": error_reason, "raw_preview": preview}
    vendors_created = 0
    products_created = 0
    links_created = 0
    for item in items:
        vendor = str(item.get("vendor") or "").strip()
        product = str(item.get("product") or "").strip()
        if not vendor or vendor.lower() in {"unknown", "n/a", "none", "null"}:
            continue
        if not product:
            continue
        existing_vendor_id = get_vendor_id_by_name(conn, vendor)
        vendor_id = existing_vendor_id or upsert_vendor(conn, vendor)
        if existing_vendor_id is None:
            vendors_created += 1
        existing_product_id = get_product_id_by_vendor_name(conn, vendor_id, product)
        product_id, _ = upsert_product(conn, vendor_id, product)
        if existing_product_id is None:
            products_created += 1
        link_article_product(
            conn,
            article_id=int(article_id),
            product_id=product_id,
            source="llm",
            evidence=item,
        )
        links_created += 1
    mark_article_products_checked(conn, int(article_id))
    log_event(
        logger,
        logging.INFO,
        "article_enrich_products",
        article_id=article_id,
        items=len(items),
        vendors_created=vendors_created,
        products_created=products_created,
        links_created=links_created,
    )
    return {
        "status": "ok",
        "article_id": article_id,
        "items": len(items),
        "vendors_created": vendors_created,
        "products_created": products_created,
        "links_created": links_created,
    }


def _handle_article_products_backfill(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    payload = job.payload or {}
    limit = int(payload.get("limit") or 200)
    article_ids = list_article_ids_missing_products(conn, limit=limit)
    enqueued = 0
    for article_id in article_ids:
        if count_products_for_article(conn, int(article_id)) > 0:
            continue
        if has_pending_article_job(conn, "article_enrich_products", int(article_id)):
            continue
        enqueue_job(conn, "article_enrich_products", {"article_id": int(article_id)})
        enqueued += 1
    return {"status": "ok", "scanned": len(article_ids), "enqueued": enqueued}


def _handle_article_threat_actors_backfill(
    conn, config, job, logger: logging.Logger
) -> dict[str, object]:
    payload = job.payload or {}
    limit = int(payload.get("limit") or 200)
    article_ids = list_article_ids_missing_threat_actors(conn, limit=limit)
    enqueued = 0
    for article_id in article_ids:
        if has_pending_article_job(conn, "article_enrich_threat_actors", int(article_id)):
            continue
        enqueue_job(conn, "article_enrich_threat_actors", {"article_id": int(article_id)})
        enqueued += 1
    log_event(
        logger,
        logging.INFO,
        "article_threat_actors_backfill",
        scanned=len(article_ids),
        enqueued=enqueued,
    )
    return {"status": "ok", "scanned": len(article_ids), "enqueued": enqueued}


def _handle_cve_threat_actors_backfill(
    conn, config, job, logger: logging.Logger
) -> dict[str, object]:
    payload = job.payload or {}
    limit = int(payload.get("limit") or 200)
    cve_ids = list_cve_ids_missing_threat_actors(conn, limit=limit)
    enqueued = 0
    for cve_id in cve_ids:
        existing = get_pending_job_id_for_cve(conn, "cve_enrich_threat_actors", cve_id)
        if existing:
            continue
        enqueue_job(conn, "cve_enrich_threat_actors", {"cve_id": cve_id})
        enqueued += 1
    log_event(
        logger,
        logging.INFO,
        "cve_threat_actors_backfill",
        scanned=len(cve_ids),
        enqueued=enqueued,
    )
    return {"status": "ok", "scanned": len(cve_ids), "enqueued": enqueued}


def _handle_events_rebuild(conn, config, payload: dict[str, object], logger: logging.Logger) -> dict[str, object]:
    settings = get_events_settings(conn)
    limit = None
    if payload and isinstance(payload.get("limit"), int):
        limit = int(payload["limit"])
    stats = rebuild_events_from_cves(
        conn,
        window_days=int(settings.get("merge_window_days", 14)),
        min_shared_products=int(settings.get("min_shared_products_to_merge", 1)),
        limit=limit,
    )
    _publish_events(conn, config, logger)
    return stats


def _handle_rebuild_vendor_products(conn, config, logger: logging.Logger) -> dict[str, object]:
    site_root = _site_root_from_output_dir(config.paths.output_dir)
    tz_name = config.app.timezone or "UTC"
    _maybe_cleanup_vendor_product_tags(conn, logger)
    stats = _write_vendor_product_indexes(conn, site_root, tz_name, logger)
    return stats


def _maybe_cleanup_vendor_product_tags(conn, logger: logging.Logger) -> None:
    if get_setting(conn, "vendor_product_tags_cleanup_done", None):
        return
    removed = delete_vendor_product_tags(conn)
    set_setting(conn, "vendor_product_tags_cleanup_done", utc_now_iso())
    log_event(
        logger,
        logging.INFO,
        "vendor_product_tags_cleanup",
        removed=removed,
    )


def _publish_events(conn, config, logger: logging.Logger) -> None:
    events: list[dict[str, object]] = []
    page = 1
    page_size = 200
    total = 0
    while True:
        items, total = list_events(
            conn,
            status=None,
            kind=None,
            severity=None,
            query=None,
            after=None,
            before=None,
            page=page,
            page_size=page_size,
        )
        if not items:
            break
        for item in items:
            publish_state = str(item.get("publish_state") or "").lower()
            lifecycle = str(item.get("lifecycle") or item.get("status") or "").lower()
            if publish_state:
                # Published events must also be confirmed.
                if publish_state != "published" or lifecycle != "confirmed":
                    continue
            else:
                # Backward-compatible fallback for older schemas without publish_state.
                if lifecycle != "confirmed":
                    continue
            detail = get_event(conn, item["id"])
            if detail:
                events.append(detail)
        if len(items) < page_size:
            break
        page += 1
    base_content_dir = os.path.dirname(config.paths.output_dir)
    base_static_dir = os.path.dirname(config.publishing.json_index_path)
    written_pages = write_events_markdown(events, base_content_dir)
    index_path = write_events_index(events, base_static_dir)
    log_event(
        logger,
        logging.INFO,
        "events_published",
        count=len(events),
        total=total,
        index_path=index_path,
        pages=len(written_pages),
    )


def _maybe_enqueue_cve_sync(conn, logger: logging.Logger) -> None:
    settings = get_cve_settings(conn)
    if not settings.get("enabled", True):
        return
    last_sync = get_setting(conn, "cve.last_successful_sync_at", None)
    now = datetime.now(tz=timezone.utc)
    if isinstance(last_sync, str):
        last_dt = _parse_iso(last_sync)
    else:
        last_dt = now - timedelta(minutes=int(settings.get("schedule_minutes", 60)) + 1)
    due = last_dt + timedelta(minutes=int(settings.get("schedule_minutes", 60))) <= now
    if due:
        enqueue_job(conn, "cve_sync", None, debounce=True)


def _queued_job_total(conn, job_types: set[str] | None = None) -> int:
    total = 0
    for row in list_queued_job_stats(conn):
        try:
            job_type = str(row.get("job_type") or "")
            if job_types is not None and job_type not in job_types:
                continue
            total += int(row.get("queued") or 0)
        except Exception:
            continue
    return total


def _maybe_enqueue_auto_catchup(
    conn,
    config,
    logger: logging.Logger,
    worker_id: str,
    allowed_types: list[str] | None,
) -> int:
    if not bool(getattr(config.jobs, "auto_catchup_enabled", False)):
        return 0
    if allowed_types is None:
        allowed_auto_types = set(_AUTO_CATCHUP_JOB_TYPES)
    else:
        allowed_auto_types = {job for job in allowed_types if job in _AUTO_CATCHUP_JOB_TYPES}
    if not allowed_auto_types:
        return 0
    lease_holder = f"{worker_id}:{uuid.uuid4().hex}"
    if not try_acquire_lease(conn, _AUTO_CATCHUP_LEASE, lease_holder, ttl_seconds=30):
        return 0
    queued_total = 0
    try:
        by_type: dict[str, int] = {}

        def _bump(job_type: str) -> None:
            by_type[job_type] = int(by_type.get(job_type, 0) or 0) + 1

        if "fetch_article_content" in allowed_auto_types:
            missing_content_ids = list_article_ids_missing_content_all(conn, limit=_AUTO_CATCHUP_BATCH_LIMIT)
            for article_id in missing_content_ids:
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                article_id = int(article_id)
                if has_pending_article_job(conn, "fetch_article_content", article_id):
                    continue
                failed_attempts = count_failed_article_jobs(conn, "fetch_article_content", article_id)
                if failed_attempts >= 3:
                    article = get_article_by_id(conn, article_id)
                    if article and str(article.get("content_error") or "") != "max_retries_exceeded":
                        update_article_content(
                            conn,
                            article_id,
                            content_text=article.get("content_text"),
                            content_html=article.get("content_html"),
                            content_fetched_at=article.get("content_fetched_at"),
                            content_error="max_retries_exceeded",
                            has_full_content=bool(article.get("has_full_content")),
                        )
                    continue
                enqueue_job(conn, "fetch_article_content", {"article_id": article_id}, dedupe=True)
                queued_total += 1
                _bump("fetch_article_content")

        summary_profile, _summary_reason = get_active_profile_for_stage(conn, "summarize_article")
        if summary_profile and "summarize_article_llm" in allowed_auto_types:
            for article_id in list_article_ids_ready_for_summary_all(conn):
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                article_id = int(article_id)
                if has_pending_article_job(conn, "summarize_article_llm", article_id):
                    continue
                enqueue_job(conn, "summarize_article_llm", {"article_id": article_id}, dedupe=True)
                queued_total += 1
                _bump("summarize_article_llm")

        context_profile, _context_reason = get_active_profile_for_stage(conn, "article_context_pack")
        if context_profile and "summarize_article_context_llm" in allowed_auto_types:
            remaining = _AUTO_CATCHUP_BATCH_LIMIT - queued_total
            if remaining > 0:
                context_ids = list_article_ids_missing_context_pack(conn, limit=remaining)
                for article_id in context_ids:
                    if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                        break
                    article_id = int(article_id)
                    if has_pending_article_job(conn, "summarize_article_context_llm", article_id):
                        continue
                    enqueue_job(
                        conn,
                        "summarize_article_context_llm",
                        {"article_id": article_id},
                        dedupe=True,
                    )
                    queued_total += 1
                    _bump("summarize_article_context_llm")

        derive_profile, _derive_reason = get_active_profile_for_stage(conn, "derive_events_from_articles")
        remaining = _AUTO_CATCHUP_BATCH_LIMIT - queued_total
        if remaining > 0 and derive_profile and "derive_events_from_articles" in allowed_auto_types:
            derive_ids = list_article_ids_without_event(conn, limit=remaining)
            for article_id in derive_ids:
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                article_id = int(article_id)
                if has_pending_article_job(conn, "derive_events_from_articles", article_id):
                    continue
                enqueue_job(
                    conn,
                    "derive_events_from_articles",
                    {"article_id": article_id},
                    dedupe=True,
                )
                queued_total += 1
                _bump("derive_events_from_articles")

        remaining = _AUTO_CATCHUP_BATCH_LIMIT - queued_total
        if remaining > 0 and "article_enrich_products" in allowed_auto_types:
            product_ids = list_article_ids_missing_products(conn, limit=remaining)
            for article_id in product_ids:
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                article_id = int(article_id)
                if has_pending_article_job(conn, "article_enrich_products", article_id):
                    continue
                enqueue_job(conn, "article_enrich_products", {"article_id": article_id}, dedupe=True)
                queued_total += 1
                _bump("article_enrich_products")

        remaining = _AUTO_CATCHUP_BATCH_LIMIT - queued_total
        if remaining > 0 and "article_enrich_threat_actors" in allowed_auto_types:
            threat_article_ids = list_article_ids_missing_threat_actors(conn, limit=remaining)
            for article_id in threat_article_ids:
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                article_id = int(article_id)
                if has_pending_article_job(conn, "article_enrich_threat_actors", article_id):
                    continue
                enqueue_job(conn, "article_enrich_threat_actors", {"article_id": article_id}, dedupe=True)
                queued_total += 1
                _bump("article_enrich_threat_actors")

        remaining = _AUTO_CATCHUP_BATCH_LIMIT - queued_total
        if remaining > 0 and "cve_enrich_kev" in allowed_auto_types:
            kev_ids = list_cve_ids_needing_kev_check(conn, limit=remaining)
            for cve_id in kev_ids:
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                cve_id = str(cve_id)
                if get_pending_job_id_for_cve(conn, "cve_enrich_kev", cve_id):
                    continue
                enqueue_job(conn, "cve_enrich_kev", {"cve_id": cve_id}, dedupe=True)
                queued_total += 1
                _bump("cve_enrich_kev")

        cve_profile, _cve_reason = get_active_profile_for_stage(conn, "cve_enrich_products")
        remaining = _AUTO_CATCHUP_BATCH_LIMIT - queued_total
        if remaining > 0 and cve_profile and "cve_enrich_llm" in allowed_auto_types:
            cve_ids = list_cve_ids_missing_products(conn, limit=remaining)
            for cve_id in cve_ids:
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                cve_id = str(cve_id)
                if get_pending_job_id_for_cve(conn, "cve_enrich_llm", cve_id):
                    continue
                enqueue_job(conn, "cve_enrich_llm", {"cve_id": cve_id}, dedupe=True)
                queued_total += 1
                _bump("cve_enrich_llm")

        remaining = _AUTO_CATCHUP_BATCH_LIMIT - queued_total
        if remaining > 0 and "cve_enrich_threat_actors" in allowed_auto_types:
            cve_threat_ids = list_cve_ids_missing_threat_actors(conn, limit=remaining)
            for cve_id in cve_threat_ids:
                if queued_total >= _AUTO_CATCHUP_BATCH_LIMIT:
                    break
                cve_id = str(cve_id)
                if get_pending_job_id_for_cve(conn, "cve_enrich_threat_actors", cve_id):
                    continue
                enqueue_job(conn, "cve_enrich_threat_actors", {"cve_id": cve_id}, dedupe=True)
                queued_total += 1
                _bump("cve_enrich_threat_actors")

        if queued_total > 0:
            log_event(
                logger,
                logging.INFO,
                "auto_catchup_enqueued",
                queued=queued_total,
                by_type=by_type,
            )
    finally:
        try:
            release_lease(conn, _AUTO_CATCHUP_LEASE, lease_holder)
        except Exception:
            pass
    return queued_total


def auto_catchup_types_for_queue(queue_name: str) -> list[str]:
    return [
        job_type
        for job_type in QUEUE_WORKER_TYPES.get(queue_name, [])
        if job_type in _AUTO_CATCHUP_JOB_TYPES
    ]

def _should_tick_ingest_due(allowed_types: list[str] | None) -> bool:
    if allowed_types is None:
        return True
    if "ingest_due_sources" in allowed_types:
        return True
    return any(
        job_type in allowed_types
        for job_type in ( "ingest_source", "html_index", "rss_index")
    )


def _maybe_enqueue_ingest_due_sources(conn, logger: logging.Logger) -> None:
    if has_pending_job(conn, "ingest_due_sources"):
        return
    debounce_seconds = int(os.environ.get("SV_INGEST_DUE_DEBOUNCE_SECONDS", "60"))
    last_enqueued = get_setting(conn, "ingest_due.last_enqueued_at", None)
    now = utc_now_iso()
    if isinstance(last_enqueued, str):
        last_dt = _parse_iso(last_enqueued)
        if last_dt + timedelta(seconds=debounce_seconds) > _parse_iso(now):
            return
    due = list_due_sources(conn, now)
    if not due:
        return
    enqueue_job(conn, "ingest_due_sources", None, debounce=True)
    set_setting(conn, "ingest_due.last_enqueued_at", now)
    log_event(logger, logging.INFO, "ingest_due_sources_enqueued", due_count=len(due))


def _maybe_pause_source(conn, source_id: str, logger: logging.Logger | None) -> None:
    enabled = bool(get_setting(conn, "alerts.pause_on_failure.enabled", True))
    if not enabled:
        return
    error_threshold = int(get_setting(conn, "alerts.pause_on_failure.error_streak", 5))
    pause_minutes = int(get_setting(conn, "alerts.pause_on_failure.pause_minutes", 1440))
    zero_threshold = int(get_setting(conn, "alerts.pause_on_failure.zero_streak", 3))
    streaks = get_source_run_streaks(conn, source_id)
    if streaks["consecutive_errors"] >= error_threshold:
        reason = f"auto_pause:error_streak:{streaks['consecutive_errors']}"
        pause_source(conn, source_id, reason, pause_minutes)
        record_health_alert(conn, source_id, "error_streak", reason)
        if logger:
            log_event(
                logger,
                logging.WARNING,
                "source_auto_paused",
                source_id=source_id,
                reason=reason,
            )
    else:
        if streaks["consecutive_zero"] < zero_threshold:
            return
        reason = f"auto_pause:zero_found_streak:{streaks['consecutive_zero']}"
        pause_source(conn, source_id, reason, pause_minutes)
        record_health_alert(conn, source_id, "zero_found_streak", reason)
        if logger:
            log_event(
                logger,
                logging.WARNING,
                "source_auto_paused",
                source_id=source_id,
                reason=reason,
            )


def _parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _article_priority(article: dict[str, object]) -> int:
    ts = article.get("published_at") or article.get("ingested_at")
    parsed = _parse_ts(str(ts)) if ts else None
    if not parsed:
        return 0
    age_hours = (datetime.now(timezone.utc) - parsed).total_seconds() / 3600
    if age_hours <= 24:
        return 10
    if age_hours <= 72:
        return 7
    if age_hours <= 168:
        return 4
    return 0


def _parse_only_types(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or None


def _resolve_allowed_types(
    raw_only_types: str | None,
    queue_name: str | None,
    logger: logging.Logger,
) -> tuple[list[str] | None, str | None]:
    allowed_types = _parse_only_types(raw_only_types)
    normalized_queue = (queue_name or "").strip().lower() or None
    if not normalized_queue:
        return allowed_types, None
    queue_types = QUEUE_WORKER_TYPES.get(normalized_queue)
    if queue_types is None:
        log_event(
            logger,
            logging.WARNING,
            "worker_unknown_queue",
            queue_name=normalized_queue,
        )
        return [], normalized_queue
    if allowed_types is None:
        return list(queue_types), normalized_queue
    allowed_set = set(allowed_types)
    filtered = [job_type for job_type in queue_types if job_type in allowed_set]
    if not filtered:
        log_event(
            logger,
            logging.WARNING,
            "worker_queue_filtered_empty",
            queue_name=normalized_queue,
            only_job_types=allowed_types,
        )
    return filtered, normalized_queue


def _scheduler_hooks_enabled() -> bool:
    return os.environ.get("SV_WORKER_SCHEDULER_HOOKS_ENABLED", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _validate_allowed_types(
    allowed_types: list[str] | None,
    raw_value: str | None,
    logger: logging.Logger,
) -> list[str] | None:
    known = set(WORKER_JOB_TYPES)
    handled = set(HANDLED_JOB_TYPES)
    mismatch_missing = sorted(known - handled)
    mismatch_extra = sorted(handled - known)
    if mismatch_missing or mismatch_extra:
        log_event(
            logger,
            logging.WARNING,
            "worker_job_type_mismatch",
            missing_in_handler=mismatch_missing,
            extra_in_handler=mismatch_extra,
        )
    if raw_value and allowed_types is None:
        log_event(
            logger,
            logging.WARNING,
            "worker_allowed_types_empty",
            raw_value=raw_value,
        )
        return []
    if allowed_types is None:
        return None
    allowed_set = set(allowed_types)
    unknown = sorted(allowed_set - known)
    unhandled = sorted(allowed_set - handled)
    if unknown:
        log_event(
            logger,
            logging.WARNING,
            "worker_unknown_job_types",
            job_types=unknown,
        )
    if unhandled:
        log_event(
            logger,
            logging.WARNING,
            "worker_unhandled_job_types",
            job_types=unhandled,
        )
    filtered = [job_type for job_type in allowed_types if job_type in known and job_type in handled]
    if not filtered and allowed_types:
        log_event(
            logger,
            logging.WARNING,
            "worker_allowed_types_filtered_empty",
            job_types=allowed_types,
        )
    return filtered


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil-worker")
    parser.add_argument("--once", action="store_true", help="Run a single job and exit")
    parser.add_argument("--sleep", type=int, default=10, help="Sleep seconds between polls")
    parser.add_argument("--worker-id", default=os.environ.get("HOSTNAME", "worker"))
    parser.add_argument("--queue", default=os.environ.get("SV_WORKER_QUEUE", ""))
    parser.add_argument("--only-job-types", default=os.environ.get("SV_WORKER_ONLY_TYPES", ""))
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("SV_WORKER_CONCURRENCY", "1")),
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=int(os.environ.get("SV_WORKER_MAX_JOBS", "0") or 0),
        help="Drain at most N claimed jobs before exiting (0 = unlimited)",
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=int(os.environ.get("SV_WORKER_MAX_RUNTIME_SECONDS", "0") or 0),
        help="Drain for at most N seconds before exiting (0 = unlimited)",
    )
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=int(os.environ.get("SV_WORKER_LEASE_SECONDS", "0") or 0),
        help="Set an explicit lease duration for claimed jobs (0 = default)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logger = _setup_logging()
    allowed_types, queue_name = _resolve_allowed_types(args.only_job_types, args.queue, logger)
    allowed_types = _validate_allowed_types(allowed_types, args.only_job_types, logger)
    max_jobs = args.max_jobs if args.max_jobs > 0 else None
    max_runtime_seconds = args.max_runtime_seconds if args.max_runtime_seconds > 0 else None
    lease_seconds = args.lease_seconds if args.lease_seconds > 0 else None
    if args.once:
        result = run_once(
            args.worker_id,
            allowed_types,
            queue_name=queue_name,
            lease_seconds=lease_seconds,
        )
        return 0 if result == _RUN_ONCE_IDLE else result
    return run_loop(
        args.worker_id,
        args.sleep,
        allowed_types,
        args.concurrency,
        queue_name=queue_name,
        max_jobs=max_jobs,
        max_runtime_seconds=max_runtime_seconds,
        lease_seconds=lease_seconds,
    )


def run_claimed_job(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    _log_job_claimed(conn, job, logger)
    if is_job_canceled(conn, job.id):
        return {"canceled": True}
    if job.job_type == "ingest_source":
        return _handle_ingest_source(conn, config, job.payload, logger, job.id)
    if job.job_type == "ingest_due_sources":
        return _handle_ingest_due_sources(conn, logger)
    if job.job_type == "test_source":
        return _handle_test_source(conn, config, job.payload, logger)
    if job.job_type == "cve_sync":
        return _handle_cve_sync(conn, config, logger, job.payload)
    if job.job_type == "cve_enrich_kev":
        return _handle_cve_enrich_kev(conn, config, job, logger)
    if job.job_type == "cve_enrich_llm":
        return _handle_cve_enrich_llm(conn, config, job, logger)
    if job.job_type == "cve_enrich_threat_actors":
        return _handle_cve_enrich_threat_actors(conn, config, job, logger)
    if job.job_type == "article_enrich_products":
        return _handle_article_enrich_products(conn, config, job, logger)
    if job.job_type == "article_enrich_threat_actors":
        return _handle_article_enrich_threat_actors(conn, config, job, logger)
    if job.job_type == "article_products_backfill":
        return _handle_article_products_backfill(conn, config, job, logger)
    if job.job_type == "article_threat_actors_backfill":
        return _handle_article_threat_actors_backfill(conn, config, job, logger)
    if job.job_type == "cve_threat_actors_backfill":
        return _handle_cve_threat_actors_backfill(conn, config, job, logger)
    if job.job_type == "events_rebuild":
        return _handle_events_rebuild(conn, config, job.payload or {}, logger)
    if job.job_type == "source_acquire":
        return _handle_source_acquire(conn, config, job, logger)
    if job.job_type == "fetch_article_content":
        return _handle_fetch_article_content(conn, config, job, job.payload, logger)
    if job.job_type == "summarize_article_llm":
        return _handle_summarize_article_llm(conn, config, job, logger)
    if job.job_type == "summarize_article_context_llm":
        return _handle_summarize_article_context_llm(conn, config, job, logger)
    if job.job_type == "build_daily_brief":
        return _handle_build_daily_brief(conn, config, job, logger)
    if job.job_type == "write_article_markdown":
        result = _handle_write_article_markdown(conn, config, job.payload, logger)
        if result.get("status") != "skipped" and not has_pending_job(
            conn, "write_article_markdown", exclude_job_id=job.id
        ):
            mark_build_dirty(conn, reason="write_article_markdown")
        return result
    if job.job_type == "derive_events_from_articles":
        return _handle_derive_events_from_articles(conn, config, job.payload or {}, logger)
    if job.job_type == "enrich_event_from_web":
        return _handle_enrich_event_from_web(conn, config, job.payload or {}, logger)
    if job.job_type == "validate_event_web_source":
        return _handle_validate_event_web_source(conn, config, job.payload or {}, logger)
    if job.job_type == "promote_event_web_source_to_article":
        return _handle_promote_event_web_source(conn, config, job.payload or {}, logger)
    if job.job_type == "enrich_event_summary_llm":
        return _handle_enrich_event_summary_llm(conn, config, job.payload or {}, logger)
    if job.job_type == "event_report_llm":
        return _handle_event_report_llm(conn, config, job.payload or {}, logger)
    if job.job_type == "rebuild_vendor_products":
        return _handle_rebuild_vendor_products(conn, config, logger)
    if job.job_type == "smoke_test":
        return _handle_smoke_test(conn, config, job, logger)
    raise ValueError(f"unsupported job type {job.job_type}")


def _log_job_claimed(conn, job, logger: logging.Logger) -> None:
    fields = {"job_id": job.id}
    fields.update(_job_context_fields(conn, job))
    log_event(logger, logging.INFO, "job_claimed", **fields)


def _job_context_fields(conn, job) -> dict[str, object]:
    base = {"job_type": job.job_type}
    if job.job_type in {"write_article_markdown", "fetch_article_content", "summarize_article_llm"}:
        payload = job.payload or {}
        source_id = str(payload.get("source_id") or "")
        source_name = get_source_name(conn, source_id) or ""
        article_url = payload.get("original_url")
        article_id = payload.get("article_id")
        if not article_url and article_id:
            article = get_article_by_id(conn, int(article_id))
            if article:
                article_url = article.get("original_url") or article.get("normalized_url")
        return {
            **base,
            "source_id": source_id,
            "source_name": source_name,
            "article_id": article_id,
            "article_url": article_url,
        }
    if job.job_type in {"ingest_source", "test_source"}:
        payload = job.payload or {}
        source_id = str(payload.get("source_id") or "")
        source_name = get_source_name(conn, source_id) or ""
        return {**base, "source_id": source_id, "source_name": source_name}
    payload = job.payload or {}
    source_id = str(payload.get("source_id") or "")
    source_name = get_source_name(conn, source_id) or ""
    return {**base, "source_id": source_id, "source_name": source_name}


def _maybe_enqueue_fetch(
    conn, config, article_id: int, source_id: str, logger: logging.Logger
) -> None:
    if os.environ.get("SV_FETCH_FULL_CONTENT", "1") != "1":
        if _maybe_enqueue_summarize(conn, article_id, source_id, logger):
            return
        _enqueue_write_from_article(conn, config, article_id, source_id)
        return
    article = get_article_by_id(conn, article_id)
    if not article:
        return
    content_error = str(article.get("content_error") or "")
    if _is_terminal_content_error(content_error):
        return
    if not ( article.get("original_url") or article.get("normalized_url")):
        return
    if article["has_full_content"]:
        if _maybe_enqueue_summarize(conn, article_id, source_id, logger):
            return
        _enqueue_write_from_article(conn, config, article_id, source_id)
        return
    if has_pending_article_job(conn, "fetch_article_content", article_id):
        return
    attempts = count_failed_article_jobs(conn, "fetch_article_content", article_id)
    backoff = [30, 120, 600]
    if attempts >= len(backoff):
        update_article_content(
            conn,
            article_id,
            content_text=None,
            content_html=None,
            content_fetched_at=article.get("content_fetched_at"),
            content_error="max_retries_exceeded",
            has_full_content=False,
        )
        _enqueue_write_from_article(conn, config, article_id, source_id)
        return
    payload = {"article_id": article_id, "source_id": source_id}
    if attempts > 0:
        delay = backoff[min(attempts - 1, len(backoff) - 1)]
        payload["not_before"] = utc_now_iso_offset(seconds=delay)
    priority = _article_priority(article)
    enqueue_job(conn, "fetch_article_content", payload, priority=priority)


def _is_terminal_content_error(content_error: str) -> bool:
    value = str(content_error or "").strip()
    if not value:
        return False
    if value in {"http_404", "http_410", "stale_older_than_week", "max_retries_exceeded"}:
        return True
    return (
        value.startswith("fetch_failed:HTTP Error 30")
        or value.startswith("fetch_failed:HTTP Error 401")
        or value.startswith("fetch_failed:HTTP Error 403")
    )


def _maybe_enqueue_summarize(
    conn, article_id: int, source_id: str, logger: logging.Logger
) -> bool:
    profile, reason = get_active_profile_for_stage(conn, "summarize_article")
    if not profile:
        log_event(
            logger,
            logging.INFO,
            "llm_stage_skipped",
            stage="summarize_article",
            reason="no_profile_routed",
            detail=reason,
            article_id=article_id,
            source_id=source_id,
        )
        return False
    article = get_article_by_id(conn, article_id)
    if not article:
        return False
    if article.get("summary_llm"):
        return False
    priority = _article_priority(article)
    enqueue_job(
        conn,
        "summarize_article_llm",
        {"article_id": article_id, "source_id": source_id, "profile_id": profile.get("id")},
        priority=priority,
    )
    return True


def _maybe_enqueue_context_pack(
    conn, article_id: int, source_id: str, logger: logging.Logger
) -> bool:
    profile, reason = get_active_profile_for_stage(conn, "article_context_pack")
    if not profile:
        log_event(
            logger,
            logging.INFO,
            "llm_stage_skipped",
            stage="article_context_pack",
            reason="no_profile_routed",
            detail=reason,
            article_id=article_id,
            source_id=source_id,
        )
        return False
    article = get_article_by_id(conn, article_id)
    if not article:
        return False
    if article.get("context_llm"):
        return False
    priority = _article_priority(article)
    enqueue_job(
        conn,
        "summarize_article_context_llm",
        {"article_id": article_id, "source_id": source_id, "profile_id": profile.get("id")},
        priority=priority,
    )
    return True


def _maybe_enqueue_article_product_enrich(
    conn, article_id: int, source_id: str | None, logger: logging.Logger
) -> bool:
    profile, reason = get_active_profile_for_stage(conn, "article_enrich_products")
    if not profile:
        return False
    if count_products_for_article(conn, article_id) > 0:
        return False
    if has_pending_article_job(conn, "article_enrich_products", article_id):
        return False
    article = get_article_by_id(conn, article_id) or {}
    priority = _article_priority(article) if article else 0
    job_id = enqueue_job(
        conn,
        "article_enrich_products",
        {"article_id": int(article_id)},
        priority=priority,
    )
    log_event(
        logger,
        logging.INFO,
        "article_enrich_products_enqueued",
        job_id=job_id,
        article_id=article_id,
        source_id=source_id,
    )
    return True


def _enqueue_write_from_article(conn, config, article_id: int, source_id: str) -> None:
    if not is_article_markdown_enabled():
        return
    article = get_article_by_id(conn, article_id)
    if not article:
        return
    stable_id = article.get("stable_id")
    if not stable_id:
        return
    summary_text = article.get("summary") or ""
    summary_llm = article.get("summary_llm")
    if summary_llm:
        try:
            parsed = json.loads(summary_llm)
            if isinstance(parsed, dict) and parsed.get("summary"):
                summary_text = parsed.get("summary") or summary_text
        except json.JSONDecodeError:
            summary_text = summary_llm
    payload = {
        "article_id": article_id,
        "stable_id": stable_id,
        "title": article.get("title"),
        "source_id": source_id,
        "published_at": article.get("published_at"),
        "published_at_source": article.get("published_at_source"),
        "ingested_at": article.get("ingested_at"),
        "summary": summary_text or None,
        "tags": get_article_tags(conn, article_id),
        "original_url": article.get("original_url"),
        "normalized_url": article.get("normalized_url"),
    }
    if ( 
        config.personalization.watchlist_enabled
        and config.personalization.watchlist_exposure_mode == "public_highlights"
    ):
        hit = compute_watchlist_hits(
            conn,
            item_type="article",
            item_key=article_id,
            min_cvss=config.scope.min_cvss,
        )
        if hit.get("hit"):
            payload["watchlist_hit"] = True
    priority = _article_priority(article)
    enqueue_job(conn, "write_article_markdown", payload, priority=priority)


if __name__ == "__main__":
    raise SystemExit(main())


def _handle_cve_description_fill(conn, api_key: str | None) -> dict:
    page = 1
    page_size = 2000
    max_total = 5000
    total_missing = 0
    attempted = 0
    updated = 0
    failed = 0
    while True:
        items, total = search_cves(
            conn,
            query=None,
            severities=None,
            min_cvss=None,
            missing_description=True,
            missing_products=None,
            kev=None,
            after=None,
            before=None,
            vendor_keywords=None,
            product_keywords=None,
            in_scope=None,
            settings=None,
            page=page,
            page_size=page_size,
        )
        if page == 1:
            total_missing = total
        if not items:
            break
        for item in items:
            cve_id = item.get("cve_id") if isinstance(item, dict) else None
            if not cve_id:
                continue
            attempted += 1
            try:
                if sync_cve_id(conn, api_key, cve_id):
                    updated += 1
            except Exception:
                failed += 1
            if attempted >= max_total:
                break
            time.sleep(0.8)
        if attempted >= max_total:
            break
        if page * page_size >= total:
            break
        page += 1
    return {
        "status": "ok",
        "total_missing": total_missing,
        "attempted": attempted,
        "updated": updated,
        "failed": failed,
        "mode": "cve_description",
    }
