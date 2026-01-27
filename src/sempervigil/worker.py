from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from .config import (
    ConfigError,
    bootstrap_cve_settings,
    bootstrap_events_settings,
    get_cve_settings,
    get_events_settings,
    load_runtime_config,
)
from .ingest import process_source
from .models import Article, Job
from .cve_sync import CveSyncConfig, isoformat_utc, sync_cves
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .publish import write_article_markdown, write_events_index, write_events_markdown, write_json_index
from .signals import build_cve_evidence, extract_cve_ids
from .pipelines.content_fetch import fetch_article_content
from .pipelines.daily_brief import write_daily_brief
from .llm.router import run_profile
from .services.ai_service import get_active_profile_for_stage
from .normalize import normalize_name
from .storage import (
    claim_next_job,
    complete_job,
    enqueue_job,
    enqueue_build_site_if_needed,
    fail_job,
    get_source,
    list_sources,
    get_setting,
    set_setting,
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
    list_summaries_for_day,
    list_jobs_by_types_since,
    requeue_job,
    has_pending_job,
    insert_llm_run,
    pause_source,
    record_health_alert,
    record_source_run,
    rebuild_events_from_cves,
    upsert_cve_links,
    upsert_event_for_cve,
    upsert_event_by_key,
    upsert_event_item,
    list_product_keys_for_cve,
    list_article_cve_ids,
    list_event_ids_for_article,
    link_event_article,
    get_source_run_streaks,
    get_source_name,
    insert_source_health_event,
    update_article_content,
    update_article_summary,
    update_job_result,
    list_article_ids_missing_content,
    list_article_ids_missing_summary,
    list_article_ids_for_source_since,
    compute_watchlist_hits,
    try_acquire_lease,
    release_lease,
    update_event_summary_from_articles,
)
from .utils import configure_logging, log_event, utc_now_iso, utc_now_iso_offset

WORKER_JOB_TYPES = [
    "ingest_source",
    "ingest_due_sources",
    "test_source",
    "cve_sync",
    "events_rebuild",
    "fetch_article_content",
    "summarize_article_llm",
    "build_daily_brief",
    "write_article_markdown",
    "derive_events_from_articles",
    "source_acquire",
    "smoke_test",
]


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.worker")


def run_once(worker_id: str, allowed_types: list[str] | None = None) -> int:
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
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))
    if _should_tick_ingest_due(allowed_types):
        _maybe_enqueue_ingest_due_sources(conn, logger)
    _maybe_enqueue_cve_sync(conn, logger)
    job = claim_next_job(
        conn,
        worker_id,
        allowed_types=allowed_types or WORKER_JOB_TYPES,
        lock_timeout_seconds=config.jobs.lock_timeout_seconds,
    )
    if not job:
        return 0
    return _process_claimed_job(conn, config, job, logger)


def _process_claimed_job(conn, config, job, logger: logging.Logger) -> int:
    if is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "job_canceled", job_id=job.id)
        return 0

    try:
        result = run_claimed_job(conn, config, job, logger)
    except Exception as exc:  # noqa: BLE001
        if is_job_canceled(conn, job.id):
            log_event(logger, logging.INFO, "job_canceled", job_id=job.id)
            return 0
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
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))
    return _process_claimed_job(conn, config, job, logger)


def run_loop(
    worker_id: str,
    sleep_seconds: int,
    allowed_types: list[str] | None = None,
    concurrency: int = 1,
) -> int:
    if concurrency <= 1:
        while True:
            run_once(worker_id, allowed_types)
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
                set_umask_from_env()
                ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))
                if _should_tick_ingest_due(allowed_types):
                    _maybe_enqueue_ingest_due_sources(conn, logger)
                _maybe_enqueue_cve_sync(conn, logger)
                job = claim_next_job(
                    conn,
                    worker_id,
                    allowed_types=allowed_types or WORKER_JOB_TYPES,
                    lock_timeout_seconds=config.jobs.lock_timeout_seconds,
                )
                conn.close()
                if not job:
                    break
                futures.add(executor.submit(_process_claimed_job_thread, worker_id, job))
            if futures:
                done, futures = wait(futures, timeout=sleep_seconds, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        future.result()
                    except Exception as exc:  # noqa: BLE001
                        log_event(logger, logging.ERROR, "job_thread_error", error=str(exc))
            else:
                time.sleep(sleep_seconds)


def _handle_ingest_source(
    conn,
    config,
    payload: dict[str, object],
    logger: logging.Logger,
    job_id: str | None = None,
) -> dict[str, object]:
    # Pipeline order: ingest creates article stubs first, then enqueues
    # fetch_article_content (if enabled + URL present). Summarization runs
    # after fetch if configured, and publish runs after summarize or fetch.
    source_id = payload.get("source_id") if payload else None
    if not source_id:
        raise ValueError("ingest_source requires source_id")
    source = get_source(conn, str(source_id))
    if source is None:
        raise ValueError(f"Source not found: {source_id}")

    started_at = utc_now_iso()
    now_dt = _parse_iso(started_at)
    if not source.enabled or (source.pause_until and _parse_iso(source.pause_until) > now_dt):
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
        (datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)).total_seconds()
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
        return {"canceled": True}

    insert_articles(conn, result.articles)
    for article in result.articles:
        if job_id and is_job_canceled(conn, job_id):
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
        if article_id is None:
            article_id = get_article_id(conn, article.source_id, article.stable_id)
        if article_id is not None:
            _maybe_enqueue_fetch(conn, config, article_id, article.source_id, logger)
        events_settings = get_events_settings(conn)
        if events_settings.get("enabled", True) and cve_ids and article_id is not None:
            window_days = int(events_settings.get("merge_window_days", 14))
            min_shared = int(events_settings.get("min_shared_products_to_merge", 1))
            for cve_id in cve_ids:
                upsert_event_for_cve(
                    conn,
                    cve_id=cve_id,
                    published_at=article.published_at or article.ingested_at,
                    window_days=window_days,
                    min_shared_products=min_shared,
                )
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
        enqueue_build_site_if_needed(conn, reason="json_index_written")
    _maybe_pause_source(conn, source.id, logger)
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
    article_id = payload.get("article_id")
    if article_id is not None:
        try:
            article_id_int = int(article_id)
        except (TypeError, ValueError):
            article_id_int = None
        if article_id_int is not None and not has_pending_article_job(
            conn, "derive_events_from_articles", article_id_int
        ):
            enqueue_job(conn, "derive_events_from_articles", {"article_id": article_id_int})
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
        result = fetch_article_content(
            url,
            timeout_seconds=config.ingest.http.timeout_seconds,
            user_agent=config.ingest.http.user_agent,
            logger=logger,
        )
        content_text = result["content_text"]
        store_html = os.environ.get("SV_STORE_ARTICLE_HTML", "0") == "1"
        content_html = result["content_html"] if store_html else None
        has_full_content = len(content_text or "") >= 500
        update_article_content(
            conn,
            int(article_id),
            content_text=content_text,
            content_html=content_html,
            content_fetched_at=utc_now_iso(),
            content_error=None,
            has_full_content=has_full_content,
        )
    except Exception as exc:  # noqa: BLE001
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
    if not _maybe_enqueue_summarize(conn, int(article_id), article["source_id"], logger):
        _enqueue_write_from_article(conn, config, int(article_id), article["source_id"])
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
        raise ValueError("article_not_found")
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
        result = run_profile(conn, profile["id"], input_text, logger)
        latency_ms = int((time.time() - start) * 1000)
        parsed = result.get("parsed")
        raw = result.get("raw") if isinstance(result, dict) else None
        if isinstance(parsed, (dict, list)):
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


def _handle_build_daily_brief(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    day = str(payload.get("date") or utc_now_iso().split("T")[0])
    items = list_summaries_for_day(conn, day)
    for item in items:
        item["source_name"] = get_source_name(conn, item["source_id"]) or ""
    base_content_dir = os.path.dirname(config.paths.output_dir)
    base_static_dir = os.path.dirname(config.publishing.json_index_path)
    result = write_daily_brief(
        base_content_dir=base_content_dir,
        base_static_dir=base_static_dir,
        day=day,
        items=items,
    )
    log_event(
        logger,
        logging.INFO,
        "daily_brief_written",
        day=day,
        count=len(items),
        markdown_path=result["markdown_path"],
        json_path=result["json_path"],
    )
    enqueue_build_site_if_needed(conn, reason="build_daily_brief")
    return {"day": day, "count": len(items), **result}


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

            _run_jobs_inline(
                conn,
                config,
                logger,
                allowed_types=["fetch_article_content", "summarize_article_llm", "write_article_markdown"],
                timeout_seconds=timeout_seconds,
            )
            jobs = list_jobs_by_types_since(
                conn,
                types=["fetch_article_content", "summarize_article_llm", "write_article_markdown"],
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
            result["build_exit_code"] = (build_job.result or {}).get("exit_code")
            update_job_result(conn, job.id, result)
    return result


def _extract_event_entity(title: str) -> str:
    if not title:
        return ""
    for sep in (":", " - ", " – ", " — "):
        if sep in title:
            return title.split(sep, 1)[0].strip()
    return title.strip().split(" ")[0]


def _derive_event_kind(text: str) -> str:
    lowered = text.lower()
    if any(word in lowered for word in ("ransomware", "extortion")):
        return "ransomware"
    if any(word in lowered for word in ("breach", "data leak", "leak")):
        return "breach"
    if any(word in lowered for word in ("compromise", "intrusion")):
        return "compromise"
    if any(word in lowered for word in ("exploit", "exploited", "zero-day", "0day")):
        return "exploit"
    if any(word in lowered for word in ("campaign", "operation")):
        return "campaign"
    if any(word in lowered for word in ("outage", "service disruption")):
        return "outage"
    if any(word in lowered for word in ("patch", "update", "advisory")):
        return "advisory"
    return "other"


def _handle_derive_events_from_articles(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    article_id = payload.get("article_id")
    if not isinstance(article_id, int):
        return {"status": "skipped", "reason": "missing_article_id"}
    if list_event_ids_for_article(conn, article_id):
        return {"status": "skipped", "reason": "already_linked"}
    article = get_article_by_id(conn, article_id)
    if not article:
        return {"status": "skipped", "reason": "article_missing"}
    title = str(article.get("title") or "")
    summary = str(article.get("summary") or "")
    content = str(article.get("content_text") or "")
    combined = " ".join(part for part in (title, summary, content) if part).strip()
    if not combined:
        return {"status": "skipped", "reason": "no_content"}
    kind = _derive_event_kind(combined)
    entity = _extract_event_entity(title) or article.get("source_id") or "unknown"
    bucket = (article.get("published_at") or article.get("ingested_at") or "")[:10]
    event_key = f"evt:{kind}:{normalize_name(str(entity))}:{bucket or 'unknown'}"
    event_id, _ = upsert_event_by_key(
        conn,
        event_key=event_key,
        kind=kind,
        title=title or f"Event: {entity}",
        severity="UNKNOWN",
        first_seen_at=article.get("published_at") or article.get("ingested_at") or utc_now_iso(),
        last_seen_at=utc_now_iso(),
        status="open",
        meta={"seed_article_id": article_id},
    )
    link_event_article(conn, event_id, article_id, "auto")
    cve_ids = list_article_cve_ids(conn, article_id)
    for cve_id in cve_ids:
        upsert_event_item(conn, event_id, "cve", cve_id)
        for product_key in list_product_keys_for_cve(conn, cve_id):
            upsert_event_item(conn, event_id, "product", product_key)
    update_event_summary_from_articles(conn, event_id)
    return {
        "status": "linked",
        "event_id": event_id,
        "cves": len(cve_ids),
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

    ingest_payload: dict[str, object] = {"source_id": source.id}
    if isinstance(limit, int):
        ingest_payload["limit"] = limit
    ingest_job_id = enqueue_job(conn, "ingest_source", ingest_payload)
    result["ingest_job_id"] = ingest_job_id
    _run_jobs_inline(
        conn,
        config,
        logger,
        allowed_types=["ingest_source"],
        timeout_seconds=timeout_seconds,
    )
    ingest_job = get_job(conn, ingest_job_id)
    result["counts"]["ingested"] = int((ingest_job.result or {}).get("accepted_count") or 0) if ingest_job else 0

    missing_content_ids = list_article_ids_missing_content(conn, source.id)
    for article_id in missing_content_ids:
        _maybe_enqueue_fetch(conn, config, article_id, source.id, logger)
    _run_jobs_inline(
        conn,
        config,
        logger,
        allowed_types=["fetch_article_content"],
        timeout_seconds=timeout_seconds,
    )

    missing_summary_ids = list_article_ids_missing_summary(conn, source.id)
    for article_id in missing_summary_ids:
        _maybe_enqueue_summarize(conn, article_id, source.id, logger)
    _run_jobs_inline(
        conn,
        config,
        logger,
        allowed_types=["summarize_article_llm"],
        timeout_seconds=timeout_seconds,
    )

    new_article_ids = list_article_ids_for_source_since(conn, source.id, start_marker)
    publish_ids = sorted(set(new_article_ids + missing_content_ids + missing_summary_ids))
    for article_id in publish_ids:
        _enqueue_write_from_article(conn, config, article_id, source.id)
    _run_jobs_inline(
        conn,
        config,
        logger,
        allowed_types=["write_article_markdown"],
        timeout_seconds=timeout_seconds,
    )

    jobs = list_jobs_by_types_since(
        conn,
        types=["fetch_article_content", "summarize_article_llm", "write_article_markdown"],
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
    result["counts"]["markdown_ok"] = sum(
        1 for j in jobs if j.job_type == "write_article_markdown" and j.status == "succeeded"
    )
    result["counts"]["markdown_failed"] = sum(
        1 for j in jobs if j.job_type == "write_article_markdown" and j.status == "failed"
    )

    if also_events:
        events_job_id = enqueue_job(conn, "events_rebuild", None)
        _run_jobs_inline(
            conn,
            config,
            logger,
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
            result["build_exit_code"] = (build_job.result or {}).get("exit_code")
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
        enqueue_job(conn, "ingest_source", {"source_id": source.id})
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
    if payload and payload.get("cve_id"):
        cve_id = str(payload.get("cve_id"))
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
    events_settings = get_events_settings(conn)
    if events_settings.get("enabled", True):
        _publish_events(conn, config, logger)
    return result


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
    enqueue_build_site_if_needed(conn, reason="events_rebuild")
    return stats


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


def _should_tick_ingest_due(allowed_types: list[str] | None) -> bool:
    if not allowed_types:
        return True
    if "ingest_due_sources" in allowed_types:
        return True
    return any(
        job_type in allowed_types
        for job_type in ("ingest_source", "html_index", "rss_index")
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
    zero_threshold = int(
        get_setting(conn, "alerts.pause_on_failure.zero_streak", error_threshold)
    )
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
    elif streaks["consecutive_zero"] >= zero_threshold:
        reason = f"auto_pause:zero_streak:{streaks['consecutive_zero']}"
        pause_source(conn, source_id, reason, pause_minutes)
        record_health_alert(conn, source_id, "zero_streak", reason)
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


def _parse_only_types(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil-worker")
    parser.add_argument("--once", action="store_true", help="Run a single job and exit")
    parser.add_argument("--sleep", type=int, default=10, help="Sleep seconds between polls")
    parser.add_argument("--worker-id", default=os.environ.get("HOSTNAME", "worker"))
    parser.add_argument("--only-job-types", default=os.environ.get("SV_WORKER_ONLY_TYPES", ""))
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.environ.get("SV_WORKER_CONCURRENCY", "1")),
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    allowed_types = _parse_only_types(args.only_job_types)
    if args.once:
        return run_once(args.worker_id, allowed_types)
    return run_loop(args.worker_id, args.sleep, allowed_types, args.concurrency)


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
    if job.job_type == "events_rebuild":
        return _handle_events_rebuild(conn, config, job.payload or {}, logger)
    if job.job_type == "source_acquire":
        return _handle_source_acquire(conn, config, job, logger)
    if job.job_type == "fetch_article_content":
        return _handle_fetch_article_content(conn, config, job, job.payload, logger)
    if job.job_type == "summarize_article_llm":
        return _handle_summarize_article_llm(conn, config, job, logger)
    if job.job_type == "build_daily_brief":
        return _handle_build_daily_brief(conn, config, job.payload, logger)
    if job.job_type == "write_article_markdown":
        result = _handle_write_article_markdown(conn, config, job.payload, logger)
        if not has_pending_job(conn, "write_article_markdown", exclude_job_id=job.id):
            enqueue_build_site_if_needed(conn, reason="write_article_markdown")
        return result
    if job.job_type == "derive_events_from_articles":
        return _handle_derive_events_from_articles(conn, config, job.payload or {}, logger)
    if job.job_type == "smoke_test":
        return _handle_smoke_test(conn, config, job, logger)
    raise ValueError(f"unsupported job type {job.job_type}")


def _log_job_claimed(conn, job, logger: logging.Logger) -> None:
    fields = {"job_id": job.id, "job_type": job.job_type}
    fields.update(_job_context_fields(conn, job))
    log_event(logger, logging.INFO, "job_claimed", **fields)


def _job_context_fields(conn, job) -> dict[str, object]:
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
            "source_id": source_id,
            "source_name": source_name,
            "article_id": article_id,
            "article_url": article_url,
        }
    if job.job_type in {"ingest_source", "test_source"}:
        payload = job.payload or {}
        source_id = str(payload.get("source_id") or "")
        source_name = get_source_name(conn, source_id) or ""
        return {"source_id": source_id, "source_name": source_name}
    payload = job.payload or {}
    source_id = str(payload.get("source_id") or "")
    source_name = get_source_name(conn, source_id) or ""
    return {"source_id": source_id, "source_name": source_name}


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
    if not (article.get("original_url") or article.get("normalized_url")):
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
    enqueue_job(conn, "fetch_article_content", payload)


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
    enqueue_job(
        conn,
        "summarize_article_llm",
        {"article_id": article_id, "source_id": source_id, "profile_id": profile.get("id")},
    )
    return True


def _enqueue_write_from_article(conn, config, article_id: int, source_id: str) -> None:
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
    enqueue_job(conn, "write_article_markdown", payload)


if __name__ == "__main__":
    raise SystemExit(main())
