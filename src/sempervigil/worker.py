from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone, timedelta

from .config import (
    ConfigError,
    bootstrap_cve_settings,
    get_cve_settings,
    get_state_db_path,
    load_runtime_config,
)
from .ingest import process_source
from .models import Article
from .cve_sync import CveSyncConfig, isoformat_utc, sync_cves
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .publish import write_article_markdown, write_json_index
from .signals import build_cve_evidence, extract_cve_ids
from .pipelines.content_fetch import fetch_article_content
from .pipelines.daily_brief import write_daily_brief
from .pipelines.summarize_llm import summarize_with_llm
from .storage import (
    claim_next_job,
    complete_job,
    enqueue_job,
    fail_job,
    get_source,
    get_setting,
    get_article_id,
    get_article_by_id,
    get_batch_job_counts,
    init_db,
    insert_articles,
    list_due_sources,
    list_summaries_for_day,
    has_pending_job,
    pause_source,
    record_health_alert,
    record_source_run,
    upsert_cve_links,
    get_source_run_streaks,
    get_source_name,
    insert_source_health_event,
    update_article_content,
    update_article_summary,
)
from .utils import configure_logging, log_event, utc_now_iso

WORKER_JOB_TYPES = [
    "ingest_source",
    "ingest_due_sources",
    "test_source",
    "cve_sync",
    "fetch_article_content",
    "summarize_article_llm",
    "build_daily_brief",
    "write_article_markdown",
]


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.worker")


def run_once(worker_id: str) -> int:
    logger = _setup_logging()
    try:
        conn = init_db(get_state_db_path())
        config = load_runtime_config(conn)
        bootstrap_cve_settings(conn)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))
    _maybe_enqueue_cve_sync(conn, logger)
    job = claim_next_job(
        conn,
        worker_id,
        allowed_types=WORKER_JOB_TYPES,
        lock_timeout_seconds=config.jobs.lock_timeout_seconds,
    )
    if not job:
        return 0

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
        return 1

    if complete_job(conn, job.id, result=result):
        fields = _job_context_fields(conn, job)
        log_event(logger, logging.INFO, "job_succeeded", job_id=job.id, **fields)
    else:
        log_event(logger, logging.ERROR, "job_complete_failed", job_id=job.id)
    return 0


def run_loop(worker_id: str, sleep_seconds: int) -> int:
    while True:
        run_once(worker_id)
        time.sleep(sleep_seconds)


def _handle_ingest_source(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
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

    insert_articles(conn, result.articles)
    batch_id = str(uuid.uuid4())
    batch_total = len(result.articles)
    if batch_total:
        log_event(
            logger,
            logging.INFO,
            "batch_start",
            job_type="write_article_markdown",
            batch_id=batch_id,
            total=batch_total,
            source_id=source.id,
            source_name=source.name,
        )
    for index, article in enumerate(result.articles, start=1):
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
            _maybe_enqueue_fetch(conn, article_id, article.source_id)
        enqueue_job(
            conn,
            "write_article_markdown",
            {
                "article_id": article_id,
                "stable_id": article.stable_id,
                "title": article.title,
                "source_id": article.source_id,
                "published_at": article.published_at,
                "published_at_source": article.published_at_source,
                "ingested_at": article.ingested_at,
                "summary": article.summary,
                "tags": article.tags,
                "original_url": article.original_url,
                "normalized_url": article.normalized_url,
                "batch_id": batch_id,
                "batch_total": batch_total,
                "batch_index": index,
            },
        )
    # TODO: attach event correlation hooks (events/event_mentions) once implemented.
    if config.publishing.write_json_index:
        write_json_index(result.articles, config.publishing.json_index_path)
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
    path = write_article_markdown(article, config.paths.output_dir)
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
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    article_id = payload.get("article_id") if payload else None
    if not article_id:
        raise ValueError("fetch_article_content requires article_id")
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise ValueError("article_not_found")
    url = article["original_url"]
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
            content_error=str(exc),
            has_full_content=False,
        )
        raise
    _maybe_enqueue_summarize(conn, int(article_id), article["source_id"])
    return {"article_id": article_id, "has_full_content": has_full_content}


def _handle_summarize_article_llm(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    article_id = payload.get("article_id") if payload else None
    if not article_id:
        raise ValueError("summarize_article_llm requires article_id")
    article = get_article_by_id(conn, int(article_id))
    if not article:
        raise ValueError("article_not_found")
    source_name = get_source_name(conn, article["source_id"]) or ""
    content = article["content_text"] or article["summary"] or article["title"]
    try:
        result = summarize_with_llm(
            title=article["title"],
            source=source_name,
            published_at=article["published_at"],
            url=article["original_url"],
            content=content,
            logger=logger,
        )
        update_article_summary(
            conn,
            int(article_id),
            summary_llm=json.dumps(result),
            summary_model=result.get("model"),
            summary_generated_at=utc_now_iso(),
            summary_error=None,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        update_article_summary(
            conn,
            int(article_id),
            summary_llm=None,
            summary_model=None,
            summary_generated_at=utc_now_iso(),
            summary_error=str(exc),
        )
        raise


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
    return {"day": day, "count": len(items), **result}


def _handle_ingest_due_sources(conn, logger: logging.Logger) -> dict[str, object]:
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


def _handle_cve_sync(conn, logger: logging.Logger) -> dict[str, object]:
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
    result = sync_cves(
        conn,
        CveSyncConfig(
            api_base=str(nvd.get("api_base") or "https://services.nvd.nist.gov/rest/json/cves/2.0"),
            results_per_page=int(nvd.get("results_per_page") or 2000),
            rate_limit_seconds=float(settings.get("rate_limit_seconds", 1.0)),
            backoff_seconds=float(settings.get("backoff_seconds", 2.0)),
            max_retries=int(settings.get("max_retries", 3)),
            prefer_v4=bool(settings.get("prefer_v4", True)),
            api_key=api_key,
            filters=settings.get("filters") or {},
        ),
        last_modified_start=start_iso,
        last_modified_end=end_iso,
    )
    result["start"] = start_iso
    result["end"] = end_iso
    return result


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil-worker")
    parser.add_argument("--once", action="store_true", help="Run a single job and exit")
    parser.add_argument("--sleep", type=int, default=10, help="Sleep seconds between polls")
    parser.add_argument("--worker-id", default=os.environ.get("HOSTNAME", "worker"))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.once:
        return run_once(args.worker_id)
    return run_loop(args.worker_id, args.sleep)


def run_claimed_job(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    _log_job_claimed(conn, job, logger)
    if job.job_type == "ingest_source":
        return _handle_ingest_source(conn, config, job.payload, logger)
    if job.job_type == "ingest_due_sources":
        return _handle_ingest_due_sources(conn, logger)
    if job.job_type == "test_source":
        return _handle_test_source(conn, config, job.payload, logger)
    if job.job_type == "cve_sync":
        return _handle_cve_sync(conn, logger)
    if job.job_type == "fetch_article_content":
        return _handle_fetch_article_content(conn, config, job.payload, logger)
    if job.job_type == "summarize_article_llm":
        return _handle_summarize_article_llm(conn, config, job.payload, logger)
    if job.job_type == "build_daily_brief":
        return _handle_build_daily_brief(conn, config, job.payload, logger)
    if job.job_type == "write_article_markdown":
        result = _handle_write_article_markdown(conn, config, job.payload, logger)
        if not has_pending_job(conn, "write_article_markdown", exclude_job_id=job.id):
            enqueue_job(conn, "build_site", None, debounce=True)
        return result
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
        return {
            "source_id": source_id,
            "source_name": source_name,
            "article_id": payload.get("article_id"),
            "article_url": payload.get("original_url"),
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


def _maybe_enqueue_fetch(conn, article_id: int, source_id: str) -> None:
    if os.environ.get("SV_FETCH_FULL_CONTENT", "1") != "1":
        _maybe_enqueue_summarize(conn, article_id, source_id)
        return
    article = get_article_by_id(conn, article_id)
    if not article:
        return
    if article["has_full_content"]:
        _maybe_enqueue_summarize(conn, article_id, source_id)
        return
    if article["content_fetched_at"]:
        return
    enqueue_job(
        conn,
        "fetch_article_content",
        {"article_id": article_id, "source_id": source_id},
    )


def _maybe_enqueue_summarize(conn, article_id: int, source_id: str) -> None:
    article = get_article_by_id(conn, article_id)
    if not article:
        return
    if article.get("summary_llm"):
        return
    enqueue_job(
        conn,
        "summarize_article_llm",
        {"article_id": article_id, "source_id": source_id},
    )


if __name__ == "__main__":
    raise SystemExit(main())
