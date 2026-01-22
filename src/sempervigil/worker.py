from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timezone, timedelta

from .config import ConfigError, load_config
from .ingest import process_source
from .models import Article
from .cve_sync import CveSyncConfig, isoformat_utc, sync_cves
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .publish import write_article_markdown, write_hugo_markdown, write_json_index, write_tag_indexes
from .signals import build_cve_evidence, extract_cve_ids
from .storage import (
    claim_next_job,
    complete_job,
    enqueue_job,
    fail_job,
    get_source,
    get_setting,
    get_article_id,
    init_db,
    insert_articles,
    list_due_sources,
    pause_source,
    record_health_alert,
    record_source_run,
    upsert_cve_links,
    get_source_run_streaks,
)
from .utils import log_event, utc_now_iso

WORKER_JOB_TYPES = [
    "ingest_source",
    "ingest_due_sources",
    "test_source",
    "cve_sync",
    "write_article_markdown",
]


def _setup_logging() -> logging.Logger:
    level_name = os.environ.get("SV_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("sempervigil")


def run_once(config_path: str | None, worker_id: str) -> int:
    logger = _setup_logging()
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))
    conn = init_db(config.paths.state_db)
    _maybe_enqueue_cve_sync(conn, config, logger)
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
        log_event(logger, logging.ERROR, "job_failed", job_id=job.id, error=str(exc))
        return 1

    if complete_job(conn, job.id, result=result):
        log_event(logger, logging.INFO, "job_succeeded", job_id=job.id)
    else:
        log_event(logger, logging.ERROR, "job_complete_failed", job_id=job.id)
    return 0


def run_loop(config_path: str | None, worker_id: str, sleep_seconds: int) -> int:
    while True:
        run_once(config_path, worker_id)
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

    if result.status != "ok":
        _maybe_pause_source(conn, source.id, logger)
        return {
            "source_id": source.id,
            "status": result.status,
            "error": result.error,
            "found_count": result.found_count,
            "accepted_count": result.accepted_count,
        }

    insert_articles(conn, result.articles)
    for article in result.articles:
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
        enqueue_job(
            conn,
            "write_article_markdown",
            {
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


def _handle_write_article_markdown(
    conn, config, payload: dict[str, object], logger: logging.Logger
) -> dict[str, object]:
    if not payload:
        raise ValueError("write_article_markdown requires payload")
    article = Article(
        id=None,
        stable_id=str(payload.get("stable_id")),
        original_url=str(payload.get("original_url")),
        normalized_url=str(payload.get("normalized_url")),
        title=str(payload.get("title")),
        source_id=str(payload.get("source_id")),
        published_at=payload.get("published_at") or None,
        published_at_source=payload.get("published_at_source") or None,
        ingested_at=str(payload.get("ingested_at")),
        summary=payload.get("summary") or None,
        tags=list(payload.get("tags") or []),
    )
    path = write_article_markdown(article, config.paths.output_dir)
    log_event(logger, logging.INFO, "article_markdown_written", path=path)
    return {"path": path}


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


def _handle_cve_sync(conn, config, logger: logging.Logger) -> dict[str, object]:
    if not config.cve.enabled:
        return {"status": "disabled"}
    now = datetime.now(tz=timezone.utc)
    last_sync = get_setting(conn, "cve.last_successful_sync_at", None)
    start = _parse_iso(last_sync) if isinstance(last_sync, str) else None
    if not start:
        start = now - timedelta(minutes=config.cve.sync_interval_minutes)
    start_iso = isoformat_utc(start)
    end_iso = isoformat_utc(now)
    api_key = os.environ.get("NVD_API_KEY")
    result = sync_cves(
        conn,
        CveSyncConfig(
            results_per_page=config.cve.results_per_page,
            rate_limit_seconds=config.cve.rate_limit_seconds,
            backoff_seconds=config.cve.backoff_seconds,
            max_retries=config.cve.max_retries,
            prefer_v4=config.cve.prefer_v4,
            api_key=api_key,
        ),
        last_modified_start=start_iso,
        last_modified_end=end_iso,
    )
    result["start"] = start_iso
    result["end"] = end_iso
    return result


def _maybe_enqueue_cve_sync(conn, config, logger: logging.Logger) -> None:
    if not config.cve.enabled:
        return
    last_sync = get_setting(conn, "cve.last_successful_sync_at", None)
    now = datetime.now(tz=timezone.utc)
    if isinstance(last_sync, str):
        last_dt = _parse_iso(last_sync)
    else:
        last_dt = now - timedelta(minutes=config.cve.sync_interval_minutes + 1)
    due = last_dt + timedelta(minutes=config.cve.sync_interval_minutes) <= now
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
    parser.add_argument("--config", dest="config", default=None)
    parser.add_argument("--once", action="store_true", help="Run a single job and exit")
    parser.add_argument("--sleep", type=int, default=10, help="Sleep seconds between polls")
    parser.add_argument("--worker-id", default=os.environ.get("HOSTNAME", "worker"))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.once:
        return run_once(args.config, args.worker_id)
    return run_loop(args.config, args.worker_id, args.sleep)


def run_claimed_job(conn, config, job, logger: logging.Logger) -> dict[str, object]:
    log_event(
        logger,
        logging.INFO,
        "job_claimed",
        job_id=job.id,
        job_type=job.job_type,
    )
    if job.job_type == "ingest_source":
        return _handle_ingest_source(conn, config, job.payload, logger)
    if job.job_type == "ingest_due_sources":
        return _handle_ingest_due_sources(conn, logger)
    if job.job_type == "test_source":
        return _handle_test_source(conn, config, job.payload, logger)
    if job.job_type == "cve_sync":
        return _handle_cve_sync(conn, config, logger)
    if job.job_type == "write_article_markdown":
        return _handle_write_article_markdown(conn, config, job.payload, logger)
    raise ValueError(f"unsupported job type {job.job_type}")


if __name__ == "__main__":
    raise SystemExit(main())
