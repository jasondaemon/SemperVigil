from __future__ import annotations

import argparse
import logging
import os
import time

from .config import ConfigError, load_config
from .ingest import process_source
from .publish import write_hugo_markdown, write_json_index, write_tag_indexes
from .storage import (
    claim_next_job,
    complete_job,
    enqueue_job,
    fail_job,
    get_source,
    init_db,
    insert_articles,
    list_due_sources,
    record_source_run,
)
from .utils import log_event, utc_now_iso

WORKER_JOB_TYPES = ["ingest_source", "ingest_due_sources", "test_source"]


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

    conn = init_db(config.paths.state_db)
    job = claim_next_job(conn, worker_id, allowed_types=WORKER_JOB_TYPES)
    if not job:
        return 0

    try:
        log_event(
            logger,
            logging.INFO,
            "job_claimed",
            job_id=job.id,
            job_type=job.job_type,
        )
        if job.job_type == "ingest_source":
            result = _handle_ingest_source(conn, config, job.payload, logger)
        elif job.job_type == "ingest_due_sources":
            result = _handle_ingest_due_sources(conn, logger)
        elif job.job_type == "test_source":
            result = _handle_test_source(conn, config, job.payload, logger)
        else:
            fail_job(conn, job.id, f"unsupported job type {job.job_type}")
            return 1
    except Exception as exc:  # noqa: BLE001
        fail_job(conn, job.id, str(exc))
        log_event(logger, logging.ERROR, "job_failed", job_id=job.id, error=str(exc))
        return 1

    complete_job(conn, job.id, result=result)
    log_event(logger, logging.INFO, "job_succeeded", job_id=job.id)
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
        return {
            "source_id": source.id,
            "status": result.status,
            "error": result.error,
            "found_count": result.found_count,
            "accepted_count": result.accepted_count,
        }

    insert_articles(conn, result.articles)
    write_hugo_markdown(result.articles, config.paths.output_dir)

    if config.publishing.write_json_index:
        write_json_index(result.articles, config.publishing.json_index_path)

    write_tag_indexes(result.articles, config.paths.output_dir, config.publishing.hugo_section)
    enqueue_job(conn, "build_site", None, debounce=True)
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


if __name__ == "__main__":
    raise SystemExit(main())
