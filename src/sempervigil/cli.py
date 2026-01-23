from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .config import ConfigError, load_config, load_sources_file
from .cve_sync import CveSyncConfig, isoformat_utc, sync_cves
from .worker import WORKER_JOB_TYPES
from .ingest import process_source
from .models import SourceTactic
from .publish import write_hugo_markdown, write_json_index, write_tag_indexes
from .storage import (
    get_source,
    get_setting,
    init_db,
    insert_articles,
    list_sources,
    record_source_run,
    upsert_source,
    upsert_tactic,
    enqueue_job,
    list_jobs,
)
from .tagger import normalize_tag
from .utils import log_event, utc_now_iso


def _setup_logging() -> logging.Logger:
    level_name = os.environ.get("SV_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("sempervigil")


def _normalize_tag_list(values: list[str]) -> list[str]:
    return [normalize_tag(value) for value in values if value and normalize_tag(value)]


def _filter_articles_by_tags(articles, include_tags: list[str], exclude_tags: list[str]):
    include_set = set(include_tags)
    exclude_set = set(exclude_tags)
    if not include_set and not exclude_set:
        return list(articles)
    filtered = []
    for article in articles:
        tag_set = set(article.tags or [])
        if include_set and not tag_set.intersection(include_set):
            continue
        if exclude_set and tag_set.intersection(exclude_set):
            continue
        filtered.append(article)
    return filtered


def _write_run_report(report_dir: str, report: dict) -> str:
    Path(report_dir).mkdir(parents=True, exist_ok=True)
    timestamp = (
        report["run_started_at"]
        .replace(":", "")
        .replace("-", "")
        .replace("+", "")
        .replace("T", "")
    )
    filename = f"run-{timestamp}.json"
    path = os.path.join(report_dir, filename)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
    return path


def _load_latest_report(report_dir: str) -> dict | None:
    path = Path(report_dir)
    if not path.exists():
        return None
    reports = sorted(path.glob("run-*.json"), key=lambda p: p.stat().st_mtime)
    if not reports:
        return None
    with reports[-1].open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _cmd_run(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    run_started_at = utc_now_iso()

    sources_all = list_sources(conn, enabled_only=False)
    enabled_sources = [source for source in sources_all if source.enabled]
    if not sources_all:
        log_event(
            logger,
            logging.ERROR,
            "no_sources",
            hint="Import sources with `sempervigil sources import /config/sources.yml`",
        )
        return 1
    if not enabled_sources:
        log_event(logger, logging.ERROR, "no_enabled_sources")
        return 1

    totals = {
        "sources_total": len(sources_all),
        "sources_enabled": len(enabled_sources),
        "sources_ok": 0,
        "sources_error": 0,
        "items_found": 0,
        "items_accepted": 0,
        "items_written": 0,
        "skipped_duplicates": 0,
        "skipped_filters": 0,
        "skipped_missing_url": 0,
    }
    written_paths: list[str] = []
    all_articles = []
    output_articles = []
    include_tags = _normalize_tag_list(args.include_tag)
    exclude_tags = _normalize_tag_list(args.exclude_tag)

    for source in enabled_sources:
        source_started_at = utc_now_iso()
        result = process_source(source, config, logger, conn, test_mode=False)
        record_source_run(
            conn,
            source_id=source.id,
            started_at=source_started_at,
            finished_at=utc_now_iso(),
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

        totals["items_found"] += result.found_count
        totals["items_accepted"] += result.accepted_count
        totals["skipped_duplicates"] += result.skipped_duplicates
        totals["skipped_filters"] += result.skipped_filters
        totals["skipped_missing_url"] += result.skipped_missing_url

        if result.status != "ok":
            totals["sources_error"] += 1
            continue

        totals["sources_ok"] += 1
        all_articles.extend(result.articles)
        insert_articles(conn, result.articles)
        filtered = _filter_articles_by_tags(result.articles, include_tags, exclude_tags)
        output_articles.extend(filtered)
        written_paths.extend(write_hugo_markdown(filtered, config.paths.output_dir))

    totals["items_written"] = len(written_paths)

    if config.publishing.write_json_index:
        write_json_index(output_articles, config.publishing.json_index_path)

    write_tag_indexes(output_articles, config.paths.output_dir, config.publishing.hugo_section)

    run_report = {
        "run_started_at": run_started_at,
        "run_finished_at": utc_now_iso(),
        **totals,
    }
    report_path = _write_run_report(config.paths.run_reports_dir, run_report)

    log_event(logger, logging.INFO, "run_complete", report_path=report_path, **totals)
    return 0


def _cmd_test_source(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    source = get_source(conn, args.source_id)
    if source is None:
        if not list_sources(conn, enabled_only=False):
            log_event(
                logger,
                logging.ERROR,
                "no_sources",
                hint="Import sources with `sempervigil sources import /config/sources.yml`",
            )
        else:
            log_event(logger, logging.ERROR, "source_not_found", source_id=args.source_id)
        return 1
    result = process_source(
        source,
        config,
        logger,
        conn,
        test_mode=True,
        ignore_dedupe=args.ignore_dedupe,
    )

    log_event(
        logger,
        logging.INFO,
        "source_health",
        source_id=source.id,
        status=result.status,
        http_status=result.http_status,
        error=result.error,
    )

    if result.status != "ok":
        return 1

    _log_test_source_report(
        logger,
        source.id,
        result,
        args.limit,
        args.verbose,
        args.show_raw,
        args.ignore_dedupe,
    )
    return 0


def _log_test_source_report(
    logger: logging.Logger,
    source_id: str,
    result,
    limit: int,
    verbose: bool,
    show_raw: bool,
    ignore_dedupe: bool,
) -> None:
    logger.info("Test Source Report")
    logger.info("-" * 60)
    logger.info("Source: %s", source_id)
    logger.info("Status: %s", result.status)
    logger.info("HTTP: %s", result.http_status if result.http_status is not None else "n/a")
    logger.info("")
    logger.info("Counts")
    logger.info("-" * 60)
    logger.info("Found: %d", result.found_count)
    logger.info("Accepted: %d", result.accepted_count)
    logger.info("Skipped (duplicates): %d", result.skipped_duplicates)
    logger.info("Skipped (filters): %d", result.skipped_filters)
    logger.info("Skipped (missing url): %d", result.skipped_missing_url)
    if ignore_dedupe:
        logger.info("Already seen (duplicates ignored): %d", result.already_seen_count)
    logger.info("")
    logger.info("Preview (limit=%d)", limit)
    logger.info("-" * 60)

    source_counts: dict[str, int] = {}
    timestamp_counts: dict[str, int] = {}
    for decision in result.decisions:
        source_key = decision.published_at_source or "unknown"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if decision.published_at:
            timestamp_counts[decision.published_at] = timestamp_counts.get(decision.published_at, 0) + 1

    preview = result.decisions[:limit]
    for decision in preview:
        reasons = ", ".join(decision.reasons) if decision.reasons else "-"
        logger.info("[%s] %s", decision.decision, decision.title)
        logger.info("  reasons: %s", reasons)
        logger.info("  url: %s", decision.normalized_url or "n/a")
        logger.info("  published_at_source: %s", decision.published_at_source or "unknown")
        logger.info("  tags: %s", ", ".join(decision.tags) if decision.tags else "n/a")
        if verbose:
            logger.info("  original_url: %s", decision.original_url or "n/a")
            logger.info("  stable_id: %s", decision.stable_id or "n/a")
            logger.info("  published_at: %s", decision.published_at or "n/a")
    if show_raw:
        logger.info("")
        logger.info("Raw entry (first item)")
        logger.info("-" * 60)
        if result.raw_entry:
            logger.info("%s", json.dumps(result.raw_entry, indent=2, sort_keys=True))
        else:
            logger.info("n/a")

    logger.info("")
    logger.info("Published_at_source summary")
    logger.info("-" * 60)
    for key in sorted(source_counts):
        logger.info("%s: %d", key, source_counts[key])

    total = len(result.decisions)
    if total > 0 and timestamp_counts:
        max_count = max(timestamp_counts.values())
        if max_count / total >= 0.8:
            logger.warning(
                "Date warning: %d/%d items share the same published_at",
                max_count,
                total,
            )


def _cmd_report(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    report = _load_latest_report(config.paths.run_reports_dir)
    if not report:
        log_event(logger, logging.WARNING, "report_missing", path=config.paths.run_reports_dir)
        return 1

    log_event(logger, logging.INFO, "last_run_report", **report)
    return 0


def _cmd_sources_import(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    sources_path = args.path
    if sources_path is None:
        if os.path.exists("/config/sources.yml"):
            sources_path = "/config/sources.yml"
        elif os.path.exists("/config/sources.example.yml"):
            sources_path = "/config/sources.example.yml"
        else:
            log_event(
                logger,
                logging.ERROR,
                "sources_import_error",
                error="no sources.yml found",
                hint="Copy config/sources.example.yml to config/sources.yml",
            )
            return 1
    log_event(logger, logging.INFO, "sources_import_path", path=sources_path)
    try:
        sources = load_sources_file(sources_path)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "sources_import_error", error=str(exc))
        return 1
    if not sources:
        log_event(logger, logging.ERROR, "sources_import_error", error="no sources found")
        return 1

    for source in sources:
        try:
            source_id = source.get("id")
            source_dict = {
                "id": source_id,
                "name": source.get("name") or source_id,
                "enabled": source.get("enabled", True),
                "base_url": source.get("base_url") or source.get("url"),
                "default_frequency_minutes": int(source.get("default_frequency_minutes", 60)),
            }
            upsert_source(conn, source_dict)

            tactic_type = source.get("type")
            feed_url = source.get("url")
            if tactic_type and feed_url:
                policy: dict[str, object] = {}
                tags = source.get("tags") or []
                overrides = source.get("overrides") or {}
                if tags:
                    policy.setdefault("tags", {})["tag_defaults"] = tags
                if isinstance(overrides, dict):
                    if overrides.get("parse", {}).get("prefer_entry_summary") is not None:
                        policy.setdefault("parse", {})["prefer_entry_summary"] = overrides[
                            "parse"
                        ]["prefer_entry_summary"]
                    if overrides.get("http_headers"):
                        policy.setdefault("fetch", {})["headers"] = overrides["http_headers"]

                config = {"feed_url": feed_url, **policy}
                tactic = SourceTactic(
                    id=None,
                    source_id=source_id,
                    tactic_type=tactic_type,
                    enabled=bool(source.get("enabled", True)),
                    priority=int(source.get("priority", 100)),
                    config=config,
                    last_success_at=None,
                    last_error_at=None,
                    error_streak=0,
                )
                upsert_tactic(conn, tactic)
        except ValueError as exc:
            log_event(
                logger,
                logging.ERROR,
                "sources_import_error",
                source_id=source.get("id"),
                error=str(exc),
            )
            return 1

    log_event(logger, logging.INFO, "sources_imported", count=len(sources))
    return 0


def _cmd_sources_list(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    sources = list_sources(conn, enabled_only=False)
    if not sources:
        log_event(
            logger,
            logging.WARNING,
            "no_sources",
            hint="Import sources with `sempervigil sources import /config/sources.yml`",
        )
        return 1

    for source in sources:
        log_event(
            logger,
            logging.INFO,
            "source",
            source_id=source.id,
            enabled=source.enabled,
            base_url=source.base_url,
            frequency_minutes=source.default_frequency_minutes,
        )

    log_event(logger, logging.INFO, "sources_listed", count=len(sources))
    return 0


def _cmd_sources_add(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    source_dict = {
        "id": args.id,
        "name": args.name,
        "enabled": args.enabled,
        "base_url": args.url,
        "default_frequency_minutes": args.frequency_minutes,
    }
    try:
        upsert_source(conn, source_dict)
        tactic = SourceTactic(
            id=None,
            source_id=args.id,
            tactic_type=args.kind,
            enabled=True,
            priority=100,
            config={"feed_url": args.url},
            last_success_at=None,
            last_error_at=None,
            error_streak=0,
        )
        upsert_tactic(conn, tactic)
    except ValueError as exc:
        log_event(logger, logging.ERROR, "source_add_error", error=str(exc))
        return 1
    log_event(logger, logging.INFO, "source_added", source_id=args.id)
    return 0


def _cmd_sources_show(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    source = get_source(conn, args.source_id)
    if source is None:
        log_event(logger, logging.ERROR, "source_not_found", source_id=args.source_id)
        return 1

    logger.info(json.dumps(source.__dict__, indent=2, sort_keys=True))
    return 0




def _cmd_sources_export(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    sources = list_sources(conn, enabled_only=False)
    payload = [source.__dict__ for source in sources]
    try:
        with open(args.out, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except OSError as exc:
        log_event(logger, logging.ERROR, "sources_export_error", error=str(exc))
        return 1

    log_event(logger, logging.INFO, "sources_exported", count=len(sources), path=args.out)
    return 0


def _cmd_db_migrate(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    init_db(config.paths.state_db)
    log_event(logger, logging.INFO, "db_migrated", path=config.paths.state_db)
    return 0


def _cmd_jobs_enqueue(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    payload = {}
    if args.source_id:
        payload["source_id"] = args.source_id
    conn = init_db(config.paths.state_db)
    job_id = enqueue_job(conn, args.job_type, payload, debounce=args.debounce)
    log_event(logger, logging.INFO, "job_enqueued", job_id=job_id, job_type=args.job_type)
    return 0


def _cmd_jobs_list(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    conn = init_db(config.paths.state_db)
    jobs = list_jobs(conn, limit=args.limit)
    for job in jobs:
        log_event(
            logger,
            logging.INFO,
            "job",
            job_id=job.id,
            job_type=job.job_type,
            status=job.status,
            requested_at=job.requested_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            error=job.error,
            result=job.result,
        )
    return 0



def _parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _cmd_cve_sync(args: argparse.Namespace, logger: logging.Logger) -> int:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1
    if not config.cve.enabled:
        log_event(logger, logging.WARNING, "cve_sync_disabled")
        return 0
    conn = init_db(config.paths.state_db)
    now = datetime.now(tz=timezone.utc)
    last_sync = get_setting(conn, "cve.last_successful_sync_at", None)
    start = _parse_iso(last_sync) if isinstance(last_sync, str) else None
    if not start:
        start = now - timedelta(minutes=config.cve.sync_interval_minutes)
    result = sync_cves(
        conn,
        CveSyncConfig(
            results_per_page=config.cve.results_per_page,
            rate_limit_seconds=config.cve.rate_limit_seconds,
            backoff_seconds=config.cve.backoff_seconds,
            max_retries=config.cve.max_retries,
            prefer_v4=config.cve.prefer_v4,
            api_key=os.environ.get("NVD_API_KEY"),
        ),
        last_modified_start=isoformat_utc(start),
        last_modified_end=isoformat_utc(now),
    )
    log_event(logger, logging.INFO, "cve_sync_complete", **result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil", description="SemperVigil CLI")
    parser.add_argument(
        "--config",
        dest="config",
        default=None,
        help="Path to config.yml (defaults to SV_CONFIG_PATH or /config/config.yml)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Fetch enabled sources and write outputs")
    run_parser.add_argument(
        "--include-tag",
        action="append",
        default=[],
        help="Only write items with matching tags (repeatable)",
    )
    run_parser.add_argument(
        "--exclude-tag",
        action="append",
        default=[],
        help="Skip items with matching tags (repeatable)",
    )
    run_parser.set_defaults(func=_cmd_run)

    test_parser = subparsers.add_parser(
        "test-source", help="Fetch/parse a single source with diagnostics"
    )
    test_parser.add_argument("source_id", help="Source id to test")
    test_parser.add_argument("--limit", type=int, default=10, help="Preview item limit")
    test_parser.add_argument(
        "--verbose", action="store_true", help="Print normalization details"
    )
    test_parser.add_argument(
        "--show-raw",
        action="store_true",
        help="Print raw entry fields for the first item only",
    )
    test_parser.add_argument(
        "--ignore-dedupe",
        action="store_true",
        help="Ignore dedupe checks for preview decisions",
    )
    test_parser.set_defaults(func=_cmd_test_source)

    report_parser = subparsers.add_parser("report", help="Print last run summary")
    report_parser.set_defaults(func=_cmd_report)

    sources_parser = subparsers.add_parser("sources", help="Manage sources")
    sources_subparsers = sources_parser.add_subparsers(dest="sources_command", required=True)

    sources_import = sources_subparsers.add_parser("import", help="Import sources from YAML")
    sources_import.add_argument("path", nargs="?", help="Path to sources YAML file")
    sources_import.set_defaults(func=_cmd_sources_import)

    sources_list = sources_subparsers.add_parser("list", help="List sources")
    sources_list.set_defaults(func=_cmd_sources_list)

    sources_add = sources_subparsers.add_parser("add", help="Add or update a source")
    sources_add.add_argument("--id", required=True, help="Source id")
    sources_add.add_argument("--name", required=True, help="Source name")
    sources_add.add_argument("--kind", required=True, choices=["rss", "atom", "html"], help="Tactic type")
    sources_add.add_argument("--url", required=True, help="Source base/feed URL")
    sources_add.add_argument(
        "--enabled",
        dest="enabled",
        action="store_true",
        default=True,
        help="Enable the source",
    )
    sources_add.add_argument(
        "--disabled",
        dest="enabled",
        action="store_false",
        help="Disable the source",
    )
    sources_add.add_argument(
        "--frequency-minutes",
        type=int,
        default=60,
        help="Default frequency in minutes",
    )
    sources_add.set_defaults(func=_cmd_sources_add)

    sources_show = sources_subparsers.add_parser("show", help="Show a source")
    sources_show.add_argument("source_id", help="Source id")
    sources_show.set_defaults(func=_cmd_sources_show)

    sources_export = sources_subparsers.add_parser(
        "export", help="Export sources to JSON"
    )
    sources_export.add_argument("--out", required=True, help="Output JSON path")
    sources_export.set_defaults(func=_cmd_sources_export)

    db_parser = subparsers.add_parser("db", help="Database maintenance")
    db_subparsers = db_parser.add_subparsers(dest="db_command", required=True)

    db_migrate = db_subparsers.add_parser("migrate", help="Apply database migrations")
    db_migrate.set_defaults(func=_cmd_db_migrate)

    jobs_parser = subparsers.add_parser("jobs", help="Job queue commands")
    jobs_subparsers = jobs_parser.add_subparsers(dest="jobs_command", required=True)

    jobs_enqueue = jobs_subparsers.add_parser("enqueue", help="Enqueue a job")
    jobs_enqueue.add_argument(
        "job_type",
        choices=[
            "ingest_source",
            "ingest_due_sources",
            "test_source",
            "build_site",
            "cve_sync",
            "write_article_markdown",
        ],
        help="Job type to enqueue",
    )
    jobs_enqueue.add_argument("--source-id", help="Source id for source-scoped jobs")
    jobs_enqueue.add_argument(
        "--debounce",
        action="store_true",
        help="Avoid enqueuing if a job of the same type is queued/running",
    )
    jobs_enqueue.set_defaults(func=_cmd_jobs_enqueue)

    jobs_list = jobs_subparsers.add_parser("list", help="List recent jobs")
    jobs_list.add_argument("--limit", type=int, default=20, help="Number of jobs to show")
    jobs_list.set_defaults(func=_cmd_jobs_list)

    cve_parser = subparsers.add_parser("cve", help="CVE ingestion commands")
    cve_subparsers = cve_parser.add_subparsers(dest="cve_command", required=True)

    cve_sync = cve_subparsers.add_parser("sync", help="Run CVE sync now")
    cve_sync.set_defaults(func=_cmd_cve_sync)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logger = _setup_logging()
    return args.func(args, logger)
