from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from dataclasses import asdict

from .config import ConfigError, load_config, load_sources_file
from .ingest import process_source
from .publish import write_hugo_markdown, write_json_index
from .storage import (
    get_source,
    init_db,
    insert_articles,
    list_sources,
    record_source_run,
    upsert_source,
)
from .utils import log_event, utc_now_iso


def _setup_logging() -> logging.Logger:
    level_name = os.environ.get("SV_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("sempervigil")


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
            hint="Import sources with `sempervigil sources import /path/to/sources.yml`",
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
    }
    written_paths: list[str] = []
    all_articles = []

    for source in enabled_sources:
        source_started_at = utc_now_iso()
        result = process_source(source, config, logger, conn, test_mode=False)
        record_source_run(
            conn,
            source_id=source.id,
            started_at=source_started_at,
            status=result.status,
            http_status=result.http_status,
            found_count=result.found_count,
            accepted_count=result.accepted_count,
            error=result.error,
        )

        totals["items_found"] += result.found_count
        totals["items_accepted"] += result.accepted_count

        if result.status != "ok":
            totals["sources_error"] += 1
            continue

        totals["sources_ok"] += 1
        all_articles.extend(result.articles)
        insert_articles(conn, result.articles)
        written_paths.extend(write_hugo_markdown(result.articles, config.paths.output_dir))

    totals["items_written"] = len(written_paths)

    if config.publishing.write_json_index:
        write_json_index(all_articles, config.publishing.json_index_path)

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
                hint="Import sources with `sempervigil sources import /path/to/sources.yml`",
            )
        else:
            log_event(logger, logging.ERROR, "source_not_found", source_id=args.source_id)
        return 1
    result = process_source(source, config, logger, conn, test_mode=True)

    log_event(
        logger,
        logging.INFO,
        "test_source_result",
        source_id=source.id,
        status=result.status,
        found_count=result.found_count,
        accepted_count=result.accepted_count,
    )

    for item in result.preview:
        log_event(
            logger,
            logging.INFO,
            "source_item",
            source_id=source.id,
            accepted=item.get("accepted"),
            title=item.get("title"),
            url=item.get("url"),
            reasons=",".join(item.get("reasons") or []),
        )

    return 0 if result.status == "ok" else 1


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
    try:
        sources = load_sources_file(args.path)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "sources_import_error", error=str(exc))
        return 1
    if not sources:
        log_event(logger, logging.ERROR, "sources_import_error", error="no sources found")
        return 1

    for source in sources:
        try:
            upsert_source(conn, asdict(source))
        except ValueError as exc:
            log_event(
                logger,
                logging.ERROR,
                "sources_import_error",
                source_id=source.id,
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
            hint="Import sources with `sempervigil sources import /path/to/sources.yml`",
        )
        return 1

    for source in sources:
        log_event(
            logger,
            logging.INFO,
            "source",
            source_id=source.id,
            enabled=source.enabled,
            type=source.type,
            url=source.url,
        )

    log_event(logger, logging.INFO, "sources_listed", count=len(sources))
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
    run_parser.set_defaults(func=_cmd_run)

    test_parser = subparsers.add_parser(
        "test-source", help="Fetch/parse a single source with diagnostics"
    )
    test_parser.add_argument("source_id", help="Source id to test")
    test_parser.set_defaults(func=_cmd_test_source)

    report_parser = subparsers.add_parser("report", help="Print last run summary")
    report_parser.set_defaults(func=_cmd_report)

    sources_parser = subparsers.add_parser("sources", help="Manage sources")
    sources_subparsers = sources_parser.add_subparsers(dest="sources_command", required=True)

    sources_import = sources_subparsers.add_parser("import", help="Import sources from YAML")
    sources_import.add_argument("path", help="Path to sources YAML file")
    sources_import.set_defaults(func=_cmd_sources_import)

    sources_list = sources_subparsers.add_parser("list", help="List sources")
    sources_list.set_defaults(func=_cmd_sources_list)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logger = _setup_logging()
    return args.func(args, logger)
