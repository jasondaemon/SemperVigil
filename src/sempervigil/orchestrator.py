from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .config import (
    ConfigError,
    bootstrap_cve_settings,
    bootstrap_events_settings,
    bootstrap_schedule_settings,
    get_schedule_settings,
    load_runtime_config,
    set_schedule_settings,
)
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import (
    enqueue_build_site_if_needed,
    enqueue_job,
    get_build_state,
    get_queue_stats,
    has_pending_job,
    init_db,
    release_lease,
    try_acquire_lease,
)
from .utils import configure_logging, log_event
from .worker import (
    _maybe_enqueue_auto_catchup,
    _maybe_enqueue_cve_sync,
    _maybe_enqueue_ingest_due_sources,
)

_SCHEDULE_JOB_TYPES = {
    "daily_brief": "build_daily_brief",
}


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.orchestrator")


def _parse_hhmm(value: str) -> tuple[int, int] | None:
    parts = value.strip().split(":")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return None
    hour = int(parts[0])
    minute = int(parts[1])
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return hour, minute


def _resolve_timezone(conn) -> timezone:
    try:
        cfg = get_schedule_settings(conn)
        tz = cfg.get("timezone")
        if tz:
            return ZoneInfo(str(tz))
    except Exception:
        pass
    try:
        runtime = load_runtime_config(conn)
        tz = getattr(runtime.app, "timezone", None)
        if tz:
            return ZoneInfo(str(tz))
    except Exception:
        pass
    return timezone.utc


def _tick_scheduled_jobs(conn, logger: logging.Logger) -> int:
    cfg = get_schedule_settings(conn)
    tasks = cfg.get("tasks") or {}
    tzinfo = _resolve_timezone(conn)
    now = datetime.now(tzinfo)
    today = now.date().isoformat()
    dirty = False
    enqueued = 0
    for task_key, task in tasks.items():
        if not isinstance(task, dict) or not task.get("enabled"):
            continue
        job_type = _SCHEDULE_JOB_TYPES.get(task_key)
        if not job_type:
            continue
        parsed = _parse_hhmm(str(task.get("time") or ""))
        if not parsed:
            continue
        hour, minute = parsed
        run_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if now < run_time:
            continue
        if task.get("last_run") == today:
            continue
        if has_pending_job(conn, job_type):
            task["last_run"] = today
            dirty = True
            continue
        job_id = enqueue_job(conn, job_type, payload=None, debounce=True, dedupe=True)
        task["last_run"] = today
        dirty = True
        enqueued += 1
        log_event(
            logger,
            logging.INFO,
            "scheduled_job_enqueued",
            job_id=job_id,
            job_type=job_type,
            schedule_key=task_key,
        )
    if dirty:
        set_schedule_settings(conn, cfg)
    return enqueued


def _tick_build_admission(conn, config, logger: logging.Logger) -> int:
    state = get_build_state(conn)
    if not state.get("dirty"):
        return 0
    if has_pending_job(conn, "build_site"):
        return 0
    reason = None
    reasons = state.get("reasons")
    if isinstance(reasons, list) and reasons:
        reason = ",".join(str(item) for item in reasons[:5])
    job_id = enqueue_build_site_if_needed(
        conn,
        reason=reason or "orchestrator_build_dirty",
        debounce_seconds=int(getattr(config.jobs, "build_debounce_seconds", 60)),
    )
    if not job_id:
        return 0
    log_event(
        logger,
        logging.INFO,
        "build_job_admitted",
        job_id=job_id,
        reason=reason,
    )
    return 1


def _log_queue_stats(conn, logger: logging.Logger) -> None:
    stats = get_queue_stats(conn)
    summary = {}
    for row in stats:
        queue_name = str(row.get("queue_name") or "default")
        queue_summary = summary.setdefault(queue_name, {})
        queue_summary[str(row.get("status") or "unknown")] = int(row.get("count") or 0)
    log_event(logger, logging.INFO, "orchestrator_queue_stats", queues=summary)


def run_once(orchestrator_id: str) -> int:
    logger = _setup_logging()
    try:
        conn = init_db()
        config = load_runtime_config(conn)
        bootstrap_cve_settings(conn)
        bootstrap_events_settings(conn)
        bootstrap_schedule_settings(conn)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    lease_name = os.environ.get("SV_ORCH_LEASE_NAME", "orchestrator")
    lease_seconds = int(os.environ.get("SV_ORCH_LEASE_SECONDS", "120") or 120)
    if not try_acquire_lease(conn, lease_name, orchestrator_id, lease_seconds):
        log_event(
            logger,
            logging.INFO,
            "orchestrator_lease_busy",
            lease_name=lease_name,
            orchestrator_id=orchestrator_id,
        )
        conn.close()
        return 0

    try:
        set_umask_from_env()
        ensure_runtime_dirs(
            build_default_paths(config.paths.data_dir, config.paths.output_dir, config.paths.logs_dir)
        )
        scheduled = _tick_scheduled_jobs(conn, logger)
        _maybe_enqueue_ingest_due_sources(conn, logger)
        _maybe_enqueue_cve_sync(conn, logger)
        _maybe_enqueue_auto_catchup(conn, config, logger, orchestrator_id, None)
        builds = _tick_build_admission(conn, config, logger)
        _log_queue_stats(conn, logger)
        log_event(
            logger,
            logging.INFO,
            "orchestrator_tick_complete",
            scheduled_jobs=scheduled,
            build_jobs=builds,
        )
        return 0
    finally:
        try:
            release_lease(conn, lease_name, orchestrator_id)
        finally:
            conn.close()


def run_loop(orchestrator_id: str, sleep_seconds: int) -> int:
    logger = _setup_logging()
    log_event(logger, logging.INFO, "orchestrator_loop_start", orchestrator_id=orchestrator_id)
    while True:
        run_once(orchestrator_id)
        time.sleep(sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil-orchestrator")
    parser.add_argument("--once", action="store_true", help="Run a single orchestration tick and exit")
    parser.add_argument(
        "--sleep",
        type=int,
        default=int(os.environ.get("SV_ORCH_TICK_SECONDS", "30")),
        help="Sleep seconds between orchestration ticks",
    )
    parser.add_argument("--orchestrator-id", default=os.environ.get("HOSTNAME", "orchestrator"))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.once:
        return run_once(args.orchestrator_id)
    return run_loop(args.orchestrator_id, args.sleep)


if __name__ == "__main__":
    raise SystemExit(main())
