from __future__ import annotations

import argparse
import logging
import os
import subprocess
import time

from .config import ConfigError, load_config
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import claim_next_job, complete_job, fail_job, init_db
from .utils import log_event


def _setup_logging() -> logging.Logger:
    level_name = os.environ.get("SV_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("sempervigil")


def _run_hugo() -> tuple[int, str]:
    cmd = ["/bin/sh", "/tools/hugo-build.sh"]
    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    output = (result.stdout or "") + (result.stderr or "")
    return result.returncode, output.strip()


def run_once(config_path: str | None, builder_id: str) -> int:
    logger = _setup_logging()
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))
    conn = init_db(config.paths.state_db)
    job = claim_next_job(
        conn,
        builder_id,
        allowed_types=["build_site"],
        lock_timeout_seconds=config.jobs.lock_timeout_seconds,
    )
    if not job:
        return 0

    log_event(logger, logging.INFO, "build_claimed", job_id=job.id)
    try:
        returncode, output = _run_hugo()
    except Exception as exc:  # noqa: BLE001
        fail_job(conn, job.id, str(exc))
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, error=str(exc))
        return 1

    if returncode != 0:
        fail_job(conn, job.id, output or f"hugo exited with {returncode}")
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, output=output)
        return 1

    if complete_job(conn, job.id, result={"output": output}):
        log_event(logger, logging.INFO, "build_succeeded", job_id=job.id)
    else:
        log_event(logger, logging.ERROR, "build_complete_failed", job_id=job.id)
    return 0


def run_loop(config_path: str | None, builder_id: str, sleep_seconds: int) -> int:
    while True:
        run_once(config_path, builder_id)
        time.sleep(sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil-builder")
    parser.add_argument("--config", dest="config", default=None)
    parser.add_argument("--once", action="store_true", help="Run a single job and exit")
    parser.add_argument("--sleep", type=int, default=10, help="Sleep seconds between polls")
    parser.add_argument("--builder-id", default=os.environ.get("HOSTNAME", "builder"))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.once:
        return run_once(args.config, args.builder_id)
    return run_loop(args.config, args.builder_id, args.sleep)


if __name__ == "__main__":
    raise SystemExit(main())
