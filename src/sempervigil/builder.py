from __future__ import annotations

import argparse
import logging
import os
import subprocess
import time
from pathlib import Path

from .config import ConfigError, load_config
from .storage import claim_next_job, complete_job, fail_job, init_db
from .utils import log_event


def _setup_logging() -> logging.Logger:
    level_name = os.environ.get("SV_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("sempervigil")


def _resolve_site_dir(output_dir: str) -> Path:
    output_path = Path(output_dir)
    if output_path.name == "posts" and output_path.parent.name == "content":
        return output_path.parent.parent
    if output_path.name == "content":
        return output_path.parent
    return Path("/site")


def _run_hugo(site_dir: Path) -> tuple[int, str]:
    cmd = [
        "hugo",
        "build",
        "--minify",
        "--gc",
        "--cleanDestinationDir",
        "--logLevel",
        "info",
    ]
    result = subprocess.run(
        cmd,
        cwd=str(site_dir),
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

    conn = init_db(config.paths.state_db)
    job = claim_next_job(conn, builder_id, allowed_types=["build_site"])
    if not job:
        return 0

    site_dir = _resolve_site_dir(config.paths.output_dir)
    log_event(logger, logging.INFO, "build_claimed", job_id=job.id, site_dir=str(site_dir))
    try:
        returncode, output = _run_hugo(site_dir)
    except Exception as exc:  # noqa: BLE001
        fail_job(conn, job.id, str(exc))
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, error=str(exc))
        return 1

    if returncode != 0:
        fail_job(conn, job.id, output or f"hugo exited with {returncode}")
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, output=output)
        return 1

    complete_job(conn, job.id, result={"site_dir": str(site_dir)})
    log_event(logger, logging.INFO, "build_succeeded", job_id=job.id)
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
