from __future__ import annotations

import argparse
import logging
import os
import subprocess
import time

from .config import ConfigError, get_state_db_path, load_runtime_config
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import claim_next_job, complete_job, fail_job, init_db, is_job_canceled
from .utils import configure_logging, log_event


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.hugo")


def _tail(text: str, max_lines: int = 120) -> str:
    lines = (text or "").splitlines()
    return "\n".join(lines[-max_lines:])


def _run_hugo_until_done(conn, job_id: str) -> tuple[int, str, str, bool]:
    cmd = ["/bin/sh", "/tools/hugo-build.sh"]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    canceled = False
    while True:
        if is_job_canceled(conn, job_id):
            canceled = True
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            break
        if proc.poll() is not None:
            break
        time.sleep(0.5)
    stdout, stderr = proc.communicate()
    stdout = (stdout or "").strip()
    stderr = (stderr or "").strip()
    return proc.returncode or 0, stdout, stderr, canceled


def run_once(builder_id: str) -> int:
    logger = _setup_logging()
    try:
        conn = init_db(get_state_db_path())
        config = load_runtime_config(conn)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        return 1

    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir))
    job = claim_next_job(
        conn,
        builder_id,
        allowed_types=["build_site"],
        lock_timeout_seconds=config.jobs.lock_timeout_seconds,
    )
    if not job:
        return 0

    if is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "build_canceled", job_id=job.id)
        return 0

    log_event(logger, logging.INFO, "build_claimed", job_id=job.id)
    start = time.time()
    try:
        returncode, stdout, stderr, canceled = _run_hugo_until_done(conn, job.id)
    except Exception as exc:  # noqa: BLE001
        fail_job(conn, job.id, str(exc))
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, error=str(exc))
        return 1

    if canceled or is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "build_canceled", job_id=job.id)
        return 0

    if returncode != 0:
        tail = _tail(stderr or stdout)
        fail_job(conn, job.id, tail or f"hugo exited with {returncode}")
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, output=tail)
        return 1

    duration = round(time.time() - start, 2)
    result_payload = {
        "exit_code": returncode,
        "stdout_tail": _tail(stdout),
        "stderr_tail": _tail(stderr),
        "duration_s": duration,
        "output_path": "/site/public",
    }
    if complete_job(conn, job.id, result=result_payload):
        log_event(logger, logging.INFO, "build_succeeded", job_id=job.id)
    else:
        log_event(logger, logging.ERROR, "build_complete_failed", job_id=job.id)
    return 0


def run_loop(builder_id: str, sleep_seconds: int) -> int:
    while True:
        run_once(builder_id)
        time.sleep(sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil-builder")
    parser.add_argument("--once", action="store_true", help="Run a single job and exit")
    parser.add_argument("--sleep", type=int, default=10, help="Sleep seconds between polls")
    parser.add_argument("--builder-id", default=os.environ.get("HOSTNAME", "builder"))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.once:
        return run_once(args.builder_id)
    return run_loop(args.builder_id, args.sleep)


if __name__ == "__main__":
    raise SystemExit(main())
