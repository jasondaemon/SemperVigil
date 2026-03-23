from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from .storage import claim_next_job, complete_job, fail_job, heartbeat_job, init_db, is_job_canceled
from .utils import configure_logging, log_event

_RUNNER_TYPES: dict[str, dict[str, object]] = {
    "fetch": {
        "launch_job_type": "launch_fetch_worker",
        "queue_name": "fetch",
        "default_max_jobs": 25,
        "default_max_runtime_seconds": 600,
        "default_lease_seconds": 900,
    },
    "llm_local": {
        "launch_job_type": "launch_llm_worker",
        "queue_name": "llm_local",
        "default_max_jobs": 5,
        "default_max_runtime_seconds": 900,
        "default_lease_seconds": 1200,
    },
    "openai": {
        "launch_job_type": "launch_openai_worker",
        "queue_name": "openai",
        "default_max_jobs": 3,
        "default_max_runtime_seconds": 1200,
        "default_lease_seconds": 1500,
    },
    "build": {
        "launch_job_type": "launch_build_worker",
        "queue_name": "build",
        "default_max_jobs": 1,
        "default_max_runtime_seconds": 3600,
        "default_lease_seconds": 5400,
    },
}

_RUNNER_IDLE = 3


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.runner")


def _run_subprocess_until_done(
    conn,
    launch_job_id: str,
    runner_id: str,
    command: list[str],
    lease_seconds: int,
) -> tuple[int, bool]:
    proc = subprocess.Popen(command)
    canceled = False
    last_heartbeat = 0.0
    while True:
        if is_job_canceled(conn, launch_job_id):
            canceled = True
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            break
        if proc.poll() is not None:
            break
        now = time.monotonic()
        if now - last_heartbeat >= 15:
            heartbeat_job(conn, launch_job_id, runner_id, lease_seconds)
            last_heartbeat = now
        time.sleep(0.5)
    proc.wait()
    return int(proc.returncode or 0), canceled


def _worker_command(queue_name: str, max_jobs: int, max_runtime_seconds: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "sempervigil.worker",
        "--queue",
        queue_name,
        "--sleep",
        "1",
        "--max-jobs",
        str(max_jobs),
        "--max-runtime-seconds",
        str(max_runtime_seconds),
    ]


def _build_command(runner_id: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "sempervigil.builder",
        "--once",
        "--builder-id",
        f"{runner_id}-build",
    ]


def run_once(runner_id: str, runner_type: str) -> int:
    logger = _setup_logging()
    runner_cfg = _RUNNER_TYPES[runner_type]
    conn = init_db()
    lease_seconds = int(os.environ.get("SV_RUNNER_LEASE_SECONDS", "0") or 0) or int(
        runner_cfg["default_lease_seconds"]
    )
    job = claim_next_job(
        conn,
        runner_id,
        allowed_types=[str(runner_cfg["launch_job_type"])],
        allowed_queues=["control"],
        lock_timeout_seconds=lease_seconds,
        lease_seconds=lease_seconds,
    )
    if not job:
        conn.close()
        return _RUNNER_IDLE

    payload = job.payload or {}
    max_jobs = int(payload.get("max_jobs") or runner_cfg["default_max_jobs"])
    max_runtime_seconds = int(
        payload.get("max_runtime_seconds") or runner_cfg["default_max_runtime_seconds"]
    )
    queue_name = str(payload.get("queue_name") or runner_cfg["queue_name"])

    command = (
        _build_command(runner_id)
        if runner_type == "build"
        else _worker_command(queue_name, max_jobs=max_jobs, max_runtime_seconds=max_runtime_seconds)
    )
    log_event(
        logger,
        logging.INFO,
        "runner_launch_claimed",
        runner_id=runner_id,
        runner_type=runner_type,
        launch_job_id=job.id,
        queue_name=queue_name,
        max_jobs=max_jobs,
        max_runtime_seconds=max_runtime_seconds,
    )
    exit_code, canceled = _run_subprocess_until_done(
        conn,
        job.id,
        runner_id,
        command,
        lease_seconds,
    )

    if canceled:
        conn.close()
        return 0

    result = {
        "runner_type": runner_type,
        "queue_name": queue_name,
        "max_jobs": max_jobs,
        "max_runtime_seconds": max_runtime_seconds,
        "exit_code": exit_code,
    }
    if exit_code == 0:
        complete_job(conn, job.id, result=result)
        conn.close()
        return 0
    fail_job(conn, job.id, f"{runner_type}_launch_exit_{exit_code}")
    conn.close()
    return 1


def run_loop(runner_id: str, runner_type: str, sleep_seconds: int) -> int:
    logger = _setup_logging()
    log_event(logger, logging.INFO, "runner_loop_start", runner_id=runner_id, runner_type=runner_type)
    while True:
        result = run_once(runner_id, runner_type)
        if result == _RUNNER_IDLE:
            time.sleep(sleep_seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sempervigil-runner")
    parser.add_argument("--runner", required=True, choices=sorted(_RUNNER_TYPES))
    parser.add_argument("--once", action="store_true", help="Run a single launch job and exit")
    parser.add_argument(
        "--sleep",
        type=int,
        default=int(os.environ.get("SV_RUNNER_POLL_SECONDS", "5")),
        help="Sleep seconds between launch job polls",
    )
    parser.add_argument("--runner-id", default=os.environ.get("HOSTNAME", "runner"))
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.once:
        result = run_once(args.runner_id, args.runner)
        return 0 if result == _RUNNER_IDLE else result
    return run_loop(args.runner_id, args.runner, args.sleep)


if __name__ == "__main__":
    raise SystemExit(main())
