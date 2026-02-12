from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import subprocess
import time
from datetime import datetime, timedelta, timezone

from .config import ConfigError, load_runtime_config
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import (
    claim_next_job,
    complete_job,
    fail_job,
    init_db,
    is_job_canceled,
    requeue_job,
)
from .utils import configure_logging, log_event


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.hugo")


def _tail(text: str, max_lines: int = 120) -> str:
    lines = (text or "").splitlines()
    return "\n".join(lines[-max_lines:])


def _tail_file(path: Path, max_lines: int = 120, max_bytes: int = 400_000) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            read_size = min(size, max_bytes)
            handle.seek(-read_size, os.SEEK_END)
            data = handle.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""
    lines = data.splitlines()
    return "\n".join(lines[-max_lines:])


def _last_successful_build_at(conn) -> datetime | None:
    row = conn.execute(
        """
        SELECT finished_at
        FROM jobs
        WHERE job_type = 'build_site' AND status = 'succeeded' AND finished_at IS NOT NULL
        ORDER BY finished_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not row or not row[0]:
        return None
    try:
        return datetime.fromisoformat(row[0])
    except ValueError:
        return None


def _truncate_log_file(path: Path, max_bytes: int) -> None:
    if max_bytes <= 0 or not path.exists():
        return
    try:
        size = path.stat().st_size
    except OSError:
        return
    if size <= max_bytes:
        return
    keep_bytes = max(1, max_bytes // 2)
    try:
        with path.open("rb") as handle:
            handle.seek(-min(size, keep_bytes), os.SEEK_END)
            data = handle.read()
        with path.open("wb") as handle:
            handle.write(data)
    except OSError:
        return


def _build_log_paths(logs_dir: str, job_id: str) -> dict[str, Path]:
    logs_dir = Path(logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "hugo-build.log"
    max_bytes = int(os.getenv("SV_HUGO_BUILD_LOG_MAX_BYTES", "5242880"))
    _truncate_log_file(log_path, max_bytes)
    return {
        "stdout": log_path,
        "stderr": log_path,
    }


def _run_hugo_until_done(
    conn, job_id: str, log_paths: dict[str, Path]
) -> tuple[int, str, str, bool, list[str]]:
    cmd = ["/bin/sh", "/tools/hugo-build.sh"]
    stdout_path = log_paths["stdout"]
    stderr_path = log_paths["stderr"]
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("a", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=log_file,
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
    proc.wait()
    stdout = _tail_file(stdout_path)
    stderr = _tail_file(stderr_path)
    return proc.returncode or 0, stdout, stderr, canceled, cmd



def _derive_product_title(md_path: Path) -> str:
    stem = md_path.stem
    if "__" in stem:
        vendor, product = stem.split("__", 1)
        vendor = vendor.replace("_", " ").strip()
        product = product.replace("_", " ").strip()
        return f"{vendor} {product}".strip()
    return stem.replace("_", " ").strip()


def _sanitize_title_value(value: str, md_path: Path) -> tuple[str, bool]:
    raw = value.strip()
    if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
        raw = raw[1:-1].replace("''", "'")
    elif raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
        raw = raw[1:-1].replace('\\"', '"')
    raw = raw.replace("\\", "")
    replaced = False
    if len(raw) > 200:
        raw = _derive_product_title(md_path)
        replaced = True
    escaped = raw.replace("'", "''")
    return f"title: '{escaped}'", replaced


def _sanitize_product_pages(source_dir: str) -> int:
    content_dir = Path(source_dir) / "content" / "products"
    if not content_dir.exists():
        return 0
    fixed = 0
    for md_path in content_dir.glob("*.md"):
        try:
            lines = md_path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        in_front_matter = False
        changed = False
        for idx, line in enumerate(lines):
            if idx == 0 and line.strip() == "---":
                in_front_matter = True
                continue
            if in_front_matter and line.strip() == "---":
                break
            if in_front_matter and line.startswith("title:"):
                new_line, replaced = _sanitize_title_value(line[len("title:"):], md_path)
                if new_line != line:
                    lines[idx] = new_line
                    changed = True
                elif replaced:
                    lines[idx] = new_line
                    changed = True
                break
        try:
            md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        except Exception:
            continue
        if changed:
            fixed += 1
    return fixed

def run_once(builder_id: str) -> int:
    logger = _setup_logging()
    log_event(logger, logging.INFO, "builder_once_start", builder_id=builder_id)
    try:
        conn = init_db()
        config = load_runtime_config(conn)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
        log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
        return 1

    set_umask_from_env()
    ensure_runtime_dirs(build_default_paths(config.paths.data_dir, config.paths.output_dir, config.paths.logs_dir))
    source_dir = os.environ.get("SV_HUGO_SOURCE_DIR")
    if not source_dir:
        try:
            source_dir = str(Path(config.paths.output_dir).parent.parent)
        except Exception:
            source_dir = None
    if source_dir:
        _sanitize_product_pages(source_dir)
    job = claim_next_job(
        conn,
        builder_id,
        allowed_types=["build_site"],
        lock_timeout_seconds=config.jobs.lock_timeout_seconds,
    )
    if not job:
        log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
        return 0

    if is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "build_canceled", job_id=job.id)
        log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
        return 0

    debounce_seconds = int(config.jobs.build_debounce_seconds)
    if debounce_seconds > 0:
        last_finished = _last_successful_build_at(conn)
        if last_finished:
            next_time = last_finished + timedelta(seconds=debounce_seconds)
            if datetime.now(tz=timezone.utc) < next_time:
                payload = dict(job.payload or {})
                payload["not_before"] = next_time.isoformat()
                requeue_job(conn, job.id, payload, payload["not_before"])
                log_event(
                    logger,
                    logging.INFO,
                    "build_debounced",
                    job_id=job.id,
                    not_before=payload["not_before"],
                )
                log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
                return 0

    log_paths = _build_log_paths(config.paths.logs_dir, job.id)
    log_event(
        logger,
        logging.INFO,
        "build_claimed",
        job_id=job.id,
        stdout_log_path=str(log_paths["stdout"]),
        stderr_log_path=str(log_paths["stderr"]),
    )
    start = time.time()
    try:
        returncode, stdout, stderr, canceled, cmd = _run_hugo_until_done(conn, job.id, log_paths)
    except Exception as exc:  # noqa: BLE001
        fail_job(conn, job.id, str(exc))
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, error=str(exc))
        log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
        return 1

    if canceled or is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "build_canceled", job_id=job.id)
        log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
        return 0

    if returncode != 0:
        tail = _tail(stderr or stdout)
        error_detail = (
            f"cmd={' '.join(cmd)}\nstdout_tail:\n{stdout}\nstderr_tail:\n{stderr}\n"
            f"stdout_log_path={log_paths['stdout']}\nstderr_log_path={log_paths['stderr']}".strip()
        )
        fail_job(conn, job.id, error_detail or f"hugo exited with {returncode}")
        log_event(
            logger,
            logging.ERROR,
            "build_failed",
            job_id=job.id,
            output=tail,
            cmd=" ".join(cmd),
        )
        log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
        return 1

    duration = round(time.time() - start, 2)
    result_payload = {
        "exit_code": returncode,
        "stdout_tail": _tail(stdout),
        "stderr_tail": _tail(stderr),
        "duration_s": duration,
        "output_path": os.environ.get("SV_HUGO_OUTPUT_DIR", "/site"),
        "stdout_log_path": str(log_paths["stdout"]),
        "stderr_log_path": str(log_paths["stderr"]),
    }
    if complete_job(conn, job.id, result=result_payload):
        log_event(logger, logging.INFO, "build_succeeded", job_id=job.id)
    else:
        log_event(logger, logging.ERROR, "build_complete_failed", job_id=job.id)
    log_event(logger, logging.INFO, "builder_once_done", builder_id=builder_id)
    return 0


def run_loop(builder_id: str, sleep_seconds: int) -> int:
    logger = _setup_logging()
    log_event(logger, logging.INFO, "builder_loop_start", builder_id=builder_id)
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
