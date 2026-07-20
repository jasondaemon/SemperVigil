from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
from pathlib import Path
import queue
import subprocess
import threading
import time
import sys
from datetime import datetime, timedelta, timezone

from .config import ConfigError, load_runtime_config
from .worker import _refresh_feed_archive_days, _refresh_feed_data_files
from .fsinit import build_default_paths, ensure_runtime_dirs, set_umask_from_env
from .storage import (
    clear_build_dirty,
    claim_next_job,
    complete_job,
    fail_job,
    get_build_state,
    heartbeat_job,
    init_db,
    is_job_canceled,
    set_setting,
)
from .utils import configure_logging, log_event


def _setup_logging() -> logging.Logger:
    return configure_logging("sempervigil.hugo")


def _site_root_from_output_dir(output_dir: str) -> str:
    path = Path(output_dir).resolve()
    if path.name == "posts" and path.parent.name == "content":
        path = path.parent.parent
    elif path.name in {"current", "public"}:
        path = path.parent
    else:
        path = path.parent
    return str(path)


def _refresh_feed_index_from_days(site_root: str, logger: logging.Logger) -> int:
    feed_dir = Path(site_root) / "static" / "feed"
    feed_days_dir = feed_dir / "days"
    if not feed_days_dir.exists():
        log_event(logger, logging.INFO, "feed_index_refresh_skipped", site_root=site_root, reason="missing_feed_days")
        return 0

    day_keys = sorted(
        {
            path.stem
            for path in feed_days_dir.glob("*.json")
            if path.is_file() and path.stem
        },
        reverse=True,
    )
    feed_index = {
        "days": day_keys,
        "latest_day": day_keys[0] if day_keys else "",
        "oldest_day": day_keys[-1] if day_keys else "",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    feed_dir.mkdir(parents=True, exist_ok=True)
    index_path = feed_dir / "index.json"
    tmp_path = index_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(feed_index, indent=2), encoding="utf-8")
    tmp_path.replace(index_path)
    log_event(
        logger,
        logging.INFO,
        "feed_index_refreshed",
        site_root=site_root,
        count=len(day_keys),
        latest_day=feed_index["latest_day"],
        oldest_day=feed_index["oldest_day"],
    )
    return len(day_keys)


def _maybe_refresh_feed_archive_days(conn, config, logger: logging.Logger) -> dict[str, object] | None:
    state = get_build_state(conn)
    metadata = state.get("metadata")
    if not isinstance(metadata, dict):
        return None
    request = metadata.get("feed_archive_refresh")
    if not isinstance(request, dict):
        return None
    mode = str(request.get("mode") or "dirty_only").strip().lower() or "dirty_only"
    stats = _refresh_feed_archive_days(conn, config, logger, mode=mode)
    next_metadata = dict(metadata)
    next_metadata.pop("feed_archive_refresh", None)
    state["metadata"] = next_metadata
    set_setting(conn, "build_site.state", state)
    log_event(
        logger,
        logging.INFO,
        "feed_archive_refresh_consumed",
        mode=mode,
        updated=stats.get("updated"),
        removed=stats.get("removed"),
        missing=stats.get("missing"),
    )
    return stats


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
    conn, job_id: str, builder_id: str, log_paths: dict[str, Path], lease_seconds: int
) -> tuple[int, str, str, bool, list[str]]:
    logger = _setup_logging()
    cmd = ["/bin/sh", "/app/tools/hugo-build.sh"]
    stdout_path = log_paths["stdout"]
    stderr_path = log_paths["stderr"]
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        line_queue: queue.Queue[str | None] = queue.Queue()

        def _drain_stdout() -> None:
            if proc.stdout is None:
                line_queue.put(None)
                return
            for raw_line in proc.stdout:
                line_queue.put(raw_line)
            line_queue.put(None)

        drain_thread = threading.Thread(target=_drain_stdout, daemon=True)
        drain_thread.start()
        canceled = False
        last_heartbeat = 0.0
        stream_closed = False
        while True:
            while True:
                try:
                    raw_line = line_queue.get_nowait()
                except queue.Empty:
                    break
                if raw_line is None:
                    stream_closed = True
                    break
                line = raw_line.rstrip("\n")
                log_file.write(raw_line)
                log_file.flush()
                sys.stdout.write(raw_line)
                sys.stdout.flush()
                if not line.strip():
                    continue
                log_event(
                    logger,
                    logging.INFO,
                    "build_hugo_output",
                    service="build_hugo",
                    runner_type="build",
                    job_id=job_id,
                    line=line,
                )
            canceled_now = is_job_canceled(conn, job_id)
            conn.commit()
            if canceled_now:
                canceled = True
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break
            if proc.poll() is not None and stream_closed:
                break
            now = time.monotonic()
            if now - last_heartbeat >= 15:
                heartbeat_job(conn, job_id, builder_id, lease_seconds)
                last_heartbeat = now
            time.sleep(0.5)
        drain_thread.join(timeout=1)
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


def _prune_legacy_posts_tree(source_dir: str) -> int:
    legacy_paths = [
        Path(source_dir) / "content" / "posts" / "content",
        Path(source_dir) / "content" / "posts" / "data",
    ]
    pruned = 0
    for path in legacy_paths:
        if not path.exists():
            continue
        try:
            shutil.rmtree(path)
        except OSError:
            continue
        pruned += 1
    return pruned


def run_once(builder_id: str) -> int:
    logger = _setup_logging()
    try:
        conn = init_db()
        config = load_runtime_config(conn)
    except ConfigError as exc:
        log_event(logger, logging.ERROR, "config_error", error=str(exc))
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
        pruned = _prune_legacy_posts_tree(source_dir)
        if pruned:
            log_event(
                logger,
                logging.INFO,
                "legacy_posts_tree_pruned",
                source_dir=source_dir,
                count=pruned,
            )
    lease_seconds = max(
        int(os.environ.get("SV_BUILDER_LEASE_SECONDS", "0") or 0),
        int(config.jobs.lock_timeout_seconds or 0),
    ) or 3600
    job = claim_next_job(
        conn,
        builder_id,
        allowed_types=["build_site"],
        allowed_queues=["build"],
        lock_timeout_seconds=config.jobs.lock_timeout_seconds,
        lease_seconds=lease_seconds,
    )
    if not job:
        return 0

    if is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "build_canceled", job_id=job.id)
        return 0

    log_paths = _build_log_paths(config.paths.logs_dir, job.id)
    source_root = source_dir or _site_root_from_output_dir(config.paths.output_dir)
    _maybe_refresh_feed_archive_days(conn, config, logger)
    _refresh_feed_data_files(conn, config, logger)
    _refresh_feed_index_from_days(source_root, logger)
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
        returncode, stdout, stderr, canceled, cmd = _run_hugo_until_done(
            conn,
            job.id,
            builder_id,
            log_paths,
            lease_seconds,
        )
    except Exception as exc:  # noqa: BLE001
        fail_job(conn, job.id, str(exc))
        log_event(logger, logging.ERROR, "build_failed", job_id=job.id, error=str(exc))
        return 1

    if canceled or is_job_canceled(conn, job.id):
        log_event(logger, logging.INFO, "build_canceled", job_id=job.id)
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
        clear_build_dirty(conn, finished_at=datetime.now(tz=timezone.utc).isoformat(), build_job_id=job.id)
        log_event(logger, logging.INFO, "build_succeeded", job_id=job.id)
    else:
        log_event(logger, logging.ERROR, "build_complete_failed", job_id=job.id)
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
