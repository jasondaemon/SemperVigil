"""Opt-in queue integration for private extractive review, never publication."""
import json
import os
from pathlib import Path

from .event_review import WORKFLOW, _aliases, draft, save, snapshot
from .investigation import EVIDENCE_SCOPE, READ_SCOPE, _text, _version, postgres_reader
from .storage import enqueue_job

JOB_TYPE = "event_review_private"


def enabled() -> bool:
    value = os.environ.get("SV_EVENT_REVIEW_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_review_enablement")
    return value == "1"


def payload_for(event_id: str, aliases: list[str]) -> dict:
    if not _text(event_id, 128) or not event_id.strip():
        raise ValueError("invalid_event_id")
    return {"event_id": event_id, "aliases": _aliases(aliases), "workflow": WORKFLOW}


def submit(connection_factory, *, event_id: str, aliases: list[str]) -> str:
    """Own the admission connection; serialize duplicate requests transactionally.

    This operation is only called behind admin authorization. It neither reads
    source bodies nor generates artifacts on the admin process.
    """
    if not enabled():
        raise PermissionError("private_review_disabled")
    payload = payload_for(event_id, aliases)
    # Serialize every review admission, including queue-size checks, not just
    # requests for one event. This is a low-volume manual pilot operation.
    lock_id = int(_version({"namespace": JOB_TYPE})[:15], 16)
    conn = connection_factory()
    try:
        conn.execute("SET LOCAL lock_timeout = '2s'")
        conn.execute("SET LOCAL statement_timeout = '3s'")
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
        event = conn.execute("SELECT id FROM events WHERE id=%s AND visibility='active'", (event_id,)).fetchone()
        if event is None:
            raise ValueError("event_unavailable")
        pending = conn.execute("""SELECT payload_json FROM jobs WHERE job_type=%s
            AND status IN ('queued','running') LIMIT 11""", (JOB_TYPE,)).fetchall()
        duplicate = False
        for row in pending:
            try:
                duplicate = duplicate or json.loads(row[0] or "null") == payload
            except (ValueError, TypeError):
                pass  # Malformed queued jobs count toward the cap, not deduplication.
        if len(pending) >= 10 and not duplicate:
            raise ValueError("private_review_queue_full")
        job_id = enqueue_job(conn, JOB_TYPE, payload, priority=-10, dedupe=True,
                             queue_name="llm_local", max_attempts=1)
        # enqueue_job commits new inserts, but not its existing-job return path.
        conn.commit()
        return job_id
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def artifact_root() -> Path:
    root = Path(os.environ.get("SV_EVENT_REVIEW_DIR", "/log/event-reviews"))
    logs = Path(os.environ.get("SV_LOG_DIR", "/log"))
    if not root.is_absolute() or not logs.is_absolute():
        raise ValueError("private_review_absolute_path_required")
    root, logs = root.resolve(), logs.resolve()
    if logs == Path("/") or root == logs or not root.is_relative_to(logs):
        raise ValueError("private_review_requires_log_subdirectory")
    forbidden = {Path(p).resolve() for p in (
        "/site", "/site-src", "/site-public", "/data",
        os.environ.get("SV_SITE_SRC_DIR", "/site-src"),
        os.environ.get("SV_SITE_PUBLIC_DIR", "/site-public"),
        os.environ.get("SV_DATA_DIR", "/data"))}
    if any(root == path or root.is_relative_to(path) for path in forbidden):
        raise ValueError("private_review_public_or_data_path")
    return root


def run(payload: dict) -> dict:
    if not enabled():
        return {"status": "skipped", "reason": "private_review_disabled", "public_eligible": False}
    if type(payload) is not dict or payload.keys() != {"event_id", "aliases", "workflow"}:
        raise ValueError("invalid_private_review_payload")
    canonical = payload_for(payload["event_id"], payload["aliases"])
    if payload != canonical:
        raise ValueError("invalid_private_review_payload")
    root = artifact_root()
    dsn = os.environ.get("SV_DB_URL")
    if not dsn:
        raise ValueError("worker_database_required")
    packet = snapshot(lambda: postgres_reader(dsn), event_id=payload["event_id"],
                      aliases=payload["aliases"], scopes=frozenset({READ_SCOPE, EVIDENCE_SCOPE}))
    page = save(packet, root)
    return {"status": "review_ready", "event_id": payload["event_id"],
            "workflow": WORKFLOW, "packet_version": packet["packet_version"],
            "artifact": str(page.relative_to(root)), "documents": len(packet["documents"]),
            "passages": len(draft(packet)["passages"]), "omissions": len(packet["omissions"]),
            "links_truncated": packet["links_truncated"], "public_eligible": False}
