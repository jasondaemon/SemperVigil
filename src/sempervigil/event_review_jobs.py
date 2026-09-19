"""Opt-in queue integration for private extractive review, never publication."""
import json
import os
import hashlib
import re
import stat
from pathlib import Path

from .event_review import WORKFLOW, _aliases, draft, save, snapshot
from .investigation import EVIDENCE_SCOPE, READ_SCOPE, _text, _version, postgres_reader
from .storage import enqueue_job

JOB_TYPE = "event_review_private"
MAX_REVIEW_BYTES = 16 * 1024 * 1024


def read_artifact(job) -> bytes:
    """Read only the immutable HTML named by a completed private job.

    Open directory/file descriptors without following symlinks so validation
    cannot race a path replacement. Never accept a client-supplied path.
    """
    result = job.result
    if (job.job_type != JOB_TYPE or job.status != "succeeded"
            or not isinstance(result, dict) or result.get("status") != "review_ready"
            or result.get("workflow") != WORKFLOW or result.get("public_eligible") is not False):
        raise ValueError("private_review_unavailable")
    version = result.get("packet_version")
    artifact = result.get("artifact")
    if not isinstance(version, str) or not re.fullmatch(r"[0-9a-f]{64}", version):
        raise ValueError("private_review_unavailable")
    if not isinstance(artifact, str) or not re.fullmatch(
            re.escape(version) + r"/review-[0-9a-f]{16}\.html", artifact):
        raise ValueError("private_review_unavailable")
    root_fd = os.open(artifact_root(), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        folder_fd = os.open(version, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        try:
            name = artifact.split("/")[1]
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder_fd)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_REVIEW_BYTES:
                    raise ValueError("private_review_unavailable")
                data = stream.read(MAX_REVIEW_BYTES + 1)
                if len(data) > MAX_REVIEW_BYTES or hashlib.sha256(data).hexdigest()[:16] != name[7:-5]:
                    raise ValueError("private_review_unavailable")
                return data
        finally:
            os.close(folder_fd)
    finally:
        os.close(root_fd)


def read_revision(job) -> bytes:
    """Resolve a job-owned receipt without accepting a client filesystem path."""
    from .event_revision import MAX_BYTES, validate_receipt
    html = read_artifact(job)
    descriptor = job.result.get("private_revision")
    version = descriptor.get("version") if isinstance(descriptor, dict) else None
    if type(version) is not str or not re.fullmatch(r"[0-9a-f]{64}", version):
        raise ValueError("private_revision_unavailable")
    root_fd = os.open(artifact_root(), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        folder = os.open(job.result["packet_version"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                         dir_fd=root_fd)
        try:
            fd = os.open("revision-" + version + ".json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=folder)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_BYTES:
                    raise ValueError("private_revision_unavailable")
                raw = stream.read(MAX_BYTES + 1)
            validate_receipt(raw, descriptor, packet_version=job.result["packet_version"],
                             event_id=job.result.get("event_id"),
                             artifact=job.result["artifact"].split("/")[1], html=html)
            return raw
        finally:
            os.close(folder)
    finally:
        os.close(root_fd)


def enabled() -> bool:
    value = os.environ.get("SV_EVENT_REVIEW_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_review_enablement")
    return value == "1"


def read_material(job) -> tuple[dict, dict]:
    """Read receipt-bound source evidence, never a client-supplied file path."""
    from .event_review import MAX_PACKET_BYTES, validate_packet
    from .event_assessment import validate_assessment
    receipt = json.loads(read_revision(job))
    root = os.open(artifact_root(), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        folder = os.open(receipt["packet_version"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                         dir_fd=root)
        try:
            fd = os.open("packet.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=folder)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_PACKET_BYTES:
                    raise ValueError("private_packet_unavailable")
                packet = validate_packet(stream.read(MAX_PACKET_BYTES + 1))
        finally:
            os.close(folder)
    finally:
        os.close(root)
    if (packet["packet_version"] != receipt["packet_version"]
            or packet["event"]["id"] != receipt["event_id"]):
        raise ValueError("private_packet_mismatch")
    validate_assessment(receipt["assessment"], packet)
    return packet, receipt


def scoped_enabled() -> bool:
    value = os.environ.get("SV_EVENT_REVIEW_SCOPE_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_scope_enablement")
    return value == "1"


def paired_enabled() -> bool:
    value = os.environ.get("SV_EVENT_REVIEW_PAIR_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_pair_enablement")
    return value == "1"


def deconstruction_enabled() -> bool:
    value = os.environ.get("SV_EVENT_DECONSTRUCTION_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_deconstruction_enablement")
    return value == "1"


def payload_for(event_id: str, aliases: list[str], *, scope: dict | None = None,
                article_id: int | None = None, paired: bool = False, deconstruct: bool = False) -> dict:
    if not _text(event_id, 128) or not event_id.strip():
        raise ValueError("invalid_event_id")
    payload = {"event_id": event_id, "aliases": _aliases(aliases), "workflow": WORKFLOW}
    if scope is not None:
        from .event_scope import declaration
        payload["scope"] = declaration(scope, event_id)
    if article_id is not None:
        if type(article_id) is not int or article_id <= 0 or scope is None:
            raise ValueError("invalid_assessment_source")
        payload["article_id"] = article_id
    if type(paired) is not bool or (paired and article_id is None):
        raise ValueError("invalid_assessment_pair")
    if paired:
        payload["paired"] = True
    if type(deconstruct) is not bool or (deconstruct and (scope is None or article_id is None or paired)):
        raise ValueError("invalid_deconstruction_request")
    if deconstruct:
        payload["deconstruct"] = True
    return payload


def submit(connection_factory, *, event_id: str, aliases: list[str], scope: dict | None = None,
           article_id: int | None = None, paired: bool = False, deconstruct: bool = False) -> str:
    """Own the admission connection; serialize duplicate requests transactionally.

    This operation is only called behind admin authorization. It neither reads
    source bodies nor generates artifacts on the admin process.
    """
    if not enabled():
        raise PermissionError("private_review_disabled")
    if scope is not None and not scoped_enabled():
        raise PermissionError("private_scope_disabled")
    payload = payload_for(event_id, aliases, scope=scope, article_id=article_id, paired=paired,
                          deconstruct=deconstruct)
    if deconstruct and not deconstruction_enabled():
        raise PermissionError("private_deconstruction_disabled")
    if paired and not paired_enabled():
        raise PermissionError("private_pair_disabled")
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


def run(payload: dict, *, complete=None) -> dict:
    if not enabled():
        return {"status": "skipped", "reason": "private_review_disabled", "public_eligible": False}
    if type(payload) is not dict or payload.keys() not in (
            {"event_id", "aliases", "workflow"}, {"event_id", "aliases", "workflow", "scope"},
            {"event_id", "aliases", "workflow", "scope", "article_id"},
            {"event_id", "aliases", "workflow", "scope", "article_id", "deconstruct"},
            {"event_id", "aliases", "workflow", "scope", "article_id", "paired"}):
        raise ValueError("invalid_private_review_payload")
    scope = payload.get("scope")
    if "scope" in payload:
        if not scoped_enabled():
            raise PermissionError("private_scope_disabled")
        if scope is None or complete is None:
            raise ValueError("private_scope_model_required")
    article_id = payload.get("article_id")
    paired = payload.get("paired", False)
    deconstruct = payload.get("deconstruct", False)
    canonical = payload_for(payload["event_id"], payload["aliases"], scope=scope, article_id=article_id,
                            paired=paired, deconstruct=deconstruct)
    if payload != canonical:
        raise ValueError("invalid_private_review_payload")
    if paired and not paired_enabled():
        raise PermissionError("private_pair_disabled")
    if deconstruct and not deconstruction_enabled():
        raise PermissionError("private_deconstruction_disabled")
    root = artifact_root()
    dsn = os.environ.get("SV_DB_URL")
    if not dsn:
        raise ValueError("worker_database_required")
    packet = snapshot(lambda: postgres_reader(dsn), event_id=payload["event_id"],
                      aliases=payload["aliases"], scopes=frozenset({READ_SCOPE, EVIDENCE_SCOPE}))
    if deconstruct:
        from .event_deconstruction import save as save_deconstruction
        page, result = save_deconstruction(packet, scope, article_id, complete, root)
        return {"status": "review_ready", "event_id": payload["event_id"],
                "workflow": WORKFLOW, "packet_version": packet["packet_version"],
                "artifact": str(page.relative_to(root)), "model_assessed": True,
                "model_cache_hit": result["cache_hit"], "public_eligible": False,
                "deconstruction": {"workflow": result["workflow"], "article_id": article_id,
                                    "claims": len(result["claims"]), "status": "unreviewed",
                                    "compilation_revision": result["compilation_revision"],
                                    "coverage": result["coverage"]}}
    cache_hit = False
    assessment = None
    if complete is None:
        page = save(packet, root)
    else:
        from .event_assessment_cache import reuse
        assessment, cache_hit = reuse(packet, complete, root, scope=scope, article_id=article_id, paired=paired)
        page = save(packet, root, assessment=assessment)
    summary = None
    revision = None
    revision_storage = None
    if assessment is not None:
        from .event_revision import save_revision
        revision = save_revision(packet, assessment, getattr(complete, "cache_identity", None), page)
        from .event_revision_store import enabled as revision_store_enabled, persist_if_enabled
        if revision_store_enabled():
            receipt = page.parent / ("revision-" + revision["version"] + ".json")
            revision_storage = persist_if_enabled(dsn, packet, receipt.read_bytes(), revision,
                                                  artifact=page.name, html=page.read_bytes())
        decisions = [item["decision"] for item in assessment["suggestions"].values()]
        summary = {"workflow": assessment["workflow"], "scope_version": (scope or {}).get("scope_version"),
                   "assessed": len(decisions), "not_assessed": assessment["omitted_passages"],
                   "included": decisions.count("include"), "held": decisions.count("hold"),
                   "excluded": decisions.count("exclude"), "status": "proposal_only"}
        if article_id is not None:
            summary["article_id"] = article_id
    return {"status": "review_ready", "event_id": payload["event_id"],
            "workflow": WORKFLOW, "packet_version": packet["packet_version"],
            "artifact": str(page.relative_to(root)), "documents": len(packet["documents"]),
            "passages": len(draft(packet)["passages"]), "omissions": len(packet["omissions"]),
            "links_truncated": packet["links_truncated"],
            "model_assessed": complete is not None and bool(assessment["suggestions"]),
            "model_cache_hit": cache_hit,
            **({"assessment_summary": summary} if summary is not None else {}),
            **({"private_revision": revision} if revision is not None else {}),
            **({"revision_storage": revision_storage} if revision_storage is not None else {}),
            "public_eligible": False}
