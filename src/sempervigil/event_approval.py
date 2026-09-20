"""Explicit human quote approval and queued promotion; no inference or builds."""
import json
import os
import re

import psycopg

from .event_projection import QUALIFICATION, prepare
from .event_review import draft
from .event_revision_store import locked_current_snapshot, source_version
from .event_review_jobs import read_material
from .investigation import _version
from .storage import enqueue_job
from .utils import utc_now_iso

JOB_TYPE = "event_promote_reviewed"
CONFIRMATION = "APPROVE_ATTRIBUTED_QUOTES"
POLICY = {"workflow": "human-event-quote-review-v1", "assertion": "attributed_quotation",
          "scope": "human_confirmed", "date": "unknown", "origin_independence": "unknown"}
MAX_APPROVAL_BYTES = 3100000
SCHEMA = """
CREATE TABLE event_review_approvals (
    approval_id TEXT PRIMARY KEY CHECK (approval_id ~ '^[0-9a-f]{64}$'),
    event_id TEXT NOT NULL,
    qualification_id TEXT NOT NULL,
    approval_json TEXT NOT NULL CHECK (octet_length(approval_json) <= 3100000),
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
    recorded_at TEXT NOT NULL,
    FOREIGN KEY(event_id,qualification_id)
        REFERENCES event_quote_qualifications(event_id,qualification_id)
);
CREATE FUNCTION guard_event_review_approval() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'event review approvals are immutable';
END;
$$;
CREATE TRIGGER event_review_approval_guard BEFORE UPDATE OR DELETE ON event_review_approvals
    FOR EACH ROW EXECUTE FUNCTION guard_event_review_approval()
"""


def enabled() -> bool:
    value = os.environ.get("SV_EVENT_HUMAN_APPROVAL_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_approval_enablement")
    return value == "1"


def connection_factory(variable: str):
    dsn = os.environ.get(variable, "")
    if not dsn:
        raise PermissionError("event_approval_database_required")
    return lambda: psycopg.connect(dsn, connect_timeout=3)


def candidate(job) -> dict:
    packet, receipt = read_material(job)
    if (receipt["generation_version"] is None or receipt["assessment"].get("scope") is None
            or packet["omissions"] or packet["links_truncated"]):
        raise ValueError("complete_scoped_review_required")
    return {"packet": packet, "receipt": receipt, "passages": draft(packet)["passages"]}


def approval_for(job, *, revision_id: str, passage_ids: list[str],
                 expected_predecessor: str | None, confirmation: str) -> dict:
    """Resolve selections server-side. Model includes never become approval."""
    if (confirmation != CONFIRMATION or type(passage_ids) is not list
            or not 1 <= len(passage_ids) <= 12
            or any(type(v) is not str or not re.fullmatch(r"[0-9a-f]{64}", v) for v in passage_ids)
            or len(set(passage_ids)) != len(passage_ids)
            or type(revision_id) is not str or not re.fullmatch(r"[0-9a-f]{64}", revision_id)
            or (expected_predecessor is not None and (type(expected_predecessor) is not str
                or not re.fullmatch(r"[0-9a-f]{64}", expected_predecessor)))):
        raise ValueError("invalid_event_approval")
    material = candidate(job)
    packet, receipt = material["packet"], material["receipt"]
    if revision_id != _version(receipt):
        raise ValueError("event_approval_revision_changed")
    passages = {p["id"]: p for p in material["passages"]}
    if set(passage_ids) - passages.keys():
        raise ValueError("event_approval_passage_unavailable")
    quotes = [{key: passages[p][key] for key in ("article_id", "start", "end", "quote")}
              for p in sorted(passage_ids)]
    scope = receipt["assessment"]["scope"]
    qualification = {"workflow": QUALIFICATION, "event_id": packet["event"]["id"],
        "source_version": source_version(packet), "scope_version": scope["scope_version"],
        "reviewer": {"kind": "human", "id": "authenticated-admin", "version": _version(POLICY)},
        "quotes": sorted(quotes, key=lambda q: (q["article_id"], q["start"]))}
    prepare(packet, scope, qualification, trusted_qualification_ids=frozenset({_version(qualification)}),
            predecessor=expected_predecessor)
    return {"workflow": "event-human-approval-v1", "review_job_id": job.id,
            "receipt": receipt, "packet": packet, "scope": scope,
            "qualification": qualification, "predecessor": expected_predecessor}


def submit(factory, job, **selection) -> dict:
    """Atomically record human authority and its queued task, with a fixed cap.

The admin records a bounded explicit decision only. The worker rechecks current
evidence and promotes using a principal unable to mint qualification records.
"""
    if not enabled():
        raise PermissionError("event_approval_disabled")
    approval = approval_for(job, **selection)
    return record_approval(factory, approval)


def record_approval(factory, approval: dict, *, authority_check=None) -> dict:
    """Persist a server-qualified decision through the restricted admission role.

    Not an API: callers must independently qualify decisions before entering here.
    """
    raw = json.dumps(approval, sort_keys=True, ensure_ascii=True)
    if len(raw.encode()) > MAX_APPROVAL_BYTES:
        raise ValueError("event_approval_too_large")
    identity, q = _version(approval), approval["qualification"]
    qid, event_id = _version(q), q["event_id"]
    with locked_current_snapshot(factory, approval["packet"]) as conn:
        if authority_check is not None:
            authority_check(conn)
        # Admission cannot promote its own approval.
        writable = conn.execute("""SELECT
            has_table_privilege(current_user,'event_public_pointers','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_public_pointers','INSERT,UPDATE')
            OR has_table_privilege(current_user,'event_public_revisions','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_public_revisions','INSERT,UPDATE')""").fetchone()[0]
        if writable:
            raise PermissionError("approval_admission_role_required")
        conn.execute("SET LOCAL lock_timeout='2s'")
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (int(_version({"namespace": JOB_TYPE})[:15], 16),))
        prior = conn.execute("SELECT approval_json,job_id FROM event_review_approvals WHERE approval_id=%s",
                             (identity,)).fetchone()
        if prior is not None:
            if prior[0] != raw:
                raise ValueError("event_approval_integrity_failure")
            return {"approval_id": identity, "job_id": prior[1], "status": "reused", "public_eligible": False}
        pointer = conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id=%s", (event_id,)).fetchone()
        if (pointer[0] if pointer else None) != approval["predecessor"]:
            raise ValueError("publication_predecessor_conflict")
        pending = conn.execute("SELECT id FROM jobs WHERE job_type=%s AND status IN ('queued','running') LIMIT 10",
                               (JOB_TYPE,)).fetchall()
        if len(pending) >= 10:
            raise ValueError("event_approval_queue_full")
        qraw = json.dumps(q, sort_keys=True, ensure_ascii=True)
        conn.execute("""INSERT INTO event_quote_qualifications
            (event_id,qualification_id,qualification_json,recorded_at) VALUES (%s,%s,%s,%s)
            ON CONFLICT(event_id,qualification_id) DO NOTHING""", (event_id, qid, qraw, utc_now_iso()))
        existing = conn.execute("SELECT qualification_json,revoked_at FROM event_quote_qualifications WHERE event_id=%s AND qualification_id=%s",
                                (event_id, qid)).fetchone()
        if existing != (qraw, None):
            raise ValueError("qualification_unavailable")
        job_id = enqueue_job(conn, JOB_TYPE, {"approval_id": identity}, priority=-10,
                             queue_name="fetch", max_attempts=1, commit=False)
        conn.execute("""INSERT INTO event_review_approvals
            (approval_id,event_id,qualification_id,approval_json,job_id,recorded_at)
            VALUES (%s,%s,%s,%s,%s,%s)""", (identity, event_id, qid, raw, job_id, utc_now_iso()))
    return {"approval_id": identity, "job_id": job_id, "status": "queued", "public_eligible": False}


def run(payload: dict, *, factory=None) -> dict:
    """Promote an immutable approved revision. Export/activation remain separate."""
    if not enabled():
        return {"status": "skipped", "reason": "event_approval_disabled", "public_eligible": False}
    if (type(payload) is not dict or payload.keys() != {"approval_id"}
            or type(payload["approval_id"]) is not str
            or not re.fullmatch(r"[0-9a-f]{64}", payload["approval_id"])):
        raise ValueError("invalid_event_approval_job")
    factory = factory or connection_factory("SV_EVENT_PROMOTION_DB_URL")
    with factory() as conn:
        conn.execute("SET LOCAL statement_timeout='3s'")
        writable = conn.execute("""SELECT
            has_table_privilege(current_user,'event_review_approvals','INSERT,UPDATE,DELETE,TRUNCATE')
            OR has_any_column_privilege(current_user,'event_review_approvals','INSERT,UPDATE')""").fetchone()[0]
        if writable:
            raise PermissionError("approval_read_only_role_required")
        row = conn.execute("SELECT approval_json,event_id,qualification_id FROM event_review_approvals WHERE approval_id=%s",
                           (payload["approval_id"],)).fetchone()
        if row is None:
            raise ValueError("event_approval_unavailable")
        approval = json.loads(row[0])
        if (_version(approval) != payload["approval_id"]
                or approval["qualification"]["event_id"] != row[1]
                or _version(approval["qualification"]) != row[2]):
            raise ValueError("event_approval_integrity_failure")
    if approval.get("workflow") == "event-composition-publication-approval-v1":
        from .event_composition_publication import run_approval
        return {**run_approval(approval, qualification_id=row[2], factory=factory),
                "approval_id": payload["approval_id"]}
    from .event_publication_store import promote
    result = promote(factory, approval["packet"], approval["scope"], qualification_id=row[2],
                     expected_predecessor=approval["predecessor"])
    return {**result, "approval_id": payload["approval_id"],
            "publication_status": "awaiting_export", "public_eligible": False}
