"""Durable, non-public article evidence revisions and explicit review decisions."""
import json

from . import article_evidence as evidence
from .investigation import _version
from .storage import get_article_by_id
from .utils import utc_now_iso

DECISIONS = {"accept", "hold", "reject"}


def _decode(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("article_evidence_stored_shape_invalid")
    return value


def store_unreviewed(conn, article: dict, candidate: dict) -> str:
    canonical = evidence.validate_context_record(candidate, article)
    if canonical["status"] != "unreviewed" or canonical["public_eligible"] is not False:
        raise ValueError("article_evidence_candidate_not_private")
    revision_id = "aer_" + _version(canonical)
    encoded = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    cursor = conn.execute(
        """
        INSERT INTO article_evidence_revisions
            (revision_id, article_id, source_version, workflow, generation_version,
             request_version, status, evidence_json, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, 'unreviewed', %s, %s)
        ON CONFLICT (article_id, source_version, generation_version, request_version)
        DO NOTHING
        RETURNING revision_id
        """,
        (revision_id, article["id"], canonical["source_version"], canonical["workflow"],
         canonical["generation_version"], canonical["request_version"], encoded, utc_now_iso()),
    )
    inserted = cursor.fetchone()
    if not inserted:
        stored = conn.execute(
            """
            SELECT revision_id, evidence_json
            FROM article_evidence_revisions
            WHERE article_id=%s AND source_version=%s AND generation_version=%s
              AND request_version=%s
            """,
            (article["id"], canonical["source_version"], canonical["generation_version"],
             canonical["request_version"]),
        ).fetchone()
        if not stored or stored[1] != encoded:
            raise ValueError("article_evidence_revision_conflict")
        revision_id = stored[0]
    conn.commit()
    return revision_id


def review(conn, revision_id: str, decision: str, *, reason: str, reviewer: str) -> dict:
    if decision not in DECISIONS:
        raise ValueError("article_evidence_review_decision_invalid")
    if not revision_id.startswith("aer_") or not reviewer.strip() or len(reviewer) > 80:
        raise ValueError("article_evidence_review_identity_invalid")
    reason = reason.strip()
    if decision != "accept" and not reason:
        raise ValueError("article_evidence_review_reason_required")
    if len(reason) > 1000:
        raise ValueError("article_evidence_review_reason_too_long")
    row = conn.execute(
        """
        SELECT article_id, source_version, status, evidence_json
        FROM article_evidence_revisions
        WHERE revision_id=%s
        FOR UPDATE
        """,
        (revision_id,),
    ).fetchone()
    if not row:
        raise ValueError("article_evidence_revision_missing")
    article_id, source_version, status, raw = row
    if status not in {"unreviewed", "held"}:
        raise ValueError("article_evidence_revision_already_decided")
    article = get_article_by_id(conn, article_id)
    if not article:
        raise ValueError("article_evidence_source_missing")
    record = evidence.validate_context_record(_decode(raw), article)
    if record["source_version"] != source_version:
        raise ValueError("article_evidence_revision_tampered")
    now = utc_now_iso()
    if decision == "accept":
        conn.execute(
            """
            UPDATE article_evidence_revisions
            SET status='superseded', reviewed_at=%s, reviewed_by=%s,
                review_reason='superseded by accepted revision',
                superseded_by_revision_id=%s
            WHERE article_id=%s AND status='accepted' AND revision_id<>%s
            """,
            (now, reviewer, revision_id, article_id, revision_id),
        )
        next_status = "accepted"
    else:
        next_status = "held" if decision == "hold" else "rejected"
    changed = conn.execute(
        """
        UPDATE article_evidence_revisions
        SET status=%s, reviewed_at=%s, reviewed_by=%s, review_reason=%s
        WHERE revision_id=%s AND status IN ('unreviewed', 'held')
        """,
        (next_status, now, reviewer, reason or None, revision_id),
    )
    if changed.rowcount != 1:
        raise ValueError("article_evidence_review_conflict")
    conn.commit()
    return {"revision_id": revision_id, "article_id": article_id, "status": next_status,
            "source_version": source_version}


def list_revisions(conn, *, status: str = "unreviewed", limit: int = 50) -> list[dict]:
    allowed = {"unreviewed", "accepted", "held", "rejected", "superseded", "all"}
    if status not in allowed or not 1 <= limit <= 200:
        raise ValueError("article_evidence_list_invalid")
    where = "" if status == "all" else "WHERE r.status=%s"
    params = [] if status == "all" else [status]
    rows = conn.execute(
        f"""
        SELECT r.revision_id, r.article_id, a.title, r.source_version, r.workflow,
               r.generation_version, r.request_version, r.status, r.evidence_json,
               r.created_at, r.reviewed_at, r.reviewed_by, r.review_reason
        FROM article_evidence_revisions r
        JOIN articles a ON a.id=r.article_id
        {where}
        ORDER BY r.created_at DESC, r.revision_id DESC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    return [{"revision_id": row[0], "article_id": row[1], "title": row[2],
             "source_version": row[3], "workflow": row[4], "generation_version": row[5],
             "request_version": row[6], "status": row[7], "evidence": _decode(row[8]),
             "created_at": row[9], "reviewed_at": row[10], "reviewed_by": row[11],
             "review_reason": row[12]} for row in rows]
