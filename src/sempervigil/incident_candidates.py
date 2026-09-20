"""Private incident candidates derived only from accepted article evidence."""
import json
import re

from .investigation import _version
from .utils import utc_now_iso

PROJECTION_VERSION = "accepted-evidence-candidate-v2"
DECISIONS = {"enroll", "hold", "reject"}
_CVE = re.compile(r"\bCVE-\d{4}-\d{4,}\b", re.IGNORECASE)
_CAMPAIGN = re.compile(r"\bcampaign (?:known as|called) [\"']([^\"']+)[\"']", re.IGNORECASE)
_INCIDENT_CUES = (
    "attack began", "attacked", "breach", "breached", "compromise", "compromised",
    "exfiltrat", "extortion", "infected", "intrusion", "ransomware", "stole", "stolen",
)


def _decode(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("incident_candidate_evidence_invalid")
    return value


def projection(record: dict, title: str) -> dict | None:
    if record.get("status") != "unreviewed" or record.get("public_eligible") is not False:
        raise ValueError("incident_candidate_evidence_shape_invalid")
    facts = record.get("facts")
    if not isinstance(facts, list) or not facts:
        raise ValueError("incident_candidate_evidence_shape_invalid")
    statements = [str(fact.get("statement") or "").strip() for fact in facts]
    lowered = " ".join(statements).lower()
    supporting = [
        str(fact.get("id") or "") for fact in facts
        if fact.get("date_role") == "incident"
        or any(cue in str(fact.get("statement") or "").lower() for cue in _INCIDENT_CUES)
    ]
    supporting = list(dict.fromkeys(item for item in supporting if item))
    if not supporting or not any(cue in lowered for cue in _INCIDENT_CUES):
        return None
    if "ransomware" in lowered:
        kind = "ransomware"
    elif "campaign" in lowered:
        kind = "campaign"
    elif "breach" in lowered:
        kind = "breach"
    elif "intrusion" in lowered:
        kind = "intrusion"
    else:
        kind = "compromise"
    incident_dates = list(dict.fromkeys(
        str(fact.get("date_text")) for fact in facts
        if fact.get("date_role") == "incident" and fact.get("date_text")
    ))
    cves = sorted({match.group(0).upper() for statement in statements for match in _CVE.finditer(statement)})
    campaigns = sorted({match.group(1).strip().rstrip(" ,;:.")
                        for statement in statements for match in _CAMPAIGN.finditer(statement)})
    return {
        "projection_version": PROJECTION_VERSION,
        "title": title.strip(),
        "kind": kind,
        "incident_dates": incident_dates,
        "campaigns": campaigns,
        "cves": cves,
        "supporting_fact_ids": supporting,
    }


def project(conn, revision_id: str) -> dict:
    row = conn.execute(
        """
        SELECT r.article_id, r.status, r.evidence_json, a.title
        FROM article_evidence_revisions r
        JOIN articles a ON a.id=r.article_id
        WHERE r.revision_id=%s
        """,
        (revision_id,),
    ).fetchone()
    if not row:
        raise ValueError("incident_candidate_evidence_missing")
    article_id, status, raw, title = row
    if status != "accepted":
        raise ValueError("incident_candidate_evidence_not_accepted")
    signals = projection(_decode(raw), str(title or ""))
    if signals is None:
        return {"status": "skipped", "reason": "no_incident_signal",
                "revision_id": revision_id, "article_id": article_id}
    candidate_id = "ic_" + _version({"revision_id": revision_id, **signals})
    encoded = json.dumps(signals, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    existing = conn.execute(
        """
        SELECT candidate_id, status, signals_json
        FROM incident_candidates WHERE evidence_revision_id=%s
        FOR UPDATE
        """,
        (revision_id,),
    ).fetchone()
    if existing and existing[2] != encoded:
        if existing[1] != "suggested":
            raise ValueError("incident_candidate_projection_conflict")
        conn.execute(
            """
            UPDATE incident_candidates
            SET candidate_id=%s, kind=%s, title=%s, signals_json=%s, created_at=%s
            WHERE evidence_revision_id=%s AND status='suggested'
            """,
            (candidate_id, signals["kind"], signals["title"], encoded, utc_now_iso(), revision_id),
        )
    conn.execute(
        """
        INSERT INTO incident_candidates
            (candidate_id, evidence_revision_id, article_id, status, kind, title,
             signals_json, created_at)
        VALUES (%s, %s, %s, 'suggested', %s, %s, %s, %s)
        ON CONFLICT (evidence_revision_id) DO NOTHING
        """,
        (candidate_id, revision_id, article_id, signals["kind"], signals["title"],
         encoded, utc_now_iso()),
    )
    stored = conn.execute(
        """
        SELECT candidate_id, status, signals_json
        FROM incident_candidates WHERE evidence_revision_id=%s
        """,
        (revision_id,),
    ).fetchone()
    if not stored or stored[2] != encoded:
        raise ValueError("incident_candidate_projection_conflict")
    conn.commit()
    return {"candidate_id": stored[0], "revision_id": revision_id,
            "article_id": article_id, "status": stored[1], "signals": signals,
            "public_eligible": False}


def review(conn, candidate_id: str, decision: str, *, reason: str, reviewer: str) -> dict:
    if decision not in DECISIONS:
        raise ValueError("incident_candidate_decision_invalid")
    if not candidate_id.startswith("ic_") or not reviewer.strip() or len(reviewer) > 80:
        raise ValueError("incident_candidate_identity_invalid")
    reason = reason.strip()
    if decision != "enroll" and not reason:
        raise ValueError("incident_candidate_reason_required")
    if len(reason) > 1000:
        raise ValueError("incident_candidate_reason_too_long")
    current = conn.execute(
        """
        SELECT c.status, r.status
        FROM incident_candidates c
        JOIN article_evidence_revisions r ON r.revision_id=c.evidence_revision_id
        WHERE c.candidate_id=%s
        FOR UPDATE
        """,
        (candidate_id,),
    ).fetchone()
    if not current or current[0] not in {"suggested", "held"}:
        raise ValueError("incident_candidate_missing_or_decided")
    if current[1] != "accepted":
        raise ValueError("incident_candidate_evidence_not_accepted")
    next_status = "enrolled" if decision == "enroll" else ("held" if decision == "hold" else "rejected")
    changed = conn.execute(
        """
        UPDATE incident_candidates
        SET status=%s, reviewed_at=%s, reviewed_by=%s, review_reason=%s
        WHERE candidate_id=%s AND status IN ('suggested', 'held')
        """,
        (next_status, utc_now_iso(), reviewer, reason or None, candidate_id),
    )
    if changed.rowcount != 1:
        raise ValueError("incident_candidate_missing_or_decided")
    conn.commit()
    return {"candidate_id": candidate_id, "status": next_status, "public_eligible": False}


def list_candidates(conn, *, status: str = "suggested", limit: int = 50) -> list[dict]:
    allowed = {"suggested", "held", "rejected", "enrolled", "all"}
    if status not in allowed or not 1 <= limit <= 200:
        raise ValueError("incident_candidate_list_invalid")
    where = "" if status == "all" else "WHERE c.status=%s"
    params = [] if status == "all" else [status]
    rows = conn.execute(
        f"""
        SELECT c.candidate_id, c.evidence_revision_id, c.article_id, c.status,
               c.kind, c.title, c.signals_json, c.created_at, c.reviewed_at,
               c.reviewed_by, c.review_reason, r.status
        FROM incident_candidates c
        JOIN article_evidence_revisions r ON r.revision_id=c.evidence_revision_id
        {where}
        ORDER BY c.created_at DESC, c.candidate_id DESC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    return [{"candidate_id": row[0], "evidence_revision_id": row[1],
             "article_id": row[2], "status": row[3], "kind": row[4],
             "title": row[5], "signals": _decode(row[6]), "created_at": row[7],
             "reviewed_at": row[8], "reviewed_by": row[9], "review_reason": row[10]}
            | {"evidence_status": row[11], "eligible_for_curation": row[11] == "accepted"}
            for row in rows]
