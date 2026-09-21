"""Private incident candidates derived only from accepted article evidence."""
import json
import re

from .investigation import _version
from .event_fact_roles import validate as validate_sections
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


def project(conn, revision_id: str, *, event_id: str | None = None) -> dict:
    if event_id is not None and (not event_id.startswith("evt_") or len(event_id) > 128):
        raise ValueError("incident_candidate_event_invalid")
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
    identity = {"revision_id": revision_id, **signals}
    if event_id is not None:
        identity["event_id"] = event_id
    candidate_id = "ic_" + _version(identity)
    encoded = json.dumps(signals, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    existing = conn.execute(
        """
        SELECT candidate_id, status, signals_json
        FROM incident_candidates WHERE evidence_revision_id=%s
          AND event_id IS NOT DISTINCT FROM %s
        FOR UPDATE
        """,
        (revision_id, event_id),
    ).fetchone()
    if existing and existing[2] != encoded:
        if existing[1] != "suggested":
            raise ValueError("incident_candidate_projection_conflict")
        conn.execute(
            """
            UPDATE incident_candidates
            SET candidate_id=%s, kind=%s, title=%s, signals_json=%s, created_at=%s
            WHERE evidence_revision_id=%s AND event_id IS NOT DISTINCT FROM %s
              AND status='suggested'
            """,
            (candidate_id, signals["kind"], signals["title"], encoded, utc_now_iso(),
             revision_id, event_id),
        )
    conn.execute(
        """
        INSERT INTO incident_candidates
            (candidate_id, evidence_revision_id, article_id, event_id, status, kind,
             title, signals_json, created_at)
        VALUES (%s, %s, %s, %s, 'suggested', %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (candidate_id, revision_id, article_id, event_id, signals["kind"], signals["title"],
         encoded, utc_now_iso()),
    )
    stored = conn.execute(
        """
        SELECT candidate_id, status, signals_json
        FROM incident_candidates WHERE evidence_revision_id=%s
          AND event_id IS NOT DISTINCT FROM %s
        """,
        (revision_id, event_id),
    ).fetchone()
    if not stored or stored[2] != encoded:
        raise ValueError("incident_candidate_projection_conflict")
    conn.commit()
    return {"candidate_id": stored[0], "revision_id": revision_id,
            "article_id": article_id, "event_id": event_id,
            "status": stored[1], "signals": signals,
            "public_eligible": False}


def review(conn, candidate_id: str, decision: str, *, reason: str, reviewer: str,
           selected_fact_ids: list[str] | None = None,
           fact_sections: dict[str, list[str]] | None = None) -> dict:
    if decision not in DECISIONS:
        raise ValueError("incident_candidate_decision_invalid")
    if not candidate_id.startswith("ic_") or not reviewer.strip() or len(reviewer) > 80:
        raise ValueError("incident_candidate_identity_invalid")
    reason = reason.strip()
    if decision != "enroll" and not reason:
        raise ValueError("incident_candidate_reason_required")
    if len(reason) > 1000:
        raise ValueError("incident_candidate_reason_too_long")
    if (selected_fact_ids is None) != (fact_sections is None):
        raise ValueError("incident_candidate_fact_selection_invalid")
    if (selected_fact_ids is not None or fact_sections is not None) and decision != "enroll":
        raise ValueError("incident_candidate_fact_selection_invalid")
    current = conn.execute(
        """
        SELECT c.status, r.status, r.evidence_json
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
    selected_json = sections_json = None
    if decision == "enroll" and selected_fact_ids is not None:
        if not selected_fact_ids:
            raise ValueError("incident_candidate_fact_selection_required")
        if (len(selected_fact_ids) > 100 or len(set(selected_fact_ids)) != len(selected_fact_ids)
                or any(not isinstance(item, str) or not item for item in selected_fact_ids)):
            raise ValueError("incident_candidate_fact_selection_invalid")
        evidence = _decode(current[2])
        facts = {str(item.get("id") or ""): item for item in evidence.get("facts", [])}
        known = set(facts)
        if not set(selected_fact_ids) <= known:
            raise ValueError("incident_candidate_fact_selection_invalid")
        assignments = [{"fact_id": fact_id, "sections": sections}
                       for fact_id, sections in (fact_sections or {}).items()]
        normalized = validate_sections(facts, set(selected_fact_ids), assignments)
        selected_json = json.dumps(sorted(selected_fact_ids), separators=(",", ":"))
        sections_json = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    next_status = "enrolled" if decision == "enroll" else ("held" if decision == "hold" else "rejected")
    changed = conn.execute(
        """
        UPDATE incident_candidates
        SET status=%s, reviewed_at=%s, reviewed_by=%s, review_reason=%s,
            selected_fact_ids_json=%s, selected_fact_sections_json=%s
        WHERE candidate_id=%s AND status IN ('suggested', 'held')
        """,
        (next_status, utc_now_iso(), reviewer, reason or None, selected_json, sections_json, candidate_id),
    )
    if changed.rowcount != 1:
        raise ValueError("incident_candidate_missing_or_decided")
    conn.commit()
    return {"candidate_id": candidate_id, "status": next_status,
            "selected_fact_ids": sorted(selected_fact_ids or []),
            "fact_sections": json.loads(sections_json) if sections_json else {},
            "public_eligible": False}


def refine_selection(conn, candidate_id: str, selected_fact_ids: list[str], *,
                     fact_sections: dict[str, list[str]], reason: str, reviewer: str) -> dict:
    """Replace an enrolled candidate's selection with a newer curated decision."""
    if (not candidate_id.startswith("ic_") or not reviewer.strip() or len(reviewer) > 80
            or not reason.strip() or len(reason.strip()) > 1000
            or not selected_fact_ids or len(selected_fact_ids) > 100
            or len(set(selected_fact_ids)) != len(selected_fact_ids)
            or not isinstance(fact_sections, dict)
            or any(not isinstance(item, str) or not item for item in selected_fact_ids)):
        raise ValueError("incident_candidate_fact_selection_invalid")
    row = conn.execute(
        """SELECT c.status,c.selected_fact_ids_json,r.status,r.evidence_json,
                  c.selected_fact_sections_json
             FROM incident_candidates c JOIN article_evidence_revisions r
               ON r.revision_id=c.evidence_revision_id
            WHERE c.candidate_id=%s FOR UPDATE""",
        (candidate_id,),
    ).fetchone()
    if not row or row[0] != "enrolled" or row[2] != "accepted":
        raise ValueError("incident_candidate_missing_or_decided")
    selected = sorted(selected_fact_ids)
    facts = {str(item.get("id") or ""): item for item in _decode(row[3]).get("facts", [])}
    assignments = [{"fact_id": fact_id, "sections": sections}
                   for fact_id, sections in fact_sections.items()]
    normalized = validate_sections(facts, set(selected), assignments)
    encoded_sections = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    if row[1] and json.loads(row[1]) == selected and row[4] == encoded_sections:
        return {"candidate_id": candidate_id, "status": "enrolled",
                "selected_fact_ids": selected, "fact_sections": normalized,
                "reused": True, "public_eligible": False}
    known = set(facts)
    if not set(selected) <= known:
        raise ValueError("incident_candidate_fact_selection_invalid")
    conn.execute(
        """UPDATE incident_candidates SET selected_fact_ids_json=%s,
                  selected_fact_sections_json=%s,reviewed_at=%s,
                  reviewed_by=%s,review_reason=%s
            WHERE candidate_id=%s AND status='enrolled'""",
        (json.dumps(selected, separators=(",", ":")), encoded_sections, utc_now_iso(), reviewer,
         reason.strip(), candidate_id),
    )
    conn.commit()
    return {"candidate_id": candidate_id, "status": "enrolled",
            "selected_fact_ids": selected, "fact_sections": normalized,
            "reused": False, "public_eligible": False}


def revise_enrollment(conn, candidate_id: str, decision: str, *,
                      reason: str, reviewer: str) -> dict:
    """Conservatively remove a previously enrolled source after newer curation."""
    if (decision not in {"hold", "reject"} or not candidate_id.startswith("ic_")
            or not reviewer.strip() or len(reviewer) > 80 or not reason.strip()
            or len(reason.strip()) > 1000):
        raise ValueError("incident_candidate_revision_invalid")
    status = "held" if decision == "hold" else "rejected"
    changed = conn.execute(
        """UPDATE incident_candidates SET status=%s,selected_fact_ids_json=NULL,
                  selected_fact_sections_json=NULL,reviewed_at=%s,reviewed_by=%s,
                  review_reason=%s WHERE candidate_id=%s AND status='enrolled'""",
        (status, utc_now_iso(), reviewer, reason.strip(), candidate_id),
    )
    if changed.rowcount != 1:
        raise ValueError("incident_candidate_missing_or_decided")
    conn.commit()
    return {"candidate_id": candidate_id, "status": status,
            "selected_fact_ids": [], "fact_sections": {}, "public_eligible": False}


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
               c.reviewed_by, c.review_reason, r.status, r.evidence_json,
               c.selected_fact_ids_json, c.event_id, c.selected_fact_sections_json
        FROM incident_candidates c
        JOIN article_evidence_revisions r ON r.revision_id=c.evidence_revision_id
        {where}
        ORDER BY c.created_at DESC, c.candidate_id DESC
        LIMIT %s
        """,
        (*params, limit),
    ).fetchall()
    result = []
    for row in rows:
        evidence = _decode(row[12])
        selected = json.loads(row[13]) if row[13] else None
        sections = json.loads(row[15]) if row[15] else None
        result.append({"candidate_id": row[0], "evidence_revision_id": row[1],
             "article_id": row[2], "status": row[3], "kind": row[4],
             "title": row[5], "signals": _decode(row[6]), "created_at": row[7],
             "reviewed_at": row[8], "reviewed_by": row[9], "review_reason": row[10]}
            | {"evidence_status": row[11], "eligible_for_curation": row[11] == "accepted",
               "selected_fact_ids": selected, "event_id": row[14],
               "fact_sections": sections,
               "facts": [{key: fact.get(key) for key in
                           ("id", "statement", "kind", "date_text", "date_role")}
                          for fact in evidence.get("facts", [])]})
    return result
