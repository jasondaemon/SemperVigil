"""Bounded autonomous progression for confirmed Event reassessments."""
from __future__ import annotations

import json
import os
from urllib.parse import urlparse

import psycopg

from .utils import utc_now_iso

WORKFLOW = "event-reassessment-automation-v1"


def enabled() -> bool:
    value = os.environ.get("SV_EVENT_REASSESSMENT_AUTOMATION_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_reassessment_automation_enablement")
    return value == "1"


def _hold(conn, event_id: str, reason: str) -> dict:
    conn.execute(
        """UPDATE event_reassessment_cases SET status='held',decision_reason=%s,updated_at=%s
            WHERE event_id=%s AND status='active'""", (reason[:1000], utc_now_iso(), event_id),
    )
    conn.commit()
    return {"status": "held", "event_id": event_id, "reason": reason[:160]}


def _touch(conn, event_id: str) -> None:
    conn.execute("UPDATE event_reassessment_cases SET updated_at=%s WHERE event_id=%s",
                 (utc_now_iso(), event_id))
    conn.commit()


def _job_state(conn, job_id: str) -> tuple[str, str]:
    row = conn.execute("SELECT status,COALESCE(error,'') FROM jobs WHERE id=%s", (job_id,)).fetchone()
    return (row[0], row[1]) if row else ("missing", "job unavailable")


def _repairable_composition(conn, ledger_revision_id: str, composition):
    """Return the current composer's audited composition, ignoring legacy derivatives."""
    if (composition[1] != "held"
            or composition[2] != "policy:event-composition-audit-v1"):
        return None
    from .event_composition_jobs import configuration as composition_configuration
    generation = composition_configuration(conn)[2]
    if composition[3] == generation:
        return composition
    return conn.execute(
        """SELECT composition_id,status,reviewed_by,generation_version
            FROM event_ledger_compositions
            WHERE ledger_revision_id=%s AND status='held'
              AND reviewed_by='policy:event-composition-audit-v1'
              AND generation_version=%s
            ORDER BY created_at DESC,composition_id DESC LIMIT 1""",
        (ledger_revision_id, generation),
    ).fetchone()


def _candidate_rows(conn, event_id: str) -> list[tuple]:
    return conn.execute(
        """SELECT c.candidate_id,c.status,c.selected_fact_ids_json,a.original_url
             FROM incident_candidates c JOIN articles a ON a.id=c.article_id
            WHERE c.event_id=%s ORDER BY c.created_at,c.candidate_id""", (event_id,),
    ).fetchall()


def advance(conn, event_id: str) -> dict:
    from .event_reassessment import snapshot, refresh_case, queue_next_evidence, exclude_source
    from .article_review_jobs import configuration as evidence_configuration

    case = conn.execute(
        "SELECT snapshot_version,snapshot_json,status,ledger_id FROM event_reassessment_cases WHERE event_id=%s",
        (event_id,),
    ).fetchone()
    if not case or case[2] != "active":
        return {"status": "unchanged", "event_id": event_id, "reason": "case_inactive"}
    try:
        current = snapshot(conn, event_id)
    except ValueError as exc:
        return _hold(conn, event_id, str(exc))
    if current["snapshot_version"] != case[0]:
        try:
            refreshed = refresh_case(conn, event_id)
        except ValueError as exc:
            return _hold(conn, event_id, str(exc))
        return {**refreshed, "action": "case_refreshed"}
    record = json.loads(case[1])
    from .event_reassessment import _pending_articles
    pending_articles = _pending_articles(conn)
    linked_ids = {int(item["article_id"]) for item in record["articles"]}
    if linked_ids & pending_articles:
        return {"status": "pending", "event_id": event_id,
                "reason": "article_evidence_pending"}

    active_job = conn.execute(
        """SELECT id,job_type,status FROM jobs
            WHERE status IN ('queued','running') AND job_type IN
              ('article_review_private','event_fact_curate','event_ledger_compose',
               'event_composition_audit','event_composition_repair',
               'event_promote_reviewed','enrich_event_from_web')
              AND (payload_json::jsonb->>'event_id'=%s OR payload_json::jsonb->>'composition_id' IN (
                    SELECT composition_id FROM event_ledger_compositions WHERE ledger_id=%s)
                   OR payload_json::jsonb->>'ledger_revision_id' IN (
                    SELECT revision_id FROM event_ledger_revisions WHERE ledger_id=%s))
            ORDER BY requested_at LIMIT 1""", (event_id, case[3] or "", case[3] or ""),
    ).fetchone()
    if active_job:
        return {"status": "pending", "event_id": event_id,
                "job_id": active_job[0], "job_type": active_job[1]}

    queued = queue_next_evidence(conn, event_id)
    if queued["status"] == "excluded":
        return {**queued, "action": "source_excluded"}
    if queued["status"] == "queued":
        status, error = _job_state(conn, queued["job_id"])
        if status == "failed":
            return {**exclude_source(conn, event_id, queued["article_id"],
                                     queued["source_version"], queued["generation_version"],
                                     "evidence extraction failed: " + error),
                    "action": "source_excluded"}
        if status == "succeeded":
            row = conn.execute("SELECT result_json FROM jobs WHERE id=%s", (queued["job_id"],)).fetchone()
            reason = "evidence extraction produced no reviewable revision"
            if row and row[0]:
                result = json.loads(row[0])
                phases = [phase for article in result.get("articles", [])
                          for phase in article.get("phases", [])]
                detail = next((phase.get("error") or phase.get("status") for phase in phases
                               if phase.get("status") != "structurally_valid_unreviewed"), None)
                if detail:
                    reason += ": " + str(detail)
            return {**exclude_source(conn, event_id, queued["article_id"],
                                     queued["source_version"], queued["generation_version"], reason),
                    "action": "source_excluded"}
        return {**queued, "action": "evidence_queued"}

    evidence_generation = evidence_configuration(conn)[3]
    for source in record["articles"]:
        if not source.get("source_version"):
            continue
        revision = conn.execute(
            """SELECT revision_id,status FROM article_evidence_revisions
                WHERE article_id=%s AND source_version=%s AND generation_version=%s
                ORDER BY created_at DESC,revision_id DESC LIMIT 1""",
            (source["article_id"], source["source_version"], evidence_generation),
        ).fetchone()
        if not revision:
            excluded = conn.execute(
                """SELECT 1 FROM event_reassessment_source_outcomes
                    WHERE event_id=%s AND article_id=%s AND source_version=%s
                      AND generation_version=%s AND status='excluded'""",
                (event_id, source["article_id"], source["source_version"], evidence_generation),
            ).fetchone()
            if excluded:
                continue
            return _hold(conn, event_id, "current evidence revision unavailable")
        candidate = conn.execute(
            """SELECT status,selected_fact_ids_json FROM incident_candidates
                WHERE event_id=%s AND evidence_revision_id=%s""", (event_id, revision[0]),
        ).fetchone()
        if revision[1] == "rejected":
            return _hold(conn, event_id, "current evidence revision rejected")
        if revision[1] == "superseded":
            return _hold(conn, event_id, "current evidence revision unexpectedly superseded")
        if candidate and (candidate[0] in {"rejected", "held"}
                          or (candidate[0] == "enrolled" and candidate[1])):
            continue
        from .event_fact_curation_jobs import submit
        try:
            job_id = submit(conn, event_id, revision[0])
        except (PermissionError, ValueError) as exc:
            return _hold(conn, event_id, str(exc))
        status, error = _job_state(conn, job_id)
        if status == "failed":
            return _hold(conn, event_id, "fact curation failed: " + error)
        if status == "succeeded":
            continue
        return {"status": "queued", "event_id": event_id, "job_id": job_id,
                "action": "fact_curation_queued"}

    candidates = _candidate_rows(conn, event_id)
    enrolled = [row for row in candidates if row[1] == "enrolled" and row[2]]
    domains = {(urlparse(str(row[3] or "")).hostname or "").lower().removeprefix("www.")
               for row in enrolled}
    domains.discard("")
    if len(enrolled) < 2 or len(domains) < 2:
        from .event_living_research import advance as research
        result = research(conn, event_id)
        if result["status"] == "research_queued":
            return {**result, "action": "corroboration_queued"}
        _touch(conn, event_id)
        return {"status": "deferred", "event_id": event_id,
                "reason": "independent_sources_required", "research": result}

    from .event_reassessment import propose_ledger
    from .event_ledger import review as review_ledger, _lineage_current
    ledger_id = case[3]
    latest = None
    if ledger_id:
        latest = conn.execute(
            """SELECT revision_id,status FROM event_ledger_revisions WHERE ledger_id=%s
                ORDER BY created_at DESC,revision_id DESC LIMIT 1""", (ledger_id,),
        ).fetchone()
    if latest and latest[1] == "accepted" and not _lineage_current(conn, latest[0]):
        result = review_ledger(conn, latest[0], "withdraw",
                               reason="superseded by Event-scoped fact selection",
                               reviewer="policy:event-reassessment-automation-v1")
        return {**result, "event_id": event_id, "action": "stale_ledger_withdrawn"}
    if not latest or latest[1] == "withdrawn":
        result = propose_ledger(conn, event_id, confirmation="CREATE_REASSESSED_EVENT_LEDGER",
                                title=str(record["event"]["title"]))
        return {**result, "action": "ledger_proposed"}
    if latest[1] == "proposed":
        result = review_ledger(conn, latest[0], "accept", reason="",
                               reviewer="policy:event-reassessment-automation-v1")
        return {**result, "event_id": event_id, "action": "ledger_accepted"}
    if latest[1] in {"held", "rejected"}:
        return _hold(conn, event_id, "ledger requires intervention")
    if latest[1] != "accepted":
        return {"status": "deferred", "event_id": event_id, "reason": "ledger_not_ready"}

    composition = conn.execute(
        """SELECT composition_id,status,reviewed_by,generation_version
            FROM event_ledger_compositions
            WHERE ledger_revision_id=%s ORDER BY created_at DESC,composition_id DESC LIMIT 1""",
        (latest[0],),
    ).fetchone()
    if not composition:
        from .event_composition_jobs import submit
        job_id = submit(conn, latest[0])
        status, error = _job_state(conn, job_id)
        if status == "failed":
            return _hold(conn, event_id, "composition failed: " + error)
        return {"status": "queued", "event_id": event_id, "job_id": job_id,
                "action": "composition_queued"}
    if composition[1] == "unreviewed":
        from .event_composition_audit_jobs import submit
        job_id = submit(conn, composition[0])
        status, error = _job_state(conn, job_id)
        if status == "failed":
            return _hold(conn, event_id, "composition audit failed: " + error)
        return {"status": "queued", "event_id": event_id, "job_id": job_id,
                "action": "composition_audit_queued"}
    repairable = _repairable_composition(conn, latest[0], composition)
    if repairable:
        audit_row = conn.execute(
            """SELECT result_json FROM jobs
                WHERE job_type='event_composition_audit' AND status='succeeded'
                  AND payload_json::jsonb->>'composition_id'=%s
                ORDER BY finished_at DESC,id DESC LIMIT 1""", (repairable[0],),
        ).fetchone()
        if audit_row and audit_row[0]:
            decision = json.loads(audit_row[0]).get("audit")
            if decision:
                from .event_composition_repair_jobs import submit
                job_id = submit(conn, repairable[0], decision)
                status, error = _job_state(conn, job_id)
                if status == "failed":
                    return _hold(conn, event_id, "composition repair failed: " + error)
                return {"status": status, "event_id": event_id, "job_id": job_id,
                        "action": "composition_repair_queued"}
    if composition[1] == "held":
        return _hold(conn, event_id, "composition support audit held the narrative")
    if (composition[1] == "accepted"
            and composition[2] == "policy:event-composition-audit-v1"):
        from .event_composition_publication import submit_automated
        result = submit_automated(conn, composition[0])
        return {**result, "action": "publication_queued"}
    return _hold(conn, event_id, "composition lacks automated support qualification")


def tick(conn) -> list[dict]:
    if not enabled():
        return []
    rows = conn.execute(
        """SELECT c.event_id FROM event_reassessment_cases c WHERE c.status='active'
            ORDER BY CASE
              WHEN EXISTS (
                SELECT 1 FROM event_ledger_compositions x
                 WHERE x.ledger_id=c.ledger_id AND x.status='unreviewed') THEN 0
              WHEN EXISTS (
                SELECT 1 FROM event_ledger_revisions r
                 WHERE r.ledger_id=c.ledger_id AND r.status IN ('proposed','accepted')) THEN 1
              ELSE 2 END,
              c.priority,c.updated_at,c.event_id LIMIT 25"""
    ).fetchall()
    results = []
    for (event_id,) in rows:
        try:
            result = advance(conn, event_id)
        except (ValueError, PermissionError, OSError, psycopg.Error) as exc:
            conn.rollback()
            result = _hold(conn, event_id, str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
        results.append(result)
        if result["status"] not in {"unchanged", "deferred", "pending"}:
            break
    return results
