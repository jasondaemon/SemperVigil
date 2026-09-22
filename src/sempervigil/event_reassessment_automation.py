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


def _resume_transient_composition_hold(conn) -> dict | None:
    """Resume one rollout-raced composition once, retaining the failed job."""
    from .event_composition_jobs import TRANSIENT_BASELINE_ERRORS, submit

    reasons = tuple("composition failed: " + error
                    for error in sorted(TRANSIENT_BASELINE_ERRORS))
    row = conn.execute(
        """SELECT c.event_id,r.revision_id
             FROM event_reassessment_cases c
             JOIN event_ledger_revisions r ON r.ledger_id=c.ledger_id
            WHERE c.status='held' AND c.decision_reason=ANY(%s)
              AND r.status='accepted'
            ORDER BY c.priority,c.updated_at,c.event_id LIMIT 1""", (list(reasons),),
    ).fetchone()
    if not row:
        return None
    job_id = submit(conn, row[1])
    status, error = _job_state(conn, job_id)
    if status not in {"queued", "running", "succeeded"}:
        return {"status": "held", "event_id": row[0], "job_id": job_id,
                "reason": "transient composition recovery failed: " + error[:120]}
    conn.execute(
        """UPDATE event_reassessment_cases
              SET status='active',decision_reason=NULL,updated_at=%s
            WHERE event_id=%s AND status='held'""", (utc_now_iso(), row[0]),
    )
    conn.commit()
    return {"status": status, "event_id": row[0], "job_id": job_id,
            "action": "composition_transient_recovery"}


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


def _is_repaired_composition(conn, composition_id: str,
                             *, generation: str | None = None) -> bool:
    generation_clause = ""
    params: tuple[str, ...] = (composition_id,)
    if generation is not None:
        generation_clause = " AND payload_json::jsonb->>'generation'=%s"
        params = (composition_id, generation)
    return bool(conn.execute(
        """SELECT 1 FROM jobs
            WHERE job_type='event_composition_repair' AND status='succeeded'
              AND result_json::jsonb->>'repaired_composition_id'=%s"""
        + generation_clause + " LIMIT 1", params,
    ).fetchone())


def _filter_detail_failures(conn, composition_id: str) -> dict | None:
    """Accept a deletion-only derivative when every rejected item is non-overview."""
    row = conn.execute(
        """SELECT result_json FROM jobs
            WHERE job_type='event_composition_audit' AND status='succeeded'
              AND payload_json::jsonb->>'composition_id'=%s
            ORDER BY finished_at DESC,id DESC LIMIT 1""",
        (composition_id,),
    ).fetchone()
    decision = json.loads(row[0]).get("audit") if row and row[0] else None
    if not decision:
        return None

    from . import event_composition, event_composition_audit as composition_audit
    from .event_composition_repair_jobs import material as repair_material
    composition, ledger_revision = repair_material(conn, composition_id)
    audit_request = composition_audit.request(
        composition_id, composition, ledger_revision["ledger"],
        decision.get("generation_version", ""),
    )
    item_sections = {
        item["id"]: item["section"]
        for item in json.loads(audit_request["input"])["items"]
    }
    failures = [
        item for item in decision.get("audits", [])
        if item.get("verdict") != "supported"
    ]
    if (not failures or any(
            item_sections.get(item.get("id")) == "overview"
            for item in failures)):
        return None
    filtered = composition_audit.filtered_record(
        composition_id, composition, ledger_revision["ledger"], decision
    )
    filtered_id = event_composition.store_unreviewed(conn, filtered)
    application = event_composition.review(
        conn, filtered_id, "accept", reason="",
        reviewer="policy:event-composition-audit-v1",
    )
    return {"composition_id": filtered_id, "application": application}


# Retained for the independent legacy publication-upgrade coordinator.
_filter_repaired_detail_failures = _filter_detail_failures


def _resume_detail_filter_hold(conn) -> dict | None:
    """Recover a case held because audit and repair item identities diverged."""
    recoverable_reasons = (
        "event_composition_repair_overview_items_missing",
        "event_composition_filter_audit_invalid",
    )
    row = conn.execute(
        """SELECT c.event_id,x.composition_id
             FROM event_reassessment_cases c
             JOIN event_ledger_revisions r
               ON r.ledger_id=c.ledger_id AND r.status='accepted'
             JOIN event_ledger_compositions x
               ON x.ledger_revision_id=r.revision_id
              AND x.status='held'
              AND x.reviewed_by='policy:event-composition-audit-v1'
            WHERE c.status='held' AND c.decision_reason=ANY(%s)
            ORDER BY c.priority,c.updated_at,c.event_id,x.created_at DESC LIMIT 1""",
        (list(recoverable_reasons),),
    ).fetchone()
    if not row:
        return None
    try:
        filtered = _filter_detail_failures(conn, row[1])
    except ValueError as exc:
        if str(exc) != "event_composition_filter_audit_invalid":
            raise
        filtered = None
    job_id = None
    if not filtered:
        from .event_composition_audit_jobs import submit
        job_id = submit(conn, row[1])
        status, error = _job_state(conn, job_id)
        if status not in {"queued", "running", "succeeded"}:
            return {"status": "held", "event_id": row[0], "job_id": job_id,
                    "reason": "composition re-audit recovery failed: " + error[:120]}
    conn.execute(
        """UPDATE event_reassessment_cases
              SET status='active',decision_reason=NULL,updated_at=%s
            WHERE event_id=%s AND status='held'
              AND decision_reason=ANY(%s)""",
        (utc_now_iso(), row[0], list(recoverable_reasons)),
    )
    conn.commit()
    if job_id:
        return {"status": status, "event_id": row[0], "job_id": job_id,
                "action": "composition_reaudit_recovery"}
    return {"status": "accepted", "event_id": row[0], **filtered,
            "action": "composition_detail_filter_recovery"}


def _repaired_derivative(conn, composition_id: str):
    """Return the composition created by a successful repair, if present."""
    row = conn.execute(
        """SELECT result_json FROM jobs
            WHERE job_type='event_composition_repair' AND status='succeeded'
              AND payload_json::jsonb->>'composition_id'=%s
            ORDER BY finished_at DESC,id DESC LIMIT 1""", (composition_id,),
    ).fetchone()
    if not row or not row[0]:
        return None
    result = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    repaired_id = result.get("repaired_composition_id")
    if not repaired_id:
        return None
    return conn.execute(
        """SELECT composition_id,status,reviewed_by,generation_version
             FROM event_ledger_compositions WHERE composition_id=%s""",
        (repaired_id,),
    ).fetchone()


def _advance_proposed_ledger(conn, event_id: str, revision_id: str) -> dict:
    from .event_ledger import review, _lineage_current
    if not _lineage_current(conn, revision_id):
        result = review(conn, revision_id, "reject",
                        reason="superseded by Event-scoped fact selection",
                        reviewer="policy:event-reassessment-automation-v1")
        return {**result, "event_id": event_id, "action": "stale_ledger_rejected"}
    result = review(conn, revision_id, "accept", reason="",
                    reviewer="policy:event-reassessment-automation-v1")
    return {**result, "event_id": event_id, "action": "ledger_accepted"}


def _candidate_rows(conn, event_id: str) -> list[tuple]:
    return conn.execute(
        """SELECT c.candidate_id,c.status,c.selected_fact_ids_json,a.original_url,
                  c.selected_fact_sections_json
             FROM incident_candidates c JOIN articles a ON a.id=c.article_id
            WHERE c.event_id=%s ORDER BY c.created_at,c.candidate_id""", (event_id,),
    ).fetchall()


def _accepted_ledger_source_rows(conn, ledger_id: str | None) -> list[tuple[int, str]]:
    if not ledger_id:
        return []
    rows = conn.execute(
        """SELECT s.article_id,a.original_url
             FROM event_ledger_revisions r
             JOIN event_ledger_revision_sources s ON s.revision_id=r.revision_id
             JOIN articles a ON a.id=s.article_id
            WHERE r.ledger_id=%s AND r.status='accepted'
            ORDER BY s.article_id""",
        (ledger_id,),
    ).fetchall()
    return [(int(row[0]), str(row[1] or "")) for row in rows]


def _next_additive_candidate(ledger: dict, candidates: list[tuple]) -> str | None:
    existing = {str(item.get("candidate_id") or "") for item in ledger.get("sources", [])}
    return next((str(row[0]) for row in candidates
                 if row[1] == "enrolled" and row[2] and row[4]
                 and str(row[0]) not in existing), None)


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

    accepted_source_rows = _accepted_ledger_source_rows(conn, case[3])
    accepted_article_ids = {row[0] for row in accepted_source_rows}
    queued = queue_next_evidence(
        conn,
        event_id,
        exclude_article_ids=accepted_article_ids,
    )
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
        if int(source["article_id"]) in accepted_article_ids:
            continue
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
            """SELECT status,selected_fact_ids_json,selected_fact_sections_json FROM incident_candidates
                WHERE event_id=%s AND evidence_revision_id=%s""", (event_id, revision[0]),
        ).fetchone()
        if revision[1] == "rejected":
            return _hold(conn, event_id, "current evidence revision rejected")
        if revision[1] == "superseded":
            return _hold(conn, event_id, "current evidence revision unexpectedly superseded")
        if candidate and (candidate[0] in {"rejected", "held"}
                          or (candidate[0] == "enrolled" and candidate[1] and candidate[2])):
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
    enrolled = [row for row in candidates if row[1] == "enrolled" and row[2] and row[4]]
    domains = {(urlparse(str(row[3] or "")).hostname or "").lower().removeprefix("www.")
               for row in enrolled}
    domains.update(
        (urlparse(url).hostname or "").lower().removeprefix("www.")
        for _, url in accepted_source_rows
    )
    domains.discard("")
    if len(enrolled) + len(accepted_source_rows) < 2 or len(domains) < 2:
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
        return _advance_proposed_ledger(conn, event_id, latest[0])
    if latest[1] in {"held", "rejected"}:
        return _hold(conn, event_id, "ledger requires intervention")
    if latest[1] != "accepted":
        return {"status": "deferred", "event_id": event_id, "reason": "ledger_not_ready"}

    latest_ledger_row = conn.execute(
        "SELECT ledger_json FROM event_ledger_revisions WHERE revision_id=%s",
        (latest[0],),
    ).fetchone()
    if not latest_ledger_row:
        return _hold(conn, event_id, "accepted ledger unavailable")
    latest_ledger = json.loads(latest_ledger_row[0]) if isinstance(
        latest_ledger_row[0], str
    ) else latest_ledger_row[0]
    additive_candidate = _next_additive_candidate(latest_ledger, candidates)
    if additive_candidate:
        from .event_ledger import propose
        result = propose(
            conn,
            additive_candidate,
            ledger_id=ledger_id,
            change_kind="additive",
        )
        return {**result, "event_id": event_id, "action": "ledger_additive_proposed"}

    from .event_composition_jobs import configuration as composition_configuration
    composition_generation = composition_configuration(conn)[2]
    composition = conn.execute(
        """SELECT composition_id,status,reviewed_by,generation_version
            FROM event_ledger_compositions
            WHERE ledger_revision_id=%s AND generation_version=%s
            ORDER BY created_at DESC,composition_id DESC LIMIT 1""",
        (latest[0], composition_generation),
    ).fetchone()
    if not composition:
        from .event_composition_jobs import submit
        job_id = submit(conn, latest[0])
        status, error = _job_state(conn, job_id)
        if status == "failed":
            return _hold(conn, event_id, "composition failed: " + error)
        return {"status": "queued", "event_id": event_id, "job_id": job_id,
                "action": "composition_queued"}
    accepted = conn.execute(
        """SELECT composition_id,status,reviewed_by,generation_version
             FROM event_ledger_compositions
            WHERE ledger_revision_id=%s AND status='accepted'
            ORDER BY created_at DESC,composition_id DESC LIMIT 1""",
        (latest[0],),
    ).fetchone()
    if accepted:
        composition = accepted
    repaired = _repaired_derivative(conn, composition[0])
    if repaired:
        composition = repaired
    if composition[1] == "unreviewed":
        from .event_composition_audit_jobs import submit
        job_id = submit(conn, composition[0])
        status, error = _job_state(conn, job_id)
        if status == "failed":
            return _hold(conn, event_id, "composition audit failed: " + error)
        return {"status": "queued", "event_id": event_id, "job_id": job_id,
                "action": "composition_audit_queued"}
    if composition[1] == "held":
        filtered = _filter_detail_failures(conn, composition[0])
        if filtered:
            return {"status": "accepted", "event_id": event_id, **filtered,
                    "action": "composition_detail_filtered"}
        if _is_repaired_composition(conn, composition[0]):
            return _hold(conn, event_id, "composition support audit held the repaired overview")
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
    recovery = _resume_detail_filter_hold(conn)
    if recovery:
        return [recovery]
    recovery = _resume_transient_composition_hold(conn)
    if recovery and recovery["status"] != "held":
        return [recovery]
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
