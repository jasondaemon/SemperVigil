"""Bounded upgrades of already-published Event compositions.

The public pointer remains authoritative until a newly composed and independently
audited revision is promoted. This coordinator advances at most one material
action per tick and never changes evidence, ledgers, or source membership.
"""
from __future__ import annotations

import json
import os

from . import event_composition

WORKFLOW = "event-published-composition-upgrade-v1"
PUBLIC_WORKFLOW = "event-composition-public-revision-v1"


def enabled() -> bool:
    value = os.environ.get("SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED", "0")
    if value not in {"0", "1"}:
        raise ValueError("invalid_event_public_composition_upgrade_enablement")
    return value == "1"


def _decode(raw: object) -> dict | None:
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def candidates(conn, *, limit: int = 25) -> list[dict]:
    if not 1 <= limit <= 100:
        raise ValueError("event_public_composition_upgrade_limit_invalid")
    rows = conn.execute(
        """SELECT p.event_id,p.revision_id,p.updated_at,r.bundle_json
             FROM event_public_pointers p
             JOIN event_public_revisions r
               ON r.event_id=p.event_id AND r.revision_id=p.revision_id
            ORDER BY p.updated_at,p.event_id
            LIMIT 200"""
    ).fetchall()
    result = []
    for event_id, revision_id, updated_at, raw in rows:
        bundle = _decode(raw)
        composition = bundle.get("composition") if bundle else None
        if (not bundle or bundle.get("workflow") != PUBLIC_WORKFLOW
                or not isinstance(composition, dict)
                or composition.get("workflow") == event_composition.WORKFLOW
                or composition.get("workflow") not in event_composition.LEGACY_WORKFLOWS):
            continue
        ledger_revision_id = bundle.get("ledger_revision_id")
        composition_id = bundle.get("composition_id")
        if (not isinstance(ledger_revision_id, str)
                or not ledger_revision_id.startswith("elr_")
                or not isinstance(composition_id, str)
                or not composition_id.startswith("elc_")):
            continue
        result.append({
            "event_id": event_id,
            "revision_id": revision_id,
            "updated_at": updated_at,
            "ledger_revision_id": ledger_revision_id,
            "published_composition_id": composition_id,
            "published_workflow": composition["workflow"],
        })
        if len(result) >= limit:
            break
    return result


def _job_state(conn, job_id: str) -> tuple[str, str]:
    row = conn.execute(
        "SELECT status,COALESCE(error,'') FROM jobs WHERE id=%s", (job_id,)
    ).fetchone()
    return (row[0], row[1]) if row else ("missing", "job unavailable")


def _compositions(conn, ledger_revision_id: str) -> list[tuple]:
    rows = conn.execute(
        """SELECT composition_id,status,reviewed_by,generation_version,
                  composition_json,created_at
             FROM event_ledger_compositions
            WHERE ledger_revision_id=%s
            ORDER BY created_at DESC,composition_id DESC""",
        (ledger_revision_id,),
    ).fetchall()
    return [row for row in rows
            if (_decode(row[4]) or {}).get("workflow") == event_composition.WORKFLOW]


def _active_job(conn, ledger_revision_id: str, composition_ids: list[str]):
    return conn.execute(
        """SELECT id,job_type,status
             FROM jobs
            WHERE status IN ('queued','running')
              AND job_type IN ('event_ledger_compose','event_composition_audit',
                  'event_composition_repair','event_promote_reviewed')
              AND (payload_json::jsonb->>'ledger_revision_id'=%s
                   OR payload_json::jsonb->>'composition_id'=ANY(%s))
            ORDER BY requested_at,id LIMIT 1""",
        (ledger_revision_id, composition_ids or [""]),
    ).fetchone()


def advance(conn, candidate: dict) -> dict:
    event_id = candidate["event_id"]
    current = conn.execute(
        "SELECT revision_id FROM event_public_pointers WHERE event_id=%s",
        (event_id,),
    ).fetchone()
    if not current or current[0] != candidate["revision_id"]:
        return {"status": "unchanged", "event_id": event_id,
                "reason": "public_pointer_advanced"}

    compositions = _compositions(conn, candidate["ledger_revision_id"])
    active = _active_job(
        conn,
        candidate["ledger_revision_id"],
        [row[0] for row in compositions],
    )
    if active:
        return {"status": "pending", "event_id": event_id,
                "job_id": active[0], "job_type": active[1]}

    accepted = next((row for row in compositions
                     if row[1] == "accepted"
                     and row[2] == "policy:event-composition-audit-v1"), None)
    if accepted:
        from .event_composition_publication import submit_automated
        result = submit_automated(conn, accepted[0])
        status, error = _job_state(conn, result["job_id"])
        if status == "failed":
            return {"status": "held", "event_id": event_id,
                    "job_id": result["job_id"],
                    "reason": "publication failed: " + error[:160]}
        return {**result, "action": "published_composition_upgrade_queued",
                "workflow": WORKFLOW}

    unreviewed = next((row for row in compositions if row[1] == "unreviewed"), None)
    if unreviewed:
        from .event_composition_audit_jobs import submit
        job_id = submit(conn, unreviewed[0])
        status, error = _job_state(conn, job_id)
        if status == "failed":
            return {"status": "held", "event_id": event_id, "job_id": job_id,
                    "reason": "composition audit failed: " + error[:160]}
        return {"status": status, "event_id": event_id, "job_id": job_id,
                "action": "published_composition_audit_queued", "workflow": WORKFLOW}

    held = next((row for row in compositions if row[1] == "held"), None)
    if held:
        from .event_reassessment_automation import (
            _is_repaired_composition,
            _repairable_composition,
        )
        from .event_composition_repair_jobs import configuration as repair_configuration
        repair_generation = repair_configuration(conn)[2]
        if _is_repaired_composition(conn, held[0], generation=repair_generation):
            return {"status": "held", "event_id": event_id,
                    "reason": "replacement overview failed support audit"}
        repairable = _repairable_composition(
            conn, candidate["ledger_revision_id"], held[:4]
        )
        if repairable:
            row = conn.execute(
                """SELECT result_json FROM jobs
                    WHERE job_type='event_composition_audit' AND status='succeeded'
                      AND payload_json::jsonb->>'composition_id'=%s
                    ORDER BY finished_at DESC,id DESC LIMIT 1""",
                (repairable[0],),
            ).fetchone()
            decision = (_decode(row[0]) or {}).get("audit") if row else None
            if decision:
                from . import event_composition_audit as composition_audit
                from .event_composition_repair_jobs import material as repair_material
                composition, ledger_revision = repair_material(conn, repairable[0])
                audit_request = composition_audit.request(
                    repairable[0], composition, ledger_revision["ledger"],
                    decision.get("generation_version", ""),
                )
                item_sections = {item["id"]: item["section"]
                                 for item in json.loads(audit_request["input"])["items"]}
                failures = [item for item in decision.get("audits", [])
                            if item.get("verdict") != "supported"]
                if failures and not any(
                        item_sections.get(item.get("id")) == "overview"
                        for item in failures):
                    filtered = composition_audit.filtered_record(
                        repairable[0], composition, ledger_revision["ledger"], decision)
                    filtered_id = event_composition.store_unreviewed(conn, filtered)
                    accepted = event_composition.review(
                        conn, filtered_id, "accept", reason="",
                        reviewer="policy:event-composition-audit-v1")
                    return {"status": "accepted", "event_id": event_id,
                            "composition_id": filtered_id, "application": accepted,
                            "action": "published_composition_detail_filtered",
                            "workflow": WORKFLOW}
                from .event_composition_repair_jobs import submit
                try:
                    job_id = submit(conn, repairable[0], decision)
                except ValueError as exc:
                    return {"status": "held", "event_id": event_id,
                            "reason": str(exc)[:160], "workflow": WORKFLOW}
                status, error = _job_state(conn, job_id)
                if status == "failed":
                    return {"status": "held", "event_id": event_id,
                            "job_id": job_id,
                            "reason": "composition repair failed: " + error[:160]}
                return {"status": status, "event_id": event_id,
                        "job_id": job_id,
                        "action": "published_composition_repair_queued",
                        "workflow": WORKFLOW}
        return {"status": "held", "event_id": event_id,
                "reason": "replacement overview requires review"}

    from .event_composition_jobs import submit
    job_id = submit(conn, candidate["ledger_revision_id"])
    status, error = _job_state(conn, job_id)
    if status == "failed":
        return {"status": "held", "event_id": event_id, "job_id": job_id,
                "reason": "composition failed: " + error[:160]}
    return {"status": status, "event_id": event_id, "job_id": job_id,
            "action": "published_composition_upgrade_queued", "workflow": WORKFLOW}


def tick(conn) -> list[dict]:
    if not enabled():
        return []
    results = []
    for candidate in candidates(conn):
        result = advance(conn, candidate)
        results.append(result)
        if result["status"] not in {"unchanged", "held"}:
            break
    return results
