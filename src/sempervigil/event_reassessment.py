"""Published-first reassessment of legacy confirmed Events.

Cases freeze the source membership used by the old Event process. They never
trust or copy the legacy narrative, and starting or advancing a case cannot
change public content.
"""
from __future__ import annotations

import json
from typing import Any

from .article_evidence import source_for
from .investigation import _version
from .utils import utc_now_iso

WORKFLOW = "confirmed-event-reassessment-v1"
START_CONFIRMATION = "START_CONFIRMED_REASSESSMENT"
EVIDENCE_CONFIRMATION = "QUEUE_EVENT_EVIDENCE"
PROJECT_CONFIRMATION = "PROJECT_ACCEPTED_EVENT_EVIDENCE"
LEDGER_CONFIRMATION = "CREATE_REASSESSED_EVENT_LEDGER"


def _decode(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("event_reassessment_snapshot_invalid")
    return value


def _evidence_title(value: object, candidate_rows: list[tuple]) -> str:
    title = str(value or "").strip()
    allowed = {str(row[3]).strip() for row in candidate_rows
               if len(row) > 3 and str(row[3] or "").strip()}
    if title not in allowed:
        raise ValueError("event_reassessment_evidence_title_required")
    return title


def _article_snapshot(row: tuple) -> dict:
    article = {"id": int(row[0]), "title": str(row[1] or ""),
               "content_text": row[3]}
    try:
        version = source_for(article)["source_version"]
        source_error = None
    except ValueError as exc:
        version = None
        source_error = str(exc)
    return {"article_id": int(row[0]), "title": str(row[1] or ""),
            "url": str(row[2] or ""), "source_version": version,
            "source_error": source_error}


def snapshot(conn: Any, event_id: str) -> dict:
    row = conn.execute(
        """SELECT e.id,e.title,e.event_key,e.publish_state,e.published_at,
                  e.site_slug,e.updated_at,e.visibility,e.lifecycle,
                  EXISTS(SELECT 1 FROM event_public_pointers p WHERE p.event_id=e.id)
             FROM events e WHERE e.id=%s""",
        (event_id,),
    ).fetchone()
    if not row or row[7] != "active" or row[8] != "confirmed":
        raise ValueError("event_reassessment_event_ineligible")
    articles = conn.execute(
        """SELECT a.id,a.title,a.original_url,a.content_text
             FROM event_articles ea JOIN articles a ON a.id=ea.article_id
            WHERE ea.event_id=%s ORDER BY a.id""",
        (event_id,),
    ).fetchall()
    if not articles:
        raise ValueError("event_reassessment_sources_missing")
    record = {
        "workflow": WORKFLOW,
        "event": {"event_id": row[0], "title": row[1], "event_key": row[2],
                  "publish_state": row[3], "published_at": row[4],
                  "site_slug": row[5], "updated_at": row[6],
                  "has_public_pointer": bool(row[9])},
        "articles": [_article_snapshot(item) for item in articles],
        "legacy_narrative_used": False,
    }
    return {**record, "snapshot_version": _version(record)}


def start_cohort(conn: Any, *, confirmation: str, created_by: str) -> dict:
    if confirmation != START_CONFIRMATION:
        raise ValueError("event_reassessment_confirmation_required")
    rows = conn.execute(
        """SELECT e.id FROM events e
            WHERE e.visibility='active' AND e.lifecycle='confirmed'
              AND COALESCE(e.event_key,'') NOT LIKE 'event-ledger:%%'
            ORDER BY CASE WHEN e.publish_state='published' OR EXISTS(
                SELECT 1 FROM event_public_pointers p WHERE p.event_id=e.id)
              THEN 0 ELSE 1 END,
              e.published_at DESC NULLS LAST,e.created_at DESC,e.id"""
    ).fetchall()
    created = reused = held = 0
    now = utc_now_iso()
    for (event_id,) in rows:
        try:
            record = snapshot(conn, event_id)
        except ValueError:
            held += 1
            continue
        public = (record["event"]["publish_state"] == "published"
                  or record["event"]["has_public_pointer"])
        changed = conn.execute(
            """INSERT INTO event_reassessment_cases
               (event_id,snapshot_version,snapshot_json,priority,status,
                created_at,created_by,updated_at)
               VALUES (%s,%s,%s,%s,'active',%s,%s,%s)
               ON CONFLICT(event_id) DO NOTHING""",
            (event_id, record["snapshot_version"],
             json.dumps(record, sort_keys=True, ensure_ascii=True),
             0 if public else 10, now, created_by, now),
        )
        if changed.rowcount == 1:
            created += 1
        else:
            reused += 1
    conn.commit()
    return {"status": "ready", "created": created, "reused": reused,
            "held": held, "total": len(rows), "public_content_changed": False}


def enroll_confirmed_draft(conn: Any, event_id: str, *, created_by: str) -> bool:
    """Admit one confirmed draft to the existing evidence-first publication path."""
    record = snapshot(conn, event_id)
    event = record["event"]
    if (event["publish_state"] != "draft" or event["has_public_pointer"]
            or str(event["event_key"] or "").startswith("event-ledger:")):
        raise ValueError("event_reassessment_draft_required")
    now = utc_now_iso()
    inserted = conn.execute(
        """INSERT INTO event_reassessment_cases
           (event_id,snapshot_version,snapshot_json,priority,status,
            created_at,created_by,updated_at)
           VALUES (%s,%s,%s,10,'active',%s,%s,%s)
           ON CONFLICT(event_id) DO NOTHING""",
        (event_id, record["snapshot_version"],
         json.dumps(record, sort_keys=True, ensure_ascii=True),
         now, created_by, now),
    ).rowcount == 1
    conn.commit()
    return inserted


def activate_update(
    conn: Any, event_id: str, *, article_id: int, created_by: str
) -> dict:
    """Open a successor reassessment for a published, ledger-backed Event."""
    if article_id <= 0 or not created_by.strip() or len(created_by) > 80:
        raise ValueError("event_reassessment_identity_invalid")
    current = snapshot(conn, event_id)
    ledger_id = str(current["event"]["event_key"] or "").removeprefix("event-ledger:")
    if not ledger_id.startswith("eld_") or not current["event"]["has_public_pointer"]:
        raise ValueError("event_reassessment_update_target_invalid")
    now = utc_now_iso()
    linked = conn.execute(
        """INSERT INTO event_articles(event_id,article_id,added_by,created_at)
           VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        (event_id, article_id, created_by, now),
    ).rowcount == 1
    conn.execute(
        """UPDATE events SET updated_at=%s,last_seen_at=%s,visibility='active'
            WHERE id=%s""",
        (now, now, event_id),
    )
    record = snapshot(conn, event_id)
    existing = conn.execute(
        "SELECT status,ledger_id FROM event_reassessment_cases WHERE event_id=%s FOR UPDATE",
        (event_id,),
    ).fetchone()
    if existing and existing[1] not in {None, ledger_id}:
        raise ValueError("event_reassessment_update_ledger_conflict")
    encoded = json.dumps(record, sort_keys=True, ensure_ascii=True)
    if existing:
        conn.execute(
            """UPDATE event_reassessment_cases
                  SET snapshot_version=%s,snapshot_json=%s,priority=0,status='active',
                      ledger_id=%s,completed_at=NULL,decision_reason=NULL,updated_at=%s
                WHERE event_id=%s""",
            (record["snapshot_version"], encoded, ledger_id, now, event_id),
        )
        status = "reopened"
    else:
        conn.execute(
            """INSERT INTO event_reassessment_cases
               (event_id,snapshot_version,snapshot_json,priority,status,ledger_id,
                created_at,created_by,updated_at)
               VALUES (%s,%s,%s,0,'active',%s,%s,%s,%s)""",
            (event_id, record["snapshot_version"], encoded, ledger_id,
             now, created_by, now),
        )
        status = "created"
    conn.execute(
        """INSERT INTO event_ledger_targets
           (ledger_id,event_id,snapshot_version,created_at,created_by)
           VALUES (%s,%s,%s,%s,%s)
           ON CONFLICT(ledger_id) DO UPDATE
             SET snapshot_version=EXCLUDED.snapshot_version""",
        (ledger_id, event_id, record["snapshot_version"], now, created_by),
    )
    conn.commit()
    return {"event_id": event_id, "ledger_id": ledger_id, "status": status,
            "article_linked": linked,
            "snapshot_version": record["snapshot_version"],
            "public_content_changed": False}


def _pending_articles(conn: Any) -> set[int]:
    rows = conn.execute(
        """SELECT payload_json FROM jobs
            WHERE job_type='article_review_private'
              AND status IN ('queued','running')"""
    ).fetchall()
    result: set[int] = set()
    for (raw,) in rows:
        payload = _decode(raw)
        for article in payload.get("articles", []):
            if isinstance(article, dict) and isinstance(article.get("id"), int):
                result.add(article["id"])
    return result


def _case_metrics(conn: Any, event_id: str, record: dict, pending: set[int]) -> dict:
    article_ids = [int(row["article_id"]) for row in record["articles"]]
    rows = conn.execute(
        """SELECT article_id,status,count(*) FROM article_evidence_revisions
            WHERE article_id=ANY(%s) GROUP BY article_id,status""",
        (article_ids,),
    ).fetchall()
    statuses: dict[int, set[str]] = {article_id: set() for article_id in article_ids}
    for article_id, status, _ in rows:
        statuses[int(article_id)].add(str(status))
    accepted = sum("accepted" in statuses[item] for item in article_ids)
    review = sum(bool(statuses[item] & {"unreviewed", "held"}) for item in article_ids)
    decided = sum(bool(statuses[item] & {"rejected", "superseded"}) for item in article_ids)
    running = sum(item in pending for item in article_ids)
    missing_source = sum(not row.get("source_version") for row in record["articles"])
    missing = sum(not statuses[item] and item not in pending for item in article_ids)
    candidates = conn.execute(
        """SELECT status,count(*) FROM incident_candidates
            WHERE article_id=ANY(%s) AND event_id=%s GROUP BY status""",
        (article_ids, event_id)
    ).fetchall()
    candidate_counts = {str(status): int(count) for status, count in candidates}
    title_rows = conn.execute(
        """SELECT DISTINCT title FROM incident_candidates
            WHERE article_id=ANY(%s) AND status='enrolled'
              AND event_id=%s
              AND title IS NOT NULL AND btrim(title)<>''
            ORDER BY title""", (article_ids, event_id)
    ).fetchall()
    if missing_source:
        stage = "source_hold"
    elif missing:
        stage = "evidence_needed"
    elif running:
        stage = "evidence_running"
    elif review:
        stage = "evidence_review"
    elif accepted < len(article_ids):
        stage = "evidence_held"
    elif candidate_counts.get("enrolled", 0) < 2:
        stage = "candidate_review"
    else:
        stage = "ledger_ready"
    return {"article_count": len(article_ids), "missing_source": missing_source,
            "evidence_missing": missing, "evidence_running": running,
            "evidence_review": review, "evidence_accepted": accepted,
            "evidence_decided": decided, "candidates": candidate_counts,
            "ledger_title_options": [str(row[0]) for row in title_rows],
            "stage": stage}


def list_cases(conn: Any, *, status: str = "active", limit: int = 200) -> list[dict]:
    if status not in {"active", "completed", "held", "withdrawn", "all"}:
        raise ValueError("event_reassessment_status_invalid")
    if not 1 <= limit <= 200:
        raise ValueError("event_reassessment_limit_invalid")
    where = "" if status == "all" else "WHERE c.status=%s"
    params: list[object] = [] if status == "all" else [status]
    rows = conn.execute(
        f"""SELECT c.event_id,c.snapshot_version,c.snapshot_json,c.priority,
                   c.status,c.ledger_id,c.created_at,c.updated_at,c.decision_reason
              FROM event_reassessment_cases c {where}
             ORDER BY c.priority,c.created_at,c.event_id LIMIT %s""",
        (*params, limit),
    ).fetchall()
    pending = _pending_articles(conn)
    result = []
    for row in rows:
        record = _decode(row[2])
        stale = False
        try:
            stale = snapshot(conn, row[0])["snapshot_version"] != row[1]
        except ValueError:
            stale = True
        metrics = _case_metrics(conn, row[0], record, pending)
        ledger_status = composition_status = None
        if row[5]:
            ledger = conn.execute(
                """SELECT status FROM event_ledger_revisions WHERE ledger_id=%s
                    ORDER BY created_at DESC LIMIT 1""", (row[5],)
            ).fetchone()
            ledger_status = ledger[0] if ledger else None
            composition = conn.execute(
                """SELECT status FROM event_ledger_compositions WHERE ledger_id=%s
                    ORDER BY created_at DESC LIMIT 1""", (row[5],)
            ).fetchone()
            composition_status = composition[0] if composition else None
            if composition_status:
                metrics["stage"] = "composition_" + composition_status
            elif ledger_status:
                metrics["stage"] = "ledger_" + ledger_status
        result.append({"event_id": row[0], "snapshot_version": row[1],
                       "priority": row[3], "status": row[4], "ledger_id": row[5],
                       "ledger_status": ledger_status,
                       "composition_status": composition_status,
                       "created_at": row[6], "updated_at": row[7],
                       "decision_reason": row[8], "snapshot_stale": stale,
                       "event": record["event"], "articles": record["articles"],
                       **metrics})
    return result


def queue_evidence(conn: Any, event_id: str, *, confirmation: str) -> dict:
    if confirmation != EVIDENCE_CONFIRMATION:
        raise ValueError("event_reassessment_evidence_confirmation_required")
    row = conn.execute(
        """SELECT snapshot_version,snapshot_json,status
             FROM event_reassessment_cases WHERE event_id=%s FOR UPDATE""",
        (event_id,),
    ).fetchone()
    if not row or row[2] != "active":
        raise ValueError("event_reassessment_case_unavailable")
    current = snapshot(conn, event_id)
    if current["snapshot_version"] != row[0]:
        raise ValueError("event_reassessment_snapshot_stale")
    record = _decode(row[1])
    pending = _pending_articles(conn)
    article_ids = [int(item["article_id"]) for item in record["articles"]
                   if item.get("source_version")]
    existing = {int(item[0]) for item in conn.execute(
        "SELECT DISTINCT article_id FROM article_evidence_revisions WHERE article_id=ANY(%s)",
        (article_ids,),
    ).fetchall()} if article_ids else set()
    eligible = [item for item in article_ids if item not in existing and item not in pending]
    jobs, held = [], []
    from .article_review_jobs import submit
    for offset in range(0, len(eligible), 3):
        cohort = eligible[offset:offset + 3]
        try:
            jobs.append(submit(conn, cohort))
        except (PermissionError, ValueError) as exc:
            # Isolate over-budget or malformed retained sources so one source
            # cannot block the rest of the Event.
            for article_id in cohort:
                try:
                    jobs.append(submit(conn, [article_id]))
                except (PermissionError, ValueError) as item_exc:
                    held.append({"article_id": article_id, "reason": str(item_exc)})
    return {"event_id": event_id, "status": "queued" if jobs else "unchanged",
            "job_ids": jobs, "held": held, "public_content_changed": False}


def queue_next_evidence(
    conn: Any, event_id: str, *, exclude_article_ids: set[int] | None = None
) -> dict:
    """Queue at most one missing source for bounded autonomous processing."""
    row = conn.execute(
        "SELECT snapshot_version,snapshot_json,status FROM event_reassessment_cases WHERE event_id=%s",
        (event_id,),
    ).fetchone()
    if not row or row[2] != "active":
        raise ValueError("event_reassessment_case_unavailable")
    current = snapshot(conn, event_id)
    if current["snapshot_version"] != row[0]:
        raise ValueError("event_reassessment_snapshot_stale")
    pending = _pending_articles(conn)
    record = _decode(row[1])
    excluded_ids = exclude_article_ids or set()
    from .article_review_jobs import configuration
    generation = configuration(conn)[3]
    for item in record["articles"]:
        article_id = int(item["article_id"])
        if article_id in excluded_ids or not item.get("source_version") or article_id in pending:
            continue
        excluded = conn.execute(
            """SELECT 1 FROM event_reassessment_source_outcomes
                WHERE event_id=%s AND article_id=%s AND source_version=%s
                  AND generation_version=%s AND status='excluded'""",
            (event_id, article_id, item["source_version"], generation),
        ).fetchone()
        if excluded:
            continue
        existing = conn.execute(
            """SELECT 1 FROM article_evidence_revisions
                WHERE article_id=%s AND source_version=%s AND generation_version=%s
                LIMIT 1""",
            (article_id, item["source_version"], generation),
        ).fetchone()
        if existing:
            continue
        from .article_review_jobs import submit
        try:
            job_id = submit(conn, [article_id])
        except ValueError as exc:
            result = exclude_source(conn, event_id, article_id, item["source_version"],
                                    generation, str(exc))
            return {**result, "public_content_changed": False}
        return {"event_id": event_id, "status": "queued", "job_id": job_id,
                "article_id": article_id, "source_version": item["source_version"],
                "generation_version": generation, "public_content_changed": False}
    return {"event_id": event_id, "status": "unchanged", "public_content_changed": False}


def exclude_source(conn: Any, event_id: str, article_id: int, source_version: str,
                   generation_version: str, reason: str) -> dict:
    """Persist a generation-scoped source exclusion without rejecting the Event."""
    reason = str(reason).strip()[:1000] or "source produced no reviewable evidence"
    conn.execute(
        """INSERT INTO event_reassessment_source_outcomes
           (event_id,article_id,source_version,generation_version,status,reason,created_at)
           VALUES (%s,%s,%s,%s,'excluded',%s,%s)
           ON CONFLICT(event_id,article_id,source_version,generation_version) DO NOTHING""",
        (event_id, article_id, source_version, generation_version, reason, utc_now_iso()),
    )
    conn.commit()
    return {"event_id": event_id, "article_id": article_id, "status": "excluded",
            "reason": reason[:160]}


def refresh_case(conn: Any, event_id: str) -> dict:
    """Rebase an active case while preserving immutable ledgers and decisions."""
    row = conn.execute(
        "SELECT snapshot_version,snapshot_json,status FROM event_reassessment_cases WHERE event_id=%s FOR UPDATE",
        (event_id,),
    ).fetchone()
    if not row or row[2] != "active":
        raise ValueError("event_reassessment_case_unavailable")
    current = snapshot(conn, event_id)
    if current["snapshot_version"] == row[0]:
        return {"event_id": event_id, "status": "unchanged"}
    old = _decode(row[1])
    old_sources = {(item["article_id"], item.get("source_version")) for item in old["articles"]}
    new_sources = {(item["article_id"], item.get("source_version")) for item in current["articles"]}
    conn.execute(
        """UPDATE event_reassessment_cases
              SET snapshot_version=%s,snapshot_json=%s,updated_at=%s
            WHERE event_id=%s""",
        (current["snapshot_version"], json.dumps(current, sort_keys=True, ensure_ascii=True),
         utc_now_iso(), event_id),
    )
    conn.execute(
        "UPDATE event_ledger_targets SET snapshot_version=%s WHERE event_id=%s",
        (current["snapshot_version"], event_id),
    )
    conn.commit()
    return {"event_id": event_id, "status": "refreshed",
            "source_membership_changed": old_sources != new_sources}


def project_accepted(conn: Any, event_id: str, *, confirmation: str) -> dict:
    if confirmation != PROJECT_CONFIRMATION:
        raise ValueError("event_reassessment_projection_confirmation_required")
    case = conn.execute(
        "SELECT snapshot_version,snapshot_json,status FROM event_reassessment_cases WHERE event_id=%s",
        (event_id,),
    ).fetchone()
    if not case or case[2] != "active" or snapshot(conn, event_id)["snapshot_version"] != case[0]:
        raise ValueError("event_reassessment_case_unavailable")
    record = _decode(case[1])
    article_ids = [int(item["article_id"]) for item in record["articles"]]
    revisions = conn.execute(
        """SELECT revision_id FROM article_evidence_revisions
            WHERE article_id=ANY(%s) AND status='accepted'
            ORDER BY article_id,revision_id""", (article_ids,)
    ).fetchall()
    from .incident_candidates import project
    results = [project(conn, revision_id, event_id=event_id) for (revision_id,) in revisions]
    return {"event_id": event_id, "status": "projected", "items": results,
            "public_content_changed": False}


def propose_ledger(conn: Any, event_id: str, *, confirmation: str,
                   title: str) -> dict:
    if confirmation != LEDGER_CONFIRMATION:
        raise ValueError("event_reassessment_ledger_confirmation_required")
    case = conn.execute(
        """SELECT snapshot_version,snapshot_json,status,ledger_id
             FROM event_reassessment_cases WHERE event_id=%s FOR UPDATE""",
        (event_id,),
    ).fetchone()
    if not case or case[2] != "active":
        raise ValueError("event_reassessment_case_unavailable")
    current = snapshot(conn, event_id)
    if current["snapshot_version"] != case[0]:
        raise ValueError("event_reassessment_snapshot_stale")
    record = _decode(case[1])
    article_ids = [int(item["article_id"]) for item in record["articles"]]
    rows = conn.execute(
        """SELECT c.candidate_id,c.article_id,a.original_url,c.title
             FROM incident_candidates c JOIN articles a ON a.id=c.article_id
            WHERE c.article_id=ANY(%s) AND c.status='enrolled' AND c.event_id=%s
            ORDER BY c.candidate_id""", (article_ids, event_id)
    ).fetchall()
    candidate_ids = [row[0] for row in rows]
    from urllib.parse import urlparse
    domains = {(urlparse(str(row[2] or "")).hostname or "").lower().removeprefix("www.")
               for row in rows}
    domains.discard("")
    if len(candidate_ids) < 2 or len(domains) < 2:
        raise ValueError("event_reassessment_independent_sources_required")
    if title != str(record["event"]["title"] or "").strip():
        title = _evidence_title(title, rows)
    ledger_id = case[3] or ("eld_" + _version(
        {"workflow": WORKFLOW, "event_id": event_id, "snapshot_version": case[0]}))
    if case[3]:
        existing = conn.execute(
            """SELECT revision_id,status,ledger_json FROM event_ledger_revisions
                WHERE ledger_id=%s ORDER BY created_at DESC,revision_id DESC LIMIT 1""",
            (ledger_id,),
        ).fetchone()
        if existing:
            prior = _decode(existing[2])
            prior_candidates = sorted(
                str(item.get("candidate_id")) for item in prior.get("sources", [])
            )
            if (existing[1] != "withdrawn" and prior.get("title") == title
                    and prior_candidates == sorted(candidate_ids)):
                return {"event_id": event_id, "ledger_id": ledger_id,
                        "revision_id": existing[0], "status": existing[1],
                        "reused": True, "public_content_changed": False}
    from .event_ledger import propose_initial_sources
    result = propose_initial_sources(conn, candidate_ids, ledger_id=ledger_id,
                                     title=title, commit=False,
                                     replace_open=bool(case[3]))
    now = utc_now_iso()
    if not case[3]:
        conn.execute(
            """INSERT INTO event_ledger_targets
               (ledger_id,event_id,snapshot_version,created_at,created_by)
               VALUES (%s,%s,%s,%s,%s)""",
            (ledger_id, event_id, case[0], now, "admin-token"),
        )
    conn.execute(
        "UPDATE event_reassessment_cases SET ledger_id=%s,updated_at=%s WHERE event_id=%s",
        (ledger_id, now, event_id),
    )
    conn.commit()
    return {**result, "event_id": event_id, "public_content_changed": False}


def target(conn: Any, ledger_id: str) -> dict | None:
    row = conn.execute(
        """SELECT t.event_id,t.snapshot_version,c.status
             FROM event_ledger_targets t
             JOIN event_reassessment_cases c ON c.event_id=t.event_id
            WHERE t.ledger_id=%s""", (ledger_id,)
    ).fetchone()
    if not row:
        return None
    return {"event_id": row[0], "snapshot_version": row[1],
            "reassessment_status": row[2]}


def assert_target_current(conn: Any, ledger_id: str) -> dict | None:
    bound = target(conn, ledger_id)
    if not bound or bound["reassessment_status"] != "active":
        return bound
    if snapshot(conn, bound["event_id"])["snapshot_version"] != bound["snapshot_version"]:
        raise ValueError("event_reassessment_snapshot_stale")
    return bound
