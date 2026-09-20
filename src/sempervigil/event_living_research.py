"""Bounded recurring research for explicitly enrolled living Events.

The scheduler only admits an existing web-research job. Search, fetching,
relevance validation, article enrichment, evidence review, composition, and
publication remain in their existing independently guarded stages.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import re
from typing import Any

import psycopg

from .storage import enqueue_job

JOB_TYPE = "enrich_event_from_web"


def enrollments() -> list[str]:
    raw = os.environ.get("SV_EVENT_RESEARCH_EVENTS", "[]")
    if len(raw) > 2048:
        raise ValueError("event_research_enrollments_too_large")
    values = json.loads(raw)
    if (type(values) is not list or len(values) > 10
            or any(type(value) is not str
                   or not re.fullmatch(r"evt_[A-Za-z0-9_-]{1,128}", value)
                   for value in values)
            or len(set(values)) != len(values)):
        raise ValueError("invalid_event_research_enrollments")
    return sorted(values)


def interval_seconds() -> int:
    try:
        value = int(os.environ.get("SV_EVENT_RESEARCH_INTERVAL_SECONDS", "86400"))
    except ValueError as exc:
        raise ValueError("invalid_event_research_interval") from exc
    if not 3600 <= value <= 604800:
        raise ValueError("invalid_event_research_interval")
    return value


def max_results() -> int:
    try:
        value = int(os.environ.get("SV_EVENT_RESEARCH_MAX_RESULTS", "12"))
    except ValueError as exc:
        raise ValueError("invalid_event_research_max_results") from exc
    if not 2 <= value <= 20:
        raise ValueError("invalid_event_research_max_results")
    return value


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def advance(conn: Any, event_id: str, *, now: datetime | None = None) -> dict[str, object]:
    available = conn.execute(
        """SELECT 1 FROM events e
             JOIN event_public_pointers p ON p.event_id=e.id
            WHERE e.id=%s AND e.visibility='active'""",
        (event_id,),
    ).fetchone()
    if not available:
        return {"status": "held", "event_id": event_id,
                "reason": "published_event_unavailable"}
    latest = conn.execute(
        """SELECT status,requested_at,id FROM jobs
            WHERE job_type=%s AND payload_json::jsonb->>'event_id'=%s
            ORDER BY requested_at DESC,id DESC LIMIT 1""",
        (JOB_TYPE, event_id),
    ).fetchone()
    if latest and latest[0] in {"queued", "running"}:
        return {"status": "research_pending", "event_id": event_id, "job_id": latest[2]}
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if latest:
        age = (current - _utc(latest[1])).total_seconds()
        if age < interval_seconds():
            return {"status": "unchanged", "event_id": event_id,
                    "reason": "research_interval", "next_in_seconds": int(interval_seconds() - age)}
    payload = {"event_id": event_id, "max_results": max_results(),
               "replace_existing": False}
    job_id = enqueue_job(conn, JOB_TYPE, payload, dedupe=True)
    return {"status": "research_queued", "event_id": event_id, "job_id": job_id}


def tick(conn: Any) -> list[dict[str, object]]:
    """Admit at most one research job globally per orchestrator pass."""
    results = []
    for event_id in enrollments():
        try:
            result = advance(conn, event_id)
        except (ValueError, OSError, psycopg.Error) as exc:
            conn.rollback()
            result = {"status": "held", "event_id": event_id, "reason": str(exc)[:160]}
        results.append(result)
        if result["status"] in {"research_queued", "research_pending"}:
            break
    return results
