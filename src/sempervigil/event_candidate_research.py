"""Bounded first research pass for recent, uncorroborated Event candidates."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .config import get_events_settings
from .storage import enqueue_job, get_event, get_setting, set_setting

LAST_SCAN_KEY = "event.candidate_research.last_scan_at"
JOB_TYPE = "enrich_event_from_web"


def tick(conn, *, now: datetime | None = None) -> dict[str, object]:
    settings = get_events_settings(conn)
    if not settings.get("enabled") or int(settings.get("enrich_min_articles", 0) or 0) < 2:
        return {"status": "disabled"}
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    prior = get_setting(conn, LAST_SCAN_KEY, None)
    if prior:
        try:
            elapsed = (current - datetime.fromisoformat(str(prior))).total_seconds()
        except (TypeError, ValueError):
            elapsed = 300
        if 0 <= elapsed < 300:
            return {"status": "unchanged", "reason": "scan_interval"}
    earliest = (current - timedelta(days=14)).isoformat()
    from .worker import EVENT_ANCHOR_VERSION, _eligible_event_anchor
    rows = conn.execute(
        """SELECT e.id FROM events e
            JOIN event_articles ea ON ea.event_id=e.id
            JOIN articles a ON a.id=ea.article_id
            WHERE e.visibility='active' AND e.lifecycle='candidate'
              AND e.publish_state='draft' AND e.created_at>=%s
              AND e.meta_json::jsonb->>'anchor_version'=%s
              AND NOT EXISTS (
                  SELECT 1 FROM jobs j WHERE j.job_type=%s
                    AND j.payload_json::jsonb->>'event_id'=e.id
              )
            GROUP BY e.id
            HAVING COUNT(DISTINCT lower(regexp_replace(
                split_part(a.original_url,'/',3), '^www[.]', ''))) = 1
            ORDER BY e.confidence DESC NULLS LAST,e.created_at DESC,e.id LIMIT 32""",
        (earliest, EVENT_ANCHOR_VERSION, JOB_TYPE),
    ).fetchall()
    set_setting(conn, LAST_SCAN_KEY, current.isoformat())
    event_id = next((row[0] for row in rows
                     if _eligible_event_anchor(get_event(conn, row[0]))), None)
    if not event_id:
        return {"status": "unchanged", "reason": "no_unresearched_candidate"}
    maximum = max(2, min(12, int(settings.get("enrich_min_articles_max_results", 6) or 6)))
    job_id = enqueue_job(conn, JOB_TYPE,
                         {"event_id": event_id, "max_results": maximum,
                          "replace_existing": False}, dedupe=True)
    return {"status": "research_queued", "event_id": event_id, "job_id": job_id}
