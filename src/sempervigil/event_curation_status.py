"""Read-only Event-specific view of the managed curation pipeline."""
from __future__ import annotations

import json
from typing import Any


def _decode(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    return value if isinstance(value, dict) else {}


def _next_step(evidence: str | None, candidate: str | None,
               ledger: str | None) -> str:
    if not evidence:
        return "extract_evidence"
    if evidence in {"unreviewed", "held"}:
        return "review_evidence"
    if evidence != "accepted":
        return "evidence_closed"
    if not candidate:
        return "project_candidate"
    if candidate in {"suggested", "held"}:
        return "review_candidate"
    if candidate != "enrolled":
        return "candidate_closed"
    if not ledger:
        return "add_to_ledger"
    if ledger in {"proposed", "held"}:
        return "review_ledger"
    return "included_in_ledger" if ledger in {"accepted", "superseded"} else "ledger_closed"


def read(conn: Any, event_id: str) -> dict[str, object]:
    event = conn.execute("SELECT event_key FROM events WHERE id=%s", (event_id,)).fetchone()
    if not event:
        raise ValueError("event_not_found")
    event_key = str(event[0] or "")
    if not event_key.startswith("event-ledger:"):
        return {"event_id": event_id, "managed": False, "sources": [],
                "next_step": "legacy_event"}
    ledger_id = event_key.removeprefix("event-ledger:")
    pointer = conn.execute(
        "SELECT revision_id,updated_at FROM event_public_pointers WHERE event_id=%s",
        (event_id,),
    ).fetchone()
    research = conn.execute(
        """SELECT id,status,requested_at,finished_at FROM jobs
            WHERE job_type='enrich_event_from_web'
              AND payload_json::jsonb->>'event_id'=%s
            ORDER BY requested_at DESC,id DESC LIMIT 1""",
        (event_id,),
    ).fetchone()
    articles = conn.execute(
        """SELECT a.id,a.title,a.original_url,a.source_id
             FROM event_articles ea JOIN articles a ON a.id=ea.article_id
            WHERE ea.event_id=%s ORDER BY a.published_at NULLS LAST,a.id LIMIT 50""",
        (event_id,),
    ).fetchall()
    sources = []
    for article_id, title, url, source_id in articles:
        evidence = conn.execute(
            """SELECT revision_id,status,created_at FROM article_evidence_revisions
                WHERE article_id=%s ORDER BY created_at DESC,revision_id DESC LIMIT 1""",
            (article_id,),
        ).fetchone()
        candidate = None
        if evidence:
            candidate = conn.execute(
                """SELECT candidate_id,status FROM incident_candidates
                    WHERE evidence_revision_id=%s LIMIT 1""",
                (evidence[0],),
            ).fetchone()
        ledger = None
        if candidate:
            ledger = conn.execute(
                """SELECT r.revision_id,r.status FROM event_ledger_revision_sources s
                    JOIN event_ledger_revisions r ON r.revision_id=s.revision_id
                    WHERE s.candidate_id=%s AND r.ledger_id=%s
                    ORDER BY r.created_at DESC,r.revision_id DESC LIMIT 1""",
                (candidate[0], ledger_id),
            ).fetchone()
        sources.append({
            "article_id": int(article_id), "title": title or "Source article",
            "url": url, "source_id": source_id,
            "evidence_revision_id": evidence[0] if evidence else None,
            "evidence_status": evidence[1] if evidence else None,
            "candidate_id": candidate[0] if candidate else None,
            "candidate_status": candidate[1] if candidate else None,
            "ledger_revision_id": ledger[0] if ledger else None,
            "ledger_status": ledger[1] if ledger else None,
            "next_step": _next_step(evidence[1] if evidence else None,
                                     candidate[1] if candidate else None,
                                     ledger[1] if ledger else None),
        })
    ledger = conn.execute(
        """SELECT revision_id,status,created_at FROM event_ledger_revisions
            WHERE ledger_id=%s ORDER BY created_at DESC,revision_id DESC LIMIT 1""",
        (ledger_id,),
    ).fetchone()
    composition = conn.execute(
        """SELECT composition_id,status,created_at FROM event_ledger_compositions
            WHERE ledger_id=%s ORDER BY created_at DESC,composition_id DESC LIMIT 1""",
        (ledger_id,),
    ).fetchone()
    counts = {}
    for source in sources:
        step = source["next_step"]
        counts[step] = counts.get(step, 0) + 1
    return {
        "event_id": event_id, "managed": True, "ledger_id": ledger_id,
        "public_revision_id": pointer[0] if pointer else None,
        "public_updated_at": pointer[1] if pointer else None,
        "latest_research": ({"job_id": research[0], "status": research[1],
                             "requested_at": research[2], "finished_at": research[3]}
                            if research else None),
        "latest_ledger": ({"revision_id": ledger[0], "status": ledger[1],
                           "created_at": ledger[2]} if ledger else None),
        "latest_composition": ({"composition_id": composition[0], "status": composition[1],
                                "created_at": composition[2]} if composition else None),
        "sources": sources, "pending_counts": counts,
        "sources_truncated": len(articles) == 50,
    }
