"""Bounded, evidence-preserving admission of pre-anchor Event candidates."""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from urllib.parse import urlparse

from .storage import (enqueue_job, get_article_by_id, get_event, get_setting,
                      list_event_articles, set_setting)
from .utils import json_dumps, utc_now_iso

JOB_TYPE = "legacy_event_revalidate"
LAST_SCAN_KEY = "event.legacy_revalidation.last_scan_at"
MIN_MATCH_CONFIDENCE = 0.9


def tick(conn, *, now: datetime | None = None) -> dict[str, object]:
    from .config import get_events_settings

    if os.environ.get("SV_LEGACY_EVENT_REVALIDATION_ENABLED", "0") != "1":
        return {"status": "unchanged", "reason": "disabled"}
    if not get_events_settings(conn).get("enabled"):
        return {"status": "unchanged", "reason": "events_disabled"}
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    prior = get_setting(conn, LAST_SCAN_KEY, None)
    if prior:
        try:
            if 0 <= (current - datetime.fromisoformat(str(prior))).total_seconds() < 60:
                return {"status": "unchanged", "reason": "scan_interval"}
        except (TypeError, ValueError):
            pass
    if conn.execute(
        "SELECT 1 FROM jobs WHERE job_type=%s AND status IN ('queued','running') LIMIT 1",
        (JOB_TYPE,),
    ).fetchone():
        return {"status": "unchanged", "reason": "job_active"}
    row = conn.execute(
        """SELECT e.id,e.updated_at FROM events e
            WHERE e.visibility='active' AND e.lifecycle='candidate'
              AND e.publish_state='draft' AND COALESCE(e.manual,0)=0
              AND COALESCE(e.is_manual,0)=0
              AND e.meta_json::jsonb->>'anchor_version' IS NULL
              AND NOT EXISTS (SELECT 1 FROM event_public_pointers p WHERE p.event_id=e.id)
              AND NOT EXISTS (SELECT 1 FROM event_public_revisions r WHERE r.event_id=e.id)
              AND NOT EXISTS (SELECT 1 FROM event_review_approvals a WHERE a.event_id=e.id)
              AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.job_type=%s
                  AND j.payload_json::jsonb->>'event_id'=e.id
                  AND j.payload_json::jsonb->>'event_updated_at'=e.updated_at)
            ORDER BY CASE WHEN lower(e.entity) IN ('not applicable','unknown','none')
                          THEN 1 ELSE 0 END,
                     (SELECT count(*) FROM events d
                       WHERE d.visibility='active' AND d.publish_state='draft'
                         AND lower(d.entity)=lower(e.entity)) DESC,
                     (SELECT count(*) FROM event_articles ea WHERE ea.event_id=e.id) DESC,
                     e.created_at,e.id LIMIT 1""",
        (JOB_TYPE,),
    ).fetchone()
    set_setting(conn, LAST_SCAN_KEY, current.isoformat())
    if not row:
        return {"status": "unchanged", "reason": "backlog_drained"}
    job_id = enqueue_job(conn, JOB_TYPE,
                         {"event_id": row[0], "event_updated_at": row[1]}, priority=-20,
                         queue_name="llm_local", max_attempts=2,
                         dedupe_key=f"legacy-revalidate:{row[0]}:{row[1]}")
    return {"status": "queued", "event_id": row[0], "job_id": job_id}


def _candidate(event: dict | None) -> bool:
    return bool(event and event.get("visibility") == "active"
                and event.get("lifecycle") == "candidate"
                and event.get("publish_state") == "draft"
                and not event.get("published_at") and not event.get("site_slug")
                and not event.get("manual") and not event.get("is_manual"))


def _canonical_target(event: dict | None) -> bool:
    return bool(event and event.get("visibility") == "active"
                and event.get("lifecycle") in {"candidate", "confirmed"}
                and event.get("publish_state") == "draft"
                and event.get("meta", {}).get("anchor_version") == "victim-role-v1")


def _day(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value or "")[:10])
    except ValueError:
        return None


def _nearby(a: dict, b: dict) -> bool:
    first = _day(a.get("incident_date")) or _day(a.get("first_seen_at"))
    second = _day(b.get("incident_date")) or _day(b.get("first_seen_at"))
    return bool(first and second and abs((first - second).days) <= 14)


def _canonical(conn, event: dict) -> dict | None:
    from .worker import EVENT_ANCHOR_VERSION

    rows = conn.execute(
        """SELECT id FROM events WHERE id<>%s AND visibility='active'
              AND lifecycle IN ('candidate','confirmed')
              AND publish_state IN ('draft','published')
              AND lower(entity)=lower(%s)
              AND (meta_json::jsonb->>'anchor_version'=%s OR publish_state='published')
            ORDER BY created_at DESC,id DESC LIMIT 20""",
        (event["id"], event.get("entity") or "", EVENT_ANCHOR_VERSION),
    ).fetchall()
    matches = [candidate for (event_id,) in rows
               if (candidate := get_event(conn, event_id)) and _nearby(event, candidate)]
    if len(matches) > 1:
        return {"ambiguous": True}
    return matches[0] if matches else None


def _source(article: dict) -> dict[str, object]:
    url = str(article.get("original_url") or article.get("normalized_url") or "")
    return {"title": article.get("title"), "url": url, "domain": urlparse(url).hostname or "",
            "snippet": article.get("summary") or ""}


def _all_match(conn, logger: logging.Logger, event: dict, articles: list[dict]) -> bool:
    from .worker import _validate_event_source_with_llm

    for article in articles:
        content = str(article.get("content_text") or "").strip()
        if not content:
            return False
        decision, _ = _validate_event_source_with_llm(
            conn, logger, event=event, source=_source(article), content=content)
        if decision.get("validator") != "llm":
            return False
        if _verified_match(decision):
            continue
        if decision.get("contradictions"):
            return False
        if not _verified_match(_adjudicate_match(conn, logger, event, article, decision)):
            return False
    return True


def _verified_match(decision: dict) -> bool:
    return bool(decision.get("related") is True
                and float(decision.get("confidence") or 0) >= MIN_MATCH_CONFIDENCE
                and not decision.get("contradictions") and decision.get("matched_facts"))


def _adjudicate_match(conn, logger: logging.Logger, event: dict, article: dict,
                      prior: dict) -> dict:
    from .worker import _event_source_validation_profile, _parse_event_web_validation
    from .llm.router import run_profile

    profile = _event_source_validation_profile(conn)
    if not profile:
        return {}
    prompt = (
        "Independently decide whether this article describes the same single incident or a "
        "later update to it. The previous validator gave an uncertain or internally "
        "inconsistent verdict; do not copy it. Compare victim, affected systems/data, "
        "mechanism, and incident timeline. Reported dates, rounded figures, and corrected "
        "totals can differ for one incident. Reject a different attack on the same victim. "
        "Return JSON only: related (bool), confidence (0..1), matched_facts (array), "
        "contradictions (array of facts proving a different incident), rationale (string).\n\n"
        f"Event: {event.get('title')} | {event.get('entity')} | {event.get('incident_date')}\n"
        f"Event summary: {event.get('summary')}\n"
        f"Article: {article.get('title')}\n"
        f"Article content: {str(article.get('content_text') or '')[:12000]}"
    )
    result = run_profile(conn, str(profile["id"]), prompt, logger,
                         context={"stage": "legacy_event_match_adjudication",
                                  "job_type": JOB_TYPE, "profile_name": profile.get("name") or ""})
    parsed, _ = _parse_event_web_validation(result if isinstance(result, dict) else {})
    return parsed or {}


def _no_public_history(conn, event_id: str) -> bool:
    return not conn.execute(
        """SELECT 1 WHERE EXISTS (SELECT 1 FROM event_public_pointers WHERE event_id=%s)
                       OR EXISTS (SELECT 1 FROM event_public_revisions WHERE event_id=%s)""",
        (event_id, event_id),
    ).fetchone()


def _archive_generic(conn, event: dict) -> dict[str, object]:
    if not _no_public_history(conn, event["id"]):
        return {"status": "held", "reason": "public_history_present"}
    conn.execute("SELECT id FROM events WHERE id=%s FOR UPDATE", (event["id"],)).fetchone()
    current = get_event(conn, event["id"])
    if (not _candidate(current) or current.get("updated_at") != event.get("updated_at")
            or not _no_public_history(conn, event["id"])):
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    meta = dict(event.get("meta") or {})
    meta["legacy_revalidation"] = {"status": "archived", "reason": "generic_entity"}
    updated = conn.execute(
        """UPDATE events SET visibility='suppressed',lifecycle='archived',
               meta_json=%s,updated_at=%s WHERE id=%s AND visibility='active'
               AND lifecycle='candidate' AND publish_state='draft'""",
        (json_dumps(meta), utc_now_iso(), event["id"]),
    )
    if updated.rowcount != 1:
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    conn.commit()
    return {"status": "archived", "reason": "generic_entity"}


def _finalize(conn, event_id: str) -> dict[str, object]:
    from .worker import (_enroll_confirmed_draft, _maybe_promote_event_lifecycle,
                         _maybe_queue_event_research)

    event = get_event(conn, event_id)
    if not _canonical_target(event):
        return {"status": "skipped", "reason": "target_changed", "event_id": event_id}
    lifecycle = _maybe_promote_event_lifecycle(conn, event_id, {})
    _enroll_confirmed_draft(conn, event_id, lifecycle)
    researched = _maybe_queue_event_research(conn, event_id) if lifecycle == "candidate" else False
    return {"status": "finalized", "event_id": event_id, "lifecycle": lifecycle,
            "research_queued": researched}


def _admit(conn, event: dict, proposal: dict, article_ids: list[int]) -> dict[str, object]:
    from .worker import EVENT_ANCHOR_VERSION

    if not _no_public_history(conn, event["id"]):
        return {"status": "held", "reason": "public_history_present"}
    conn.execute("SELECT id FROM events WHERE id=%s FOR UPDATE", (event["id"],)).fetchone()
    current = get_event(conn, event["id"])
    linked = {int(item["article_id"]) for item in list_event_articles(conn, event["id"])}
    if (not _candidate(current) or current.get("updated_at") != event.get("updated_at")
            or linked != set(article_ids) or not _no_public_history(conn, event["id"])):
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    meta = dict(event.get("meta") or {})
    meta["anchor_version"] = EVENT_ANCHOR_VERSION
    meta["legacy_revalidation"] = {"status": "accepted", "seed_article_id": proposal["seed_article_id"]}
    updated = conn.execute(
        """UPDATE events SET meta_json=%s,title=%s,summary=%s,entity=%s,kind=%s,
               incident_date=%s,updated_at=%s
             WHERE id=%s AND visibility='active' AND lifecycle='candidate'
               AND publish_state='draft'""",
        (json_dumps(meta), proposal["headline"], proposal["summary"],
         proposal["entity"], proposal["kind"], proposal["incident_date"],
         utc_now_iso(), event["id"]),
    )
    if updated.rowcount != 1:
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    conn.commit()
    return {**_finalize(conn, event["id"]), "status": "admitted"}


def _merge(conn, event: dict, canonical: dict, article_ids: list[int]) -> dict[str, object]:
    if not _no_public_history(conn, event["id"]) or not _no_public_history(conn, canonical["id"]):
        return {"status": "held", "reason": "public_history_present"}
    conn.execute("SELECT id FROM events WHERE id=ANY(%s) ORDER BY id FOR UPDATE",
                 ([event["id"], canonical["id"]],)).fetchall()
    current = get_event(conn, event["id"])
    target = get_event(conn, canonical["id"])
    linked = {int(item["article_id"]) for item in list_event_articles(conn, event["id"])}
    if (not _candidate(current) or not _canonical_target(target)
            or current.get("updated_at") != event.get("updated_at")
            or target.get("updated_at") != canonical.get("updated_at")
            or target.get("meta", {}).get("anchor_version") != "victim-role-v1"
            or linked != set(article_ids) or not _no_public_history(conn, event["id"])
            or not _no_public_history(conn, canonical["id"])):
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    updated = conn.execute(
        """INSERT INTO event_articles(event_id,article_id,added_by,created_at)
            SELECT %s,article_id,%s,%s FROM event_articles WHERE event_id=%s
            ON CONFLICT DO NOTHING""",
        (canonical["id"], JOB_TYPE, utc_now_iso(), event["id"]),
    )
    conn.execute(
        """INSERT INTO event_items(event_id,item_type,item_key,created_at)
            SELECT %s,item_type,item_key,%s FROM event_items WHERE event_id=%s
            ON CONFLICT DO NOTHING""",
        (canonical["id"], utc_now_iso(), event["id"]),
    )
    meta = dict(current.get("meta") or {})
    meta["legacy_revalidation"] = {"status": "merged", "canonical_event_id": canonical["id"]}
    conn.execute(
        """UPDATE events SET visibility='suppressed',lifecycle='archived',meta_json=%s,
               updated_at=%s WHERE id=%s""",
        (json_dumps(meta), utc_now_iso(), event["id"]),
    )
    if updated.rowcount != 1:
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    conn.commit()
    final = _finalize(conn, canonical["id"])
    return {"status": "merged", "event_id": event["id"],
            "canonical_event_id": canonical["id"], "lifecycle": final.get("lifecycle"),
            "articles": len(article_ids)}


def _merge_published(conn, event: dict, canonical: dict, article_ids: list[int]) -> dict[str, object]:
    from .event_reassessment import activate_update

    if not _no_public_history(conn, event["id"]):
        return {"status": "held", "reason": "source_public_history_present"}
    current_target = get_event(conn, canonical["id"])
    if (not current_target or current_target.get("visibility") != "active"
            or current_target.get("publish_state") != "published"
            or current_target.get("updated_at") != canonical.get("updated_at")
            or not str(current_target.get("event_key") or "").startswith("event-ledger:")):
        return {"status": "held", "reason": "published_target_changed"}
    for article_id in article_ids:
        activate_update(conn, canonical["id"], article_id=article_id,
                        created_by=JOB_TYPE)
    conn.execute("SELECT id FROM events WHERE id=%s FOR UPDATE", (event["id"],)).fetchone()
    current = get_event(conn, event["id"])
    linked = {int(item["article_id"]) for item in list_event_articles(conn, event["id"])}
    if (not _candidate(current) or current.get("updated_at") != event.get("updated_at")
            or linked != set(article_ids) or not _no_public_history(conn, event["id"])):
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    meta = dict(current.get("meta") or {})
    meta["legacy_revalidation"] = {"status": "merged", "canonical_event_id": canonical["id"]}
    updated = conn.execute(
        """UPDATE events SET visibility='suppressed',lifecycle='archived',meta_json=%s,
               updated_at=%s WHERE id=%s AND visibility='active'
               AND lifecycle='candidate' AND publish_state='draft'""",
        (json_dumps(meta), utc_now_iso(), event["id"]),
    )
    if updated.rowcount != 1:
        conn.rollback()
        return {"status": "held", "reason": "concurrent_event_change"}
    conn.commit()
    return {"status": "merged", "event_id": event["id"],
            "canonical_event_id": canonical["id"], "lifecycle": "published",
            "articles": len(article_ids)}


def run(conn, job, logger: logging.Logger) -> dict[str, object]:
    from .worker import (_STRICT_EVENT_TYPES, _event_classification_input,
                         _is_generic_event_entity, _normalize_entity,
                         _normalize_event_type, _parse_event_classification)
    from .llm.router import run_pipeline_stage
    from .services.ai_service import get_active_profile_for_stage

    event_id = str((job.payload or {}).get("event_id") or "")
    if job.job_type != JOB_TYPE or not event_id.startswith("evt_"):
        raise ValueError("legacy_event_revalidation_invalid_job")
    event = get_event(conn, event_id)
    legacy = (event or {}).get("meta", {}).get("legacy_revalidation") or {}
    if legacy.get("status") == "accepted":
        return _finalize(conn, event_id)
    if legacy.get("status") == "merged" and legacy.get("canonical_event_id"):
        return _finalize(conn, legacy["canonical_event_id"])
    if not _candidate(event) or event.get("meta", {}).get("anchor_version"):
        return {"status": "skipped", "reason": "candidate_changed", "event_id": event_id}
    linked = list_event_articles(conn, event_id)
    seed_id = int(event.get("meta", {}).get("seed_article_id") or 0)
    article_ids = [int(item["article_id"]) for item in linked]
    if not article_ids:
        return {"status": "held", "reason": "no_articles", "event_id": event_id}
    if seed_id not in article_ids:
        seed_id = article_ids[0]
    articles = [get_article_by_id(conn, article_id) for article_id in article_ids]
    if any(not article or not str(article.get("content_text") or "").strip()
           for article in articles):
        return {"status": "held", "reason": "source_content_missing", "event_id": event_id}
    seed = next(article for article in articles if article["id"] == seed_id)
    profile, _ = get_active_profile_for_stage(conn, "derive_events_from_articles")
    if not profile:
        return {"status": "held", "reason": "classification_profile_missing", "event_id": event_id}
    result = run_pipeline_stage(
        conn, "derive_events_from_articles", _event_classification_input(seed), logger,
        profile_id=profile["id"],
        context={"stage": "derive_events_from_articles", "job_type": JOB_TYPE},
    )
    parsed, error = _parse_event_classification(result if isinstance(result, dict) else {})
    if not error and parsed and not parsed.get("is_event") and _is_generic_event_entity(
            str(event.get("entity") or "")):
        return {"event_id": event_id, **_archive_generic(conn, event)}
    if error or not parsed or not parsed.get("is_event"):
        return {"status": "held", "reason": error or "not_a_specific_incident", "event_id": event_id}
    entity = _normalize_entity(str(parsed.get("victim") or ""))
    kind = _normalize_event_type(str(parsed.get("event_type") or ""))
    if (not entity or _is_generic_event_entity(entity)
            or not str(parsed.get("what_compromised") or "").strip()
            or int(parsed.get("confidence") or 0) < 75):
        return {"status": "held", "reason": "incident_anchor_unverified", "event_id": event_id}
    if (entity.casefold() != str(event.get("entity") or "").casefold()
            and not _is_generic_event_entity(str(event.get("entity") or ""))):
        return {"status": "held", "reason": "victim_identity_changed", "event_id": event_id,
                "proposed_victim": entity}
    proposal = {**event, "title": str(parsed.get("headline") or seed["title"]),
                "summary": str(parsed.get("summary") or ""), "kind": kind,
                "incident_date": parsed.get("incident_date") or event.get("incident_date")}
    if len(articles) > 1 and not _all_match(conn, logger, proposal,
                                           [item for item in articles if item["id"] != seed_id]):
        return {"status": "held", "reason": "linked_articles_not_same_incident", "event_id": event_id}
    canonical = _canonical(conn, proposal)
    if canonical:
        if canonical.get("ambiguous"):
            return {"status": "held", "reason": "multiple_canonical_events", "event_id": event_id}
        if not _all_match(conn, logger, canonical, articles):
            return {"status": "held", "reason": "duplicate_match_unverified", "event_id": event_id,
                    "candidate_canonical_id": canonical["id"]}
        if canonical.get("publish_state") == "published":
            return _merge_published(conn, event, canonical, article_ids)
        return _merge(conn, event, canonical, article_ids)
    if kind not in _STRICT_EVENT_TYPES:
        return {"status": "held", "reason": "incident_type_unverified", "event_id": event_id}
    return _admit(conn, event, {"seed_article_id": seed_id, "entity": entity,
                                "kind": kind, "incident_date": proposal["incident_date"],
                                "headline": proposal["title"], "summary": proposal["summary"]},
                  article_ids)
