import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from sempervigil import legacy_event_revalidation as legacy

pytestmark = pytest.mark.offline


def test_nearby_uses_incident_date_and_rejects_distant_incidents():
    left = {"incident_date": "2026-09-21", "first_seen_at": "2026-10-01T00:00:00+00:00"}
    assert legacy._nearby(left, {"incident_date": "2026-09-30"})
    assert not legacy._nearby(left, {"incident_date": "2026-10-07"})
    assert not legacy._nearby(left, {"incident_date": "unknown"})


def test_canonical_lookup_holds_multiple_plausible_records(monkeypatch):
    class Conn:
        def execute(self, sql, params=()):
            assert "lifecycle IN ('candidate','confirmed')" in sql
            return SimpleNamespace(fetchall=lambda: [("evt_a",), ("evt_b",)])

    records = {
        "evt_a": {"id": "evt_a", "incident_date": "2026-09-25"},
        "evt_b": {"id": "evt_b", "incident_date": "2026-09-26"},
    }
    monkeypatch.setattr(legacy, "get_event", lambda _conn, event_id: records[event_id])
    assert legacy._canonical(Conn(), {"id": "evt_old", "entity": "Bitget",
                                      "incident_date": "2026-09-24"}) == {"ambiguous": True}


def test_all_match_rejects_non_model_fallback(monkeypatch):
    from sempervigil import worker

    monkeypatch.setattr(worker, "_validate_event_source_with_llm",
                        lambda *_args, **_kwargs: ({
                            "validator": "fallback_no_profile", "related": True,
                            "confidence": 1.0, "matched_facts": ["same victim"],
                            "contradictions": [],
                        }, "fallback_no_profile"))
    assert not legacy._all_match(None, logging.getLogger(__name__), {"id": "evt_a"}, [
        {"title": "Story", "original_url": "https://example.org/story",
         "content_text": "A specific incident."},
    ])


def test_all_match_requires_uncontradicted_source_facts(monkeypatch):
    from sempervigil import worker

    answer = {"validator": "llm", "related": True, "confidence": 0.95,
              "matched_facts": ["same victim"], "contradictions": ["different attack"]}
    monkeypatch.setattr(worker, "_validate_event_source_with_llm",
                        lambda *_args, **_kwargs: (answer, ""))
    monkeypatch.setattr(legacy, "_adjudicate_match", lambda *_args: {})
    article = {"title": "Story", "original_url": "https://example.org/story",
               "content_text": "A specific incident."}
    assert not legacy._all_match(None, logging.getLogger(__name__), {}, [article])
    answer["contradictions"] = []
    assert legacy._all_match(None, logging.getLogger(__name__), {}, [article])


def test_inconsistent_model_verdict_needs_independent_adjudication(monkeypatch):
    from sempervigil import worker

    monkeypatch.setattr(worker, "_validate_event_source_with_llm",
                        lambda *_args, **_kwargs: ({
                            "validator": "llm", "related": False, "confidence": 1.0,
                            "matched_facts": ["same victim", "same loss"],
                            "contradictions": [],
                        }, ""))
    article = {"title": "Follow-up", "content_text": "The same attack was updated."}
    monkeypatch.setattr(legacy, "_adjudicate_match", lambda *_args: {
        "related": False, "confidence": 1.0, "matched_facts": ["same victim"],
        "contradictions": [],
    })
    assert not legacy._all_match(None, logging.getLogger(__name__), {}, [article])
    monkeypatch.setattr(legacy, "_adjudicate_match", lambda *_args: {
        "related": True, "confidence": 0.95, "matched_facts": ["same victim", "same loss"],
        "contradictions": [],
    })
    assert legacy._all_match(None, logging.getLogger(__name__), {}, [article])
    monkeypatch.setattr(worker, "_validate_event_source_with_llm",
                        lambda *_args, **_kwargs: ({
                            "validator": "llm", "related": False, "confidence": 0.6,
                            "matched_facts": ["same victim"],
                            "contradictions": ["Possible date conflict"],
                        }, ""))
    assert legacy._all_match(None, logging.getLogger(__name__), {}, [article])


def test_source_validator_distinguishes_incident_conflicts_from_updates(monkeypatch):
    from sempervigil import worker

    prompts = []
    monkeypatch.setattr(worker, "_event_source_validation_profile", lambda _conn: {"id": "profile"})
    monkeypatch.setattr(worker, "run_profile", lambda _conn, _id, prompt, _logger, **_kw:
                        prompts.append(prompt) or {"parsed": {
                            "related": True, "confidence": 0.99,
                            "matched_facts": ["same incident"], "contradictions": [],
                        }})
    decision, _ = worker._validate_event_source_with_llm(
        None, logging.getLogger(__name__), event={"title": "Bitget theft"},
        source={"title": "Bitget update", "domain": "example.org"}, content="Update")
    assert decision["related"] is True
    assert "rounding differences" in prompts[0]
    assert "different incident in contradictions" in prompts[0]


def test_run_holds_changed_victim_without_mutating_event(monkeypatch):
    from sempervigil import worker
    from sempervigil.llm import router
    from sempervigil.services import ai_service

    event = {"id": "evt_old", "entity": "Security Publisher", "visibility": "active",
             "lifecycle": "candidate", "publish_state": "draft", "meta": {"seed_article_id": 7}}
    article = {"id": 7, "title": "Acme breached", "content_text": "Acme was breached.",
               "original_url": "https://example.org/acme"}
    monkeypatch.setattr(legacy, "get_event", lambda *_args: event)
    monkeypatch.setattr(legacy, "list_event_articles", lambda *_args: [{"article_id": 7}])
    monkeypatch.setattr(legacy, "get_article_by_id", lambda *_args: article)
    monkeypatch.setattr(ai_service, "get_active_profile_for_stage",
                        lambda *_args: ({"id": "profile"}, None))
    monkeypatch.setattr(router, "run_pipeline_stage",
                        lambda *_args, **_kwargs: {"parsed": {
                            "is_event": True, "victim": "Acme", "event_type": "breach",
                            "what_compromised": "customer records", "confidence": 95,
                            "headline": "Acme breached", "summary": "Acme lost data.",
                        }})
    monkeypatch.setattr(worker, "_normalize_event_type", lambda _value: "breach")
    monkeypatch.setattr(legacy, "_admit", lambda *_args: pytest.fail("must not admit"))
    result = legacy.run(None, SimpleNamespace(job_type=legacy.JOB_TYPE,
                        payload={"event_id": "evt_old"}), logging.getLogger(__name__))
    assert result["status"] == "held"
    assert result["reason"] == "victim_identity_changed"


def test_generic_legacy_victim_is_reclassified_before_disposition(monkeypatch):
    from sempervigil import worker
    from sempervigil.llm import router
    from sempervigil.services import ai_service

    event = {"id": "evt_old", "entity": "not applicable", "visibility": "active",
             "lifecycle": "candidate", "publish_state": "draft", "meta": {"seed_article_id": 7}}
    article = {"id": 7, "title": "Acme breached", "content_text": "Acme was breached.",
               "original_url": "https://example.org/acme"}
    monkeypatch.setattr(legacy, "get_event", lambda *_args: event)
    monkeypatch.setattr(legacy, "list_event_articles", lambda *_args: [{"article_id": 7}])
    monkeypatch.setattr(legacy, "get_article_by_id", lambda *_args: article)
    monkeypatch.setattr(ai_service, "get_active_profile_for_stage",
                        lambda *_args: ({"id": "profile"}, None))
    monkeypatch.setattr(router, "run_pipeline_stage",
                        lambda *_args, **_kwargs: {"parsed": {
                            "is_event": True, "victim": "Acme", "event_type": "breach",
                            "what_compromised": "customer records", "confidence": 95,
                            "headline": "Acme breached", "summary": "Acme lost data.",
                        }})
    monkeypatch.setattr(worker, "_normalize_event_type", lambda _value: "breach")
    monkeypatch.setattr(legacy, "_canonical", lambda *_args: None)
    captured = {}
    monkeypatch.setattr(legacy, "_admit", lambda _conn, _event, proposal, _ids:
                        captured.update(proposal) or {"status": "admitted"})
    job = SimpleNamespace(job_type=legacy.JOB_TYPE, payload={"event_id": "evt_old"})
    assert legacy.run(None, job, logging.getLogger(__name__))["status"] == "admitted"
    assert captured["entity"] == "Acme"


def test_run_merges_only_after_source_and_canonical_match(monkeypatch):
    from sempervigil import worker
    from sempervigil.llm import router
    from sempervigil.services import ai_service

    event = {"id": "evt_old", "entity": "Bitget", "visibility": "active",
             "lifecycle": "candidate", "publish_state": "draft", "meta": {"seed_article_id": 7}}
    articles = {
        7: {"id": 7, "title": "Bitget theft", "content_text": "Bitget lost funds."},
        8: {"id": 8, "title": "Bitget update", "content_text": "Bitget described the theft."},
    }
    monkeypatch.setattr(legacy, "get_event", lambda *_args: event)
    monkeypatch.setattr(legacy, "list_event_articles",
                        lambda *_args: [{"article_id": 7}, {"article_id": 8}])
    monkeypatch.setattr(legacy, "get_article_by_id", lambda _conn, article_id: articles[article_id])
    monkeypatch.setattr(ai_service, "get_active_profile_for_stage",
                        lambda *_args: ({"id": "profile"}, None))
    monkeypatch.setattr(router, "run_pipeline_stage",
                        lambda *_args, **_kwargs: {"parsed": {
                            "is_event": True, "victim": "Bitget", "event_type": "other",
                            "what_compromised": "funds", "confidence": 95,
                            "headline": "Bitget theft", "summary": "Bitget lost funds.",
                        }})
    monkeypatch.setattr(legacy, "_canonical", lambda *_args: {"id": "evt_new"})
    outcomes = iter([True, False])
    monkeypatch.setattr(legacy, "_all_match", lambda *_args: next(outcomes))
    monkeypatch.setattr(legacy, "_merge", lambda *_args: pytest.fail("must hold"))
    job = SimpleNamespace(job_type=legacy.JOB_TYPE, payload={"event_id": "evt_old"})
    result = legacy.run(None, job, logging.getLogger(__name__))
    assert result["reason"] == "duplicate_match_unverified"
    monkeypatch.setattr(legacy, "_all_match", lambda *_args: True)
    monkeypatch.setattr(legacy, "_merge", lambda _conn, source, target, ids: {
        "status": "merged", "event_id": source["id"],
        "canonical_event_id": target["id"], "articles": len(ids),
    })
    assert legacy.run(None, job, logging.getLogger(__name__)) == {
        "status": "merged", "event_id": "evt_old",
        "canonical_event_id": "evt_new", "articles": 2,
    }
    monkeypatch.setattr(legacy, "_canonical", lambda *_args: None)
    assert legacy.run(None, job, logging.getLogger(__name__))["reason"] == "incident_type_unverified"
    monkeypatch.setattr(legacy, "_canonical", lambda *_args: {
        "id": "evt_new", "publish_state": "published"})
    monkeypatch.setattr(legacy, "_merge_published", lambda _conn, source, target, ids: {
        "status": "merged", "event_id": source["id"],
        "canonical_event_id": target["id"], "articles": len(ids),
    })
    assert legacy.run(None, job, logging.getLogger(__name__))["canonical_event_id"] == "evt_new"


def test_published_merge_requires_unchanged_ledger_target(monkeypatch):
    monkeypatch.setattr(legacy, "_no_public_history", lambda *_args: True)
    monkeypatch.setattr(legacy, "get_event", lambda *_args: {
        "visibility": "active", "publish_state": "published",
        "updated_at": "later", "event_key": "event-ledger:eld_123",
    })
    assert legacy._merge_published(None, {"id": "evt_old"},
                                   {"id": "evt_new", "updated_at": "earlier"}, [7]) == {
        "status": "held", "reason": "published_target_changed"}


def test_two_article_merge_checks_archive_update_not_insert_count(monkeypatch):
    source = {"id": "evt_old", "visibility": "active", "lifecycle": "candidate",
              "publish_state": "draft", "updated_at": "source-v1", "meta": {}}
    target = {"id": "evt_new", "visibility": "active", "lifecycle": "candidate",
              "publish_state": "draft", "updated_at": "target-v1",
              "meta": {"anchor_version": "victim-role-v1"}}

    class Result:
        def __init__(self, count=0):
            self.rowcount = count

        def fetchall(self):
            return []

    class Conn:
        def __init__(self):
            self.commits = 0

        def execute(self, sql, params=()):
            if "INSERT INTO event_articles" in sql:
                return Result(2)
            if "UPDATE events SET visibility='suppressed'" in sql:
                return Result(1)
            return Result()

        def commit(self):
            self.commits += 1

        def rollback(self):
            pytest.fail("valid merge must not roll back")

    monkeypatch.setattr(legacy, "get_event", lambda _conn, event_id:
                        source if event_id == "evt_old" else target)
    monkeypatch.setattr(legacy, "list_event_articles", lambda *_args: [
        {"article_id": 7}, {"article_id": 8}])
    monkeypatch.setattr(legacy, "_no_public_history", lambda *_args: True)
    monkeypatch.setattr(legacy, "_finalize", lambda *_args: {"lifecycle": "confirmed"})
    conn = Conn()
    result = legacy._merge(conn, source, target, [7, 8])
    assert result["status"] == "merged"
    assert result["articles"] == 2
    assert conn.commits == 1


def test_only_anchored_drafts_compete_for_confirmation():
    from sempervigil import worker

    class Conn:
        def execute(self, sql, params):
            assert "meta_json::jsonb->>'anchor_version'=%s" in sql
            assert params[1] == worker.EVENT_ANCHOR_VERSION
            return SimpleNamespace(fetchone=lambda: None)

    assert not worker._has_competing_draft(Conn(), {
        "id": "evt_new", "entity": "Bitget", "first_seen_at": "2026-09-24"})


def test_tick_enqueues_one_low_priority_job(monkeypatch):
    from sempervigil import config

    class Result:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class Conn:
        def execute(self, sql, params=()):
            if "FROM jobs WHERE job_type" in sql and "status IN" in sql:
                return Result(None)
            assert "SELECT e.id,e.updated_at FROM events e" in sql
            return Result(("evt_old", "2026-09-24T00:00:00+00:00"))

    monkeypatch.setattr(config, "get_events_settings", lambda _conn: {"enabled": True})
    monkeypatch.setenv("SV_LEGACY_EVENT_REVALIDATION_ENABLED", "1")
    monkeypatch.setattr(legacy, "get_setting", lambda *_args: None)
    monkeypatch.setattr(legacy, "set_setting", lambda *_args: None)
    captured = {}
    monkeypatch.setattr(legacy, "enqueue_job", lambda _conn, kind, payload, **kw:
                        captured.update(kind=kind, payload=payload, **kw) or "job_old")
    result = legacy.tick(Conn(), now=datetime(2026, 10, 1, tzinfo=timezone.utc))
    assert result == {"status": "queued", "event_id": "evt_old", "job_id": "job_old"}
    assert captured["priority"] < 0
    assert captured["queue_name"] == "llm_local"
    assert captured["payload"]["event_updated_at"] == "2026-09-24T00:00:00+00:00"
