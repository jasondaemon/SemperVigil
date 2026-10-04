import logging

import pytest

from sempervigil import worker

from sempervigil.worker import (
    _draft_event_in_scope,
    _is_regulatory_only_event_classification,
    _legacy_event_key,
    _same_event_entity,
    _select_existing_event_match,
)

pytestmark = pytest.mark.offline


def test_regulatory_rule_breach_is_not_a_cyber_incident():
    parsed = {
        "headline": "Google fined for EU location data rule breach",
        "summary": "A regulator imposed a penalty for unlawful processing.",
        "what_compromised": "Location data was mishandled under privacy rules.",
    }

    assert _is_regulatory_only_event_classification(parsed)


def test_regulatory_action_after_real_intrusion_remains_eligible():
    parsed = {
        "headline": "Company fined after attackers stole customer records",
        "summary": "The regulator penalized the company after a confirmed intrusion.",
        "what_compromised": "Customer data was stolen after unauthorized access.",
    }

    assert not _is_regulatory_only_event_classification(parsed)


def test_legacy_event_key_separates_incidents_by_date():
    assert _legacy_event_key("breach", "Google", "2026-09-21") == (
        "event:breach:google:2026-09-21"
    )
    assert _legacy_event_key("breach", "Google", "2026-05-21") != (
        _legacy_event_key("breach", "Google", "2026-09-21")
    )


def test_legacy_event_key_keeps_same_incident_stable():
    assert _legacy_event_key("breach", "Example Corp", "2026-09-21") == (
        _legacy_event_key("breach", "Example Corp", "2026-09-21")
    )


def test_existing_event_match_requires_one_high_confidence_supported_match():
    waterplum = {"id": "evt_waterplum"}
    unrelated = {"id": "evt_unrelated"}
    selected, reason = _select_existing_event_match([
        (waterplum, {"related": True, "confidence": 0.93, "contradictions": [],
                     "matched_facts": ["same victim and intrusion"]}),
        (unrelated, {"related": False, "confidence": 0.97, "contradictions": [],
                     "matched_facts": []}),
    ])

    assert selected == waterplum
    assert reason == "unique_match"


def test_existing_event_match_abstains_when_multiple_events_match():
    selected, reason = _select_existing_event_match([
        ({"id": "evt_one"}, {"related": True, "confidence": 0.9, "contradictions": [],
                             "matched_facts": ["same victim"]}),
        ({"id": "evt_two"}, {"related": True, "confidence": 0.91, "contradictions": [],
                             "matched_facts": ["same victim"]}),
    ])

    assert selected is None
    assert reason == "ambiguous_match"


def test_existing_event_match_rejects_contradicted_or_low_confidence_results():
    selected, reason = _select_existing_event_match([
        ({"id": "evt_one"}, {"related": True, "confidence": 0.79, "contradictions": [],
                             "matched_facts": ["same victim"]}),
        ({"id": "evt_two"}, {"related": True, "confidence": 0.99,
                              "contradictions": ["different campaign"],
                              "matched_facts": ["same victim"]}),
    ])

    assert selected is None
    assert reason == "no_match"


def test_existing_event_match_requires_llm_supported_facts():
    selected, reason = _select_existing_event_match([
        ({"id": "evt_bare"}, {"related": True, "confidence": 0.99,
                              "contradictions": [], "matched_facts": []}),
        ({"id": "evt_fallback"}, {"related": True, "confidence": 0.99,
                                  "contradictions": [], "matched_facts": ["entity:FBI"],
                                  "validator": "fallback"}),
    ])
    assert selected is None
    assert reason == "no_match"


def test_draft_match_accepts_victim_acronym_and_adjacent_report_dates():
    assert _same_event_entity("FBI", "U.S. Federal Bureau of Investigation")
    event = {"entity": "FBI", "incident_date": "2026-09-22"}
    assert _draft_event_in_scope(
        event, entity="U.S. Federal Bureau of Investigation",
        incident_date="2026-09-23", window_days=14,
    )


def test_draft_match_excludes_other_victims_and_distant_incidents():
    event = {"entity": "Clop", "incident_date": "2026-09-22"}
    assert not _draft_event_in_scope(
        event, entity="FBI", incident_date="2026-09-23", window_days=14,
    )
    assert not _draft_event_in_scope(
        {"entity": "FBI", "incident_date": "2026-05-23"},
        entity="FBI", incident_date="2026-09-23", window_days=14,
    )


def test_draft_gains_sources_then_confirms_at_two_publishers(monkeypatch):
    updates = []
    sources = ["publisher-one"]
    monkeypatch.setattr(worker, "list_event_articles", lambda _conn, _id: [
        {"source_id": source} for source in sources
    ])
    monkeypatch.setattr(worker, "update_event", lambda _conn, _id, **fields: updates.append(fields))
    monkeypatch.setattr(worker, "get_event", lambda *_: {"id": "evt_test", "entity": "Example Corp",
                                                         "first_seen_at": "2026-09-29",
                                                         "meta": {"anchor_version": worker.EVENT_ANCHOR_VERSION}})
    monkeypatch.setattr(worker, "_has_competing_draft", lambda *_: False)

    assert worker._maybe_promote_event_lifecycle(None, "evt_test", {}) == "candidate"
    sources.append("publisher-two")
    assert worker._maybe_promote_event_lifecycle(None, "evt_test", {}) == "confirmed"
    assert updates[-1] == {"candidate": False, "lifecycle": "confirmed", "status": "confirmed"}


def test_web_research_counts_real_sites_not_synthetic_source(monkeypatch):
    updates = []
    monkeypatch.setattr(worker, "list_event_articles", lambda _conn, _id: [
        {"source_id": "news", "url": "https://www.news.example/incident"},
        {"source_id": "web_enrich", "url": "https://news.example/follow-up"},
        {"source_id": "web_enrich", "url": "https://other.example/report"},
        {"source_id": "web_enrich", "url": "https://third.example/report"},
    ])
    monkeypatch.setattr(worker, "update_event", lambda _conn, _id, **fields: updates.append(fields))
    monkeypatch.setattr(worker, "get_event", lambda *_: {"id": "evt_test", "entity": "Example Corp",
                                                         "first_seen_at": "2026-09-29",
                                                         "meta": {"anchor_version": worker.EVENT_ANCHOR_VERSION}})
    monkeypatch.setattr(worker, "_has_competing_draft", lambda *_: False)
    assert worker._maybe_promote_event_lifecycle(None, "evt_test", {}) == "confirmed"
    assert updates[-1]["lifecycle"] == "confirmed"


def test_invalid_model_incident_dates_are_not_stored():
    assert worker._valid_incident_date("2026-09-21") == "2026-09-21"
    assert worker._valid_incident_date("2026-06-00|unknown") == ""
    assert worker._valid_incident_date("2026-09-00") == ""
    assert worker._valid_incident_date("2026-02-30") == ""


def test_placeholder_or_multi_victim_is_not_an_event_anchor():
    for entity in ("not applicable", "unknown", "Affected Organization",
                   "Company A; Company B", "Azure tenants"):
        assert worker._is_generic_event_entity(entity)
    assert not worker._is_generic_event_entity("Bitget")
    assert not worker._eligible_event_anchor({"entity": "Bitget", "meta": {}})
    assert worker._eligible_event_anchor({"entity": "Bitget", "meta": {
        "anchor_version": worker.EVENT_ANCHOR_VERSION,
    }})


def test_competing_recent_draft_holds_confirmation(monkeypatch):
    class Connection:
        def execute(self, sql, params):
            assert params == ("evt_new", worker.EVENT_ANCHOR_VERSION,
                              "Bitget", "2026-09-16", "2026-10-14")
            return self

        def fetchone(self):
            return ("evt_old",)

    monkeypatch.setattr(worker, "list_event_articles", lambda *_: [
        {"url": "https://first.example/report"},
        {"url": "https://second.example/report"},
    ])
    monkeypatch.setattr(worker, "get_event", lambda *_: {
        "id": "evt_new", "entity": "Bitget", "first_seen_at": "2026-09-30",
        "meta": {"anchor_version": worker.EVENT_ANCHOR_VERSION},
    })
    monkeypatch.setattr(worker, "update_event", lambda *_args, **_kwargs: pytest.fail("promoted duplicate"))
    assert worker._maybe_promote_event_lifecycle(Connection(), "evt_new", {}) == "candidate"


def test_draft_match_uses_first_seen_for_legacy_malformed_date():
    assert _draft_event_in_scope(
        {"entity": "Bitget", "incident_date": "2026-09-00",
         "first_seen_at": "2026-09-28T10:00:00+00:00"},
        entity="Bitget", incident_date="2026-09-30", window_days=14,
    )


def test_candidate_research_admission_is_per_event_not_global(monkeypatch):
    monkeypatch.setattr(worker, "get_events_settings", lambda _conn: {
        "enrich_min_articles": 2, "enrich_min_articles_max_results": 6,
    })
    monkeypatch.setattr(worker, "list_event_articles", lambda _conn, _id: [
        {"source_id": "one", "url": "https://one.example/story"},
    ])
    monkeypatch.setattr(worker, "get_event", lambda *_: {
        "entity": "Example Corp", "meta": {"anchor_version": worker.EVENT_ANCHOR_VERSION},
    })
    queued = []
    monkeypatch.setattr(worker, "enqueue_job", lambda *args, **kwargs: queued.append((args[2], kwargs)))

    assert worker._maybe_queue_event_research(None, "evt_one")
    assert worker._maybe_queue_event_research(None, "evt_two")
    assert [item[0]["event_id"] for item in queued] == ["evt_one", "evt_two"]
    assert all(item[1] == {"dedupe": True} for item in queued)


def test_validated_research_source_can_confirm_and_enroll_draft(monkeypatch):
    from types import SimpleNamespace

    calls = []
    monkeypatch.setattr(worker, "get_event_web_source", lambda *_: {
        "id": "src_1", "event_id": "evt_1", "status": "new",
        "url": "https://independent.example/report",
    })
    monkeypatch.setattr(worker, "get_event", lambda *_: {
        "id": "evt_1", "publish_state": "draft", "entity": "Example",
    })
    monkeypatch.setattr(worker, "get_recent_event_source_version", lambda *_, **__: {
        "source_version": "v1", "content_text": "Example disclosed an intrusion.",
        "published_at": "2026-09-30",
    })
    monkeypatch.setattr(worker, "_event_source_validator_version", lambda *_: "validator-v1")
    monkeypatch.setattr(worker, "get_event_relevance_receipt", lambda *_: None)
    monkeypatch.setattr(worker, "_validate_event_source_with_llm", lambda *_, **__: ({
        "related": True, "confidence": 0.93, "validator": "llm",
        "matched_facts": ["same incident"], "contradictions": [],
    }, ""))
    monkeypatch.setattr(worker, "store_event_relevance_receipt", lambda *_, **__: None)
    monkeypatch.setattr(worker, "update_event_web_source_status", lambda *args, **kwargs: calls.append(("status", args[2])))
    monkeypatch.setattr(worker, "update_event_web_source_published_at", lambda *_, **__: None)
    monkeypatch.setattr(worker, "promote_event_web_source_to_article", lambda *_: 42)
    monkeypatch.setattr(worker, "update_article_content", lambda *_, **__: calls.append(("content", 42)))
    monkeypatch.setattr(worker, "get_article_by_id", lambda *_: {"id": 42, "source_id": "web_enrich"})
    monkeypatch.setattr(worker, "_maybe_promote_event_lifecycle", lambda *_, **__: calls.append(("confirm", 42)) or "confirmed")
    monkeypatch.setattr(worker, "_enroll_confirmed_draft", lambda *args: calls.append(("enroll", args[2])))
    monkeypatch.setattr(worker, "enqueue_job", lambda *args, **kwargs: calls.append(("job", args[1])))

    result = worker._handle_validate_event_web_source(
        None, SimpleNamespace(ingest=SimpleNamespace()), {"source_id": "src_1"}, None,
    )

    assert result["status"] == "promoted"
    assert ("confirm", 42) in calls
    assert ("enroll", "confirmed") in calls
    assert calls.index(("content", 42)) < calls.index(("confirm", 42))


def test_draft_update_rebuilds_report_without_publishing(monkeypatch):
    calls = []
    monkeypatch.setattr(worker, "link_event_article", lambda *_args: calls.append("link"))
    monkeypatch.setattr(worker, "_maybe_promote_event_lifecycle", lambda *_args: "confirmed")
    monkeypatch.setattr(worker, "_enroll_confirmed_draft", lambda *_args: calls.append("enroll"))
    monkeypatch.setattr(worker, "update_event_summary_from_articles", lambda *_args: calls.append("summary"))
    monkeypatch.setattr(worker, "enqueue_job", lambda *_args, **_kwargs: calls.append("report"))
    monkeypatch.setattr(worker, "_maybe_queue_event_research", lambda *_args: calls.append("research"))

    result = worker._link_existing_draft_update(
        None, event_id="evt_test", article_id=123, article={},
    )

    assert result["lifecycle"] == "confirmed"
    assert calls == ["link", "enroll", "summary", "report", "research"]


def test_draft_matching_uses_victim_and_validator_not_just_actor(monkeypatch):
    events = [
        {"id": "evt_fbi", "entity": "FBI", "incident_date": "2026-09-22",
         "publish_state": "draft", "title": "FBI jobs portal breach"},
        {"id": "evt_clop", "entity": "Clop", "incident_date": "2026-09-22",
         "publish_state": "draft", "title": "Clop leak-site breach"},
    ]
    checked = []
    monkeypatch.setattr(worker, "_existing_event_candidates_for_article", lambda *_args, **_kwargs: events)

    def validate(_conn, _logger, *, event, source, content):
        checked.append(event["id"])
        return {"related": True, "confidence": 0.92, "contradictions": [],
                "matched_facts": ["same FBI jobs-site incident"]}, "llm"

    monkeypatch.setattr(worker, "_validate_event_source_with_llm", validate)
    selected, reason = worker._match_existing_event_for_article(
        None, None, article_id=123,
        article={"title": "FBI jobs portal hacked", "original_url": "https://publisher.test/story"},
        content="ShinyHunters claims a compromise of the FBI jobs portal.",
        entity="U.S. Federal Bureau of Investigation", incident_date="2026-09-23",
    )

    assert selected["id"] == "evt_fbi"
    assert reason == "unique_match"
    assert checked == ["evt_fbi"]


def test_draft_lookup_does_not_require_threat_actor_tag(monkeypatch):
    class Connection:
        def execute(self, sql, params):
            if "FROM article_threat_actors current_actor" in sql:
                return self
            assert params == ("2026-09-09", "2026-10-07", "2026-09-09", "2026-10-07", 200)
            self.draft_query = True
            return self

        def fetchall(self):
            return [("evt_fbi",)] if getattr(self, "draft_query", False) else []

    monkeypatch.setattr(worker, "get_event", lambda _conn, event_id: {"id": event_id})
    found = worker._existing_event_candidates_for_article(
        Connection(), 123, incident_date="2026-09-23", window_days=14,
    )
    assert found == [{"id": "evt_fbi"}]


def test_published_lookup_uses_victim_without_actor_tag(monkeypatch):
    class Connection:
        def execute(self, sql, params):
            self.sql = sql
            if "FROM article_threat_actors current_actor" in sql:
                self.rows = []
            elif "e.publish_state='published'" in sql:
                assert params == ("Example Corp", 8)
                self.rows = [("evt_published",)]
            else:
                self.rows = []
            return self

        def fetchall(self):
            return self.rows

    monkeypatch.setattr(worker, "get_event", lambda _conn, event_id: {
        "id": event_id, "entity": "Example Corp", "publish_state": "published",
    })
    found = worker._existing_event_candidates_for_article(
        Connection(), 123, entity="Example Corp", incident_date="2026-09-23",
    )
    assert [event["id"] for event in found] == ["evt_published"]


def test_shared_actor_draft_reaches_validator_when_article_is_classified_non_event(monkeypatch):
    event = {"id": "evt_arrest", "entity": "FBI", "incident_date": None,
             "publish_state": "draft", "_shared_actor_candidate": True}
    monkeypatch.setattr(
        worker, "_existing_event_candidates_for_article",
        lambda *_args, **_kwargs: [event],
    )
    checked = []

    def validate(_conn, _logger, *, event, source, content):
        checked.append(event["id"])
        return {"related": True, "confidence": 0.94, "contradictions": [],
                "matched_facts": ["same FBI jobs-site incident"]}, "llm"

    monkeypatch.setattr(worker, "_validate_event_source_with_llm", validate)
    selected, reason = worker._match_existing_event_for_article(
        None, None, article_id=36682,
        article={"title": "Suspect detained", "original_url": "https://example.test"},
        content="A suspected member was detained and is cooperating.",
        entity="", incident_date="2026-10-03",
    )
    assert selected["id"] == "evt_arrest"
    assert reason == "unique_match"
    assert checked == ["evt_arrest"]


def test_unique_published_match_wins_over_overlapping_private_drafts(monkeypatch):
    events = [
        {"id": "evt_draft", "publish_state": "draft", "entity": "FBI",
         "incident_date": "2026-09-23", "_shared_actor_candidate": True},
        {"id": "evt_published", "publish_state": "published", "entity": "FBI",
         "incident_date": "2026-09-22"},
    ]
    checked = []
    monkeypatch.setattr(worker, "_existing_event_candidates_for_article", lambda *_args, **_kwargs: events)

    def validate(_conn, _logger, *, event, source, content):
        checked.append(event["id"])
        return {"related": True, "confidence": 0.95, "contradictions": [],
                "matched_facts": ["same FBI jobs-site incident"]}, "llm"

    monkeypatch.setattr(worker, "_validate_event_source_with_llm", validate)
    selected, reason = worker._match_existing_event_for_article(
        None, None, article_id=36682,
        article={"title": "Suspect detained", "original_url": "https://example.test/story"},
        content="The suspect is cooperating after the FBI jobs-site incident.",
        entity="FBI", incident_date="2026-10-03",
    )
    assert selected["id"] == "evt_published"
    assert reason == "unique_match"
    assert checked == ["evt_published"]


def test_ambiguous_published_matches_do_not_fall_through_to_draft(monkeypatch):
    events = [
        {"id": "evt_public_one", "publish_state": "published"},
        {"id": "evt_public_two", "publish_state": "published"},
        {"id": "evt_draft", "publish_state": "draft", "_shared_actor_candidate": True},
    ]
    checked = []
    monkeypatch.setattr(worker, "_existing_event_candidates_for_article", lambda *_args, **_kwargs: events)

    def validate(_conn, _logger, *, event, source, content):
        checked.append(event["id"])
        return {"related": True, "confidence": 0.95, "contradictions": [],
                "matched_facts": ["same FBI jobs-site incident"]}, "llm"

    monkeypatch.setattr(worker, "_validate_event_source_with_llm", validate)
    selected, reason = worker._match_existing_event_for_article(
        None, None, article_id=36682,
        article={"title": "Suspect detained", "original_url": "https://example.test/story"},
        content="A suspect was detained.", entity="FBI", incident_date="2026-10-03",
    )
    assert selected is None
    assert reason == "ambiguous_match"
    assert checked == ["evt_public_one", "evt_public_two"]


def test_actor_marker_survives_missing_incident_date(monkeypatch):
    class Connection:
        def execute(self, sql, params):
            if "FROM article_threat_actors current_actor" in sql:
                assert "COUNT(DISTINCT current_actor.actor_id) DESC" in sql
                assert "(e.publish_state='published') DESC" in sql
            self.rows = [("evt_actor",)] if "FROM article_threat_actors current_actor" in sql else []
            return self

        def fetchall(self):
            return self.rows

    monkeypatch.setattr(worker, "get_event", lambda _conn, event_id: {
        "id": event_id, "publish_state": "draft",
    })
    found = worker._existing_event_candidates_for_article(
        Connection(), 36682, incident_date="unknown",
    )
    assert found == [{"id": "evt_actor", "publish_state": "draft",
                      "_shared_actor_candidate": True}]


def test_ambiguous_existing_events_hold_article_without_creating_draft(monkeypatch):
    checked = []
    monkeypatch.setattr(worker, "list_event_ids_for_article", lambda *_args: [])
    monkeypatch.setattr(worker, "get_article_by_id", lambda *_args: {
        "id": 36682, "title": "Suspect detained", "content_text": "An actor was detained.",
        "published_at": "2026-10-03T19:09:38+00:00",
    })
    monkeypatch.setattr(worker, "_event_admission_policy", lambda *_args: {})
    monkeypatch.setattr(worker, "list_article_cve_ids", lambda *_args: [])
    monkeypatch.setattr(worker, "_is_primary_source", lambda *_args: False)
    monkeypatch.setattr(worker, "get_active_profile_for_stage", lambda *_args: ({"id": "profile"}, ""))
    monkeypatch.setattr(worker, "run_pipeline_stage", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(worker, "_parse_event_classification", lambda *_args: ({
        "is_event": True, "victim": "FBI", "incident_date": "",
    }, None))
    monkeypatch.setattr(worker, "_match_existing_event_for_article",
                        lambda *_args, **_kwargs: (None, "ambiguous_match"))
    monkeypatch.setattr(worker, "mark_article_events_checked",
                        lambda _conn, article_id, **_kwargs: checked.append(article_id))
    monkeypatch.setattr(worker, "upsert_event_by_key",
                        lambda *_args, **_kwargs: pytest.fail("created duplicate event"))

    result = worker._handle_derive_events_from_articles(
        None, None, {"article_id": 36682}, logging.getLogger(__name__),
    )
    assert result == {"status": "held", "reason": "ambiguous_existing_events"}
    assert checked == [36682]
