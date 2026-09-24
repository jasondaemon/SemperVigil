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
        (waterplum, {"related": True, "confidence": 0.93, "contradictions": []}),
        (unrelated, {"related": False, "confidence": 0.97, "contradictions": []}),
    ])

    assert selected == waterplum
    assert reason == "unique_match"


def test_existing_event_match_abstains_when_multiple_events_match():
    selected, reason = _select_existing_event_match([
        ({"id": "evt_one"}, {"related": True, "confidence": 0.9, "contradictions": []}),
        ({"id": "evt_two"}, {"related": True, "confidence": 0.91, "contradictions": []}),
    ])

    assert selected is None
    assert reason == "ambiguous_match"


def test_existing_event_match_rejects_contradicted_or_low_confidence_results():
    selected, reason = _select_existing_event_match([
        ({"id": "evt_one"}, {"related": True, "confidence": 0.79, "contradictions": []}),
        ({"id": "evt_two"}, {"related": True, "confidence": 0.99,
                              "contradictions": ["different campaign"]}),
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


def test_draft_gains_sources_then_confirms_at_three_publishers(monkeypatch):
    updates = []
    sources = ["publisher-one", "publisher-two"]
    monkeypatch.setattr(worker, "list_event_articles", lambda _conn, _id: [
        {"source_id": source} for source in sources
    ])
    monkeypatch.setattr(worker, "update_event", lambda _conn, _id, **fields: updates.append(fields))

    assert worker._maybe_promote_event_lifecycle(None, "evt_test", {}) == "candidate"
    sources.append("publisher-three")
    assert worker._maybe_promote_event_lifecycle(None, "evt_test", {}) == "confirmed"
    assert updates[-1] == {"candidate": False, "lifecycle": "confirmed", "status": "confirmed"}


def test_draft_update_rebuilds_report_without_publishing(monkeypatch):
    calls = []
    monkeypatch.setattr(worker, "link_event_article", lambda *_args: calls.append("link"))
    monkeypatch.setattr(worker, "_maybe_promote_event_lifecycle", lambda *_args: "confirmed")
    monkeypatch.setattr(worker, "update_event_summary_from_articles", lambda *_args: calls.append("summary"))
    monkeypatch.setattr(worker, "enqueue_job", lambda *_args, **_kwargs: calls.append("report"))
    monkeypatch.setattr(worker, "_maybe_queue_event_research", lambda *_args: calls.append("research"))

    result = worker._link_existing_draft_update(
        None, event_id="evt_test", article_id=123, article={},
    )

    assert result["lifecycle"] == "confirmed"
    assert calls == ["link", "summary", "report", "research"]


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
        return {"related": True, "confidence": 0.92, "contradictions": []}, "llm"

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
            assert params == ("2026-09-09", "2026-10-07", 200)
            self.draft_query = True
            return self

        def fetchall(self):
            return [("evt_fbi",)] if getattr(self, "draft_query", False) else []

    monkeypatch.setattr(worker, "get_event", lambda _conn, event_id: {"id": event_id})
    found = worker._existing_event_candidates_for_article(
        Connection(), 123, incident_date="2026-09-23", window_days=14,
    )
    assert found == [{"id": "evt_fbi"}]
