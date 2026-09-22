import pytest

from sempervigil.worker import (
    _is_regulatory_only_event_classification,
    _legacy_event_key,
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
