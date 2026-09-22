import pytest

from sempervigil.worker import (
    _is_regulatory_only_event_classification,
    _legacy_event_key,
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
