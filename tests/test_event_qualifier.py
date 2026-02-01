from sempervigil.worker import _derive_confidence, _has_event_qualifier, _non_event_reason


def test_non_event_reason_blocks_surveys():
    text = "Annual ransomware survey report shows trends"
    assert _non_event_reason(text) is not None


def test_event_qualifier_requires_incident_cues():
    text = "Acme publishes a security report on trends and prevention"
    ok, reasons = _has_event_qualifier(text, "Acme")
    assert ok is False
    assert reasons == []


def test_event_qualifier_accepts_incident():
    text = "Acme suffers ransomware attack and data exposure"
    ok, reasons = _has_event_qualifier(text, "Acme")
    assert ok is True
    assert any(r.startswith("incident:") for r in reasons)


def test_derive_confidence_candidate_for_speculative_language():
    text = "The company reportedly may have been breached"
    score, candidate, evidence = _derive_confidence(text)
    assert candidate is True
    assert score <= 0.7
    assert evidence


def test_derive_confidence_confirmed_for_strong_cues():
    text = "Breach confirmed and data stolen; ransomware attack disclosed"
    score, candidate, evidence = _derive_confidence(text)
    assert candidate is False
    assert score >= 0.7
    assert evidence
