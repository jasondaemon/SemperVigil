import json

import pytest

from sempervigil import event_fact_curation as curation

pytestmark = pytest.mark.offline
GENERATION = "a" * 64


def material():
    event = {"event_id": "evt_test", "title": "Acme ransomware incident"}
    article = {"id": 7, "title": "Acme reports ransomware attack"}
    evidence = {"revision_id": "aer_test", "facts": [
        {"id": "f1", "statement": "Acme reported a ransomware attack.",
         "kind": "reported_fact", "date_text": None, "date_role": "none",
         "evidence_passages": [{"id": "p001", "text": "Acme reported a ransomware attack."}]},
        {"id": "f2", "statement": "The company restored affected systems.",
         "kind": "reported_fact", "date_text": None, "date_role": "none",
         "evidence_passages": [{"id": "p002", "text": "The company restored affected systems."}]},
        {"id": "f3", "statement": "A different company was breached in 2024.",
         "kind": "reported_fact", "date_text": "2024", "date_role": "incident",
         "evidence_passages": [{"id": "p003", "text": "A different company was breached in 2024."}]},
    ]}
    return event, article, evidence


def test_curation_selects_only_event_scoped_facts():
    req = curation.request(*material(), GENERATION, {"f1"})
    payload = json.loads(req["input"])
    assert [row["incident_anchor"] for row in payload["facts"]] == [True, False, False]
    raw = {"evidence_verdict": "supported", "incident_verdict": "same_incident",
           "selected_fact_ids": ["f1", "f2"], "reason": "The first two facts concern Acme."}
    result = curation.validate(json.dumps(raw).encode(), req, {"f1"})
    assert result["selected_fact_ids"] == ["f1", "f2"]
    assert result["public_eligible"] is False


def test_curation_conservatively_discards_unsafe_selections():
    req = curation.request(*material(), GENERATION, {"f1"})
    raw = {"evidence_verdict": "supported", "incident_verdict": "same_incident",
           "selected_fact_ids": ["f2"], "reason": "Recovery only."}
    result = curation.validate(json.dumps(raw).encode(), req, {"f1"})
    assert result["incident_verdict"] == "ambiguous"
    assert result["selected_fact_ids"] == []
    assert result["conservative_resolution"] == "incident_anchor_missing"
    raw.update(evidence_verdict="hold", incident_verdict="ambiguous")
    result = curation.validate(json.dumps(raw).encode(), req, {"f1"})
    assert result["selected_fact_ids"] == []
    assert result["conservative_resolution"] == "evidence_hold_selection_discarded"


def test_curation_rejects_unknown_fact_ids_in_schema():
    req = curation.request(*material(), GENERATION)
    raw = {"evidence_verdict": "supported", "incident_verdict": "same_incident",
           "selected_fact_ids": ["missing"], "reason": "Unknown."}
    with pytest.raises(ValueError, match="invalid_shape"):
        curation.validate(json.dumps(raw).encode(), req, {"f1"})


def test_curation_rejects_duplicate_fact_ids_without_unsupported_schema_keyword():
    req = curation.request(*material(), GENERATION)
    assert "uniqueItems" not in req["schema"]["properties"]["selected_fact_ids"]
    raw = {"evidence_verdict": "supported", "incident_verdict": "same_incident",
           "selected_fact_ids": ["f1", "f1"], "reason": "Duplicate selection."}
    with pytest.raises(ValueError, match="duplicate_selection"):
        curation.validate(json.dumps(raw).encode(), req, {"f1"})
