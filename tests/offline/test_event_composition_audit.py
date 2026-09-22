import json

import pytest

from sempervigil import event_composition_audit as audit

pytestmark = pytest.mark.offline
GENERATION = "b" * 64


def material():
    from sempervigil.event_composition import SECTIONS
    sections = {section: [] for section in SECTIONS}
    sections["overview"] = [{"text": "Acme reported unauthorized access.", "fact_ids": ["f1"]}]
    sections["impact"] = [{"text": "Customer records were exposed.", "fact_ids": ["f2"]}]
    composition = {"ledger_revision_id": "elr_test", "sections": sections}
    ledger = {"facts": [
        {"fact_id": "f1", "statement": "Acme reported unauthorized access.",
         "kind": "reported_fact", "date_text": None, "date_role": "none"},
        {"fact_id": "f2", "statement": "Acme said customer records were exposed.",
         "kind": "reported_fact", "date_text": None, "date_role": "none"},
    ]}
    return composition, ledger


def test_audit_requires_every_item_and_computes_readiness():
    req = audit.request("elc_test", *material(), GENERATION)
    raw = {"audits": [
        {"id": "C01", "verdict": "supported", "reason": "Directly stated."},
        {"id": "C02", "verdict": "supported", "reason": "Directly stated."},
    ]}
    result = audit.validate(json.dumps(raw).encode(), req)
    assert result["ready"] is True
    raw["audits"][1]["verdict"] = "unsupported"
    assert audit.validate(json.dumps(raw).encode(), req)["ready"] is False


def test_audit_rejects_duplicate_or_missing_items():
    req = audit.request("elc_test", *material(), GENERATION)
    raw = {"audits": [
        {"id": "C01", "verdict": "supported", "reason": "Direct."},
        {"id": "C01", "verdict": "supported", "reason": "Duplicate."},
    ]}
    with pytest.raises(ValueError, match="incomplete"):
        audit.validate(json.dumps(raw).encode(), req)


def test_audit_accepts_schema_legal_large_response():
    from sempervigil.event_composition import SECTIONS
    sections = {section: [] for section in SECTIONS}
    sections["overview"] = [
        {"text": f"Supported item {index}", "fact_ids": ["f1"]}
        for index in range(42)
    ]
    composition = {"ledger_revision_id": "elr_large", "sections": sections}
    ledger = {"facts": [{"fact_id": "f1", "statement": "Direct support.",
                          "kind": "reported_fact", "date_text": None,
                          "date_role": "none"}]}
    req = audit.request("elc_large", composition, ledger, GENERATION)
    raw = json.dumps({"audits": [
        {"id": item_id, "verdict": "supported", "reason": "R" * 280}
        for item_id in req["item_ids"]
    ]}).encode()
    assert len(raw) > 12000
    assert len(raw) <= audit.MAX_OUTPUT_BYTES
    assert audit.validate(raw, req)["ready"] is True


def test_filter_removes_only_unsupported_items_and_preserves_required_content():
    composition, ledger = material()
    composition.update({"workflow": "event-ledger-composition-v4",
                        "ledger_id": "eld_test", "generation_version": "c" * 64,
                        "request_version": "d" * 64, "status": "held",
                        "public_eligible": False, "section_policy": "deterministic-sections-v2",
                        "change": {}})
    req = audit.request("elc_test", composition, ledger, GENERATION)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "supported", "reason": "Direct."},
        {"id": "C02", "verdict": "unsupported", "reason": "Overstated."},
    ]}).encode(), req)
    result = audit.filtered_record("elc_test", composition, ledger, decision)
    assert result["sections"]["overview"] == composition["sections"]["overview"]
    assert result["sections"]["impact"] == []
    assert result["status"] == "unreviewed"
    assert result["generation_version"] != composition["generation_version"]


def test_filter_holds_when_audit_removes_every_overview_item():
    composition, ledger = material()
    composition.update({"generation_version": "c" * 64, "request_version": "d" * 64})
    req = audit.request("elc_test", composition, ledger, GENERATION)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "unsupported", "reason": "Overstated."},
        {"id": "C02", "verdict": "supported", "reason": "Direct."},
    ]}).encode(), req)
    with pytest.raises(ValueError, match="overview_required"):
        audit.filtered_record("elc_test", composition, ledger, decision)


def test_v9_filter_removes_rejected_detail_and_preserves_supported_overview():
    from sempervigil.event_composition import WORKFLOW
    composition, ledger = material()
    composition.update({"workflow": WORKFLOW, "ledger_id": "eld_test",
                        "generation_version": "c" * 64,
                        "request_version": "d" * 64, "status": "held",
                        "public_eligible": False, "section_policy": "curated-sections-v3",
                        "change": {}})
    req = audit.request("elc_test", composition, ledger, GENERATION)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "supported", "reason": "Direct."},
        {"id": "C02", "verdict": "unsupported", "reason": "Overstated."},
    ]}).encode(), req)
    result = audit.filtered_record("elc_test", composition, ledger, decision)
    assert result["sections"]["overview"] == composition["sections"]["overview"]
    assert result["sections"]["impact"] == []


def test_current_audit_uses_canonical_section_order():
    from sempervigil.event_composition import SECTIONS, WORKFLOW

    composition, ledger = material()
    composition["workflow"] = WORKFLOW
    composition["sections"] = {
        section: composition["sections"][section] for section in reversed(SECTIONS)
    }

    req = audit.request("elc_test", composition, ledger, GENERATION)
    items = json.loads(req["input"])["items"]

    assert [(item["id"], item["section"]) for item in items] == [
        ("C01", "overview"), ("C02", "impact"),
    ]
