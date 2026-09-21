import json

import pytest

from sempervigil import event_composition_audit as audit

pytestmark = pytest.mark.offline
GENERATION = "b" * 64


def material():
    composition = {"ledger_revision_id": "elr_test", "sections": {
        "overview": [{"text": "Acme reported unauthorized access.", "fact_ids": ["f1"]}],
        "impact": [{"text": "Customer records were exposed.", "fact_ids": ["f2"]}],
    }}
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
