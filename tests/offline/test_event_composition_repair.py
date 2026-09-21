import json

import pytest

from sempervigil import event_composition_audit as audit
from sempervigil import event_composition_repair as repair

pytestmark = pytest.mark.offline


def material():
    from sempervigil.event_composition import SECTIONS, WORKFLOW
    sections = {section: [] for section in SECTIONS}
    sections["overview"] = [{"text": "Acme confirmed records were stolen.",
                             "fact_ids": ["f1"]}]
    composition = {"workflow": WORKFLOW, "ledger_revision_id": "elr_test", "sections": sections}
    ledger = {"public_eligible": False, "title": "Acme incident", "kind": "breach",
              "facts": [{"fact_id": "f1",
                         "statement": "Acme said records may have been exposed.",
                         "kind": "allegation", "date_text": None, "date_role": "none",
                         "sections": ["impact", "open_question"]}],
              "superseded_fact_ids": [], "conflict_fact_ids": []}
    revision = {"revision_id": "elr_test", "ledger_id": "eld_test", "status": "accepted",
                "lineage_current": True, "ledger": ledger, "change": {}}
    audit_req = audit.request("elc_test", composition, ledger, "a" * 64)
    decision = audit.validate(json.dumps({"audits": [{"id": "C01",
        "verdict": "unsupported", "reason": "The source only says may."}]}).encode(), audit_req)
    return composition, revision, decision


def test_repair_rewrites_only_rejected_text_and_revalidates_composition():
    composition, revision, decision = material()
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    record = repair.validate(json.dumps({"repairs": [{"id": "C01",
        "text": "Acme said records may have been exposed."}]}).encode(),
        req, composition, revision)
    assert record["sections"]["overview"] == [{
        "text": "Acme said records may have been exposed.", "fact_ids": ["f1"]}]
    assert record["status"] == "unreviewed"


def test_repair_requires_every_rejected_item_exactly_once():
    composition, revision, decision = material()
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    with pytest.raises(ValueError, match="invalid_shape|incomplete"):
        repair.validate(json.dumps({"repairs": []}).encode(), req, composition, revision)
