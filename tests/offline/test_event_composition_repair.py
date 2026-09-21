import json

import pytest

from sempervigil import event_composition_audit as audit
from sempervigil import event_composition_repair as repair
from sempervigil import event_composition_repair_jobs as repair_jobs

pytestmark = pytest.mark.offline


def material():
    from sempervigil.event_composition import SECTIONS, WORKFLOW
    sections = {section: [] for section in SECTIONS}
    sections["overview"] = [{"text": "Acme confirmed records were stolen.",
                             "fact_ids": ["f1"]}]
    sections["impact"] = [{"text": "The incident may have exposed records.",
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
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "unsupported", "reason": "The source only says may."},
        {"id": "C02", "verdict": "supported", "reason": "Directly supported."},
    ]}).encode(), audit_req)
    return composition, revision, decision


def test_repair_rewrites_only_rejected_text_and_revalidates_composition():
    composition, revision, decision = material()
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    record = repair.validate(json.dumps({"repairs": [{"id": "C01",
        "text": "Acme said records may have been exposed."}]}).encode(),
        req, composition, revision)
    assert record["sections"]["overview"] == [{
        "text": "Acme said records may have been exposed.", "fact_ids": ["f1"]}]
    assert record["sections"]["impact"] == [{
        "text": "The incident may have exposed records.", "fact_ids": ["f1"]}]
    assert record["status"] == "unreviewed"


def test_v9_repair_ids_match_generated_section_audit_ids():
    composition, revision, decision = material()
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    item = json.loads(req["input"])["items"][0]
    assert item["id"] == "C01"
    assert item["section"] == "overview"
    assert item["text"] == "Acme confirmed records were stolen."


def test_v9_repair_drops_rejected_detail_instead_of_rewriting_it():
    composition, revision, _ = material()
    audit_req = audit.request("elc_test", composition, revision["ledger"], "a" * 64)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "unsupported", "reason": "Certainty drift."},
        {"id": "C02", "verdict": "unsupported", "reason": "Redundant detail."},
    ]}).encode(), audit_req)
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    assert req["item_ids"] == ["C01"]
    assert req["drop_item_ids"] == ["C02"]
    record = repair.validate(json.dumps({"repairs": [{"id": "C01",
        "text": "Acme said records may have been exposed."}]}).encode(),
        req, composition, revision)
    assert record["sections"]["impact"] == []


def test_repair_requires_every_rejected_item_exactly_once():
    composition, revision, decision = material()
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    with pytest.raises(ValueError, match="invalid_shape|incomplete"):
        repair.validate(json.dumps({"repairs": []}).encode(), req, composition, revision)


def test_submit_recovers_once_from_transient_baseline_failure(monkeypatch):
    class Result:
        def __init__(self, row=None):
            self.row = row

        def fetchone(self):
            return self.row

    class Conn:
        def __init__(self):
            self.commits = 0

        def execute(self, sql, params=()):
            if "pg_advisory_xact_lock" in sql:
                return Result()
            if "SELECT id,status" in sql:
                return Result(("job_failed", "failed",
                               "event_composition_repair_baseline_changed"))
            if "SELECT id FROM jobs" in sql:
                return Result(None)
            raise AssertionError(sql)

        def commit(self):
            self.commits += 1

    composition, revision, decision = material()
    monkeypatch.setattr(repair_jobs, "require_enabled", lambda: None)
    monkeypatch.setattr(repair_jobs, "configuration",
                        lambda _conn: ({}, {}, "b" * 64))
    monkeypatch.setattr(repair_jobs, "material",
                        lambda _conn, _composition_id: (composition, revision))
    captured = {}

    def enqueue(_conn, job_type, payload, **kwargs):
        captured.update(job_type=job_type, payload=payload, kwargs=kwargs)
        return "job_recovery"

    monkeypatch.setattr(repair_jobs, "enqueue_job", enqueue)
    assert repair_jobs.submit(Conn(), "elc_test", decision) == "job_recovery"
    assert captured["kwargs"]["parent_job_id"] == "job_failed"
    assert captured["kwargs"]["dedupe_key"].endswith(":transient-recovery")
