import json

import pytest

from sempervigil import event_composition_audit as audit
from sempervigil import event_composition_repair as repair
from sempervigil import event_composition_repair_jobs as repair_jobs

pytestmark = pytest.mark.offline


def replacement(text, refs=("F01",)):
    return {"text": text, "fact_refs": list(refs),
            "claim_type": "sourced_finding", "confidence": None}


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
    record = repair.validate(json.dumps({"repairs": {"C01": [
        replacement("Acme said records may have been exposed.")]}}).encode(),
        req, composition, revision)
    assert record["sections"]["overview"] == [{
        "text": "Acme said records may have been exposed.", "fact_ids": ["f1"],
        "claim_type": "sourced_finding", "confidence": None}]
    assert record["sections"]["impact"] == [{
        "text": "The incident may have exposed records.", "fact_ids": ["f1"],
        "claim_type": "sourced_finding", "confidence": None}]
    assert record["status"] == "unreviewed"


def test_v9_repair_ids_match_generated_section_audit_ids():
    composition, revision, decision = material()
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    item = json.loads(req["input"])["items"][0]
    assert item["id"] == "C01"
    assert item["section"] == "overview"
    assert item["text"] == "Acme confirmed records were stolen."
    assert req["schema"]["properties"]["repairs"]["properties"]["C01"]["maxItems"] == 2


def test_v11_repair_rewrites_rejected_detail_and_preserves_required_coverage():
    composition, revision, _ = material()
    audit_req = audit.request("elc_test", composition, revision["ledger"], "a" * 64)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "unsupported", "reason": "Certainty drift."},
        {"id": "C02", "verdict": "unsupported", "reason": "Redundant detail."},
    ]}).encode(), audit_req)
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    assert req["item_ids"] == ["C01", "C02"]
    record = repair.validate(json.dumps({"repairs": {
        "C01": [replacement("Acme said records may have been exposed.")],
        "C02": [replacement("The reported exposure may include Acme records.")],
    }}).encode(),
        req, composition, revision)
    assert record["sections"]["impact"][0]["text"] == (
        "The reported exposure may include Acme records."
    )


def test_repair_requires_every_rejected_item_exactly_once():
    composition, revision, decision = material()
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    with pytest.raises(ValueError, match="invalid_shape|incomplete"):
        repair.validate(json.dumps({"repairs": {}}).encode(), req, composition, revision)


def test_repair_can_split_dense_item_and_reselect_allowed_citations():
    composition, revision, decision = material()
    revision["ledger"]["facts"].append({
        "fact_id": "f2", "statement": "Acme opened an investigation.",
        "kind": "reported_fact", "date_text": None, "date_role": "none",
        "sections": ["context"],
    })
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    item = json.loads(req["input"])["items"][0]
    assert item["cited_facts"][0]["ref"] == "F01"
    assert {row["ref"] for row in item["allowed_facts"]} == {"F02"}
    record = repair.validate(json.dumps({"repairs": {"C01": [
        replacement("Acme said records may have been exposed."),
        replacement("Acme opened an investigation.", refs=("F02",)),
    ]}}).encode(), req, composition, revision)
    assert [row["fact_ids"] for row in record["sections"]["overview"]] == [["f1"], ["f2"]]


def test_repair_schema_rejects_fact_reference_outside_section_allowlist():
    composition, revision, _ = material()
    revision["ledger"]["facts"].append({
        "fact_id": "f2", "statement": "Users should rotate passwords.",
        "kind": "guidance", "date_text": None, "date_role": "none",
        "sections": ["mitigation"],
    })
    audit_req = audit.request("elc_test", composition, revision["ledger"], "a" * 64)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "supported", "reason": "Directly supported."},
        {"id": "C02", "verdict": "unsupported", "reason": "Wrong section fact."},
    ]}).encode(), audit_req)
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    with pytest.raises(ValueError, match="invalid_shape"):
        repair.validate(json.dumps({"repairs": {"C02": [
            replacement("Users should rotate passwords.", refs=("F02",)),
        ]}}).encode(), req, composition, revision)


def test_repair_does_not_offer_legacy_section_ineligible_citation():
    composition, revision, _ = material()
    revision["ledger"]["facts"].append({
        "fact_id": "f2", "statement": "Users should rotate passwords.",
        "kind": "guidance", "date_text": None, "date_role": "none",
        "sections": ["mitigation"],
    })
    composition["sections"]["impact"][0]["fact_ids"] = ["f2"]
    audit_req = audit.request("elc_test", composition, revision["ledger"], "a" * 64)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "supported", "reason": "Directly supported."},
        {"id": "C02", "verdict": "unsupported", "reason": "Wrong section fact."},
    ]}).encode(), audit_req)
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    item = json.loads(req["input"])["items"][0]
    assert item["cited_facts"] == []
    assert {row["ref"] for row in item["allowed_facts"]} == {"F01"}
    assert req["allowed_refs"]["C02"] == ["F01"]


def test_repair_normalizes_supported_legacy_item_with_too_many_citations():
    composition, revision, _ = material()
    for index in range(2, 10):
        revision["ledger"]["facts"].append({
            "fact_id": f"f{index}", "statement": f"Supported impact fact {index}.",
            "kind": "reported_fact", "date_text": None, "date_role": "none",
            "sections": ["impact"],
        })
    composition["sections"]["impact"][0]["fact_ids"] = [f"f{i}" for i in range(1, 10)]
    audit_req = audit.request("elc_test", composition, revision["ledger"], "a" * 64)
    decision = audit.validate(json.dumps({"audits": [
        {"id": "C01", "verdict": "unsupported", "reason": "Certainty drift."},
        {"id": "C02", "verdict": "supported", "reason": "Directly supported."},
    ]}).encode(), audit_req)
    req = repair.request("elc_test", composition, revision, decision, "b" * 64)
    assert req["item_ids"] == ["C01", "C02"]
    item = json.loads(req["input"])["items"][1]
    assert "atomic content contract" in item["audit_reason"]


def test_repair_deduplicates_cross_batch_narrative_before_validation():
    row = replacement("Acme opened an investigation.")
    result = repair._deduplicate({"overview": [row], "impact": [dict(row)]})
    assert result == {"overview": [row], "impact": []}


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


def test_complete_classifies_reasoning_length_zero_output(monkeypatch):
    from sempervigil.llm import router
    monkeypatch.setattr(repair_jobs, "configuration",
                        lambda _conn: ({"id": "model", "model_name": "gpt-5.6-sol"},
                                       {"id": "provider", "type": "openai_compatible",
                                        "base_url": "https://example.invalid"}, "g"))
    monkeypatch.setattr(repair_jobs, "load_provider_secret", lambda *_args: "secret")
    monkeypatch.setattr(router, "_http_request", lambda *_args, **_kwargs: {
        "choices": [{"finish_reason": "length",
                     "message": {"content": "", "refusal": None}}]})
    runs = []
    monkeypatch.setattr(repair_jobs, "insert_llm_run",
                        lambda *_args, **kwargs: runs.append(kwargs))

    with pytest.raises(ValueError, match=repair_jobs.REASONING_LENGTH_ERROR):
        repair_jobs.complete(object(), "job", {"generation": "g", "input": "x", "schema": {}})

    assert runs[0]["ok"] is True
    assert runs[0]["output_chars"] == 0
    assert runs[0]["error"] is None


def test_complete_does_not_classify_refusal_as_reasoning_length(monkeypatch):
    from sempervigil.llm import router
    monkeypatch.setattr(repair_jobs, "configuration",
                        lambda _conn: ({"id": "model", "model_name": "gpt-5.6-sol"},
                                       {"id": "provider", "type": "openai_compatible",
                                        "base_url": "https://example.invalid"}, "g"))
    monkeypatch.setattr(repair_jobs, "load_provider_secret", lambda *_args: "secret")
    monkeypatch.setattr(router, "_http_request", lambda *_args, **_kwargs: {
        "choices": [{"finish_reason": "length",
                     "message": {"content": "", "refusal": "blocked"}}]})
    monkeypatch.setattr(repair_jobs, "insert_llm_run", lambda *_args, **_kwargs: None)

    assert repair_jobs.complete(
        object(), "job", {"generation": "g", "input": "x", "schema": {}}
    ) == ""
