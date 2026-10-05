import json

import pytest

from sempervigil.canary_artifacts import CallNotResumable, Journal

pytestmark = pytest.mark.offline


def test_completed_call_resumes_without_duplicate_invoke(tmp_path):
    journal = Journal(tmp_path, {"ledger_revision_id": "elr_1", "composition_id": "elc_1"})
    invoked = []
    request = {"input": "evidence", "schema": {"type": "object"}}
    response = {"output": "ok", "usage": {"prompt_tokens": 7,
                                             "completion_tokens": 3,
                                             "total_tokens": 10}}
    assert journal.run_call(request, lambda: invoked.append(True) or response) == response
    resumed = Journal(tmp_path, journal.identity)
    assert resumed.run_call(request, lambda: invoked.append(True) or {}) == response
    assert invoked == [True]
    assert json.loads((tmp_path / "summary.json").read_text())["usage"]["total_tokens"] == 10


def test_failure_is_persisted_and_identical_call_is_not_reissued(tmp_path):
    journal = Journal(tmp_path, {"ledger_revision_id": "elr_1"})
    request = {"input": "evidence"}

    def fail():
        raise RuntimeError("schema failure")

    with pytest.raises(RuntimeError, match="schema failure"):
        journal.run_call(request, fail)
    invoked = []
    with pytest.raises(CallNotResumable, match="already_attempted"):
        Journal(tmp_path, journal.identity).run_call(
            request, lambda: invoked.append(True) or {})
    assert invoked == []
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["failed_calls"] == 1
    assert summary["terminal_error"]["message"] == "schema failure"


def test_pass_artifact_and_final_summary_survive_restart(tmp_path):
    identity = {"ledger_revision_id": "elr_1", "audit_request_version": "v1"}
    Journal(tmp_path, identity).record_pass(1, {"failures": ["C03"]})
    resumed = Journal(tmp_path, identity)
    summary = resumed.write_summary(terminal_error={"type": "Budget", "message": "held"})
    assert summary["passes"] == 1
    assert summary["terminal_error"] == {"type": "Budget", "message": "held"}
    assert json.loads((tmp_path / "passes" / "pass-01.json").read_text()) == {
        "failures": ["C03"]}


def test_resume_rejects_stale_or_different_identity(tmp_path):
    Journal(tmp_path, {"ledger_revision_id": "elr_1"})
    with pytest.raises(ValueError, match="identity_mismatch"):
        Journal(tmp_path, {"ledger_revision_id": "elr_2"})


def test_fake_provider_resume_across_repair_batches_and_final_audit(tmp_path):
    identity = {"ledger_revision_id": "elr_1", "composition_id": "elc_1",
                "audit_request_version": "audit-v1"}
    calls = []

    def provider(batch):
        calls.append(batch["id"])
        return {"output": batch["id"], "usage": {"prompt_tokens": 4,
                                                   "completion_tokens": 1,
                                                   "total_tokens": 5}}

    first = Journal(tmp_path, identity)
    assert first.run_batches(stage="repair", pass_number=1,
                             batches=[{"id": "r1"}], invoke=provider)[0]["output"] == "r1"
    first.record_checkpoint("pass-01-composition", {"sections": {"overview": ["fixed"]}})

    resumed = Journal(tmp_path, identity)
    repaired = resumed.run_batches(stage="repair", pass_number=1,
                                   batches=[{"id": "r1"}, {"id": "r2"}], invoke=provider)
    audited = resumed.run_batches(stage="final-audit", pass_number=1,
                                  batches=[{"id": "a1"}, {"id": "a2"}], invoke=provider)
    resumed.record_pass(1, {"composition": resumed.checkpoint("pass-01-composition"),
                            "audit": [row["output"] for row in audited]})
    assert [row["output"] for row in repaired] == ["r1", "r2"]
    assert calls == ["r1", "r2", "a1", "a2"]
    summary = resumed.write_summary()
    assert summary["completed_calls"] == 4
    assert summary["usage"]["total_tokens"] == 20
    assert summary["passes"] == 1


def test_budget_exit_preserves_exact_usage_and_intermediate_composition(tmp_path):
    journal = Journal(tmp_path, {"ledger_revision_id": "elr_1"})
    journal.run_batches(
        stage="repair", pass_number=1, batches=[{"id": "r1"}],
        invoke=lambda _batch: {"output": "fixed", "usage": {
            "prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}},
    )
    with pytest.raises(ValueError, match="token_budget_exhausted"):
        journal.require_budget(estimated_next_tokens=11, max_total_tokens=20,
                               checkpoint={"sections": {"overview": ["fixed"]}})
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["usage"]["total_tokens"] == 10
    assert summary["terminal_error"]["exact_consumed_tokens"] == 10
    assert journal.checkpoint("terminal-composition") == {
        "sections": {"overview": ["fixed"]}}
