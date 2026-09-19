import copy
import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sempervigil import event_assessment as assessment, event_review as review, event_review_jobs as jobs, worker
from sempervigil.services import ai_service
from sempervigil.llm import router
from test_event_review import database, get_packet, resign

pytestmark = pytest.mark.offline


def response(packet, decision="include", reason="same_incident"):
    return {"decisions": [{"id": key, "decision": decision, "reason": reason}
            for key in assessment.request_for(packet)["mapping"]]}


def test_bounded_context_and_exact_source_mapping(database):
    packet = get_packet(database)
    request = assessment.request_for(packet)
    assert len((request["system"] + request["input"]).encode()) <= assessment.MAX_INPUT_BYTES
    assert request == assessment.request_for(packet)
    values = json.loads(request["input"])["items"]
    assert values[0]["quote"] in packet["documents"][0]["text"]
    assert "Shared company names" in request["system"]
    data = json.loads(request["input"])
    assert data["required_ids"] == list(request["mapping"])
    assert data["target_event"] == packet["event"]["title"]
    assert list(data)[-2:] == ["target_event", "required_ids"]


def test_prompt_examples_match_validator_without_slash_shorthand():
    examples = [json.loads(line) for line in assessment.SYSTEM_PROMPT.splitlines()
                if line.startswith('{"decision":')]
    assert {(row["decision"], row["reason"]) for row in examples} == assessment.PAIRS
    assert all(row.keys() == {"decision", "reason"} for row in examples)
    assert "first passage" in assessment.SYSTEM_PROMPT


def test_context_is_lossless_without_duplicating_quote(database):
    packet = get_packet(database)
    request = assessment.request_for(packet)
    passages = {p["id"]: p for p in review.draft(packet)["passages"]}
    documents = {d["article_id"]: d for d in packet["documents"]}
    for item in json.loads(request["input"])["items"]:
        passage = passages[request["mapping"][item["id"]]]
        text = documents[passage["article_id"]]["text"]
        assert item["context_before"] + item["quote"] + item["context_after"] == text[
            max(0, passage["start"] - 200):passage["end"] + 200]
        assert len(item["context_before"]) <= 200 and len(item["context_after"]) <= 200
        assert "context" not in item


@pytest.mark.parametrize("reason", ["same_incident", "Both passages describe the same breach."])
def test_pilot_joined_decision_values_remain_rejected(database, reason):
    packet = get_packet(database)
    bad = response(packet, decision="include/same_incident", reason=reason)
    with pytest.raises(ValueError, match="invalid_assessment_values"):
        assessment.validate_response(json.dumps(bad).encode(), packet)


def test_free_text_reason_is_not_silently_coerced(database):
    packet = get_packet(database)
    bad = response(packet, reason="Both passages describe the same breach.")
    with pytest.raises(ValueError, match="invalid_assessment_values"):
        assessment.validate_response(json.dumps(bad).encode(), packet)


def test_model_suggestions_never_approve_reading_view(database, tmp_path):
    packet = get_packet(database)
    complete = Mock(return_value=response(packet))
    result = assessment.assess(packet, complete)
    complete.assert_called_once()
    assert result["public_eligible"] is False
    page = review.save(packet, tmp_path, assessment=result).read_text()
    assert "Model suggestion: include" in page
    assert 'value="hold" selected' in page
    assert 'value="include" selected' not in page
    assert len(list(tmp_path.rglob("assessment-*.json"))) == 1


@pytest.mark.parametrize("bad", [
    {"decisions":[]}, {"decisions":[], "publish":True},
    {"decisions":[{"id":"unknown", "decision":"include", "reason":"same_incident"}]},
    {"decisions":[{"id":"p1", "decision":"include", "reason":"different_incident"}]},
    {"decisions":[{"id":[], "decision":"include", "reason":"same_incident"}]},
])
def test_invalid_model_output_fails_closed(database, bad):
    with pytest.raises(ValueError):
        assessment.validate_response(json.dumps(bad).encode(), get_packet(database))


def test_duplicate_ids_and_oversized_output(database):
    packet = get_packet(database)
    value = response(packet)
    value["decisions"].append(value["decisions"][0])
    with pytest.raises(ValueError): assessment.validate_response(json.dumps(value).encode(), packet)
    with pytest.raises(ValueError): assessment.validate_response(b" " * 12001, packet)


def test_changed_evidence_invalidates_assessment(database):
    packet = get_packet(database)
    result = assessment.assess(packet, lambda text: response(packet))
    packet["documents"][0]["text"] += " Changed evidence."
    resign(packet)
    with pytest.raises(ValueError): assessment.validate_assessment(result, packet)


def test_empty_evidence_never_calls_model(database):
    packet = get_packet(database)
    packet["documents"] = []
    resign(packet)
    result = assessment.assess(packet, Mock(side_effect=AssertionError("must not infer")))
    assert result["suggestions"] == {}


def test_multi_source_budget_and_explicit_coverage(database):
    packet = get_packet(database)
    first = packet["documents"][0]
    packet["documents"] = [{**first, "article_id": n + 1,
        "text": ("Acme " + "x" * 1000 + ". ") * 4} for n in range(12)]
    resign(packet)
    req = assessment.request_for(packet)
    assert 0 < len(req["mapping"]) <= 12
    assert len((req["system"] + req["input"]).encode()) <= assessment.MAX_INPUT_BYTES
    assert req["omitted_passages"] == 48 - len(req["mapping"])
    assert json.loads(req["input"])["required_ids"] == list(req["mapping"])


def configured(monkeypatch):
    monkeypatch.setenv("SV_EVENT_REVIEW_MODEL_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_PROFILE_ID", "review-profile")
    monkeypatch.setattr(worker, "insert_llm_run", Mock())
    profile = {"id":"review-profile", "primary_model_id":"local-model", "primary_provider_id":"local",
               "prompt_id":"review-prompt", "fallback":[], "params":{
                   "temperature":0, "max_tokens":1024, "max_input_chars":12000}}
    monkeypatch.setattr(worker, "get_profile", lambda *a: profile)
    monkeypatch.setattr(worker, "get_active_profile_for_stage", lambda *a: (copy.deepcopy(profile), "ok"))
    monkeypatch.setattr(worker, "get_model", lambda *a: {"model_name":"ollama/qwen2.5:7b-instruct-16k"})
    monkeypatch.setattr(worker, "get_provider", lambda *a: {"id":"local", "type":"openai_compatible"})
    monkeypatch.setattr(ai_service, "get_prompt", lambda *a: {
        "system_template":assessment.SYSTEM_PROMPT, "user_template":"{{input}}"})
    return profile


def test_worker_default_makes_no_model_lookup(monkeypatch):
    monkeypatch.delenv("SV_EVENT_REVIEW_MODEL_ENABLED", raising=False)
    monkeypatch.setattr(worker, "get_profile", Mock(side_effect=AssertionError("must not read")))
    assert worker._private_review_completion(None, None, None) is None


def test_worker_calls_existing_router_with_job_attribution(monkeypatch):
    configured(monkeypatch)
    call = Mock(return_value={"parsed":{"decisions":[]}, "schema_valid":True})
    monkeypatch.setattr(worker, "run_profile", call)
    fn = worker._private_review_completion(None, SimpleNamespace(id="job-test"), logging.getLogger())
    assert fn("input") == {"decisions": []}
    assert call.call_args.kwargs["context"]["job_id"] == "job-test"
    assert call.call_args.args[1] == "review-profile"


def test_real_router_envelope_reaches_assessment_validator(monkeypatch, database):
    profile = configured(monkeypatch)
    packet = get_packet(database)
    monkeypatch.setattr(router, "get_profile", lambda *a: profile)
    monkeypatch.setattr(router, "get_model", worker.get_model)
    monkeypatch.setattr(router, "get_provider", worker.get_provider)
    monkeypatch.setattr(router, "get_prompt", ai_service.get_prompt)
    monkeypatch.setattr(router, "load_provider_secret", lambda *a: None)
    monkeypatch.setattr(router, "load_runtime_config", lambda *a: {})
    transport = Mock(return_value=json.dumps(response(packet)))
    monkeypatch.setattr(router, "_call_provider", transport)
    # Keep the actual run_profile / parsing / envelope path, not a router mock.
    callback = worker._private_review_completion(None, SimpleNamespace(id="real-router"), logging.getLogger())
    result = assessment.assess(packet, callback)
    assert result["suggestions"] and result["public_eligible"] is False
    transport.assert_called_once()
    assert transport.call_args.args[5]["max_tokens"] == 1024
    assert transport.call_args.kwargs["context"]["job_id"] == "real-router"


@pytest.mark.parametrize("valid", [True, False])
def test_model_worker_dispatch_is_private_and_fails_closed(monkeypatch, tmp_path, database, valid):
    configured(monkeypatch)
    packet = get_packet(database)
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_DB_URL", "unused-test-dsn")
    monkeypatch.setattr(jobs, "snapshot", lambda *a, **k: packet)
    monkeypatch.setattr(worker, "_log_job_claimed", lambda *a: None)
    monkeypatch.setattr(worker, "is_job_canceled", lambda *a: False)
    call = Mock(return_value={"parsed":response(packet) if valid else {"publish": True}, "schema_valid":True})
    monkeypatch.setattr(worker, "run_profile", call)
    for name in ("mark_build_dirty", "update_event_report", "_handle_event_report_llm"):
        monkeypatch.setattr(worker, name, Mock(side_effect=AssertionError("must not publish")))
    job = SimpleNamespace(id="test", job_type=jobs.JOB_TYPE, payload=jobs.payload_for("event", ["Acme"]))
    if valid:
        result = worker.run_claimed_job(None, None, job, logging.getLogger("test"))
        assert result["model_assessed"] and result["public_eligible"] is False
        assert result["assessment_summary"] == {
            "workflow": assessment.WORKFLOW, "scope_version": None,
            "assessed": 1, "not_assessed": 0, "included": 1, "held": 0,
            "excluded": 0, "status": "proposal_only"}
        page = (tmp_path / "private" / result["artifact"]).read_text()
        assert "Model suggestion: include" in page
        assert 'value="include" selected' not in page
        assert result["model_cache_hit"] is False
        repeated = worker.run_claimed_job(None, None, job, logging.getLogger("test"))
        assert repeated["model_cache_hit"] is True
        assert repeated["artifact"] == result["artifact"]
        worker.insert_llm_run.assert_called_once()
    else:
        with pytest.raises(ValueError):
            worker.run_claimed_job(None, None, job, logging.getLogger("test"))
        assert not list(tmp_path.rglob("*.html"))
    call.assert_called_once()


@pytest.mark.parametrize("change", ["fallback", "schema", "tokens", "extra_params", "prompt", "cloud"])
def test_worker_rejects_unsafe_profiles(monkeypatch, change):
    profile = configured(monkeypatch)
    if change == "fallback": profile["fallback"] = [{"provider_id":"cloud", "model_id":"other"}]
    if change == "schema": profile["schema_id"] = "would-trigger-repair-inference"
    if change == "tokens": profile["params"]["max_tokens"] = 9000
    if change == "extra_params": profile["params"]["max_output_tokens"] = 100000
    if change == "prompt": monkeypatch.setattr(ai_service, "get_prompt", lambda *a: {})
    if change == "cloud": monkeypatch.setattr(worker, "get_model", lambda *a: {"model_name":"cloud"})
    with pytest.raises(ValueError): worker._private_review_completion(None, None, None)
