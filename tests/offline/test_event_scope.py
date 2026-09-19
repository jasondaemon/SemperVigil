import copy
import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sempervigil import event_scope as scope, event_assessment as assessment
from sempervigil import event_assessment_cache as cache, event_review as review
from sempervigil import event_review_jobs as jobs, worker, admin
from sempervigil.services import ai_service
from test_event_review import database, get_packet, resign
from test_event_assessment import configured
from test_event_review_jobs import Connection

pytestmark = pytest.mark.offline


def proposal(packet):
    text = packet["documents"][0]["text"]
    left = text.index("contact system")
    return scope.propose(packet, article_id=1, start=0, end=len(text), focus=[
        {"role": "entity", "start": 0, "end": 4},
        {"role": "affected_system", "start": left, "end": left + len("contact system")},
    ])


def completion(packet, value):
    calls = []
    def run(text):
        calls.append(text)
        return {"decisions": [{"id": key, "decision": "hold", "reason": "insufficient_context"}
                for key in assessment.request_for(packet, scope=value)["mapping"]]}
    run.cache_identity = "a" * 64
    return run, calls


def test_exact_immutable_source_proposal(database):
    packet = get_packet(database)
    value = proposal(packet)
    assert scope.validate(value, packet) == value == proposal(packet)
    assert value["public_eligible"] is False and value["scope_status"] == "proposal_only"
    assert value["anchor"]["quote"] == packet["documents"][0]["text"]
    assert {f["quote"] for f in value["focus"]} == {"Acme", "contact system"}


@pytest.mark.parametrize("change", ["text", "url", "title", "event", "quote", "version", "approved", "extra"])
def test_changed_evidence_or_authority_rejected(database, change):
    packet = get_packet(database)
    value = proposal(packet)
    if change in {"text", "url", "title"}:
        packet["documents"][0][change] += "changed"
        resign(packet)
    if change == "event":
        packet["event"]["id"] = "other"
        resign(packet)
    if change == "quote": value["anchor"]["quote"] += "!"
    if change == "version": value["scope_version"] = "b" * 64
    if change == "approved": value["public_eligible"] = True
    if change == "extra": value["qualified"] = True
    with pytest.raises(ValueError): scope.validate(value, packet)


def test_report_timestamp_does_not_change_scope_identity(database):
    packet = get_packet(database)
    value = proposal(packet)
    packet["event"]["updated_at"] = "2026-09-19"
    resign(packet)
    assert scope.validate(value, packet) == value


@pytest.mark.parametrize("change", ["bool_id", "bool_span", "outside", "short", "missing_entity", "duplicate", "company_only", "invented", "unknown_role"])
def test_invalid_declarations_fail(database, change):
    packet = get_packet(database)
    value = proposal(packet)
    args = {k: value["anchor"][k] for k in ("article_id", "start", "end")}
    args["focus"] = [{k: f[k] for k in ("role", "start", "end")} for f in value["focus"]]
    if change == "bool_id": args["article_id"] = True
    if change == "bool_span": args["start"] = False
    if change == "outside": args["end"] += 1
    if change == "short": args["end"] = 20
    if change == "missing_entity": args["focus"] = [args["focus"][0]]
    if change == "duplicate": args["focus"][1]["role"] = args["focus"][0]["role"]
    if change == "company_only":
        for f in args["focus"]: f.update(start=0, end=4)
    if change == "invented": args["focus"][0]["quote"] = "invented"
    if change == "unknown_role": args["focus"][0]["role"] = "verified_fact"
    with pytest.raises(ValueError): scope.propose(packet, **args)


def test_scoped_request_is_bounded_distinct_and_strict(database):
    packet = get_packet(database)
    value = proposal(packet)
    normal = assessment.request_for(packet)
    request = assessment.request_for(packet, scope=value)
    assert normal["request_version"] != request["request_version"]
    assert len((request["system"] + request["input"]).encode()) <= assessment.MAX_INPUT_BYTES
    assert request["workflow"] == assessment.SCOPED_WORKFLOW
    assert json.loads(request["input"])["incident_scope"] == scope.model_context(value, packet)
    run, calls = completion(packet, value)
    result = assessment.assess(packet, run, scope=value)
    assert len(calls) == 1 and result["scope"] == value and result["public_eligible"] is False
    assert assessment.validate_assessment(result, packet) == result
    with pytest.raises(ValueError, match="incomplete_assessment"):
        assessment.validate_response(b'{"decisions":[]}', packet, scope=value)


def test_scope_cache_reuse_and_cross_scope_poison_rejection(database, tmp_path):
    packet = get_packet(database)
    value = proposal(packet)
    run, calls = completion(packet, value)
    first, hit = cache.reuse(packet, run, tmp_path, scope=value)
    assert not hit
    assert cache.reuse(packet, run, tmp_path, scope=value) == (first, True)
    assert len(calls) == 1
    path = next((tmp_path / "assessment-cache").glob("*.json"))
    entry = json.loads(path.read_bytes())
    alternate = copy.deepcopy(value)
    fields = [{k: f[k] for k in ("role", "start", "end")} for f in alternate["focus"]]
    fields[0]["role"] = "reported_reference"
    alternate = scope.propose(packet, article_id=1, start=0, end=len(packet["documents"][0]["text"]), focus=fields)
    other_run, _ = completion(packet, alternate)
    entry["assessment"] = assessment.assess(packet, other_run, scope=alternate)
    path.write_text(json.dumps(entry))
    with pytest.raises(ValueError, match="stale_assessment_cache"):
        cache.reuse(packet, run, tmp_path, scope=value)
    assert len(calls) == 1


def test_review_explains_scope_without_preapproving(database, tmp_path):
    packet = get_packet(database)
    packet["documents"][0]["title"] = '<script>alert(1)</script>'
    resign(packet)
    value = proposal(packet)
    run, _ = completion(packet, value)
    result = assessment.assess(packet, run, scope=value)
    page = review.save(packet, tmp_path, assessment=result).read_text()
    assert 'id="incident-scope"' in page
    assert "not independently qualified or approved" in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page
    assert '<script>alert(1)</script>' not in page
    assert 'value="hold" selected' in page and 'value="include" selected' not in page
    assert len(list(tmp_path.rglob("assessment-*.json"))) == 1


def test_scoped_admission_disabled_before_connection(monkeypatch, database):
    value = proposal(get_packet(database))
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.delenv("SV_EVENT_REVIEW_SCOPE_ENABLED", raising=False)
    connection = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(PermissionError):
        jobs.submit(connection, event_id="event", aliases=["Acme"], scope=value)
    connection.assert_not_called()


def test_scope_admitted_as_bounded_unqualified_proposal(monkeypatch, database):
    value = proposal(get_packet(database))
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_SCOPE_ENABLED", "1")
    conn = Connection()
    enqueue = Mock(return_value="job")
    monkeypatch.setattr(jobs, "enqueue_job", enqueue)
    assert jobs.submit(lambda: conn, event_id="event", aliases=["Acme"], scope=value) == "job"
    assert enqueue.call_args.args[2]["scope"] == value
    assert enqueue.call_args.kwargs["priority"] == -10
    assert enqueue.call_args.kwargs["max_attempts"] == 1
    assert conn.closes == 1


@pytest.mark.parametrize("mode", ["disabled", "no_model", "wrong_prompt", "no_profile"])
def test_worker_scope_profile_fail_closed(monkeypatch, database, mode):
    configured(monkeypatch)
    monkeypatch.setenv("SV_EVENT_REVIEW_SCOPE_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_SCOPE_PROFILE_ID", "scoped-profile")
    job = SimpleNamespace(payload={"scope": proposal(get_packet(database))})
    if mode == "disabled": monkeypatch.delenv("SV_EVENT_REVIEW_SCOPE_ENABLED")
    if mode == "no_model": monkeypatch.setenv("SV_EVENT_REVIEW_MODEL_ENABLED", "0")
    if mode == "no_profile": monkeypatch.delenv("SV_EVENT_REVIEW_SCOPE_PROFILE_ID")
    with pytest.raises((PermissionError, ValueError)):
        worker._private_review_completion(None, job, None)


@pytest.mark.parametrize("stale", [False, True])
def test_scoped_real_dispatch_one_call_or_stale_rejection(monkeypatch, tmp_path, database, stale):
    configured(monkeypatch)
    packet = get_packet(database)
    value = proposal(packet)
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_SCOPE_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_SCOPE_PROFILE_ID", "scoped-profile")
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_DB_URL", "unused-test-dsn")
    monkeypatch.setattr(ai_service, "get_prompt", lambda *a: {
        "system_template": assessment.SCOPED_SYSTEM_PROMPT, "user_template": "{{input}}"})
    monkeypatch.setattr(jobs, "snapshot", lambda *a, **k: packet)
    monkeypatch.setattr(worker, "_log_job_claimed", lambda *a: None)
    monkeypatch.setattr(worker, "is_job_canceled", lambda *a: False)
    run, _ = completion(packet, value)
    output = run("")
    router = Mock(return_value={"parsed": output, "schema_valid": True})
    monkeypatch.setattr(worker, "run_profile", router)
    for name in ("mark_build_dirty", "update_event_report", "_handle_event_report_llm"):
        monkeypatch.setattr(worker, name, Mock(side_effect=AssertionError("must not publish")))
    job = SimpleNamespace(id="test", job_type=jobs.JOB_TYPE,
                          payload=jobs.payload_for("event", ["Acme"], scope=value))
    if stale:
        packet["documents"][0]["text"] += " Changed evidence."
        resign(packet)
        with pytest.raises(ValueError): worker.run_claimed_job(None, None, job, logging.getLogger())
        router.assert_not_called()
        assert not list(tmp_path.rglob("*.html"))
    else:
        result = worker.run_claimed_job(None, None, job, logging.getLogger())
        assert result["model_assessed"] and not result["public_eligible"]
        again = worker.run_claimed_job(None, None, job, logging.getLogger())
        assert again["model_cache_hit"] and again["artifact"] == result["artifact"]
        router.assert_called_once()
        assert router.call_args.args[1] == "scoped-profile"
        assert json.loads(router.call_args.args[2])["incident_scope"]["scope_status"] == "proposal_only"
        worker.insert_llm_run.assert_called_once()


def test_admin_passes_scope_without_inference(monkeypatch, database):
    value = proposal(get_packet(database))
    monkeypatch.setenv("SV_ADMIN_TOKEN", "test-only")
    submit = Mock(return_value="job")
    monkeypatch.setattr(jobs, "submit", submit)
    monkeypatch.setattr(worker, "run_profile", Mock(side_effect=AssertionError("no admin inference")))
    result = admin.api_event_private_review("event", admin.EventPrivateReviewRequest(aliases=["Acme"], scope=value))
    assert result["public_eligible"] is False and submit.call_args.kwargs["scope"] == value
