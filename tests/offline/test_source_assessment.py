"""Source-level assessment keeps full coverage and fails before inference."""
import copy
import json
from unittest.mock import Mock

import pytest

from sempervigil import event_assessment as assessment, event_review as review
from sempervigil import event_assessment_cache as cache
from sempervigil import event_review_jobs as jobs, admin
from test_event_review import database, get_packet, resign
from test_event_scope import proposal
from test_assessment_evaluation import checker

pytestmark = pytest.mark.offline


def packet_with_sources(database):
    packet = get_packet(database)
    other = copy.deepcopy(packet["documents"][0])
    other.update(article_id=2, text="Acme provides consulting services to many customers worldwide.")
    packet["documents"].append(other)
    return resign(packet)


def test_source_selection_preserves_packet_and_omitted_coverage(database):
    packet = packet_with_sources(database)
    before = copy.deepcopy(packet)
    scope = proposal(packet)
    request = assessment.request_for(packet, scope=scope, article_id=2)
    expected = {p["id"] for p in review.draft(packet)["passages"] if p["article_id"] == 2}
    assert set(request["mapping"].values()) == expected
    assert request["omitted_passages"] == 1
    assert request["article_id"] == 2 and request["workflow"] == assessment.SOURCE_WORKFLOW
    assert request["system"] == assessment.SCOPED_SYSTEM_PROMPT
    assert len((request["system"] + request["input"]).encode()) <= assessment.MAX_INPUT_BYTES
    assert packet == before
    complete = Mock(return_value={"decisions": [
        {"id": key, "decision": "hold", "reason": "insufficient_context"}
        for key in request["mapping"]]})
    result = assessment.assess(packet, complete, scope=scope, article_id=2)
    complete.assert_called_once_with(request["input"])
    assert result["article_id"] == 2 and not result["public_eligible"]
    assert assessment.validate_assessment(result, packet) == result
    result["article_id"] = 1
    with pytest.raises(ValueError):
        assessment.validate_assessment(result, packet)


@pytest.mark.parametrize("article_id", [True, False, 0, -1, "1", 1.0, [], 99])
def test_bad_or_unavailable_source_rejected_before_inference(database, article_id):
    packet = get_packet(database)
    complete = Mock(side_effect=AssertionError("must not infer"))
    with pytest.raises(ValueError):
        assessment.assess(packet, complete, scope=proposal(packet), article_id=article_id)
    complete.assert_not_called()


def test_source_requires_scope_and_has_distinct_identity(database):
    packet = packet_with_sources(database)
    with pytest.raises(ValueError, match="invalid_assessment_source"):
        assessment.request_for(packet, article_id=1)
    scope = proposal(packet)
    requests = [assessment.request_for(packet, scope=scope, article_id=value)
                for value in (None, 1, 2)]
    assert len({r["request_version"] for r in requests}) == 3
    assert "article_id" not in requests[0]
    assert assessment.request_for(packet, scope=scope) == requests[0]


def test_available_source_without_candidates_is_not_fabricated(database):
    packet = packet_with_sources(database)
    packet["documents"][1]["text"] = "Unrelated company supplies services worldwide without a named incident."
    resign(packet)
    complete = Mock(side_effect=AssertionError("no candidates"))
    result = assessment.assess(packet, complete, scope=proposal(packet), article_id=2)
    assert result["suggestions"] == {} and result["omitted_passages"] == 1
    complete.assert_not_called()


def test_missing_source_decisions_fail_closed(database):
    packet = get_packet(database)
    with pytest.raises(ValueError, match="incomplete_assessment"):
        assessment.validate_response(json.dumps({"decisions": []}).encode(), packet,
                                     scope=proposal(packet), article_id=1)


def test_cache_separates_identical_sources_and_rebinds_timestamp(database, tmp_path):
    packet = packet_with_sources(database)
    packet["documents"][1]["text"] = packet["documents"][0]["text"]
    resign(packet)
    scope = proposal(packet)
    calls = []
    def complete(text):
        calls.append(text)
        return {"decisions": [{"id": key, "decision": "hold", "reason": "insufficient_context"}
                              for key in json.loads(text)["required_ids"]]}
    complete.cache_identity = "a" * 64
    one, hit = cache.reuse(packet, complete, tmp_path, scope=scope, article_id=1)
    assert not hit
    two, hit = cache.reuse(packet, complete, tmp_path, scope=scope, article_id=2)
    assert not hit and len(calls) == 2
    assert one["request_version"] != two["request_version"]
    packet["event"]["updated_at"] = "2026-09-20"
    resign(packet)
    rebound, hit = cache.reuse(packet, complete, tmp_path, scope=scope, article_id=2)
    assert hit and len(calls) == 2
    assert rebound["article_id"] == 2
    assert rebound["suggestions"].keys() != two["suggestions"].keys()
    assert assessment.validate_assessment(rebound, packet) == rebound


def test_cache_rejects_other_source_planted_in_entry(database, tmp_path):
    packet = packet_with_sources(database)
    scope = proposal(packet)
    def complete(text):
        return {"decisions": [{"id": key, "decision": "hold", "reason": "insufficient_context"}
                              for key in json.loads(text)["required_ids"]]}
    complete.cache_identity = "b" * 64
    cache.reuse(packet, complete, tmp_path, scope=scope, article_id=1)
    path = next((tmp_path / "assessment-cache").glob("*.json"))
    entry = json.loads(path.read_bytes())
    entry["assessment"] = assessment.assess(packet, complete, scope=scope, article_id=2)
    path.write_text(json.dumps(entry))
    guarded = Mock(side_effect=AssertionError("must not infer"))
    guarded.cache_identity = complete.cache_identity
    with pytest.raises(ValueError, match="stale_assessment_cache"):
        cache.reuse(packet, guarded, tmp_path, scope=scope, article_id=1)
    guarded.assert_not_called()


def test_admin_source_option_is_strict_and_passed_to_queue(database, monkeypatch):
    scope = proposal(get_packet(database))
    monkeypatch.setenv("SV_ADMIN_TOKEN", "test-only")
    submit = Mock(return_value="job")
    monkeypatch.setattr(jobs, "submit", submit)
    request = admin.EventPrivateReviewRequest(aliases=["Acme"], scope=scope, article_id=1)
    assert admin.api_event_private_review("event", request)["job_id"] == "job"
    assert submit.call_args.kwargs["article_id"] == 1
    for value in (True, "1", 1.0):
        with pytest.raises(ValueError):
            admin.EventPrivateReviewRequest(aliases=["Acme"], scope=scope, article_id=value)


def test_source_job_full_packet_single_call_and_cache(database, monkeypatch, tmp_path):
    packet = packet_with_sources(database)
    scope = proposal(packet)
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_SCOPE_ENABLED", "1")
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    monkeypatch.setenv("SV_DB_URL", "unused-test")
    monkeypatch.setattr(jobs, "snapshot", lambda *a, **k: packet)
    complete = Mock(return_value={"decisions": [
        {"id": "p1", "decision": "hold", "reason": "insufficient_context"}]})
    complete.cache_identity = "c" * 64
    payload = jobs.payload_for("event", ["Acme"], scope=scope, article_id=2)
    result = jobs.run(payload, complete=complete)
    assert result["documents"] == 2 and result["passages"] == 2
    assert result["assessment_summary"]["article_id"] == 2
    assert result["assessment_summary"]["not_assessed"] == 1
    assert not result["public_eligible"]
    assert jobs.run(payload, complete=complete)["model_cache_hit"]
    complete.assert_called_once()
    unavailable = jobs.payload_for("event", ["Acme"], scope=scope, article_id=99)
    with pytest.raises(ValueError, match="unavailable_assessment_source"):
        jobs.run(unavailable, complete=complete)
    complete.assert_called_once()


def test_unscoped_source_submission_rejected_before_connection(monkeypatch):
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    connect = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(ValueError, match="invalid_assessment_source"):
        jobs.submit(connect, event_id="event", aliases=["Acme"], article_id=1)
    connect.assert_not_called()


def source_cohort(database):
    packet = packet_with_sources(database)
    scope = proposal(packet)
    cases = {"event_id": packet["event"]["id"], "packet_version": packet["packet_version"],
             "scope_version": scope["scope_version"], "generation_version": "a" * 64,
             "source_requests": {}, "checks": []}
    bundle = {"generation_version": "a" * 64, "assessments": []}
    for article_id in (1, 2):
        request = assessment.request_for(packet, scope=scope, article_id=article_id)
        result = assessment.validate_response(json.dumps({"decisions": [
            {"id": key, "decision": "hold", "reason": "insufficient_context"}
            for key in request["mapping"]]}).encode(), packet, scope=scope, article_id=article_id)
        bundle["assessments"].append({"generation_version": "a" * 64, "assessment": result})
        cases["source_requests"][str(article_id)] = request["request_version"]
        cases["checks"].extend({"passage_id": identity, "label": "Case " + str(article_id),
                                "allowed": ["hold", "exclude"]} for identity in result["suggestions"])
    return packet, bundle, cases


def test_source_cohort_requires_all_cases_and_remains_private(database):
    packet, bundle, cases = source_cohort(database)
    report = checker.evaluate_sources(packet, bundle, cases)
    assert report["passed"] and report["checked_passages"] == 2 and report["sources"] == 2
    assert report["public_eligible"] is False
    cases["checks"][0]["allowed"] = ["include"]
    assert not checker.evaluate_sources(packet, bundle, cases)["passed"]


@pytest.mark.parametrize("change", ["missing", "duplicate", "generation", "scope", "packet",
                                    "request", "duplicate_case", "missing_case", "unassessed"])
def test_source_cohort_rejects_mixed_or_incomplete_evidence(database, change):
    packet, bundle, cases = source_cohort(database)
    if change == "missing": bundle["assessments"].pop()
    if change == "duplicate": bundle["assessments"][1] = bundle["assessments"][0]
    if change == "generation": bundle["assessments"][0]["generation_version"] = "b" * 64
    if change == "scope": cases["scope_version"] = "b" * 64
    if change == "packet": cases["packet_version"] = "b" * 64
    if change == "request": cases["source_requests"]["1"] = "b" * 64
    if change == "duplicate_case": cases["checks"].append(cases["checks"][0])
    if change == "missing_case": cases["checks"].pop()
    if change == "unassessed": cases["checks"][0]["passage_id"] = "b" * 64
    with pytest.raises(ValueError):
        checker.evaluate_sources(packet, bundle, cases)
