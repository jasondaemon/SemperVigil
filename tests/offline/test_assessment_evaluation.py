import copy
import importlib.util
import json
from pathlib import Path

import pytest

from sempervigil import event_assessment as assessment
from test_event_review import database, get_packet

pytestmark = pytest.mark.offline
spec = importlib.util.spec_from_file_location("assessment_checker", Path(__file__).parents[2] / "tools/check-event-assessment.py")
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def sample(database, decision="include", allowed=None):
    packet = get_packet(database)
    request = assessment.request_for(packet)
    result = assessment.validate_response(json.dumps({"decisions": [
        {"id": key, "decision": decision, "reason": "same_incident" if decision == "include" else "insufficient_context"}
        for key in request["mapping"]]}).encode(), packet)
    cases = {"event_id": packet["event"]["id"], "packet_version": packet["packet_version"],
             "request_version": request["request_version"], "checks": [{
                 "passage_id": next(iter(result["suggestions"])), "label": "reviewed case", "allowed": allowed or ["include"]}]}
    return packet, result, cases


def test_correct_subset_is_not_publication_approval(database):
    result = checker.evaluate(*sample(database))
    assert result["passed"] and result["public_eligible"] is False
    assert result["checked_passages"] == 1


def test_unsafe_include_is_not_success(database):
    result = checker.evaluate(*sample(database, allowed=["exclude", "hold"]))
    assert not result["passed"] and result["checks"][0]["unsafe_include"]


def test_all_hold_is_not_success(database):
    assert not checker.evaluate(*sample(database, decision="hold"))["passed"]


@pytest.mark.parametrize("field", ["event_id", "packet_version", "request_version"])
def test_changed_inputs_fail_instead_of_using_wrong_labels(database, field):
    packet, result, cases = sample(database)
    cases[field] = "wrong"
    with pytest.raises(ValueError, match="snapshot_mismatch"):
        checker.evaluate(packet, result, cases)


@pytest.mark.parametrize("problem", ["empty", "duplicate", "missing", "bad_label", "bad_allowed"])
def test_invalid_cases_cannot_pass(database, problem):
    packet, result, cases = sample(database)
    if problem == "empty": cases["checks"] = []
    if problem == "duplicate": cases["checks"] *= 2
    if problem == "missing": cases["checks"][0]["passage_id"] = "not-assessed"
    if problem == "bad_label": cases["checks"][0]["label"] = ""
    if problem == "bad_allowed": cases["checks"][0]["allowed"] = ["publish"]
    with pytest.raises(ValueError): checker.evaluate(packet, result, cases)


def test_malformed_model_output_cannot_be_scored(database):
    packet, result, cases = sample(database)
    bad = copy.deepcopy(result)
    next(iter(bad["suggestions"].values()))["reason"] = "unvalidated prose"
    with pytest.raises(ValueError): checker.evaluate(packet, bad, cases)


def test_real_fixtures_pin_both_positive_and_negative_cases():
    fixtures = Path(__file__).parents[1] / "fixtures/events"
    rows = [json.loads(p.read_text()) for p in fixtures.glob("assessment-*-v2.json")]
    assert len(rows) == 3
    assert sum(len(row["checks"]) for row in rows) == 8
    assert all(any(c["allowed"] == ["include"] for c in row["checks"]) for row in rows)


def test_v3_retains_identical_quality_expectations():
    fixtures = Path(__file__).parents[1] / "fixtures/events"
    for old_path in fixtures.glob("assessment-*-v2.json"):
        old = json.loads(old_path.read_text())
        new = json.loads(old_path.with_name(old_path.name.replace("-v2", "-v3")).read_text())
        assert old["request_version"] != new["request_version"]
        assert {k: v for k, v in old.items() if k != "request_version"} == {
            k: v for k, v in new.items() if k != "request_version"}
