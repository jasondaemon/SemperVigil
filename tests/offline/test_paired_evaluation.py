import pytest

from sempervigil import event_assessment as assessment
from test_assessment_evaluation import checker
from test_event_review import database, get_packet, resign
from test_event_scope import proposal

pytestmark = pytest.mark.offline


def cohort(database):
    packet = get_packet(database)
    scope = proposal(packet)
    request = assessment.request_for(packet, scope=scope, article_id=1, paired=True)
    result = assessment.assess(packet, lambda *a: {"decisions": [
        {"id": key, "decision": "include", "reason": "same_incident"}
        for key in request["mapping"]]}, scope=scope, article_id=1, paired=True)
    cases = {"event_id": packet["event"]["id"], "packet_version": packet["packet_version"],
             "generation_version": "a" * 64, "scope": scope,
             "source_requests": {"1": request["request_version"]}, "checks": [{
                 "passage_id": next(iter(result["suggestions"])),
                 "label": "positive retained", "allowed": ["include"]}]}
    bundle = {"generation_version": "a" * 64, "assessments": [
        {"generation_version": "a" * 64, "assessment": result}]}
    return packet, bundle, cases


def test_paired_cohort_is_not_public_permission(database):
    report = checker.evaluate_pairs(*cohort(database))
    assert report["passed"] and not report["public_eligible"]
    assert report["checked_passages"] == 1 and report["unassessed_checks"] == 0


@pytest.mark.parametrize("fault", ["missing", "duplicate", "generation", "pin", "scope", "fake_budget"])
def test_paired_cohort_rejects_incomplete_or_mixed_evidence(database, fault):
    packet, bundle, cases = cohort(database)
    if fault == "missing": bundle["assessments"] = []
    if fault == "duplicate": bundle["assessments"] *= 2
    if fault == "generation": bundle["assessments"][0]["generation_version"] = "b" * 64
    if fault == "pin": cases["source_requests"]["1"] = "b" * 64
    if fault == "scope": cases["scope"] = {}
    if fault == "fake_budget": cases["source_requests"]["1"] = "assessment_pair_over_budget"
    with pytest.raises(ValueError): checker.evaluate_pairs(packet, bundle, cases)


def test_real_budget_refusal_is_recomputed_not_counted_as_model_success(database):
    packet, bundle, cases = cohort(database)
    packet["documents"][0]["text"] += " Context." * 1900
    resign(packet)
    from sempervigil.event_scope import propose
    old = cases["scope"]
    scope = propose(packet, article_id=1, start=old["anchor"]["start"], end=old["anchor"]["end"],
                    focus=[{k: v for k, v in f.items() if k != "quote"} for f in old["focus"]])
    from sempervigil.event_review import draft
    cases.update(packet_version=packet["packet_version"], scope=scope,
                 source_requests={"1": "assessment_pair_over_budget"})
    cases["checks"][0]["passage_id"] = draft(packet)["passages"][0]["id"]
    bundle["assessments"] = []
    report = checker.evaluate_pairs(packet, bundle, cases)
    assert not report["passed"] and not report["assessed_checks_passed"]
    assert report["checked_passages"] == 0 and report["unassessed_checks"] == 1
    assert report["budget_refusals"][0]["article_id"] == 1
