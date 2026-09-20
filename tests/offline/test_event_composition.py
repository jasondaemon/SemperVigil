import copy
import json
from types import SimpleNamespace

import pytest

from sempervigil import event_composition as composition
from sempervigil import event_composition_jobs as jobs
from sempervigil import worker

pytestmark = pytest.mark.offline
GENERATION = "a" * 64


def ledger_revision():
    facts = [
        {"fact_id": "f1", "statement": "Acme reported unauthorized access on July 4.",
         "kind": "reported_fact", "date_text": "July 4", "date_role": "incident",
         "sections": ["timeline", "attack_path"],
         "exact_passages": [{"passage_id": "p1", "text": "Acme reported unauthorized access on July 4."}]},
        {"fact_id": "f2", "statement": "Recovery remains unconfirmed.",
         "kind": "uncertainty", "date_text": None, "date_role": "none",
         "sections": ["open_question"],
         "exact_passages": [{"passage_id": "p2", "text": "Recovery remains unconfirmed."}]},
        {"fact_id": "f3", "statement": "An earlier report named a different date.",
         "kind": "reported_fact", "date_text": "July 3", "date_role": "incident",
         "sections": ["timeline"],
         "exact_passages": [{"passage_id": "p3", "text": "An earlier report named July 3."}]},
    ]
    return {"revision_id": "elr_" + "1" * 64, "ledger_id": "eld_" + "2" * 64,
            "status": "accepted", "lineage_current": True,
            "change": {"kind": "correction", "added_fact_ids": ["f1", "f2"],
                       "supersedes_fact_ids": ["f3"], "conflict_fact_ids": []},
            "ledger": {"title": "Acme incident", "kind": "intrusion", "public_eligible": False,
                       "facts": facts, "superseded_fact_ids": ["f3"], "conflict_fact_ids": []}}


def valid_output():
    output = {section: [] for section in composition.SECTIONS}
    output["overview"] = [{"text": "Acme disclosed an intrusion affecting its environment.",
                           "fact_refs": ["F01"]}]
    output["timeline"] = [{"text": "Acme reported that unauthorized access occurred.",
                           "fact_refs": ["F01"]}]
    output["open_questions"] = [{"text": "The available evidence does not confirm recovery.",
                                 "fact_refs": ["F02"]}]
    return output


def test_request_uses_only_active_exact_evidence_and_remains_private():
    req = composition.request(ledger_revision(), GENERATION)
    payload = json.loads(req["input"])
    assert [fact["ref"] for fact in payload["facts"]] == ["F01", "F02"]
    assert payload["required_timeline_refs"] == ["F01"]
    assert payload["facts"][0]["allowed_sections"] == ["overview", "attack_vector", "attack_path", "timeline"]
    properties = req["schema"]["properties"]
    assert properties["attack_path"]["items"]["properties"]["fact_refs"]["items"]["enum"] == ["F01"]
    assert properties["open_questions"]["items"]["properties"]["fact_refs"]["items"]["enum"] == ["F02"]
    assert properties["response_recovery"]["maxItems"] == 0
    record = composition.validate(json.dumps(valid_output()).encode(), ledger_revision(), GENERATION)
    assert record["public_eligible"] is False and record["status"] == "unreviewed"
    assert record["change"] == ledger_revision()["change"]


def test_allowed_sections_reclassify_existing_ledger_facts_without_mutation():
    response = {"statement": "The company patched the flaw and notified affected users.",
                "kind": "reported_fact", "date_text": None, "date_role": "none",
                "sections": ["context"]}
    assert "response_recovery" in composition._allowed_sections(response)
    access = {"statement": "Attackers breached the customer system and downloaded records.",
              "kind": "reported_fact", "date_text": None, "date_role": "none",
              "sections": ["context"]}
    assert "attack_path" in composition._allowed_sections(access)


def test_allowed_sections_can_remove_stale_unsafe_permissions():
    stolen = {"statement": "The exposed information included email addresses and phone numbers.",
              "kind": "reported_fact", "date_text": None, "date_role": "none",
              "sections": ["attack_path"]}
    assert "attack_path" not in composition._allowed_sections(stolen)
    assert "impact" in composition._allowed_sections(stolen)


def test_timeline_normalization_ignores_publication_and_collapses_equivalent_dates():
    aliases = {
        "F01": {"fact_id": "one", "statement": "The incident began in June 2025.",
                 "kind": "reported_fact", "date_text": "June 2025", "date_role": "incident"},
        "F02": {"fact_id": "two", "statement": "The compromise is believed to have begun in June 2025.",
                 "kind": "reported_fact", "date_text": "June 2025", "date_role": "incident"},
        "F03": {"fact_id": "three", "statement": "All malicious activity was terminated.",
                 "kind": "reported_fact", "date_text": "December 2, 2025", "date_role": "incident"},
        "F04": {"fact_id": "four", "statement": "The findings were no longer observed.",
                 "kind": "reported_fact", "date_text": "2nd of December, 2025", "date_role": "incident"},
        "F05": {"fact_id": "five", "statement": "The article was published.",
                 "kind": "reported_fact", "date_text": "2 February 2026", "date_role": "publication"},
    }
    refs = composition._timeline_refs(aliases)
    assert len(refs) == 2
    assert set(refs) <= {"F01", "F02", "F03", "F04"}


def test_timeline_normalization_matches_missing_year_to_one_explicit_year():
    aliases = {
        "F01": {"fact_id": "one", "statement": "Credentials remained exposed until the cutoff.",
                 "kind": "reported_fact", "date_text": "2nd of December", "date_role": "incident"},
        "F02": {"fact_id": "two", "statement": "Activity was terminated at the cutoff.",
                 "kind": "reported_fact", "date_text": "December 2, 2025", "date_role": "incident"},
    }
    assert len(composition._timeline_refs(aliases)) == 1


def test_timeline_normalization_fails_closed_over_eight_distinct_milestones():
    aliases = {
        f"F{index:02d}": {"fact_id": str(index), "statement": f"Milestone {index} occurred.",
                          "kind": "reported_fact", "date_text": f"January {index}, 2026",
                          "date_role": "incident"}
        for index in range(1, 10)
    }
    with pytest.raises(ValueError, match="timeline_over_budget"):
        composition._timeline_refs(aliases)


def test_response_schema_uses_openai_supported_subset_and_duplicates_fail_closed():
    req = composition.request(ledger_revision(), GENERATION)
    assert "uniqueItems" not in json.dumps(req["schema"])
    output = valid_output()
    output["overview"][0]["fact_refs"] = ["F01", "F01"]
    with pytest.raises(ValueError, match="duplicate_fact_ref"):
        composition.validate(json.dumps(output).encode(), ledger_revision(), GENERATION)


def test_validation_rejects_unknown_or_unsupported_evidence():
    output = valid_output()
    output["overview"][0]["fact_refs"] = ["unknown"]
    with pytest.raises(ValueError, match="invalid_shape"):
        composition.validate(json.dumps(output).encode(), ledger_revision(), GENERATION)
    output = valid_output()
    output["timeline"][0]["fact_refs"] = ["F02"]
    with pytest.raises(ValueError, match="invalid_shape"):
        composition.validate(json.dumps(output).encode(), ledger_revision(), GENERATION)
    output = valid_output()
    output["open_questions"][0]["fact_refs"] = ["F01"]
    with pytest.raises(ValueError, match="invalid_shape"):
        composition.validate(json.dumps(output).encode(), ledger_revision(), GENERATION)


def test_dated_fact_is_deterministically_added_to_timeline():
    output = valid_output()
    output["timeline"] = []
    with pytest.raises(ValueError, match="timeline_incomplete"):
        composition.validate(json.dumps(output).encode(), ledger_revision(), GENERATION)


def test_model_prose_is_stored_with_immutable_evidence_ids():
    output = valid_output()
    output["overview"][0]["text"] = "A varied, readable account grounded in accepted evidence."
    record = composition.validate(json.dumps(output).encode(), ledger_revision(), GENERATION)
    assert record["sections"]["overview"][0] == {
        "text": output["overview"][0]["text"], "fact_ids": ["f1"]}
    assert record["sections"]["timeline"][0]["date_text"] == "July 4"


def test_noncurrent_or_public_ledger_is_never_composed():
    for change in ({"lineage_current": False}, {"status": "superseded"}):
        revision = ledger_revision(); revision.update(change)
        with pytest.raises(ValueError, match="not_current"):
            composition.request(revision, GENERATION)
    revision = ledger_revision(); revision["ledger"]["public_eligible"] = True
    with pytest.raises(ValueError, match="not_current"):
        composition.request(revision, GENERATION)


@pytest.fixture
def harness(monkeypatch):
    monkeypatch.setenv("SV_EVENT_LEDGER_COMPOSITION_ENABLED", "1")
    monkeypatch.setattr(jobs, "configuration", lambda conn: ({}, {}, GENERATION))
    monkeypatch.setattr(jobs, "ledger_revision", lambda conn, revision_id: copy.deepcopy(ledger_revision()))
    saved = []
    monkeypatch.setattr(jobs, "update_job_result",
                        lambda conn, job_id, result: saved.append(copy.deepcopy(result)) or True)
    monkeypatch.setattr(composition, "store_unreviewed", lambda conn, record: "elc_" + "3" * 64)
    return saved


def running_job():
    req = composition.request(ledger_revision(), GENERATION)
    return SimpleNamespace(id="test", job_type=jobs.JOB_TYPE, result=None, attempt_count=0,
        max_attempts=1, queue_name="openai", status="running", payload={
            "workflow": composition.WORKFLOW, "ledger_revision_id": ledger_revision()["revision_id"],
            "generation": GENERATION, "request_version": req["request_version"]})


def test_job_is_one_attempt_private_and_review_gated(harness):
    calls = []
    def generate(request):
        calls.append(request)
        assert harness[-1]["status"] == "started" and harness[-1]["attempts"] == 1
        return json.dumps(valid_output())
    result = jobs.run(object(), running_job(), generate=generate)
    assert len(calls) == 1 and result["status"] == "review_required"
    assert result["public_eligible"] is False and result["attempts"] == 1
    assert result["composition_id"].startswith("elc_")
    assert json.loads(result["raw"])["overview"][0]["fact_refs"] == ["F01"]


@pytest.mark.parametrize("change", [{"attempt_count": 1}, {"max_attempts": 2},
    {"queue_name": "llm_local"}, {"status": "queued"}, {"result": {"status": "started"}}])
def test_replay_or_wrong_lane_never_calls_model(harness, change):
    current = running_job()
    for key, value in change.items():
        setattr(current, key, value)
    with pytest.raises(ValueError, match="invalid_or_replayed"):
        jobs.run(object(), current, generate=lambda request: pytest.fail("inference"))
    assert not harness


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("SV_EVENT_LEDGER_COMPOSITION_ENABLED", raising=False)
    with pytest.raises(PermissionError):
        jobs.run(object(), running_job())


def test_worker_registry_and_queue_mapping():
    from sempervigil.storage import get_queue_name_for_job_type
    assert get_queue_name_for_job_type(jobs.JOB_TYPE) == "openai"
    assert jobs.JOB_TYPE not in worker._LLM_JOB_TYPES
    assert jobs.JOB_TYPE in worker.QUEUE_WORKER_TYPES["openai"]
    assert jobs.JOB_TYPE in worker.HANDLED_JOB_TYPES
