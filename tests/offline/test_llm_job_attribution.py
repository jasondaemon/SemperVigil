import logging
from types import SimpleNamespace

import pytest

from sempervigil import worker

pytestmark = pytest.mark.offline


@pytest.fixture
def harness(monkeypatch):
    article = {"id": 7, "source_id": "example", "title": "Example", "content_text": "Evidence"}
    profile = {"id": "profile_example", "name": "Example", "primary_provider_id": "provider_example",
               "primary_model_id": "model_example"}
    records, calls, releases, updates, errors = [], [], [], [], []
    monkeypatch.setenv("SV_LLM_MAX_INFLIGHT", "1")
    monkeypatch.setattr(worker, "get_article_by_id", lambda *a: article)
    monkeypatch.setattr(worker, "get_active_profile_for_stage", lambda *a: (profile, ""))
    monkeypatch.setattr(worker, "get_source_name", lambda *a: "Example")
    monkeypatch.setattr(worker, "try_acquire_lease", lambda *a, **kw: True)
    monkeypatch.setattr(worker, "release_lease", lambda *a: releases.append(a))
    monkeypatch.setattr(worker, "insert_llm_run", lambda conn, **kw: records.append(kw))
    monkeypatch.setattr(worker, "list_article_cve_ids", lambda *a: [])
    monkeypatch.setattr(worker, "list_event_ids_for_article", lambda *a: [])
    monkeypatch.setattr(worker, "has_pending_article_job", lambda *a: True)
    for name in ("mark_build_dirty", "_maybe_enqueue_article_product_enrich", "_maybe_enqueue_context_pack", "_enqueue_write_from_article"):
        monkeypatch.setattr(worker, name, lambda *a, **kw: None)
    for name in ("update_article_summary", "update_article_context_pack"):
        monkeypatch.setattr(worker, name, lambda *a, **kw: updates.append(kw))
    monkeypatch.setattr(worker, "record_article_enrichment_error", lambda *a, **kw: errors.append(kw))
    return SimpleNamespace(records=records, calls=calls, releases=releases, updates=updates, errors=errors)


@pytest.mark.parametrize("job_type,stage", [
    ("summarize_article_llm", "summarize_article"),
    ("summarize_article_context_llm", "article_context_pack"),
])
@pytest.mark.parametrize("fail", [False, True])
def test_records_actual_job_for_success_and_failure(monkeypatch, harness, job_type, stage, fail):
    def pipeline(conn, actual_stage, text, logger, **kwargs):
        harness.calls.append((actual_stage, text, kwargs))
        if fail:
            raise ValueError("provider_test_failure")
        data = {"summary": "Result"} if stage == "summarize_article" else {
            **{k: [] for k in ("facts", "numbers", "iocs", "cves", "timeline", "uncertainties")},
            "entities": {k: [] for k in ("orgs", "people", "products", "vendors", "threat_actors", "countries")}}
        return {"parsed": data, "schema_valid": True}
    monkeypatch.setattr(worker, "run_pipeline_stage", pipeline)
    job = SimpleNamespace(id="job_test", job_type=job_type, payload={"article_id": 7})
    handler = getattr(worker, f"_handle_{job_type}")
    if fail:
        with pytest.raises(ValueError, match="provider_test_failure"):
            handler(None, None, job, logging.getLogger("test"))
    else:
        assert handler(None, None, job, logging.getLogger("test"))["ok"]
    assert len(harness.calls) == len(harness.records) == len(harness.releases) == 1
    actual_stage, text, kwargs = harness.calls[0]
    assert actual_stage == stage
    assert text.endswith("Content:\nEvidence\n")
    assert kwargs == {"profile_id": "profile_example",
                      "context": {"stage": stage, "job_type": job_type}}
    record = harness.records[0]
    assert record["job_id"] == job.id
    assert record["provider_id"] == "provider_example"
    assert record["model_id"] == "model_example"
    assert record["ok"] is (not fail)
    assert record["error"] == ("provider_test_failure" if fail else None)
    assert harness.releases[0][2] == "job_test:7"
    assert len(harness.updates) == (0 if fail else 1)
    assert len(harness.errors) == (1 if fail else 0)


@pytest.mark.parametrize("job_type", ["summarize_article_llm", "summarize_article_context_llm"])
@pytest.mark.parametrize("result", [
    {"schema_valid": False, "parsed": {"summary": "Do not publish"}, "raw": "Fallback"},
    {"schema_valid": True, "parsed": None, "raw": "Unstructured text"},
    {"schema_valid": True, "parsed": ["Not an object"]},
    {"schema_valid": True, "parsed": {}},
])
def test_invalid_outputs_do_not_replace_content_or_trigger_enrichment(monkeypatch, harness, job_type, result):
    monkeypatch.setattr(worker, "run_pipeline_stage", lambda *a, **kw: result)
    def forbidden(*a, **kw): pytest.fail("Invalid output must not feed downstream enrichment")
    for name in ("mark_build_dirty", "_maybe_enqueue_article_product_enrich", "_maybe_enqueue_context_pack", "enqueue_job"):
        monkeypatch.setattr(worker, name, forbidden)
    job = SimpleNamespace(id="bad_output", job_type=job_type, payload={"article_id": 7})
    with pytest.raises(ValueError, match="article_"):
        getattr(worker, f"_handle_{job_type}")(None, None, job, logging.getLogger("test"))
    assert not harness.updates
    assert len(harness.errors) == len(harness.releases) == 1
    assert harness.records[0]["ok"] is False
