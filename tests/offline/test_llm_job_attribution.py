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
    records, calls, releases, updates = [], [], [], []
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
    return SimpleNamespace(records=records, calls=calls, releases=releases, updates=updates)


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
        return {"parsed": {"summary": "Result"}}
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
