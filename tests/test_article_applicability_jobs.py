from types import SimpleNamespace

import pytest

from sempervigil import article_applicability as contract
from sempervigil import article_applicability_jobs as jobs

pytestmark = pytest.mark.offline

ARTICLE = {
    "id": 17,
    "title": "Regulator action",
    "content_text": "A regulator in Australia issued a privacy decision about Acme.",
}
GENERATION = "b" * 64


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED", raising=False)
    with pytest.raises(PermissionError, match="disabled"):
        jobs.require_enabled()


def test_private_run_only_updates_own_job_result(monkeypatch):
    monkeypatch.setenv("SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED", "1")
    monkeypatch.setattr(jobs, "configuration", lambda conn: ({}, {}, {}, GENERATION))
    monkeypatch.setattr(jobs, "snapshot", lambda conn, article_id: ARTICLE)
    saved = []
    monkeypatch.setattr(jobs, "update_job_result", lambda conn, job_id, value: saved.append(
        {**value, "articles": list(value["articles"])}
    ) or True)
    job = SimpleNamespace(
        id="job_test", job_type=jobs.JOB_TYPE, result=None, attempt_count=0,
        max_attempts=1, queue_name="llm_local", status="running",
        payload={"workflow": contract.WORKFLOW, "generation": GENERATION,
                 "articles": [ARTICLE]},
    )
    raw = ('{"kinds":[{"type":"regulatory_enforcement","passage_ids":["p001"]}],'
           '"jurisdictions":[{"code":"AU","role":"legal_scope",'
           '"passage_ids":["p001"]}],"organizations":[]}')
    result = jobs.run(None, job, generate=lambda request: raw)
    assert result["public_eligible"] is False
    assert result["attempts"] == 1
    assert result["articles"][0]["status"] == "private_comparison"
    assert len(saved) == 3


def test_private_run_stops_at_time_budget(monkeypatch):
    monkeypatch.setenv("SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED", "1")
    monkeypatch.setattr(jobs, "configuration", lambda conn: ({}, {}, {}, GENERATION))
    monkeypatch.setattr(jobs, "snapshot", lambda conn, article_id: ARTICLE)
    monkeypatch.setattr(jobs, "update_job_result", lambda conn, job_id, value: True)
    times = iter((0, 601))
    monkeypatch.setattr(jobs.time, "monotonic", lambda: next(times))
    job = SimpleNamespace(
        id="job_test", job_type=jobs.JOB_TYPE, result=None, attempt_count=0,
        max_attempts=1, queue_name="llm_local", status="running",
        payload={"workflow": contract.WORKFLOW, "generation": GENERATION,
                 "articles": [ARTICLE]},
    )
    result = jobs.run(None, job, generate=lambda request: pytest.fail("unexpected call"))
    assert result["status"] == "time_budget_exhausted"
    assert result["attempts"] == 0


def test_invalid_model_output_is_held_not_published(monkeypatch):
    monkeypatch.setenv("SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED", "1")
    monkeypatch.setattr(jobs, "configuration", lambda conn: ({}, {}, {}, GENERATION))
    monkeypatch.setattr(jobs, "snapshot", lambda conn, article_id: ARTICLE)
    monkeypatch.setattr(jobs, "update_job_result", lambda conn, job_id, value: True)
    job = SimpleNamespace(
        id="job_test", job_type=jobs.JOB_TYPE, result=None, attempt_count=0,
        max_attempts=1, queue_name="llm_local", status="running",
        payload={"workflow": contract.WORKFLOW, "generation": GENERATION,
                 "articles": [ARTICLE]},
    )
    result = jobs.run(None, job, generate=lambda request: "not json")
    assert result["articles"][0]["status"] == "held"
    assert result["articles"][0]["error"] == "ValueError"


def test_replay_and_changed_source_refused_before_model_call(monkeypatch):
    monkeypatch.setenv("SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED", "1")
    monkeypatch.setattr(jobs, "configuration", lambda conn: ({}, {}, {}, GENERATION))
    monkeypatch.setattr(jobs, "snapshot", lambda conn, article_id: ARTICLE)
    job = SimpleNamespace(
        id="job_test", job_type=jobs.JOB_TYPE, result={"partial": True},
        attempt_count=0, max_attempts=1, queue_name="llm_local", status="running",
        payload={"workflow": contract.WORKFLOW, "generation": GENERATION,
                 "articles": [ARTICLE]},
    )
    with pytest.raises(ValueError, match="replayed"):
        jobs.run(None, job, generate=lambda request: pytest.fail("unexpected call"))
    job.result = None
    job.payload["articles"] = [{**ARTICLE, "content_text": "changed"}]
    with pytest.raises(ValueError, match="baseline_changed"):
        jobs.run(None, job, generate=lambda request: pytest.fail("unexpected call"))
