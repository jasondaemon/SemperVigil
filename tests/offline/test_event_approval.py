import json
import logging
import os
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi.testclient import TestClient
import pytest

from sempervigil import admin, event_approval as approval, event_review_jobs as jobs, worker
from sempervigil.storage import enqueue_job, registered_job_types, get_queue_name_for_job_type
from test_event_review import database, get_packet
from test_event_scope import proposal, completion

pytestmark = pytest.mark.offline


@pytest.fixture
def review(database, tmp_path, monkeypatch):
    packet = get_packet(database)
    for name in ("SV_EVENT_REVIEW_ENABLED", "SV_EVENT_REVIEW_SCOPE_ENABLED", "SV_EVENT_HUMAN_APPROVAL_ENABLED"):
        monkeypatch.setenv(name, "1")
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    monkeypatch.setenv("SV_DB_URL", "unused-test")
    monkeypatch.setattr(jobs, "snapshot", lambda *a, **kw: packet)
    scope = proposal(packet)
    complete, calls = completion(packet, scope)
    result = jobs.run(jobs.payload_for("event", ["Acme"], scope=scope), complete=complete)
    job = SimpleNamespace(id="private-review", job_type=jobs.JOB_TYPE, status="succeeded", result=result)
    material = approval.candidate(job)
    selection = {"revision_id": result["private_revision"]["version"],
                 "passage_ids": [material["passages"][0]["id"]], "expected_predecessor": None,
                 "confirmation": approval.CONFIRMATION}
    return job, selection, calls


def test_quote_approval_is_explicit_and_bound_to_original_evidence(review):
    job, selected, calls = review
    result = approval.approval_for(job, **selected)
    assert len(calls) == 1  # only the fixture's original private assessment
    assert result["qualification"]["reviewer"]["kind"] == "human"
    assert result["qualification"]["quotes"][0]["quote"] == result["packet"]["documents"][0]["text"]
    assert result["receipt"]["assessment"]["suggestions"][selected["passage_ids"][0]]["decision"] == "hold"
    # The human can select held evidence, but the model cannot select for them.
    with pytest.raises(ValueError):
        approval.approval_for(job, **{**selected, "passage_ids": []})


@pytest.mark.parametrize("fault", ["revision", "unknown", "duplicate", "boolean", "confirmation", "predecessor", "many"])
def test_invalid_selection_never_admits(review, fault):
    job, selected, _ = review
    if fault == "revision": selected["revision_id"] = "b"*64
    if fault == "unknown": selected["passage_ids"] = ["b"*64]
    if fault == "duplicate": selected["passage_ids"] *= 2
    if fault == "boolean": selected["passage_ids"] = [True]
    if fault == "confirmation": selected["confirmation"] = "yes"
    if fault == "predecessor": selected["expected_predecessor"] = "old"
    if fault == "many": selected["passage_ids"] = ["a"*64]*13
    factory = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(ValueError): approval.submit(factory, job, **selected)
    factory.assert_not_called()


@pytest.mark.parametrize("fault", ["symlink", "fifo", "tamper", "oversize", "missing"])
def test_source_material_is_fail_closed(review, fault):
    job, _, _ = review
    path = jobs.artifact_root() / job.result["packet_version"] / "packet.json"
    if fault in {"symlink", "fifo", "missing"}: path.unlink()
    if fault == "symlink": path.symlink_to(path.parent / "other.json")
    if fault == "fifo": os.mkfifo(path)
    if fault == "tamper": path.write_text("{}")
    if fault == "oversize": path.write_bytes(b" "*3100000)
    with pytest.raises((ValueError, OSError)): approval.candidate(job)


def test_disabled_before_files_or_connections(monkeypatch):
    monkeypatch.delenv("SV_EVENT_HUMAN_APPROVAL_ENABLED", raising=False)
    factory = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(PermissionError): approval.submit(factory, None)
    assert approval.run({"approval_id": "bad"}, factory=factory)["status"] == "skipped"
    factory.assert_not_called()
    monkeypatch.setenv("SV_EVENT_HUMAN_APPROVAL_ENABLED", "true")
    with pytest.raises(ValueError): approval.enabled()


@pytest.mark.parametrize("payload", [None, {}, {"approval_id": True}, {"approval_id": "../file"},
    {"approval_id": "a"*64, "qualification": {}}, {"approval_id": "a"*64, "publish": True}])
def test_worker_rejects_inline_authority_before_database(monkeypatch, payload):
    monkeypatch.setenv("SV_EVENT_HUMAN_APPROVAL_ENABLED", "1")
    factory = Mock(side_effect=AssertionError("must not connect"))
    with pytest.raises(ValueError): approval.run(payload, factory=factory)
    factory.assert_not_called()


def test_no_fallback_to_general_database_credentials(monkeypatch):
    monkeypatch.setenv("SV_EVENT_HUMAN_APPROVAL_ENABLED", "1")
    monkeypatch.setenv("SV_DB_URL", "general-credentials-must-not-be-used")
    monkeypatch.delenv("SV_EVENT_PROMOTION_DB_URL", raising=False)
    with pytest.raises(PermissionError, match="database_required"):
        approval.run({"approval_id": "a"*64})


def test_new_task_is_registered_visible_and_does_not_use_llm_or_build(monkeypatch):
    kind = approval.JOB_TYPE
    assert kind in registered_job_types() and kind in worker.WORKER_JOB_TYPES and kind in worker.HANDLED_JOB_TYPES
    assert kind in worker.QUEUE_WORKER_TYPES["fetch"] and kind not in worker._LLM_JOB_TYPES
    assert get_queue_name_for_job_type(kind) == "fetch"
    assert kind in admin._DASHBOARD_FETCH_JOB_TYPES
    monkeypatch.setattr(worker, "_log_job_claimed", lambda *a: None)
    monkeypatch.setattr(worker, "is_job_canceled", lambda *a: False)
    for name in ("run_profile", "mark_build_dirty", "_publish_events", "_handle_event_report_llm"):
        monkeypatch.setattr(worker, name, Mock(side_effect=AssertionError("not allowed")))
    run = Mock(return_value={"publication_status": "awaiting_export", "public_eligible": False})
    monkeypatch.setattr(approval, "run", run)
    job = SimpleNamespace(id="job", job_type=kind, payload={"approval_id": "a"*64})
    assert worker.run_claimed_job(None, None, job, logging.getLogger())["public_eligible"] is False
    run.assert_called_once_with(job.payload)


def test_enqueue_retains_default_commit_but_supports_owned_transaction():
    conn = Mock()
    enqueue_job(conn, approval.JOB_TYPE, {"approval_id": "a"*64}, commit=False)
    conn.commit.assert_not_called()
    enqueue_job(conn, "event_review_private", {})
    conn.commit.assert_called_once_with()


def test_api_auth_csrf_and_disabled_fail_closed(monkeypatch):
    monkeypatch.setenv("SV_ADMIN_TOKEN", "test-only")
    monkeypatch.delenv("SV_EVENT_HUMAN_APPROVAL_ENABLED", raising=False)
    factory = Mock(side_effect=AssertionError("must not connect"))
    monkeypatch.setattr(admin, "_get_conn", factory)
    client = TestClient(admin.app)
    url = "/admin/api/jobs/job/event-approval"
    payload = {"revision_id": "a"*64, "passage_ids": ["a"*64], "expected_predecessor": None,
               "confirmation": approval.CONFIRMATION}
    assert client.get(url).status_code == 401
    headers = {"X-Admin-Token": "test-only"}
    assert client.get(url, headers=headers).status_code == 503
    assert client.post(url, json=payload, headers=headers).status_code == 403
    headers["X-SV-Event-Approval"] = "1"
    assert client.post(url, json=payload, headers={**headers, "Origin": "https://other.example"}).status_code == 403
    assert client.post(url, json=payload, headers=headers).status_code == 503
    monkeypatch.delenv("SV_ADMIN_TOKEN")
    monkeypatch.setenv("SV_EVENT_HUMAN_APPROVAL_ENABLED", "1")
    assert client.get(url).status_code == 503
    factory.assert_not_called()


def test_api_preview_approval_and_template_keep_evidence_private(review, monkeypatch):
    job, selected, _ = review
    monkeypatch.setenv("SV_ADMIN_TOKEN", "test-only")
    conn = Mock()
    monkeypatch.setattr(admin, "_get_conn", lambda: conn)
    monkeypatch.setattr(admin, "get_job", lambda *a: job)
    from unittest.mock import MagicMock
    database_conn = MagicMock()
    database_conn.__enter__.return_value = database_conn
    database_conn.execute.return_value.fetchone.return_value = None
    monkeypatch.setattr(approval, "connection_factory", lambda _: lambda: database_conn)
    client = TestClient(admin.app)
    headers = {"X-Admin-Token": "test-only", "X-SV-Event-Approval": "1"}
    url = "/admin/api/jobs/private-review/event-approval"
    response = client.get(url, headers=headers)
    assert response.status_code == 200 and "no-store" in response.headers["cache-control"]
    assert response.json()["passages"][0]["id"] == selected["passage_ids"][0]
    assert response.json()["public_eligible"] is False
    submit = Mock(return_value={"job_id": "promotion-job", "status": "queued", "public_eligible": False})
    monkeypatch.setattr(approval, "submit", submit)
    result = client.post(url, json=selected, headers=headers)
    assert result.status_code == 200 and result.json()["job_id"] == "promotion-job"
    assert submit.call_args.kwargs == selected
    page = client.get("/ui/jobs/private-review/event-approval", headers=headers)
    assert page.status_code == 200 and "no-store" in page.headers["cache-control"]
    assert "Approve selected quotes for staging" in page.text
    assert 'id="event-approval-confirm" checked' not in page.text
