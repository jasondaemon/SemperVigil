import json
import logging
from types import SimpleNamespace

import pytest

from sempervigil import admin, event_review_jobs as jobs, worker
from sempervigil.storage import registered_job_types, get_queue_name_for_job_type
from test_event_review import database, get_packet

pytestmark = pytest.mark.offline


def test_registration_and_no_model_admission():
    assert jobs.JOB_TYPE in registered_job_types()
    assert jobs.JOB_TYPE in worker.WORKER_JOB_TYPES
    assert jobs.JOB_TYPE in worker.HANDLED_JOB_TYPES
    assert jobs.JOB_TYPE in worker.QUEUE_WORKER_TYPES["llm_local"]
    assert jobs.JOB_TYPE not in worker._LLM_JOB_TYPES
    assert get_queue_name_for_job_type(jobs.JOB_TYPE) == "llm_local"


def test_disabled_before_connection_or_payload_processing(monkeypatch):
    monkeypatch.delenv("SV_EVENT_REVIEW_ENABLED", raising=False)
    def forbidden():
        pytest.fail("must not connect")
    with pytest.raises(PermissionError):
        jobs.submit(forbidden, event_id="event", aliases=["Acme"])
    assert jobs.run({"publish": True})["reason"] == "private_review_disabled"


@pytest.mark.parametrize("value", ["true", "2", "", "yes"])
def test_invalid_enablement(monkeypatch, value):
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", value)
    with pytest.raises(ValueError):
        jobs.enabled()


class Connection:
    def __init__(self, pending=(), event=True):
        self.pending, self.event = pending, event
        self.calls = []
        self.commits = self.rollbacks = self.closes = 0

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return SimpleNamespace(fetchone=lambda: ("event",) if self.event else None,
                               fetchall=lambda: [(json.dumps(p),) for p in self.pending])

    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1
    def close(self): self.closes += 1


def test_submit_uses_lock_canonical_payload_low_priority_and_closes(monkeypatch):
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    conn = Connection()
    calls = []
    def enqueue(*args, **kwargs):
        calls.append((args, kwargs))
        return "job"
    monkeypatch.setattr(jobs, "enqueue_job", enqueue)
    assert jobs.submit(lambda: conn, event_id="event", aliases=["Zulu", "Acme", "Acme"]) == "job"
    assert calls[0][0][2]["aliases"] == ["Acme", "Zulu"]
    assert calls[0][1] == dict(priority=-10, dedupe=True, queue_name="llm_local", max_attempts=1)
    assert "pg_advisory_xact_lock" in conn.calls[2][0]
    assert conn.commits == conn.closes == 1


def test_queue_cap_and_event_validation_rollback(monkeypatch):
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    for conn, reason in ((Connection([{}] * 10), "queue_full"), (Connection(event=False), "event_unavailable")):
        with pytest.raises(ValueError, match=reason):
            jobs.submit(lambda: conn, event_id="event", aliases=["Acme"])
        assert conn.rollbacks == conn.closes == 1


def test_duplicate_at_cap_still_returns_existing_job(monkeypatch):
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    payload = jobs.payload_for("event", ["Acme"])
    conn = Connection([payload] + [{}] * 9)
    monkeypatch.setattr(jobs, "enqueue_job", lambda *a, **k: "existing")
    assert jobs.submit(lambda: conn, event_id="event", aliases=["Acme"]) == "existing"
    assert conn.commits == conn.closes == 1


@pytest.mark.parametrize("root,logs", [("relative", "/log"), ("/log", "/log"),
    ("/site/review", "/site"), ("/data/review", "/data"), ("/site-src/review", "/site-src"),
    ("/tmp/review", "/log"), ("/tmp/review", "/")])
def test_private_path_guards(monkeypatch, root, logs):
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", root)
    monkeypatch.setenv("SV_LOG_DIR", logs)
    with pytest.raises(ValueError): jobs.artifact_root()


def test_real_dispatch_writes_private_artifacts_not_public_report(monkeypatch, tmp_path, database):
    packet = get_packet(database)
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_DB_URL", "unused-test-dsn")
    monkeypatch.setattr(jobs, "snapshot", lambda *a, **k: packet)
    monkeypatch.setattr(worker, "_log_job_claimed", lambda *a: None)
    monkeypatch.setattr(worker, "is_job_canceled", lambda *a: False)
    def forbidden(*a, **k): pytest.fail("Private jobs cannot infer or publish")
    for name in ("run_profile", "mark_build_dirty", "update_event_report", "_handle_event_report_llm"):
        monkeypatch.setattr(worker, name, forbidden)
    job = SimpleNamespace(id="test", job_type=jobs.JOB_TYPE, payload=jobs.payload_for("event", ["Acme"]))
    result = worker.run_claimed_job(None, None, job, logging.getLogger("test"))
    assert result["status"] == "review_ready" and result["public_eligible"] is False
    path = tmp_path / "private" / result["artifact"]
    assert path.is_file()
    before = path.stat().st_mtime_ns
    assert worker.run_claimed_job(None, None, job, logging.getLogger("test")) == result
    assert path.stat().st_mtime_ns == before
    assert "Exact" not in json.dumps(result)


@pytest.mark.parametrize("payload", [{}, {"event_id": "event", "aliases": ["Acme"], "workflow": "other"},
    {"event_id": "event", "aliases": ["Acme"], "workflow": jobs.WORKFLOW, "output": "/site"}])
def test_invalid_payload_rejected_before_files_or_database(monkeypatch, payload):
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    with pytest.raises(ValueError): jobs.run(payload)


def test_all_registered_and_historical_types_have_one_dashboard_group():
    visible = admin._dashboard_visible_job_types(["retired_job"])
    assert set(registered_job_types()) <= set(visible)
    assert "build_daily_brief" in visible
    groups = admin._dashboard_job_groups(visible)
    grouped = [kind for group in groups for kind in group["job_types"]]
    assert set(grouped) == set(visible) and len(grouped) == len(set(grouped))
    rows = admin._dashboard_display_rows({"job_counts_by_type_status": {
        "retired_job": {"queued": 2, "running": 1, "failed": 3, "succeeded": 4, "canceled": 5}}})
    row = next(row for row in rows if row["job_type"] == "retired_job")
    assert (row["queued"], row["running"], row["failed"], row["complete"], row["canceled"]) == (2, 1, 3, 4, 5)


def test_private_api_is_authenticated_and_rejects_disabled(monkeypatch):
    route = next(r for r in admin.app.routes if getattr(r, "path", "") == "/admin/api/events/{event_id}/private-review")
    assert any(dep.call is admin._require_admin_token for dep in route.dependant.dependencies)
    monkeypatch.delenv("SV_EVENT_REVIEW_ENABLED", raising=False)
    with pytest.raises(admin.HTTPException) as caught:
        admin.api_event_private_review("event", admin.EventPrivateReviewRequest(aliases=["Acme"]))
    assert caught.value.status_code == 503


def test_jobs_catalog_only_queried_when_requested(monkeypatch):
    conn = Connection()
    monkeypatch.setattr(admin, "_get_conn", lambda: conn)
    monkeypatch.setattr(admin, "list_jobs_filtered", lambda *a, **k: ([], 0))
    assert "job_types" not in admin.jobs()
    assert not conn.calls
    response = admin.jobs(include_types=True)
    assert jobs.JOB_TYPE in response["job_types"]
    assert "build_daily_brief" in response["job_types"]
    assert "DISTINCT job_type" in conn.calls[0][0]
