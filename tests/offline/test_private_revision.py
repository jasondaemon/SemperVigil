import json
import os
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi.testclient import TestClient
import pytest

from sempervigil import admin, event_assessment, event_review_jobs as jobs
from test_event_review import database, get_packet

pytestmark = pytest.mark.offline


@pytest.fixture
def revision(database, tmp_path, monkeypatch):
    packet = get_packet(database)
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    monkeypatch.setenv("SV_DB_URL", "unused-test")
    monkeypatch.setattr(jobs, "snapshot", lambda *a, **kw: packet)
    request = event_assessment.request_for(packet)
    complete = Mock(return_value={"decisions": [
        {"id": key, "decision": "hold", "reason": "insufficient_context"}
        for key in request["mapping"]]})
    complete.cache_identity = "a" * 64
    payload = jobs.payload_for("event", ["Acme"])
    result = jobs.run(payload, complete=complete)
    job = SimpleNamespace(job_type=jobs.JOB_TYPE, status="succeeded", result=result)
    path = jobs.artifact_root() / result["packet_version"] / ("revision-" + result["private_revision"]["version"] + ".json")
    return job, path, complete, payload


def test_worker_receipt_reuse_without_inference_or_overwrite(revision):
    job, path, complete, payload = revision
    stamp = path.stat().st_mtime_ns
    raw = jobs.read_revision(job)
    record = json.loads(raw)
    assert record["generation_version"] == "a" * 64
    assert record["status"] == "proposal_only" and record["public_eligible"] is False
    assert "incident_qualification" in record["publication_gates"]
    assert record["assessment"]["packet_version"] == job.result["packet_version"]
    assert jobs.run(payload, complete=complete)["private_revision"] == job.result["private_revision"]
    assert path.stat().st_mtime_ns == stamp and path.read_bytes() == raw
    assert os.stat(path).st_mode & 0o777 == 0o600
    complete.assert_called_once()


@pytest.mark.parametrize("fault", ["traversal", "symlink", "fifo", "tamper", "event", "missing", "public"])
def test_receipt_reader_fails_closed(revision, fault):
    job, path, _, _ = revision
    if fault == "traversal": job.result["private_revision"]["version"] = "../../secret"
    if fault == "symlink":
        other = path.with_name("other.json")
        path.rename(other)
        path.symlink_to(other)
    if fault == "fifo":
        path.unlink()
        os.mkfifo(path)
    if fault == "tamper": path.write_bytes(b'{}')
    if fault == "event": job.result["event_id"] = "another-event"
    if fault == "missing": del job.result["private_revision"]
    if fault == "public": job.result["private_revision"]["public_eligible"] = True
    with pytest.raises((OSError, ValueError)): jobs.read_revision(job)


def test_authenticated_download_and_legacy_job_not_found(revision, monkeypatch):
    job, path, _, _ = revision
    monkeypatch.setenv("SV_ADMIN_TOKEN", "test-only")
    conn = Mock()
    monkeypatch.setattr(admin, "_get_conn", lambda: conn)
    monkeypatch.setattr(admin, "get_job", lambda *a: job)
    client = TestClient(admin.app)
    url = "/admin/api/jobs/test/private-revision"
    assert client.get(url).status_code == 401
    response = client.get(url, headers={"X-Admin-Token": "test-only"})
    assert response.status_code == 200 and response.content == path.read_bytes()
    assert "no-store" in response.headers["cache-control"]
    assert response.headers["content-security-policy"].startswith("sandbox;")
    del job.result["private_revision"]
    assert client.get(url, headers={"X-Admin-Token": "test-only"}).status_code == 404


def test_missing_auth_config_cannot_open_database(monkeypatch):
    monkeypatch.delenv("SV_ADMIN_TOKEN", raising=False)
    monkeypatch.setattr(admin, "_get_conn", Mock(side_effect=AssertionError("must not connect")))
    assert TestClient(admin.app).get("/admin/api/jobs/test/private-revision").status_code == 503
