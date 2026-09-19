import hashlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from sempervigil import admin, event_review_jobs as jobs

pytestmark = pytest.mark.offline


@pytest.fixture
def artifact(tmp_path, monkeypatch):
    root = tmp_path / "logs" / "reviews"
    monkeypatch.setenv("SV_LOG_DIR", str(root.parent))
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(root))
    data = b"<!doctype html><p>Private review</p>"
    version = "a" * 64
    folder = root / version
    folder.mkdir(parents=True)
    name = "review-" + hashlib.sha256(data).hexdigest()[:16] + ".html"
    path = folder / name
    path.write_bytes(data)
    job = SimpleNamespace(job_type=jobs.JOB_TYPE, status="succeeded", result={
        "status": "review_ready", "workflow": jobs.WORKFLOW, "public_eligible": False,
        "packet_version": version, "artifact": version + "/" + name})
    return job, path, data


def test_download_bytes_and_auth_headers(artifact, monkeypatch):
    job, _, data = artifact
    conn = Mock()
    monkeypatch.setenv("SV_ADMIN_TOKEN", "test-only-token")
    monkeypatch.setattr(admin, "_get_conn", lambda: conn)
    monkeypatch.setattr(admin, "get_job", lambda *a: job)
    # Do not run application startup/database initialization.
    client = TestClient(admin.app)
    url = "/admin/api/jobs/test/private-review"
    assert client.get(url).status_code == 401
    response = client.get(url, headers={"X-Admin-Token": "test-only-token"})
    assert response.status_code == 200 and response.content == data
    assert response.headers["content-disposition"].startswith("attachment;")
    assert "no-store" in response.headers["cache-control"]
    assert response.headers["content-security-policy"].startswith("sandbox;")
    conn.close.assert_called_once()


def test_unconfigured_auth_fails_closed(monkeypatch):
    monkeypatch.delenv("SV_ADMIN_TOKEN", raising=False)
    monkeypatch.setattr(admin, "_get_conn", Mock(side_effect=AssertionError("must not connect")))
    assert TestClient(admin.app).get("/admin/api/jobs/test/private-review").status_code == 503


@pytest.mark.parametrize("key,value", [
    ("artifact", "../../secret"), ("artifact", "/etc/passwd"),
    ("packet_version", "../"), ("public_eligible", True),
    ("workflow", "public"), ("status", "skipped"),
])
def test_invalid_result_rejected(artifact, key, value):
    job, _, _ = artifact
    job.result[key] = value
    with pytest.raises(ValueError): jobs.read_artifact(job)


@pytest.mark.parametrize("kind", ["file_symlink", "directory_symlink", "tampered", "oversize", "fifo"])
def test_unsafe_files_rejected(artifact, kind, monkeypatch):
    job, path, data = artifact
    if kind == "directory_symlink":
        folder = path.parent
        moved = folder.with_name("moved")
        folder.rename(moved)
        folder.symlink_to(moved, target_is_directory=True)
    elif kind == "file_symlink":
        other = path.with_name("other")
        path.rename(other)
        path.symlink_to(other)
    elif kind == "fifo":
        import os
        path.unlink()
        os.mkfifo(path)
    elif kind == "oversize":
        monkeypatch.setattr(jobs, "MAX_REVIEW_BYTES", 2)
    else:
        path.write_bytes(data + b"tampered")
    with pytest.raises((ValueError, OSError)): jobs.read_artifact(job)


@pytest.mark.parametrize("status,kind", [("running", jobs.JOB_TYPE), ("succeeded", "event_report_llm")])
def test_other_jobs_cannot_download(artifact, status, kind):
    job, _, _ = artifact
    job.status, job.job_type = status, kind
    with pytest.raises(ValueError): jobs.read_artifact(job)
