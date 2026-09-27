import pytest

from sempervigil import admin, storage

pytestmark = pytest.mark.offline


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _JobsConnection:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""

    def execute(self, sql):
        self.sql = sql
        return _Cursor(self.rows)


def test_active_queue_queries_do_not_group_terminal_history(monkeypatch):
    monkeypatch.setattr(storage, "_table_exists", lambda _conn, _table: True)
    queue_conn = _JobsConnection([("fetch", 2, 1, "2026-09-26T12:00:00+00:00")])
    job_conn = _JobsConnection([("fetch", "fetch_article_content", "queued", 2)])

    queue = storage.get_active_queue_stats(queue_conn)
    jobs = storage.get_active_job_metrics(job_conn)

    assert "WHERE status IN ('queued', 'running')" in queue_conn.sql
    assert "WHERE status IN ('queued', 'running')" in job_conn.sql
    assert queue[0]["queued"] == 2
    assert jobs[0]["count"] == 2


def test_need_snapshot_is_reused_until_refresh(monkeypatch):
    clock = [1000.0]
    calls = []
    monkeypatch.setattr(admin.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(admin, "_metrics_need_cache", None)

    def build(_conn, *, need_only):
        assert need_only
        calls.append(True)
        return {"queueable_by_job_type": {"cve_enrich_llm": len(calls)}}

    monkeypatch.setattr(admin, "_build_dashboard_metrics_payload", build)
    assert admin._cached_metrics_need(object())[0]["cve_enrich_llm"] == 1
    clock[0] += 60
    assert admin._cached_metrics_need(object()) == ({"cve_enrich_llm": 1}, 60.0)
    clock[0] += 240
    assert admin._cached_metrics_need(object())[0]["cve_enrich_llm"] == 2
    assert len(calls) == 2


def test_metrics_render_uses_active_state_and_cached_need(monkeypatch):
    monkeypatch.setattr(admin, "get_active_queue_stats", lambda _conn: [
        {"queue_name": "fetch", "queued": 2, "running": 0, "oldest_requested_at": None}
    ])
    monkeypatch.setattr(admin, "get_active_job_metrics", lambda _conn: [
        {"queue_name": "fetch", "job_type": "fetch_article_content", "status": "queued", "count": 2}
    ])
    monkeypatch.setattr(admin, "get_runner_health_stats", lambda _conn: [])
    monkeypatch.setattr(admin, "get_queue_worker_health", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(admin, "get_build_state", lambda _conn: {"dirty": False})
    monkeypatch.setattr(admin, "get_build_status", lambda *_args, **_kwargs: {"status": "idle"})
    monkeypatch.setattr(admin, "get_runner_stats", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(admin, "get_stale_job_stats", lambda _conn: [])
    monkeypatch.setattr(admin, "get_source_ingest_state_counts", lambda _conn: {"queued": 0, "running": 0})
    monkeypatch.setattr(
        admin, "_build_dashboard_metrics_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("dashboard scanned")),
    )

    rendered = admin._render_metrics_text(
        object(), need_snapshot=({"fetch_article_content": 7}, 42.0)
    )

    assert 'sempervigil_queue_jobs{queue_name="fetch",status="queued"} 2' in rendered
    assert 'sempervigil_dashboard_current{column="need",job_type="fetch_article_content",worker_group="fetch"} 7' in rendered
    assert "sempervigil_need_snapshot_age_seconds 42" in rendered
