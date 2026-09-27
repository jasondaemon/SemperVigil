from unittest.mock import Mock

import pytest

from sempervigil import admin, storage

pytestmark = pytest.mark.offline


def test_counter_only_storage_skips_content_queries(monkeypatch):
    conn = Mock()
    conn.execute.return_value.fetchall.side_effect = [
        [("event_review_private", "queued", 2)], []
    ]
    monkeypatch.setattr(storage, "_table_exists", lambda c, t: t == "jobs")
    monkeypatch.setattr(storage, "get_setting", lambda *a: None)
    monkeypatch.setattr(storage, "get_article_state_counts_by_source", Mock(side_effect=AssertionError("backlog queried")))
    result = storage.get_dashboard_metrics(conn, include_backlog=False)
    assert result["job_counts_by_type_status"]["event_review_private"]["queued"] == 2
    assert conn.execute.call_count == 2


def test_dashboard_uses_active_jobs_and_terminal_counters(monkeypatch):
    conn = Mock()
    conn.execute.return_value.fetchall.side_effect = [
        [("build_site", "queued", 2)], [],
        [("build_site", "failed", 3), ("build_site", "succeeded", 7),
         ("build_site", "canceled", 1)],
    ]
    monkeypatch.setattr(storage, "_table_exists", lambda c, t: t in {"jobs", "dashboard_job_status_counts"})
    monkeypatch.setattr(storage, "get_setting", lambda c, k, d: "2026-09-21" if k == "dashboard_job_counts_since" else d)
    result = storage.get_dashboard_metrics(conn, include_backlog=False)
    assert result["job_counts_by_type_status"]["build_site"] == {
        "queued": 2, "failed": 3, "succeeded": 7, "canceled": 1,
    }
    queries = [call.args[0] for call in conn.execute.call_args_list]
    assert "WHERE status IN ('queued', 'running')" in queries[0]
    assert "FROM dashboard_job_status_counts" in queries[2]
    assert all("COALESCE(finished_at, requested_at)" not in sql for sql in queries)


def test_counter_only_payload_has_all_jobs_and_unknown_need(monkeypatch):
    metrics = Mock(return_value={"job_counts_by_type_status": {"legacy-job": {"canceled": 1}}})
    monkeypatch.setattr(admin, "get_dashboard_metrics", metrics)
    monkeypatch.setattr(admin, "get_build_state", lambda c: {})
    monkeypatch.setattr(admin, "get_build_status", Mock(side_effect=AssertionError("full metrics queried")))
    conn = object()
    result = admin._build_dashboard_metrics_payload(conn, include_backlog=False)
    metrics.assert_called_once_with(conn, include_backlog=False)
    assert {"legacy-job", "event_review_private", "build_daily_brief"} <= set(result["job_types"])
    assert result["queueable_by_job_type"] == {}
