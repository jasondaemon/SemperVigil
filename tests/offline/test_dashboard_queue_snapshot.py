from unittest.mock import Mock

import pytest

from sempervigil import storage

pytestmark = pytest.mark.offline


class _EmptyConn:
    def execute(self, *_args, **_kwargs):
        raise AssertionError("unexpected database query")


def test_queue_health_reuses_supplied_dashboard_snapshot(monkeypatch):
    monkeypatch.setattr(storage, "_table_exists", lambda *_args: False)
    monkeypatch.setattr(storage, "get_queue_stats", Mock(side_effect=AssertionError("recount")))
    monkeypatch.setattr(
        storage, "get_runner_health_stats", Mock(side_effect=AssertionError("recount"))
    )

    rows = storage.get_queue_worker_health(
        _EmptyConn(),
        queue_stats=[{"queue_name": "build", "queued": 2, "running": 0}],
        runner_health_stats=[{"runner_type": "build", "health": "idle", "count": 1}],
    )

    build = {row["metric"]: row["count"] for row in rows if row["queue_name"] == "build"}
    assert build["queued_jobs"] == 2
    assert build["idle_with_backlog"] == 1


def test_build_status_reuses_supplied_queue_health(monkeypatch):
    monkeypatch.setattr(storage, "get_build_state", lambda _conn: {"dirty": False})
    monkeypatch.setattr(storage, "_table_exists", lambda *_args: False)
    monkeypatch.setattr(
        storage, "get_queue_worker_health", Mock(side_effect=AssertionError("recount"))
    )

    result = storage.get_build_status(
        _EmptyConn(),
        queue_worker_health=[{"queue_name": "build", "metric": "queued_jobs", "count": 2}],
    )

    assert result["status"] == "building"
    assert result["reason"] == "build_queued"
    assert result["queued"] == 2
