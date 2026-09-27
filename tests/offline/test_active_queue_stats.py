from unittest.mock import Mock

import pytest

from sempervigil import storage

pytestmark = pytest.mark.offline


def test_queue_stats_only_counts_active_jobs(monkeypatch):
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [
        ("build", 1, 2, "2026-09-27T00:00:00+00:00"),
    ]
    monkeypatch.setattr(storage, "_table_exists", lambda _conn, table: table == "jobs")

    rows = storage.get_queue_stats(conn)

    sql = conn.execute.call_args.args[0]
    assert "WHERE status IN ('queued', 'running')" in sql
    assert rows == [{
        "queue_name": "build",
        "queued": 1,
        "running": 2,
        "oldest_requested_at": "2026-09-27T00:00:00+00:00",
    }]
