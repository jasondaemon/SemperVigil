import pytest

from sempervigil import migrations_pg

pytestmark = pytest.mark.offline


class _RecordingConnection:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((" ".join(sql.split()), params))


def test_budget_requeue_targets_only_exact_historical_hold(monkeypatch):
    monkeypatch.setattr(migrations_pg, "utc_now_iso", lambda: "2026-10-05T00:00:00+00:00")
    conn = _RecordingConnection()

    migrations_pg._migrate_event_composition_audit_budget_requeue(conn)

    assert conn.calls == [(
        "UPDATE event_reassessment_cases SET status='active',decision_reason=NULL,updated_at=%s "
        "WHERE status='held' AND decision_reason='event_composition_audit_input_over_budget'",
        ("2026-10-05T00:00:00+00:00",),
    )]
