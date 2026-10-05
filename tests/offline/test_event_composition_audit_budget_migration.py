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


def test_interrupted_requeue_requires_failed_started_job_without_output(monkeypatch):
    monkeypatch.setattr(migrations_pg, "utc_now_iso", lambda: "2026-10-05T00:00:00+00:00")
    conn = _RecordingConnection()

    migrations_pg._migrate_event_composition_interrupted_requeue(conn)

    sql, params = conn.calls[0]
    assert "decision_reason='composition failed: event_composition_invalid_or_replayed_job'" in sql
    assert "j.error='event_composition_invalid_or_replayed_job'" in sql
    assert "j.result_json::jsonb->>'status'='started'" in sql
    assert "NOT (j.result_json::jsonb ?| ARRAY['raw','output_chars','composition_id'])" in sql
    assert params == ("2026-10-05T00:00:00+00:00",)


def test_final_recovery_requires_exact_interruption_hold_and_no_output(monkeypatch):
    monkeypatch.setattr(migrations_pg, "utc_now_iso", lambda: "2026-10-05T00:00:00+00:00")
    conn = _RecordingConnection()

    migrations_pg._migrate_event_composition_final_recovery(conn)

    sql, params = conn.calls[0]
    assert "decision_reason='composition failed: event_composition_interrupted_after_start'" in sql
    assert "j.error='event_composition_interrupted_after_start'" in sql
    assert "NOT (j.result_json::jsonb ?| ARRAY['raw','output_chars','composition_id'])" in sql
    assert params == ("2026-10-05T00:00:00+00:00",)


def test_zero_output_recovery_requires_exact_failed_repair_and_successful_empty_run(monkeypatch):
    monkeypatch.setattr(migrations_pg, "utc_now_iso", lambda: "2026-10-05T00:00:00+00:00")
    conn = _RecordingConnection()

    migrations_pg._migrate_event_composition_zero_output_recovery(conn)

    sql, params = conn.calls[0]
    assert "decision_reason='composition repair failed: input_size'" in sql
    assert "j.job_type='event_composition_repair' AND j.status='failed'" in sql
    assert "j.error='input_size'" in sql
    assert "j.result_json::jsonb->>'status'='started'" in sql
    assert "NOT (j.result_json::jsonb ?| ARRAY['raw','output_chars','repaired_composition_id'])" in sql
    assert "l.ok=1 AND l.output_chars=0 AND l.error IS NULL" in sql
    assert params == ("2026-10-05T00:00:00+00:00",)


def test_fallback_overview_recovery_requires_exact_new_hold_and_same_empty_run(monkeypatch):
    monkeypatch.setattr(migrations_pg, "utc_now_iso", lambda: "2026-10-05T00:00:00+00:00")
    conn = _RecordingConnection()

    migrations_pg._migrate_event_composition_fallback_overview_recovery(conn)

    sql, params = conn.calls[0]
    assert "decision_reason='event_composition_invalid_shape'" in sql
    assert "j.job_type='event_composition_repair' AND j.status='failed'" in sql
    assert "j.error='input_size'" in sql
    assert "l.ok=1 AND l.output_chars=0 AND l.error IS NULL" in sql
    assert params == ("2026-10-05T00:00:00+00:00",)


def test_derivative_identity_recovery_requires_exact_conflict_and_same_empty_run(monkeypatch):
    monkeypatch.setattr(migrations_pg, "utc_now_iso", lambda: "2026-10-05T00:00:00+00:00")
    conn = _RecordingConnection()

    migrations_pg._migrate_event_composition_derivative_identity_recovery(conn)

    sql, params = conn.calls[0]
    assert "decision_reason='event_composition_storage_conflict'" in sql
    assert "j.job_type='event_composition_repair' AND j.status='failed'" in sql
    assert "j.error='input_size'" in sql
    assert "l.ok=1 AND l.output_chars=0 AND l.error IS NULL" in sql
    assert params == ("2026-10-05T00:00:00+00:00",)
