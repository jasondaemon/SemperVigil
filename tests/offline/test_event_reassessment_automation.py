import pytest

from sempervigil import event_composition_jobs
from sempervigil import event_reassessment_automation as automation

pytestmark = pytest.mark.offline


class _Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class _Conn:
    def __init__(self, row):
        self.row = row
        self.params = None

    def execute(self, sql, params=()):
        assert "generation_version=%s" in sql
        self.params = params
        return _Result(self.row)


def test_repairable_composition_skips_newer_legacy_derivative(monkeypatch):
    generation = "a" * 64
    original = ("elc_original", "held", "policy:event-composition-audit-v1", generation)
    conn = _Conn(original)
    monkeypatch.setattr(event_composition_jobs, "configuration",
                        lambda _conn: ({}, {}, generation))

    selected = automation._repairable_composition(
        conn,
        "elr_revision",
        ("elc_legacy", "held", "policy:event-composition-audit-v1", "b" * 64),
    )

    assert selected == original
    assert conn.params == ("elr_revision", generation)


def test_repairable_composition_reuses_current_generation_without_query(monkeypatch):
    generation = "a" * 64
    current = ("elc_current", "held", "policy:event-composition-audit-v1", generation)
    monkeypatch.setattr(event_composition_jobs, "configuration",
                        lambda _conn: ({}, {}, generation))

    assert automation._repairable_composition(object(), "elr_revision", current) == current


def test_tick_skips_waiting_case_but_stops_after_one_advancement(monkeypatch):
    class TickConn:
        def execute(self, sql, params=()):
            assert "FROM event_reassessment_cases c" in sql
            return type("Rows", (), {"fetchall": lambda _self: [
                ("evt_waiting",), ("evt_advanced",), ("evt_not_reached",),
            ]})()

    calls = []

    def advance(_conn, event_id):
        calls.append(event_id)
        if event_id == "evt_waiting":
            return {"status": "pending", "event_id": event_id}
        return {"status": "queued", "event_id": event_id}

    monkeypatch.setattr(automation, "enabled", lambda: True)
    monkeypatch.setattr(automation, "advance", advance)

    assert automation.tick(TickConn()) == [
        {"status": "pending", "event_id": "evt_waiting"},
        {"status": "queued", "event_id": "evt_advanced"},
    ]
    assert calls == ["evt_waiting", "evt_advanced"]
