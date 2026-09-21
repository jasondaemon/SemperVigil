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
