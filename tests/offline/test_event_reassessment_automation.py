import json

import pytest

from sempervigil import event_composition, event_composition_audit
from sempervigil import event_composition_jobs, event_composition_repair_jobs
from sempervigil import event_ledger
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


def test_repaired_composition_lineage_comes_from_successful_job_result():
    class RepairConn:
        def __init__(self, row):
            self.row = row
            self.params = None

        def execute(self, sql, params=()):
            assert "job_type='event_composition_repair'" in sql
            assert "status='succeeded'" in sql
            assert "repaired_composition_id" in sql
            self.params = params
            return _Result(self.row)

    found = RepairConn((1,))
    missing = RepairConn(None)
    assert automation._is_repaired_composition(found, "elc_repaired") is True
    assert found.params == ("elc_repaired",)
    assert automation._is_repaired_composition(missing, "elc_original") is False


def test_repaired_composition_can_be_scoped_to_current_repair_generation():
    class RepairConn:
        def execute(self, sql, params=()):
            assert "payload_json::jsonb->>'generation'=%s" in sql
            assert params == ("elc_repaired", "g" * 64)
            return _Result((1,))

    assert automation._is_repaired_composition(
        RepairConn(), "elc_repaired", generation="g" * 64
    ) is True


def test_detail_failures_are_filtered_before_holding(monkeypatch):
    class Conn:
        def execute(self, sql, params=()):
            assert "job_type='event_composition_audit'" in sql
            return _Result((json.dumps({"audit": {
                "generation_version": "a" * 64,
                "audits": [{"id": "C01", "verdict": "unsupported"}],
            }}),))

    composition = {"sections": {"impact": [{"text": "bad", "fact_ids": ["f1"]}]}}
    ledger_revision = {"ledger": {"facts": [{"fact_id": "f1"}]}}
    monkeypatch.setattr(event_composition_repair_jobs, "material",
                        lambda *_args: (composition, ledger_revision))
    monkeypatch.setattr(event_composition_audit, "request", lambda *_args: {
        "input": json.dumps({"items": [{"id": "C01", "section": "impact"}]})
    })
    monkeypatch.setattr(event_composition_audit, "filtered_record",
                        lambda *_args: {"status": "unreviewed"})
    monkeypatch.setattr(event_composition, "store_unreviewed",
                        lambda *_args: "elc_filtered")
    monkeypatch.setattr(event_composition, "review",
                        lambda *_args, **_kwargs: {"status": "accepted"})

    result = automation._filter_detail_failures(Conn(), "elc_repaired")
    assert result == {
        "composition_id": "elc_filtered", "application": {"status": "accepted"},
    }
    assert automation._filter_repaired_detail_failures is automation._filter_detail_failures


def test_repaired_derivative_resolves_successful_repair_result():
    repaired = ("elc_repaired", "unreviewed", None, "generation")

    class RepairConn:
        def execute(self, sql, params=()):
            if "result_json FROM jobs" in sql:
                assert params == ("elc_original",)
                return _Result(({"repaired_composition_id": "elc_repaired"},))
            assert "FROM event_ledger_compositions" in sql
            assert params == ("elc_repaired",)
            return _Result(repaired)

    assert automation._repaired_derivative(RepairConn(), "elc_original") == repaired


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
    monkeypatch.setattr(automation, "_resume_detail_filter_hold", lambda _conn: None)
    monkeypatch.setattr(automation, "_resume_transient_composition_hold", lambda _conn: None)
    monkeypatch.setattr(automation, "advance", advance)

    assert automation.tick(TickConn()) == [
        {"status": "pending", "event_id": "evt_waiting"},
        {"status": "queued", "event_id": "evt_advanced"},
    ]
    assert calls == ["evt_waiting", "evt_advanced"]


def test_tick_prioritizes_one_viable_transient_composition_recovery(monkeypatch):
    recovery = {"status": "queued", "event_id": "evt_recover",
                "job_id": "job_recover", "action": "composition_transient_recovery"}
    monkeypatch.setattr(automation, "enabled", lambda: True)
    monkeypatch.setattr(automation, "_resume_detail_filter_hold", lambda _conn: None)
    monkeypatch.setattr(automation, "_resume_transient_composition_hold",
                        lambda _conn: recovery)
    monkeypatch.setattr(automation, "advance",
                        lambda *_args: pytest.fail("active cases must wait"))

    assert automation.tick(object()) == [recovery]


def test_tick_prioritizes_detail_filter_recovery(monkeypatch):
    recovery = {"status": "accepted", "event_id": "evt_recover",
                "action": "composition_detail_filter_recovery"}
    monkeypatch.setattr(automation, "enabled", lambda: True)
    monkeypatch.setattr(automation, "_resume_detail_filter_hold",
                        lambda _conn: recovery)
    monkeypatch.setattr(automation, "_resume_transient_composition_hold",
                        lambda _conn: pytest.fail("detail recovery must run first"))

    assert automation.tick(object()) == [recovery]


def test_detail_filter_recovery_reaudits_obsolete_decision(monkeypatch):
    class Conn:
        def __init__(self):
            self.updated = False

        def execute(self, sql, params=()):
            if "SELECT c.event_id,x.composition_id" in sql:
                return _Result(("evt_recover", "elc_held"))
            if "UPDATE event_reassessment_cases" in sql:
                self.updated = True
                return _Result(None)
            raise AssertionError(sql)

        def commit(self):
            pass

    conn = Conn()
    monkeypatch.setattr(
        automation,
        "_filter_detail_failures",
        lambda *_args: (_ for _ in ()).throw(
            ValueError("event_composition_filter_audit_invalid")
        ),
    )
    from sempervigil import event_composition_audit_jobs
    monkeypatch.setattr(event_composition_audit_jobs, "submit",
                        lambda *_args: "job_reaudit")
    monkeypatch.setattr(automation, "_job_state",
                        lambda *_args: ("queued", ""))

    assert automation._resume_detail_filter_hold(conn) == {
        "status": "queued", "event_id": "evt_recover",
        "job_id": "job_reaudit", "action": "composition_reaudit_recovery",
    }
    assert conn.updated is True


def test_stale_proposed_ledger_is_rejected_for_deterministic_rebuild(monkeypatch):
    decisions = []
    monkeypatch.setattr(event_ledger, "_lineage_current", lambda _conn, _revision: False)
    monkeypatch.setattr(
        event_ledger,
        "review",
        lambda _conn, revision, decision, **kwargs: decisions.append(
            (revision, decision, kwargs)
        ) or {"status": "rejected"},
    )

    result = automation._advance_proposed_ledger(object(), "evt_test", "elr_stale")

    assert result["action"] == "stale_ledger_rejected"
    assert decisions == [("elr_stale", "reject", {
        "reason": "superseded by Event-scoped fact selection",
        "reviewer": "policy:event-reassessment-automation-v1",
    })]


def test_next_additive_candidate_ignores_sources_already_in_accepted_ledger():
    ledger = {"sources": [{"candidate_id": "ic_existing"}]}
    candidates = [
        ("ic_existing", "enrolled", "[\"f1\"]", "https://one.test", "{}"),
        ("ic_held", "held", None, "https://two.test", None),
        ("ic_new", "enrolled", "[\"f2\"]", "https://three.test", "{}"),
    ]

    assert automation._next_additive_candidate(ledger, candidates) == "ic_new"


def test_next_additive_candidate_requires_curated_fact_sections():
    candidates = [
        ("ic_unselected", "enrolled", None, "https://one.test", None),
        ("ic_unassigned", "enrolled", "[\"f1\"]", "https://two.test", None),
    ]

    assert automation._next_additive_candidate({"sources": []}, candidates) is None
