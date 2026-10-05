import json

import pytest

from sempervigil import event_composition
from sempervigil import event_published_composition_upgrade as upgrade
from unittest.mock import Mock

pytestmark = pytest.mark.offline


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


def _bundle(workflow: str, *, composition_id: str = "elc_old",
            generation_version: str | None = None) -> str:
    return json.dumps({
        "workflow": upgrade.PUBLIC_WORKFLOW,
        "event_id": "evt_test",
        "ledger_revision_id": "elr_revision",
        "composition_id": composition_id,
        "composition": {"workflow": workflow,
                        "generation_version": generation_version},
    })


def test_candidates_select_legacy_and_stale_current_compositions(monkeypatch):
    monkeypatch.setattr(
        "sempervigil.event_composition_jobs.configuration",
        lambda _conn: (None, None, "generation-current"),
    )
    class Conn:
        def execute(self, sql, params=()):
            assert "FROM event_public_pointers" in sql
            assert "l.status='accepted'" in sql
            return _Rows([
                ("evt_old", "a" * 64, "2026-09-20", _bundle("event-ledger-composition-v4")),
                ("evt_stale", "b" * 64, "2026-09-21",
                 _bundle(event_composition.WORKFLOW,
                         generation_version="generation-old")),
                ("evt_current", "d" * 64, "2026-09-22",
                 _bundle(event_composition.WORKFLOW,
                         generation_version="generation-current")),
                ("evt_legacy", "c" * 64, "2026-09-19", json.dumps({"workflow": "other"})),
            ])

    assert upgrade.candidates(Conn()) == [{
        "event_id": "evt_old",
        "revision_id": "a" * 64,
        "updated_at": "2026-09-20",
        "ledger_revision_id": "elr_revision",
        "published_composition_id": "elc_old",
        "published_workflow": "event-ledger-composition-v4",
    }, {
        "event_id": "evt_stale",
        "revision_id": "b" * 64,
        "updated_at": "2026-09-21",
        "ledger_revision_id": "elr_revision",
        "published_composition_id": "elc_old",
        "published_workflow": event_composition.WORKFLOW,
    }]


def test_advance_does_not_publish_accepted_stale_generation(monkeypatch):
    candidate = {
        "event_id": "evt_test", "revision_id": "r" * 64,
        "ledger_revision_id": "elr_revision",
    }
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = (candidate["revision_id"],)
    monkeypatch.setattr(upgrade, "_compositions", lambda *_: [
        ("elc_stale", "accepted", "policy:event-composition-audit-v1",
         "generation-old", "{}", "2026-09-20"),
    ])
    monkeypatch.setattr(upgrade, "_active_job", lambda *_: None)
    monkeypatch.setattr(
        "sempervigil.event_composition_jobs.configuration",
        lambda _conn: (None, None, "generation-current"),
    )
    submit = Mock(return_value="job_new")
    monkeypatch.setattr("sempervigil.event_composition_jobs.submit", submit)
    monkeypatch.setattr(upgrade, "_job_state", lambda *_: ("queued", ""))

    result = upgrade.advance(conn, candidate)

    assert result["job_id"] == "job_new"
    submit.assert_called_once_with(conn, "elr_revision")


def test_advance_publishes_only_accepted_current_generation(monkeypatch):
    candidate = {
        "event_id": "evt_test", "revision_id": "r" * 64,
        "ledger_revision_id": "elr_revision",
    }
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = (candidate["revision_id"],)
    monkeypatch.setattr(upgrade, "_compositions", lambda *_: [
        ("elc_stale", "accepted", "policy:event-composition-audit-v1",
         "generation-old", "{}", "2026-09-20"),
        ("elc_current", "accepted", "policy:event-composition-audit-v1",
         "generation-current", "{}", "2026-09-21"),
    ])
    monkeypatch.setattr(upgrade, "_active_job", lambda *_: None)
    monkeypatch.setattr(
        "sempervigil.event_composition_jobs.configuration",
        lambda _conn: (None, None, "generation-current"),
    )
    submit = Mock(return_value={"job_id": "job_publish", "status": "queued"})
    monkeypatch.setattr(
        "sempervigil.event_composition_publication.submit_automated", submit)
    monkeypatch.setattr(upgrade, "_job_state", lambda *_: ("queued", ""))

    result = upgrade.advance(conn, candidate)

    assert result["job_id"] == "job_publish"
    submit.assert_called_once_with(conn, "elc_current")


def test_tick_stops_after_one_material_upgrade(monkeypatch):
    rows = [{"event_id": "evt_held"}, {"event_id": "evt_queued"},
            {"event_id": "evt_not_reached"}]
    calls = []

    def advance(_conn, candidate):
        calls.append(candidate["event_id"])
        if candidate["event_id"] == "evt_held":
            return {"status": "held", "event_id": candidate["event_id"]}
        return {"status": "queued", "event_id": candidate["event_id"]}

    monkeypatch.setattr(upgrade, "enabled", lambda: True)
    monkeypatch.setattr(upgrade, "candidates", lambda _conn: rows)
    monkeypatch.setattr(upgrade, "advance", advance)
    monkeypatch.setattr(upgrade, "_attempt_version", lambda *_: "version")
    monkeypatch.setattr(upgrade, "get_setting", lambda *_: {})
    monkeypatch.setattr(upgrade, "set_setting", lambda *_: None)

    assert upgrade.tick(Mock()) == [
        {"status": "held", "event_id": "evt_held"},
        {"status": "queued", "event_id": "evt_queued"},
    ]
    assert calls == ["evt_held", "evt_queued"]


def test_tick_isolates_stale_legacy_upgrade_candidate(monkeypatch):
    class Conn:
        rolled_back = False

        def rollback(self):
            self.rolled_back = True

        def commit(self):
            pass

    conn = Conn()
    rows = [{"event_id": "evt_stale"}, {"event_id": "evt_next"}]
    monkeypatch.setattr(upgrade, "enabled", lambda: True)
    monkeypatch.setattr(upgrade, "candidates", lambda _conn: rows)
    monkeypatch.setattr(upgrade, "_attempt_version", lambda *_: "version")
    monkeypatch.setattr(upgrade, "get_setting", lambda *_: {})
    monkeypatch.setattr(upgrade, "set_setting", lambda *_: None)

    def advance(_conn, candidate):
        if candidate["event_id"] == "evt_stale":
            raise ValueError("event_ledger_revision_missing")
        return {"status": "queued", "event_id": candidate["event_id"]}

    monkeypatch.setattr(upgrade, "advance", advance)
    assert upgrade.tick(conn) == [
        {"status": "held", "event_id": "evt_stale",
         "reason": "event_ledger_revision_missing", "workflow": upgrade.WORKFLOW},
        {"status": "queued", "event_id": "evt_next"},
    ]
    assert conn.rolled_back is True


@pytest.mark.parametrize("raises", [False, True])
def test_unchanged_hold_is_not_retried_but_changed_material_is(monkeypatch, raises):
    conn = Mock()
    candidate = {"event_id": "evt_test"}
    holds = {}
    version = ["first"]
    advance = Mock(side_effect=ValueError("invalid_audit") if raises else None,
                   return_value={"status": "held", "event_id": "evt_test", "reason": "invalid_audit"})
    monkeypatch.setattr(upgrade, "enabled", lambda: True)
    monkeypatch.setattr(upgrade, "candidates", lambda _: [candidate])
    monkeypatch.setattr(upgrade, "_attempt_version", lambda *_: version[0])
    monkeypatch.setattr(upgrade, "get_setting", lambda _, key, default: holds.get(key, default))
    monkeypatch.setattr(upgrade, "set_setting", lambda _, key, value: holds.update({key: value}))
    monkeypatch.setattr(upgrade, "advance", advance)
    assert upgrade.tick(conn)[0]["status"] == "held"
    assert upgrade.tick(conn)[0]["reason"] == "held_inputs_unchanged"
    assert advance.call_count == 1
    assert conn.rollback.call_count == int(raises)
    version[0] = "new-audit"
    assert upgrade.tick(conn)[0]["status"] == "held"
    assert advance.call_count == 2


def test_attempt_version_changes_with_audit_and_policy(monkeypatch):
    from sempervigil import event_composition_audit
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [("id", "digest")]
    candidate = {"event_id": "evt_test", "ledger_revision_id": "elr_test"}
    first = upgrade._attempt_version(conn, candidate)
    conn.execute.return_value.fetchall.return_value = [("id", "changed")]
    second = upgrade._attempt_version(conn, candidate)
    assert first != second
    monkeypatch.setattr(event_composition_audit, "SYSTEM_PROMPT", "new policy")
    assert upgrade._attempt_version(conn, candidate) != second


def test_upgrade_enablement_is_explicit(monkeypatch):
    monkeypatch.delenv("SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED", raising=False)
    assert upgrade.enabled() is False
    monkeypatch.setenv("SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED", "1")
    assert upgrade.enabled() is True
    monkeypatch.setenv("SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED", "yes")
    with pytest.raises(ValueError, match="invalid_event_public_composition_upgrade_enablement"):
        upgrade.enabled()
