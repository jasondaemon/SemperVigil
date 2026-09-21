import json

import pytest

from sempervigil import event_composition
from sempervigil import event_published_composition_upgrade as upgrade

pytestmark = pytest.mark.offline


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


def _bundle(workflow: str, *, composition_id: str = "elc_old") -> str:
    return json.dumps({
        "workflow": upgrade.PUBLIC_WORKFLOW,
        "event_id": "evt_test",
        "ledger_revision_id": "elr_revision",
        "composition_id": composition_id,
        "composition": {"workflow": workflow},
    })


def test_candidates_select_only_published_legacy_compositions():
    class Conn:
        def execute(self, sql, params=()):
            assert "FROM event_public_pointers" in sql
            return _Rows([
                ("evt_old", "a" * 64, "2026-09-20", _bundle("event-ledger-composition-v4")),
                ("evt_current", "b" * 64, "2026-09-21", _bundle(event_composition.WORKFLOW)),
                ("evt_legacy", "c" * 64, "2026-09-19", json.dumps({"workflow": "other"})),
            ])

    assert upgrade.candidates(Conn()) == [{
        "event_id": "evt_old",
        "revision_id": "a" * 64,
        "updated_at": "2026-09-20",
        "ledger_revision_id": "elr_revision",
        "published_composition_id": "elc_old",
        "published_workflow": "event-ledger-composition-v4",
    }]


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

    assert upgrade.tick(object()) == [
        {"status": "held", "event_id": "evt_held"},
        {"status": "queued", "event_id": "evt_queued"},
    ]
    assert calls == ["evt_held", "evt_queued"]


def test_upgrade_enablement_is_explicit(monkeypatch):
    monkeypatch.delenv("SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED", raising=False)
    assert upgrade.enabled() is False
    monkeypatch.setenv("SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED", "1")
    assert upgrade.enabled() is True
    monkeypatch.setenv("SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED", "yes")
    with pytest.raises(ValueError, match="invalid_event_public_composition_upgrade_enablement"):
        upgrade.enabled()
