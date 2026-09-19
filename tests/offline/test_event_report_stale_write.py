import json
import logging
import sqlite3
from unittest.mock import Mock

import pytest

from sempervigil import storage, worker

pytestmark = pytest.mark.offline


@pytest.fixture
def database(monkeypatch):
    raw = sqlite3.connect(":memory:")
    raw.execute("CREATE TABLE events(id TEXT PRIMARY KEY, meta_json TEXT, updated_at TEXT)")
    raw.execute("INSERT INTO events VALUES ('event', ?, 'before')", (json.dumps({
        "report": {"overview": "Previous report"}, "unrelated": "preserve"}),))
    raw.commit()
    class Connection:
        before_update = None
        def execute(self, sql, params=()):
            if sql.lstrip().startswith("UPDATE events") and self.before_update:
                callback, self.before_update = self.before_update, None
                callback()
            return raw.execute(sql.replace("%s", "?"), params)
        def commit(self): raw.commit()
    monkeypatch.setattr(storage, "_table_exists", lambda *a: True)
    monkeypatch.setattr(storage, "utc_now_iso", lambda: "after")
    yield raw, Connection()
    raw.close()


def test_success_preserves_metadata_and_profile_fields(database):
    raw, conn = database
    assert storage.update_event_report(conn, "event", {"overview": "New"},
        expected_updated_at="before", profile_id="profile", model_id="model")
    meta, stamp = raw.execute("SELECT meta_json,updated_at FROM events").fetchone()
    value = json.loads(meta)
    assert value["report"] == {"overview": "New"}
    assert value["unrelated"] == "preserve" and stamp == "after"
    assert value["report_profile_id"] == "profile" and value["report_model_id"] == "model"


def test_version_changed_during_generation_preserves_old_report(database):
    raw, conn = database
    before = raw.execute("SELECT * FROM events").fetchone()
    assert not storage.update_event_report(conn, "event", {"overview": "New"}, expected_updated_at="older")
    assert raw.execute("SELECT * FROM events").fetchone() == before


@pytest.mark.parametrize("change", ["metadata", "timestamp", "delete"])
def test_race_after_read_cannot_overwrite_other_writer(database, change):
    raw, conn = database
    sql = {"metadata": "UPDATE events SET meta_json='{}'",
           "timestamp": "UPDATE events SET updated_at='newer'",
           "delete": "DELETE FROM events"}[change]
    conn.before_update = lambda: raw.execute(sql)
    assert not storage.update_event_report(conn, "event", {"overview": "New"}, expected_updated_at="before")
    row = raw.execute("SELECT meta_json,updated_at FROM events").fetchone()
    if change == "metadata": assert row == ("{}", "before")
    if change == "timestamp": assert json.loads(row[0])["report"]["overview"] == "Previous report" and row[1] == "newer"
    if change == "delete": assert row is None


def test_null_metadata_and_existing_call_signature_remain_supported(database):
    raw, conn = database
    raw.execute("UPDATE events SET meta_json=NULL")
    assert storage.update_event_report(conn, "event", {"overview": "New"})
    assert json.loads(raw.execute("SELECT meta_json FROM events").fetchone()[0])["report"] == {"overview": "New"}


@pytest.mark.parametrize("version", [None, "", 123])
def test_missing_worker_version_skips_before_inference(monkeypatch, version):
    monkeypatch.setattr(worker, "get_event", lambda *a: {"id": "event", "updated_at": version})
    infer = Mock(side_effect=AssertionError("must not infer"))
    monkeypatch.setattr(worker, "run_profile", infer)
    result = worker._handle_event_report_llm(None, None, {"event_id": "event"}, logging.getLogger())
    assert result["reason"] == "missing_event_version"
    infer.assert_not_called()


@pytest.mark.parametrize("updated", [True, False])
def test_worker_passes_starting_version_and_marks_only_success_dirty(monkeypatch, updated):
    monkeypatch.setattr(worker, "get_event", lambda *a: {
        "id": "event", "updated_at": "starting-version", "publish_state": "published", "lifecycle": "confirmed"})
    monkeypatch.setattr(worker, "list_event_web_sources", lambda *a, **k: [])
    monkeypatch.setattr(worker, "_event_report_profile", lambda *a: ({"id": "profile"}, "ok"))
    monkeypatch.setattr(worker, "_build_event_report_input", lambda *a: "input")
    monkeypatch.setattr(worker, "_parse_event_report_output", lambda *a, **k: {"overview": "New"})
    infer = Mock(return_value={})
    monkeypatch.setattr(worker, "run_profile", infer)
    update = Mock(return_value=updated)
    monkeypatch.setattr(worker, "update_event_report", update)
    dirty = Mock()
    monkeypatch.setattr(worker, "mark_build_dirty", dirty)
    result = worker._handle_event_report_llm(None, None, {"event_id": "event"}, logging.getLogger())
    assert update.call_args.kwargs["expected_updated_at"] == "starting-version"
    assert result["status"] == ("ok" if updated else "skipped")
    assert dirty.call_count == int(updated)
    if not updated: assert result["reason"] == "stale_or_unavailable_event"
    infer.assert_called_once()
