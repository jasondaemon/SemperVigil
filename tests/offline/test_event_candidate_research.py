from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from sempervigil import event_candidate_research as research
from sempervigil import worker

pytestmark = pytest.mark.offline


NOW = datetime(2026, 9, 30, 12, tzinfo=timezone.utc)


def test_bounded_research_admits_one_unsearched_candidate(monkeypatch):
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [("evt_bad",), ("evt_one",)]
    marker = []
    enqueue = Mock(return_value="job_one")
    monkeypatch.setattr(research, "get_events_settings", lambda _: {
        "enabled": True, "enrich_min_articles": 2,
        "enrich_min_articles_max_results": 6,
    })
    monkeypatch.setattr(research, "get_setting", lambda *_: None)
    monkeypatch.setattr(research, "set_setting", lambda *args: marker.append(args))
    monkeypatch.setattr(research, "enqueue_job", enqueue)
    monkeypatch.setattr(research, "get_event", lambda _, event_id: {
        "entity": "not applicable" if event_id == "evt_bad" else "Example Corp",
        "meta": {"anchor_version": worker.EVENT_ANCHOR_VERSION},
    })

    assert research.tick(conn, now=NOW) == {
        "status": "research_queued", "event_id": "evt_one", "job_id": "job_one",
    }
    assert conn.execute.call_args.args[1] == (
        (NOW - timedelta(days=14)).isoformat(), worker.EVENT_ANCHOR_VERSION,
        research.JOB_TYPE,
    )
    assert "NOT EXISTS" in conn.execute.call_args.args[0]
    assert "COUNT(DISTINCT" in conn.execute.call_args.args[0]
    enqueue.assert_called_once_with(conn, research.JOB_TYPE, {
        "event_id": "evt_one", "max_results": 6, "replace_existing": False,
    }, dedupe=True)
    assert marker[0][1] == research.LAST_SCAN_KEY


def test_scan_interval_and_explicit_opt_out(monkeypatch):
    conn = Mock()
    monkeypatch.setattr(research, "get_events_settings", lambda _: {
        "enabled": True, "enrich_min_articles": 2,
    })
    monkeypatch.setattr(research, "get_setting", lambda *_: (NOW - timedelta(minutes=1)).isoformat())
    assert research.tick(conn, now=NOW)["status"] == "unchanged"
    conn.execute.assert_not_called()
    monkeypatch.setattr(research, "get_events_settings", lambda _: {
        "enabled": True, "enrich_min_articles": 0,
    })
    assert research.tick(conn, now=NOW) == {"status": "disabled"}
