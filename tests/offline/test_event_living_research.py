from datetime import datetime, timedelta, timezone
import json
from unittest.mock import Mock

import pytest

from sempervigil import event_living_research as research

pytestmark = pytest.mark.offline


def test_default_disabled_never_reads_database(monkeypatch):
    monkeypatch.delenv("SV_EVENT_RESEARCH_EVENTS", raising=False)
    conn = Mock()
    assert research.tick(conn) == []
    conn.execute.assert_not_called()


@pytest.mark.parametrize("value", ["{}", "null", '["event"]', '[1]',
    json.dumps([f"evt_{number}" for number in range(11)]),
    '["evt_same","evt_same"]', "x" * 2049])
def test_invalid_enrollments_rejected(monkeypatch, value):
    monkeypatch.setenv("SV_EVENT_RESEARCH_EVENTS", value)
    with pytest.raises(ValueError):
        research.enrollments()


def test_enrollments_are_stable(monkeypatch):
    monkeypatch.setenv("SV_EVENT_RESEARCH_EVENTS", '["evt_two","evt_one"]')
    assert research.enrollments() == ["evt_one", "evt_two"]


@pytest.mark.parametrize("value", ["no", "3599", "604801"])
def test_invalid_interval_rejected(monkeypatch, value):
    monkeypatch.setenv("SV_EVENT_RESEARCH_INTERVAL_SECONDS", value)
    with pytest.raises(ValueError):
        research.interval_seconds()


@pytest.mark.parametrize("value", ["no", "1", "21"])
def test_invalid_result_limit_rejected(monkeypatch, value):
    monkeypatch.setenv("SV_EVENT_RESEARCH_MAX_RESULTS", value)
    with pytest.raises(ValueError):
        research.max_results()


def _connection(latest=None, available=(1,)):
    conn = Mock()
    conn.execute.side_effect = [Mock(fetchone=Mock(return_value=available)),
                                Mock(fetchone=Mock(return_value=latest))]
    return conn


def test_due_published_event_queues_existing_research_job(monkeypatch):
    now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    conn = _connection(("succeeded", now - timedelta(days=2), "old-job"))
    enqueue = Mock(return_value="new-job")
    monkeypatch.setattr(research, "enqueue_job", enqueue)
    result = research.advance(conn, "evt_waterplum", now=now)
    assert result == {"status": "research_queued", "event_id": "evt_waterplum",
                      "job_id": "new-job"}
    enqueue.assert_called_once_with(conn, "enrich_event_from_web", {
        "event_id": "evt_waterplum", "max_results": 12, "replace_existing": False,
    }, dedupe=True)


def test_recent_and_pending_research_are_not_duplicated(monkeypatch):
    now = datetime(2026, 9, 20, 12, tzinfo=timezone.utc)
    enqueue = Mock(side_effect=AssertionError("must not enqueue"))
    monkeypatch.setattr(research, "enqueue_job", enqueue)
    recent = research.advance(
        _connection(("succeeded", now - timedelta(hours=2), "recent-job")),
        "evt_waterplum", now=now)
    assert recent["status"] == "unchanged" and recent["next_in_seconds"] == 79200
    pending = research.advance(
        _connection(("running", now - timedelta(days=2), "running-job")),
        "evt_waterplum", now=now)
    assert pending == {"status": "research_pending", "event_id": "evt_waterplum",
                       "job_id": "running-job"}


def test_unpublished_event_is_held_without_job(monkeypatch):
    enqueue = Mock(side_effect=AssertionError("must not enqueue"))
    monkeypatch.setattr(research, "enqueue_job", enqueue)
    result = research.advance(_connection(available=None), "evt_missing")
    assert result == {"status": "held", "event_id": "evt_missing",
                      "reason": "published_event_unavailable"}


def test_tick_admits_only_one_research_job(monkeypatch):
    monkeypatch.setenv("SV_EVENT_RESEARCH_EVENTS", '["evt_one","evt_two"]')
    advance = Mock(return_value={"status": "research_queued", "event_id": "evt_one",
                                 "job_id": "job"})
    monkeypatch.setattr(research, "advance", advance)
    assert research.tick(Mock()) == [{"status": "research_queued", "event_id": "evt_one",
                                      "job_id": "job"}]
    advance.assert_called_once()
