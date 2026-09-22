import pytest

from sempervigil import event_reassessment

pytestmark = pytest.mark.offline


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _SnapshotConn:
    def execute(self, sql, params=()):
        if "FROM events e WHERE e.id" in sql:
            return _Result([(
                "evt_old", "Legacy title", "event:breach:example", "published",
                "2026-02-01T00:00:00Z", "evt_old", "2026-02-02T00:00:00Z",
                "active", "confirmed", False,
            )])
        if "FROM event_articles" in sql:
            return _Result([(7, "Source title", "https://example.test/report", "Full retained text")])
        raise AssertionError(sql)


def test_snapshot_uses_retained_sources_but_never_legacy_narrative():
    result = event_reassessment.snapshot(_SnapshotConn(), "evt_old")

    assert result["event"]["event_id"] == "evt_old"
    assert result["articles"][0]["source_version"]
    assert result["legacy_narrative_used"] is False
    assert "summary" not in result["event"]


def test_reassessment_title_must_come_from_enrolled_evidence_candidate():
    rows = [
        ("ic_one", 7, "https://one.test/report", "Source-backed incident title"),
        ("ic_two", 8, "https://two.test/report", "Independent report title"),
    ]

    assert event_reassessment._evidence_title(
        " Source-backed incident title ", rows
    ) == "Source-backed incident title"
    with pytest.raises(ValueError, match="event_reassessment_evidence_title_required"):
        event_reassessment._evidence_title("Legacy unsupported title", rows)


def test_snapshot_accepts_managed_event_for_living_updates():
    conn = _SnapshotConn()
    original = conn.execute

    def execute(sql, params=()):
        result = original(sql, params)
        if "FROM events e WHERE e.id" in sql:
            row = list(result.fetchone())
            row[2] = "event-ledger:eld_managed"
            return _Result([tuple(row)])
        return result

    conn.execute = execute
    result = event_reassessment.snapshot(conn, "evt_managed")
    assert result["event"]["event_key"] == "event-ledger:eld_managed"


def test_mutating_operations_require_exact_confirmation_before_database_access():
    class NoDatabase:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("database must not be accessed")

    with pytest.raises(ValueError, match="event_reassessment_confirmation_required"):
        event_reassessment.start_cohort(NoDatabase(), confirmation="wrong", created_by="test")
    with pytest.raises(ValueError, match="event_reassessment_evidence_confirmation_required"):
        event_reassessment.queue_evidence(NoDatabase(), "evt_old", confirmation="wrong")
    with pytest.raises(ValueError, match="event_reassessment_ledger_confirmation_required"):
        event_reassessment.propose_ledger(
            NoDatabase(), "evt_old", confirmation="wrong", title="Source title"
        )
