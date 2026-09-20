from unittest.mock import Mock

import pytest

from sempervigil import event_curation_status as status

pytestmark = pytest.mark.offline


def _result(row=None, rows=None):
    result = Mock()
    result.fetchone.return_value = row
    result.fetchall.return_value = rows or []
    return result


def test_legacy_event_has_no_managed_pipeline_queries():
    conn = Mock()
    conn.execute.return_value = _result(("legacy:event",))
    assert status.read(conn, "evt_legacy") == {
        "event_id": "evt_legacy", "managed": False, "sources": [],
        "next_step": "legacy_event",
    }
    assert conn.execute.call_count == 1


def test_managed_event_reports_each_guarded_stage():
    conn = Mock()
    conn.execute.side_effect = [
        _result(("event-ledger:eld_test",)),
        _result(("public-revision", "2026-09-20T12:00:00+00:00")),
        _result(("research-job", "succeeded", "requested", "finished")),
        _result(rows=[(1, "First", "https://one.test/a", "feed"),
                      (2, "Second", "https://two.test/b", "web_enrich")]),
        _result(("aer_one", "accepted", "created")),
        _result(("ic_one", "enrolled")),
        _result(("elr_one", "accepted")),
        _result(("aer_two", "unreviewed", "created")),
        _result(None),
        _result(("elr_current", "accepted", "created")),
        _result(("elc_current", "unreviewed", "created")),
    ]
    result = status.read(conn, "evt_managed")
    assert result["managed"] is True
    assert result["public_revision_id"] == "public-revision"
    assert result["latest_research"]["status"] == "succeeded"
    assert [source["next_step"] for source in result["sources"]] == [
        "included_in_ledger", "review_evidence",
    ]
    assert result["pending_counts"] == {"included_in_ledger": 1, "review_evidence": 1}


@pytest.mark.parametrize(("evidence", "candidate", "ledger", "expected"), [
    (None, None, None, "extract_evidence"),
    ("accepted", None, None, "project_candidate"),
    ("accepted", "suggested", None, "review_candidate"),
    ("accepted", "enrolled", None, "add_to_ledger"),
    ("accepted", "enrolled", "proposed", "review_ledger"),
    ("rejected", None, None, "evidence_closed"),
])
def test_next_step_is_deterministic(evidence, candidate, ledger, expected):
    assert status._next_step(evidence, candidate, ledger) == expected
