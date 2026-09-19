import pytest

from sempervigil import event_projection as projection
from sempervigil.event_revision_store import source_version
from sempervigil.investigation import _version
from test_event_review import database, get_packet, resign
from test_event_scope import proposal

pytestmark = pytest.mark.offline


def inputs(database):
    packet = get_packet(database)
    scope = proposal(packet)
    text = packet["documents"][0]["text"]
    qualified = {"workflow": projection.QUALIFICATION, "event_id": "event",
                 "source_version": source_version(packet), "scope_version": scope["scope_version"],
                 "reviewer": {"kind": "policy", "id": "test-only-policy", "version": "a" * 64},
                 "quotes": [{"article_id": 1, "start": 0, "end": len(text), "quote": text}]}
    return packet, scope, qualified


def prepare(packet, scope, qualified, **kwargs):
    return projection.prepare(packet, scope, qualified,
                              trusted_qualification_ids=frozenset({_version(qualified)}), **kwargs)


def test_quote_projection_is_not_public_approval(database):
    packet, scope, qualified = inputs(database)
    result = prepare(packet, scope, qualified)
    assert result == prepare(packet, scope, qualified)
    assert result["public_eligible"] is False
    assert result["publication_status"] == "not_promoted"
    entry = result["entries"][0]
    assert entry["quote"] == packet["documents"][0]["text"]
    assert entry["feed_day"] == "2026-09-01" and entry["incident_date"] is None
    assert entry["origin_independence"] == "unknown"
    assert entry["assertion_role"] == "attributed_quotation"
    qualified["quotes"][0]["quote"] = "changed"
    qualified["reviewer"]["id"] = "changed"
    assert result["entries"][0] == entry and result["reviewer"]["id"] == "test-only-policy"


def test_default_trust_refuses_valid_hashed_record(database):
    packet, scope, qualified = inputs(database)
    with pytest.raises(ValueError, match="untrusted"):
        projection.prepare(packet, scope, qualified)


@pytest.mark.parametrize("fault", ["event", "source", "scope", "model", "reviewer", "version",
                                   "invented", "boolean", "unknown", "duplicate", "date", "empty",
                                   "prose", "predecessor"])
def test_even_trusted_records_must_pass_structural_guards(database, fault):
    packet, scope, q = inputs(database)
    kwargs = {}
    if fault == "event": q["event_id"] = "another"
    if fault == "source": q["source_version"] = "b" * 64
    if fault == "scope": q["scope_version"] = "b" * 64
    if fault == "model": q["reviewer"]["kind"] = "llm"
    if fault == "reviewer": q["reviewer"]["id"] = ""
    if fault == "version": q["reviewer"]["version"] = "unversioned"
    if fault == "invented": q["quotes"][0]["quote"] += " 10 million affected."
    if fault == "boolean": q["quotes"][0]["start"] = False
    if fault == "unknown": q["quotes"][0]["article_id"] = 100
    if fault == "duplicate": q["quotes"] *= 2
    if fault == "date": q["quotes"][0]["incident_date"] = "2026-09-01"
    if fault == "empty": q["quotes"] = []
    if fault == "prose": q["summary"] = "Invented narrative"
    if fault == "predecessor": kwargs["predecessor"] = "legacy"
    with pytest.raises(ValueError): prepare(packet, scope, q, **kwargs)


def test_timestamp_only_reuse_but_no_stale_source_qualification(database):
    packet, scope, q = inputs(database)
    first = prepare(packet, scope, q)
    packet["event"]["updated_at"] = "2026-09-19"
    resign(packet)
    assert prepare(packet, scope, q) == first
    packet["documents"][0]["text"] += " New context outside quotation."
    resign(packet)
    new_scope = proposal(packet)
    with pytest.raises(ValueError, match="stale"):
        prepare(packet, new_scope, q)


def test_incomplete_snapshot_refused(database):
    packet, scope, q = inputs(database)
    packet["omissions"] = [{"reason": "content_missing"}]
    resign(packet)
    with pytest.raises(ValueError, match="incomplete"):
        prepare(packet, scope, q)


def test_predecessor_changes_identity_without_mutating_evidence(database):
    packet, scope, q = inputs(database)
    first = prepare(packet, scope, q)
    second = prepare(packet, scope, q, predecessor=first["revision_id"])
    assert second["revision_id"] != first["revision_id"]
    assert second["entries"] == first["entries"]


def test_unselected_sources_remain_explicit_and_input_order_is_preserved(database):
    database[1](id=2, text="Acme was mentioned in unrelated financial reporting.")
    packet, scope, q = inputs(database)
    original = [d["article_id"] for d in packet["documents"]]
    result = prepare(packet, scope, q)
    assert result["unrepresented_article_ids"] == [2]
    assert [d["article_id"] for d in packet["documents"]] == original


def test_partial_quote_overlap_rejected(database):
    packet, scope, q = inputs(database)
    text = packet["documents"][0]["text"]
    q["quotes"].append({"article_id": 1, "start": 5, "end": len(text), "quote": text[5:]})
    with pytest.raises(ValueError, match="overlapping"):
        prepare(packet, scope, q)
