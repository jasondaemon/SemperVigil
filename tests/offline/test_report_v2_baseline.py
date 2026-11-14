import pytest
from sempervigil import event_report_contract_v2 as contract
from sempervigil.event_source_reports_v2 import meaningful_change
pytestmark=pytest.mark.offline

def material(rows):
    from sempervigil.event_source_reports_v2 import _source_material
    return _source_material("evt_example", "Incident", rows, [r[0] for r in rows])


def source_row(aid, text="Original disclosure.", title=None, published="2026-10-01", retrieved="2026-10-02"):
    return (aid, title or "Source " + str(aid), "https://example.org/" + str(aid), text, published, retrieved, {})


def test_body_deduplication_preserves_ordered_membership_and_duplicate_metadata():
    original = material([source_row(3), source_row(2)])
    assert original == material([source_row(2), source_row(3)])
    assert [m["article_id"] for m in original["membership"]] == [2, 3]
    assert original["sources"][0]["duplicates"] == [original["membership"][1]]
    assert len(original["sources"]) == 1
    assert original["membership"][1]["published_at"] == "2026-10-01"


@pytest.mark.parametrize("field", ["title", "url", "text", "published", "retrieved"])
def test_duplicate_identity_changes_are_freshness_changes(field):
    row = list(source_row(3))
    row[{"title":1, "url":2, "text":3, "published":4, "retrieved":5}[field]] += " " if field == "text" else "-changed"
    old = material([source_row(2), source_row(3)])
    new = material([source_row(2), tuple(row)])
    assert old["source_version"] != new["source_version"]
    assert meaningful_change(old, new) is (field != "retrieved")


def test_new_lower_canonical_duplicate_has_no_false_evidence_delta():
    old = material([source_row(2), source_row(3)])
    new = material([source_row(1), source_row(2), source_row(3)])
    assert meaningful_change(old, new) is False
    result = contract.update_context(new, "generator_upgrade", old)
    assert result["evidence_delta"] == {"baseline":"known", "new":[], "changed":[], "removed":[]}
    assert result["membership_delta"] == {"new":[1], "changed":[], "removed":[]}


def test_duplicate_member_correction_and_removal_are_not_novelty_filtered():
    old = material([source_row(2), source_row(3)])
    new = material([source_row(2), source_row(3, "Original disclosure. Correction.")])
    assert meaningful_change(old, new) is True
    result = contract.update_context(new, "evidence_change", old)
    assert result["evidence_delta"] == {"baseline":"known", "new":[], "changed":["S3"], "removed":[]}
    assert result["membership_delta"]["changed"] == [3]
    removed = material([source_row(2)])
    assert meaningful_change(old, removed) is True
    assert contract.update_context(removed, "evidence_change", old)["membership_delta"]["removed"] == [3]


def test_material_rejects_missing_or_repeated_members():
    from sempervigil.event_source_reports_v2 import _source_material
    for ids in ([2, 3], [2, 2]):
        with pytest.raises(ValueError, match="sources_missing"):
            _source_material("evt_example", "Incident", [source_row(2)], ids)


def test_replaced_membership_does_not_make_identical_body_removed_or_new():
    old = material([source_row(2), source_row(3)])
    new = material([source_row(1)])
    assert meaningful_change(old, new) is True  # Lost original provenance requires reconciliation.
    result = contract.update_context(new, "evidence_change", old)
    assert result["evidence_delta"] == {"baseline":"known", "new":[], "changed":[], "removed":[]}
    assert result["membership_delta"] == {"new":[1], "changed":[], "removed":[2,3]}
