import json

import pytest

from sempervigil import event_assessment as assessment, event_assessment_cache as cache
from sempervigil.investigation import _version
from test_event_review import database, get_packet, resign
from test_event_scope import proposal, completion

pytestmark = pytest.mark.offline


def test_report_only_timestamp_reuses_and_rebinds_passages(database, tmp_path):
    packet = get_packet(database)
    scope = proposal(packet)
    complete, calls = completion(packet, scope)
    first, hit = cache.reuse(packet, complete, tmp_path, scope=scope)
    assert not hit
    packet["event"]["updated_at"] = "2026-09-19T07:30:00Z"
    resign(packet)
    second, hit = cache.reuse(packet, complete, tmp_path, scope=scope)
    assert hit and len(calls) == 1
    assert second["packet_version"] != first["packet_version"]
    assert second["request_version"] != first["request_version"]
    assert second["suggestions"].keys() != first["suggestions"].keys()
    assert list(second["suggestions"].values()) == list(first["suggestions"].values())
    assert assessment.validate_assessment(second, packet) == second
    assert second["scope"] == scope and second["public_eligible"] is False
    assert len(list((tmp_path / "assessment-cache").glob("*.json"))) == 1


@pytest.mark.parametrize("change", ["title", "aliases", "omissions", "truncation", "model"])
def test_other_semantic_or_coverage_changes_invalidate(database, tmp_path, change):
    packet = get_packet(database)
    scope = proposal(packet)
    complete, calls = completion(packet, scope)
    cache.reuse(packet, complete, tmp_path, scope=scope)
    if change == "title": packet["event"]["title"] += " updated"
    if change == "aliases": packet["aliases"].append("Other")
    if change == "omissions": packet["omissions"].append({"reason": "unavailable"})
    if change == "truncation": packet["links_truncated"] = True
    if change == "model": complete.cache_identity = "b" * 64
    resign(packet)
    assert cache.reuse(packet, complete, tmp_path, scope=scope)[1] is False
    assert len(calls) == 2


def test_full_source_changes_invalidate_even_outside_model_context(database, tmp_path):
    packet = get_packet(database)
    packet["documents"].append({**packet["documents"][0], "article_id": 2,
        "text": "Acme reported a separate incident affecting another system. " + "x" * 1500})
    resign(packet)
    scope = proposal(packet)
    complete, calls = completion(packet, scope)
    before = assessment.request_for(packet, scope=scope)
    cache.reuse(packet, complete, tmp_path, scope=scope)
    packet["documents"][1]["text"] += " unseen change"
    resign(packet)
    after = assessment.request_for(packet, scope=scope)
    assert before["input"] == after["input"]
    assert cache.reuse(packet, complete, tmp_path, scope=scope)[1] is False
    assert len(calls) == 2


def test_import_existing_exact_scoped_cache_without_inference(database, tmp_path):
    packet = get_packet(database)
    scope = proposal(packet)
    complete, calls = completion(packet, scope)
    old = assessment.assess(packet, complete, scope=scope)
    request = old["request_version"]
    name = _version({"request": request, "generation": complete.cache_identity}) + ".json"
    folder = tmp_path / "assessment-cache"
    folder.mkdir()
    legacy = folder / name
    legacy.write_text(json.dumps({"request_version": request, "generation_version": complete.cache_identity,
                                  "assessment": old}))
    original_bytes = legacy.read_bytes()
    result, hit = cache.reuse(packet, complete, tmp_path, scope=scope)
    assert hit and result == old and len(calls) == 1
    assert legacy.read_bytes() == original_bytes
    packet["event"]["updated_at"] = "2026-09-19"
    resign(packet)
    assert cache.reuse(packet, complete, tmp_path, scope=scope)[1] is True
    assert len(calls) == 1


@pytest.mark.parametrize("damage", ["timestamp", "assessment", "extra", "wrong_scope"])
def test_rebinding_cannot_legitimize_invalid_stored_entry(database, tmp_path, damage):
    packet = get_packet(database)
    scope = proposal(packet)
    complete, calls = completion(packet, scope)
    cache.reuse(packet, complete, tmp_path, scope=scope)
    path = next((tmp_path / "assessment-cache").glob("*.json"))
    entry = json.loads(path.read_bytes())
    if damage == "timestamp": entry["event_updated_at"] = False
    if damage == "assessment": entry["assessment"]["request_version"] = "a" * 64
    if damage == "extra": entry["approved"] = True
    if damage == "wrong_scope": entry["assessment"]["scope"]["scope_version"] = "a" * 64
    path.write_text(json.dumps(entry))
    packet["event"]["updated_at"] = "2026-09-19"
    resign(packet)
    with pytest.raises(ValueError): cache.reuse(packet, complete, tmp_path, scope=scope)
    assert len(calls) == 1
