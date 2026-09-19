import json
from pathlib import Path

import pytest

from sempervigil import publish
from test_event_review import database
from test_event_qualified_render import approved_fixture
from test_event_export_reuse import event

pytestmark = pytest.mark.offline


def export(tmp_path, bundle, identity, events=None):
    return publish.write_events_exports(events if events is not None else [event("event")],
        str(tmp_path / "content"), str(tmp_path / "static"),
        qualified_revisions={"event": bundle}, promoted_revision_ids={"event": identity})


def state(tmp_path):
    return {str(p.relative_to(tmp_path)): (p.read_bytes(), p.stat().st_mtime_ns)
            for p in tmp_path.rglob("*") if p.is_file()}


def test_page_index_same_revision_and_no_legacy_contamination(database, tmp_path):
    bundle, identity = approved_fixture(database)
    pages, index = export(tmp_path, bundle, identity)
    value = json.loads(Path(index).read_text())[0]
    assert value["event_revision"] == identity and identity in Path(pages[0]).read_text()
    assert value["quotations"][0]["quote"] == bundle["packet"]["documents"][0]["text"]
    assert value["counts"] == {"articles": 1, "cves": 0, "products": 0}
    assert value["severity"] is None and value["first_seen_at"] is None
    assert "Reported disruption" not in Path(index).read_text() and "CVE-2026-12345" not in Path(index).read_text()


def test_repeated_exports_leave_both_outputs_untouched(database, tmp_path, monkeypatch):
    bundle, identity = approved_fixture(database)
    export(tmp_path, bundle, identity)
    before = state(tmp_path)
    monkeypatch.setattr(publish, "atomic_write_text", lambda *a: pytest.fail("unchanged write"))
    export(tmp_path, bundle, identity)
    assert state(tmp_path) == before


@pytest.mark.parametrize("fault", ["pointer", "quote", "serialization", "slug", "unmatched"])
def test_content_failures_never_replace_either_output(database, tmp_path, fault):
    bundle, identity = approved_fixture(database)
    export(tmp_path, bundle, identity)
    before = state(tmp_path)
    events = [event("event")]
    if fault == "pointer": identity = "b" * 64
    if fault == "quote": bundle["qualification"]["quotes"][0]["quote"] = "changed"
    if fault == "serialization": events.append({**event("other"), "summary": {object(): "invalid key"}})
    if fault == "slug": events[0]["site_slug"] = "../escape"
    if fault == "unmatched": events = []
    with pytest.raises((ValueError, TypeError)):
        export(tmp_path, bundle, identity, events)
    assert state(tmp_path) == before


def test_legacy_index_payload_compatibility(tmp_path):
    item = event()
    path = publish.write_events_index([item], str(tmp_path))
    expected = {"event_id": item["id"], **{k: item[k] for k in (
        "title", "summary", "severity", "kind", "status", "first_seen_at", "last_seen_at")},
        **item["items"], "counts": {"cves": 1, "products": 1, "articles": 1}}
    assert Path(path).read_text() == json.dumps([expected], indent=2)


@pytest.mark.parametrize("target", ["directory", "file"])
def test_index_symlinks_refused_before_page_changes(database, tmp_path, target):
    bundle, identity = approved_fixture(database)
    export(tmp_path, bundle, identity)
    index_dir = tmp_path / "static/index"
    if target == "file":
        (index_dir / "events.json").rename(index_dir / "original.json")
        (index_dir / "events.json").symlink_to(index_dir / "original.json")
    else:
        index_dir.rename(tmp_path / "original-index")
        index_dir.symlink_to(tmp_path / "original-index", target_is_directory=True)
    before = state(tmp_path)
    with pytest.raises(ValueError, match="index"):
        export(tmp_path, bundle, identity)
    assert state(tmp_path) == before
