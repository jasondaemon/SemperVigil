import copy
import hashlib
from pathlib import Path

import pytest

from sempervigil import publish

pytestmark = pytest.mark.offline


def event(identity="evt_one"):
    return {"id": identity, "title": "Acme incident", "severity": "HIGH",
            "summary": "Reported disruption.", "kind": "breach", "status": "confirmed",
            "publish_state": "published", "published_at": "2026-09-18T12:00:00Z",
            "first_seen_at": "2026-09-18T10:00:00Z", "last_seen_at": "2026-09-18T12:00:00Z",
            "narrative": {"summary": "An attributed account.", "bullets": ["A reported fact."],
                          "sections": {"impact": {"title": "Reported impact", "points": ["A service interruption."]}}},
            "report": {"overview": "Source reports a disruption.", "impact": ["Reported impact."],
                       "attribution": {"responsible_actor": "Unconfirmed", "confidence": "low",
                                       "rationale": ["An allegation."], "disputed_claims": ["Not independently verified."]},
                       "timeline": [{"date": "2026-09-18", "event": "Disclosure", "evidence": ["A statement appeared."]}]},
            "items": {"cves": [{"cve_id": "CVE-2026-12345", "preferred_base_severity": "HIGH", "preferred_base_score": 8.1}],
                      "products": [{"vendor_name": "Acme", "product_name": "Portal"}],
                      "articles": [{"title": "Disclosure", "url": "https://example.com/disclosure"}]}}


def test_legacy_rendered_bytes_stay_identical(tmp_path):
    paths = publish.write_events_markdown([event()], str(tmp_path))
    assert hashlib.sha256(Path(paths[0]).read_bytes()).hexdigest() == "249738a8cee011b4638506624462ccc155e389bba3abdcb8b23d672a2b9b161d"


def test_unchanged_pages_keep_bytes_inode_and_timestamp(tmp_path, monkeypatch):
    paths = publish.write_events_markdown([event(), event("evt_two")], str(tmp_path))
    before = [(Path(p).read_bytes(), Path(p).stat()) for p in paths]
    monkeypatch.setattr(publish, "atomic_write_text", lambda *a: pytest.fail("unchanged file rewritten"))
    assert publish.write_events_markdown([event(), event("evt_two")], str(tmp_path)) == paths
    for path, (content, info) in zip(paths, before):
        assert Path(path).read_bytes() == content
        assert Path(path).stat().st_mtime_ns == info.st_mtime_ns
        assert Path(path).stat().st_ino == info.st_ino


def test_only_changed_page_is_written(tmp_path, monkeypatch):
    publish.write_events_markdown([event(), event("evt_two")], str(tmp_path))
    changed = copy.deepcopy(event())
    changed["report"]["overview"] = "Updated source account."
    writes = []
    original = publish.atomic_write_text
    def record(path, text):
        writes.append(Path(path).name)
        original(path, text)
    monkeypatch.setattr(publish, "atomic_write_text", record)
    publish.write_events_markdown([changed, event("evt_two")], str(tmp_path))
    assert writes == ["evt_one.md"]


def test_cleanup_retains_index_and_removes_withdrawn_page(tmp_path):
    publish.write_events_markdown([event(), event("evt_two")], str(tmp_path))
    root = tmp_path / "events"
    (root / "_index.md").write_text("Landing page")
    (root / "notes.txt").write_text("Not owned by this writer")
    publish.write_events_markdown([event()], str(tmp_path))
    assert not (root / "evt_two.md").exists()
    publish.write_events_markdown([], str(tmp_path))
    assert sorted(p.name for p in root.iterdir()) == ["_index.md", "notes.txt"]


@pytest.mark.parametrize("slug", ["../escape", "/absolute", "a/b", "a\\b", "_index", "_INDEX", ".hidden", "x" * 241])
def test_bad_slug_does_not_remove_existing_pages(tmp_path, slug):
    path = Path(publish.write_events_markdown([event()], str(tmp_path))[0])
    content, info = path.read_bytes(), path.stat()
    bad = {**event("evt_other"), "site_slug": slug}
    with pytest.raises(ValueError, match="slug"):
        publish.write_events_markdown([bad], str(tmp_path))
    assert path.read_bytes() == content and path.stat().st_mtime_ns == info.st_mtime_ns


def test_duplicate_slug_case_collision_fails_before_writes(tmp_path):
    with pytest.raises(ValueError, match="slug"):
        publish.write_events_markdown([{**event(), "site_slug": "Case"}, {**event("other"), "site_slug": "case"}], str(tmp_path))
    assert list((tmp_path / "events").iterdir()) == []


def test_missing_id_does_not_look_like_intentionally_empty_export(tmp_path):
    path = Path(publish.write_events_markdown([event()], str(tmp_path))[0])
    before = path.read_bytes()
    with pytest.raises(ValueError, match="event_id_required"):
        publish.write_events_markdown([{**event(), "id": ""}], str(tmp_path))
    assert path.read_bytes() == before


def test_serialize_failure_does_not_prune_or_replace(tmp_path):
    path = Path(publish.write_events_markdown([event()], str(tmp_path))[0])
    before = path.read_bytes()
    bad = {**event("evt_other"), "summary": 123}
    with pytest.raises(AttributeError):
        publish.write_events_markdown([{**event(), "title": "Changed"}, bad], str(tmp_path))
    assert path.read_bytes() == before


def test_write_failure_preserves_stale_pages_for_retry(tmp_path, monkeypatch):
    paths = publish.write_events_markdown([event(), event("evt_two")], str(tmp_path))
    def fail(*args):
        raise OSError("write failed")
    monkeypatch.setattr(publish, "atomic_write_text", fail)
    with pytest.raises(OSError):
        publish.write_events_markdown([{**event(), "title": "Changed"}], str(tmp_path))
    assert all(Path(p).exists() for p in paths)


@pytest.mark.parametrize("target", ["directory", "file"])
def test_output_symlinks_are_not_followed(tmp_path, target):
    outside = tmp_path / "outside"
    outside.mkdir()
    if target == "directory":
        (tmp_path / "events").symlink_to(outside, target_is_directory=True)
    else:
        (tmp_path / "events").mkdir()
        (outside / "file.md").write_text("Do not alter")
        (tmp_path / "events/evt_one.md").symlink_to(outside / "file.md")
    with pytest.raises(ValueError): publish.write_events_markdown([event()], str(tmp_path))
    if target == "file": assert (outside / "file.md").read_text() == "Do not alter"


def test_cleanup_failure_is_not_silently_accepted(tmp_path, monkeypatch):
    publish.write_events_markdown([event()], str(tmp_path))
    def fail(*args, **kwargs):
        raise PermissionError("cannot withdraw stale page")
    monkeypatch.setattr(Path, "unlink", fail)
    with pytest.raises(PermissionError): publish.write_events_markdown([], str(tmp_path))
