import hashlib
import json
from types import SimpleNamespace

import pytest

from sempervigil import event_release as release
from sempervigil.event_render import render, index_entry
from sempervigil.event_activation import validate_manifest
from test_event_qualified_render import approved_fixture
from test_event_review import database

pytestmark = pytest.mark.offline


def candidate(tmp_path, bundle, revision):
    page = tmp_path / "events/stable/index.html"
    page.parent.mkdir(parents=True)
    fragment = render(bundle, event_id="event", expected_revision=revision)[1]
    page.write_text("<!doctype html><html><body><article>" + fragment + "</article></body></html>")
    index = tmp_path / release.INDEX_PATH
    index.parent.mkdir(parents=True)
    entry = index_entry(bundle, event_id="event", expected_revision=revision)
    history = [{"event_revision": revision, "published_at": "2026-09-21T12:00:00Z"}]
    entry.update({"url": "/events/stable/", "revision_published_at": "2026-09-21T12:00:00Z",
                  "publication_history": history})
    index.write_text(json.dumps([entry]))
    manifest = {"workflow": "event-release-authorization-v2", "revisions": {"event": revision}, "withdrawn": {},
        "pages": {"event": "stable"}, "fragments": {"event": release.fragment_identity(fragment)},
        "index_sha256": hashlib.sha256(index.read_bytes()).hexdigest()}
    return manifest, page, index


def test_candidate_binds_rendered_html_and_index(database, tmp_path):
    bundle, revision = approved_fixture(database)
    manifest, page, _ = candidate(tmp_path, bundle, revision)
    history = [{"event_revision": revision, "published_at": "2026-09-21T12:00:00Z"}]
    validate_manifest(manifest)
    release.verify_release(tmp_path, manifest, {"event": bundle}, {"event": history})
    page.write_text(page.read_text().replace('\n', '').replace('data-event-id="event"', 'data-event-id=event'))
    release.verify_release(tmp_path, manifest, {"event": bundle}, {"event": history})


@pytest.mark.parametrize("fault", ["quote", "duplicate", "missing", "index", "wrong_id", "withdrawal", "path", "unbound"])
def test_candidate_mismatches_refuse(database, tmp_path, fault):
    bundle, revision = approved_fixture(database)
    manifest, page, index = candidate(tmp_path, bundle, revision)
    if fault == "quote": page.write_text(page.read_text().replace("Acme", "Fabricated"))
    if fault == "duplicate": page.write_text(page.read_text()*2)
    if fault == "missing": page.unlink()
    if fault == "index": index.write_text("[]")
    if fault == "wrong_id": manifest["fragments"]["event"] = "a"*64
    if fault == "withdrawal": manifest.update(revisions={}, fragments={}, withdrawn={"event": revision})
    if fault == "path":
        other = page.with_name("other.html")
        page.rename(other)
        page.symlink_to(other)
    if fault == "unbound": manifest["workflow"] = "event-release-authorization-v1"
    with pytest.raises((ValueError, OSError)):
        release.verify_release(tmp_path, manifest, {"event": bundle}, {
            "event": [{"event_revision": revision, "published_at": "2026-09-21T12:00:00Z"}]})


def test_builder_preparation_and_unchanged_reuse(database, tmp_path, monkeypatch):
    from sempervigil import worker, storage
    bundle, revision = approved_fixture(database)
    class Read:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, *args):
            if "FROM event_public_revisions r" in args[0]:
                rows = [("event", revision, "2026-09-21T12:00:00Z")]
            else:
                rows = [("event", revision, "2026-09-21T12:00:00Z")]
            return SimpleNamespace(fetchall=lambda: rows)
    monkeypatch.setattr(release, "database", lambda: Read())
    monkeypatch.setattr(release, "load_export", lambda *a: {
        "managed_event_ids": ["event"], "promoted_revision_ids": {"event": revision},
        "qualified_revisions": {"event": bundle}, "withheld": {}, "withdrawn": {}})
    monkeypatch.setattr(worker, "_collect_published_events", lambda *a: ([], 0))
    monkeypatch.setattr(storage, "get_event", lambda *a: {"id": "event", "site_slug": "stable"})
    config = SimpleNamespace(paths=SimpleNamespace(output_dir=str(tmp_path / "content/posts")))
    assert release.prepare_site(None, config, None)["published"] == 1
    files = [tmp_path / "content/events/stable.md", tmp_path / "static" / release.INDEX_PATH,
             tmp_path / "static" / release.MANIFEST]
    before = [(f.read_bytes(), f.stat().st_mtime_ns) for f in files]
    release.prepare_site(None, config, None)
    assert before == [(f.read_bytes(), f.stat().st_mtime_ns) for f in files]


def test_enabled_worker_defers_source_writes_to_builder(monkeypatch):
    from sempervigil import worker
    monkeypatch.setenv("SV_EVENT_PUBLICATION_ENABLED", "1")
    calls = []
    monkeypatch.setattr(worker, "mark_build_dirty", lambda *a, **kw: calls.append(kw))
    monkeypatch.setattr(worker, "_collect_published_events", lambda *a: pytest.fail("worker source write"))
    worker._publish_events(None, None, None)
    assert calls == [{"reason": "qualified_events_refresh"}]
