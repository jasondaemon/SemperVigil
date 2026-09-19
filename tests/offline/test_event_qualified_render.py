import copy
from pathlib import Path

import pytest

from sempervigil import event_render, publish
from sempervigil.event_revision_store import source_version
from test_event_projection import inputs, prepare
from test_event_review import database, resign
from test_event_export_reuse import event
from test_event_scope import proposal

pytestmark = pytest.mark.offline


def approved_fixture(database):
    packet, scope, q = inputs(database)
    bundle = {"packet": packet, "scope": scope, "qualification": q, "predecessor": None}
    identity = prepare(packet, scope, q)["revision_id"]
    return bundle, identity


def write(tmp_path, bundle, identity, events=None):
    return publish.write_events_markdown(events or [{**event("event"), "site_slug": "stable-url"}],
        str(tmp_path), qualified_revisions={"event": bundle}, promoted_revision_ids={"event": identity})


def test_separate_branch_preserves_url_and_excludes_all_legacy_content(database, tmp_path):
    bundle, identity = approved_fixture(database)
    path = Path(write(tmp_path, bundle, identity)[0])
    text = path.read_text()
    assert path.name == "stable-url.md"
    assert "Source-backed coverage" in text and identity in text
    assert "contact system" in text and "Source title" in text
    for forbidden in ["Reported disruption", "Source reports a disruption", "A reported fact",
                      "Reported impact", "Responsible actor", "CVE-2026-12345", "Portal", "## Timeline"]:
        assert forbidden not in text
    assert "Incident date: unknown" in text and "Feed date: 2026-09-01" in text


def test_qualified_unchanged_export_reuses_existing_file(database, tmp_path, monkeypatch):
    bundle, identity = approved_fixture(database)
    path = Path(write(tmp_path, bundle, identity)[0])
    before = path.stat()
    monkeypatch.setattr(publish, "atomic_write_text", lambda *a: pytest.fail("rewritten"))
    write(tmp_path, bundle, identity)
    assert path.stat().st_mtime_ns == before.st_mtime_ns and path.stat().st_ino == before.st_ino


@pytest.mark.parametrize("fault", ["pointer", "quote", "scope", "event", "missing", "unmatched", "duplicate"])
def test_invalid_qualified_input_preserves_existing_outputs(database, tmp_path, fault):
    bundle, identity = approved_fixture(database)
    path = Path(write(tmp_path, bundle, identity)[0])
    before = path.read_bytes(), path.stat().st_mtime_ns
    events = None
    if fault == "pointer": identity = "b" * 64
    if fault == "quote": bundle["qualification"]["quotes"][0]["quote"] = "fabricated"
    if fault == "scope": bundle["scope"]["anchor"]["quote"] = "fabricated"
    if fault == "event": bundle["packet"]["event"]["id"] = "other"
    if fault == "unmatched": events = [event("other")]
    if fault == "duplicate": events = [{**event("event"), "site_slug": "one"}, {**event("event"), "site_slug": "two"}]
    with pytest.raises(ValueError):
        if fault == "missing":
            publish.write_events_markdown([event("event")], str(tmp_path), qualified_revisions={"event": bundle})
        else:
            write(tmp_path, bundle, identity, events)
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    assert len(list((tmp_path / "events").glob("*.md"))) == 1


def test_source_syntax_cannot_become_markdown_html_or_hugo_shortcode(database):
    bundle, _ = approved_fixture(database)
    packet = bundle["packet"]
    text = 'Acme reported a breach of its contact system. {{< evil >}} <script>x</script> [x](javascript:alert(1))'
    packet["documents"][0].update(text=text, title='Title {{% evil %}} </a>',
                                   url='https://example.org/a){{test}}?q="x"')
    resign(packet)
    scope = proposal(packet)
    q = copy.deepcopy(bundle["qualification"])
    q.update(source_version=source_version(packet), scope_version=scope["scope_version"],
             quotes=[{"article_id": 1, "start": 0, "end": len(text), "quote": text}])
    identity = prepare(packet, scope, q)["revision_id"]
    _, rendered = event_render.render({"packet": packet, "scope": scope, "qualification": q,
                                       "predecessor": None}, event_id="event", expected_revision=identity)
    assert "{{" not in rendered and "<script>" not in rendered and "[x](javascript:" not in rendered
    assert "&#123;" in rendered and "%29%7B%7Btest%7D%7D" in rendered
