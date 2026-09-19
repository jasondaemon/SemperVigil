from pathlib import Path
import sys
import threading

import pytest

from sempervigil import release_switch

pytestmark = pytest.mark.offline


def releases(tmp_path):
    for name in ("old", "new"):
        path = tmp_path / "releases" / name
        path.mkdir(parents=True)
        (path / "index.html").write_text(name)
    current = tmp_path / "current"
    current.symlink_to("releases/old")
    return current, tmp_path / "releases/new"


@pytest.mark.skipif(sys.platform != "linux", reason="Linux production pathname-lookup stress test")
def test_atomic_switch_has_no_missing_name_for_concurrent_readers(tmp_path):
    current, new = releases(tmp_path)
    ready, stop = threading.Event(), threading.Event()
    reads, failures = [], []
    def reader():
        while not stop.is_set():
            try:
                reads.append((current / "index.html").read_text())
                ready.set()
            except Exception as exc:
                failures.append((type(exc).__name__, getattr(exc, "errno", None), str(exc)))
    thread = threading.Thread(target=reader)
    thread.start()
    try:
        assert ready.wait(2)
        for i in range(100):
            release_switch.atomic_switch(new if i % 2 == 0 else new.with_name("old"), current)
    finally:
        stop.set()
        thread.join(2)
    assert reads and set(reads) <= {"old", "new"} and not failures
    assert not list(tmp_path.glob(".current-*"))


def test_switch_replaces_link_without_removing_live_name(tmp_path, monkeypatch):
    current, new = releases(tmp_path)
    replace = release_switch.os.replace
    calls = []
    def checked_replace(source, destination):
        assert destination == current
        assert source.parent == current.parent
        assert current.readlink() == Path("releases/old")
        assert source.readlink() == Path("releases/new")
        calls.append((source, destination))
        replace(source, destination)
    monkeypatch.setattr(release_switch.os, "replace", checked_replace)
    release_switch.atomic_switch(new, current)
    assert len(calls) == 1
    assert (current / "index.html").read_text() == "new"
    assert not list(tmp_path.glob(".current-*"))


def test_failed_rename_preserves_old_release_and_cleans_temporary(tmp_path, monkeypatch):
    current, new = releases(tmp_path)
    def fail(*args):
        raise OSError("injected rename failure")
    monkeypatch.setattr(release_switch.os, "replace", fail)
    with pytest.raises(OSError, match="injected"):
        release_switch.atomic_switch(new, current)
    assert current.readlink() == Path("releases/old")
    assert not list(tmp_path.glob(".current-*"))


def test_invalid_candidate_does_not_replace_current(tmp_path):
    current, new = releases(tmp_path)
    (new / "index.html").unlink()
    with pytest.raises(ValueError, match="invalid_release_switch"):
        release_switch.atomic_switch(new, current)
    assert current.readlink() == Path("releases/old")
