from __future__ import annotations

from types import SimpleNamespace

from sempervigil.worker import _feed_archive_dir


def test_feed_archive_dir_uses_env_override(tmp_path, monkeypatch) -> None:
    archive_dir = tmp_path / "public" / "shared" / "feed"
    monkeypatch.setenv("SV_FEED_ARCHIVE_DIR", str(archive_dir))
    config = SimpleNamespace(paths=SimpleNamespace(output_dir=str(tmp_path / "site" / "content" / "posts")))

    assert _feed_archive_dir(config) == archive_dir


def test_feed_archive_dir_defaults_to_hugo_static_feed(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SV_FEED_ARCHIVE_DIR", raising=False)
    source_dir = tmp_path / "site-src"
    monkeypatch.setenv("SV_HUGO_SOURCE_DIR", str(source_dir))
    config = SimpleNamespace(paths=SimpleNamespace(output_dir=str(tmp_path / "site" / "content" / "posts")))

    assert _feed_archive_dir(config) == source_dir / "static" / "feed"
