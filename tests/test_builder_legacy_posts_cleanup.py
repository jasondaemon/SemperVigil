from __future__ import annotations

from pathlib import Path

from sempervigil.builder import _prune_legacy_posts_tree


def _touch(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_prune_legacy_posts_tree_removes_nested_posts_artifacts(tmp_path: Path) -> None:
    source_dir = tmp_path / "site"
    _touch(source_dir / "content" / "daily" / "2026-03-31.md", "root daily")
    _touch(source_dir / "data" / "briefs" / "2026-03-31.json", "root brief")
    _touch(source_dir / "content" / "posts" / "content" / "daily" / "2026-03-31.md", "legacy daily")
    _touch(source_dir / "content" / "posts" / "data" / "briefs" / "2026-03-31.json", "legacy brief")

    pruned = _prune_legacy_posts_tree(str(source_dir))

    assert pruned == 2
    assert (source_dir / "content" / "daily" / "2026-03-31.md").exists()
    assert (source_dir / "data" / "briefs" / "2026-03-31.json").exists()
    assert not (source_dir / "content" / "posts" / "content").exists()
    assert not (source_dir / "content" / "posts" / "data").exists()
