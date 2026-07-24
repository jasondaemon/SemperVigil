from __future__ import annotations

import logging
from pathlib import Path

from sempervigil.builder import (
    _migrate_legacy_static_feed,
    _prune_legacy_entity_render_inputs,
    _prune_legacy_posts_tree,
)


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


def test_migrate_legacy_static_feed_moves_archive_out_of_hugo_static(tmp_path) -> None:
    source_dir = tmp_path / "site-src"
    shared_feed = tmp_path / "site-public" / "shared" / "feed"
    _touch(source_dir / "static" / "feed" / "index.json", '{"days":["2026-03-31"]}')
    _touch(source_dir / "static" / "feed" / "day-manifest.json", '{"days":{}}')
    _touch(source_dir / "static" / "feed" / "days" / "2026-03-31.json", '{"items":[]}')
    _touch(source_dir / "static" / "feed" / "extra.txt", "preserve")

    result = _migrate_legacy_static_feed(str(source_dir), shared_feed, logging.getLogger("test"))

    assert result["errors"] == 0
    assert (shared_feed / "index.json").read_text(encoding="utf-8") == '{"days":["2026-03-31"]}'
    assert (shared_feed / "day-manifest.json").read_text(encoding="utf-8") == '{"days":{}}'
    assert (shared_feed / "days" / "2026-03-31.json").read_text(encoding="utf-8") == '{"items":[]}'
    assert not (source_dir / "static" / "feed" / "index.json").exists()
    assert not (source_dir / "static" / "feed" / "day-manifest.json").exists()
    assert not (source_dir / "static" / "feed" / "days").exists()
    assert (source_dir / "static" / "feed" / "extra.txt").exists()


def test_prune_legacy_entity_render_inputs_removes_heavy_hugo_data(tmp_path: Path) -> None:
    source_dir = tmp_path / "site-src"
    _touch(source_dir / "data" / "product_map.json", "{}")
    _touch(source_dir / "data" / "vendor_map.json", "{}")
    _touch(source_dir / "data" / "cves.json", "[]")
    _touch(source_dir / "data" / "articles" / "today.json", "[]")
    _touch(source_dir / "static" / "sempervigil" / "entities" / "products.json", "[]")
    _touch(source_dir / "layouts" / "entities" / "list.html", '{{ readFile "data/product_map.json" }}')
    _touch(source_dir / "layouts" / "metrics" / "list.html", "metrics")

    result = _prune_legacy_entity_render_inputs(str(source_dir))

    assert result["files"] == 4
    assert not (source_dir / "data" / "product_map.json").exists()
    assert not (source_dir / "data" / "vendor_map.json").exists()
    assert not (source_dir / "data" / "cves.json").exists()
    assert not (source_dir / "layouts" / "entities" / "list.html").exists()
    assert (source_dir / "data" / "articles" / "today.json").exists()
    assert (source_dir / "static" / "sempervigil" / "entities" / "products.json").exists()
    assert (source_dir / "layouts" / "metrics" / "list.html").exists()
