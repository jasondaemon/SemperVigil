from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sempervigil import worker

pytestmark = pytest.mark.offline


@pytest.fixture
def archive(tmp_path, monkeypatch):
    root = tmp_path / "shared" / "feed"
    source = tmp_path / "source"
    monkeypatch.setenv("SV_FEED_ARCHIVE_DIR", str(root))
    monkeypatch.setenv("SV_HUGO_SOURCE_DIR", str(source))
    config = SimpleNamespace(
        paths=SimpleNamespace(output_dir=str(source / "content" / "posts")),
        app=SimpleNamespace(timezone="UTC"),
    )
    rows = [
        dict(day=day, article_count=1, article_updated_at="2026-09-18T10:00:00Z",
             cve_count=0, cve_updated_at="")
        for day in ("2026-03-10", "2026-09-18")
    ]
    monkeypatch.setattr(worker, "list_feed_content_inventory", lambda *args: rows)
    build = Mock(side_effect=lambda conn, **kw: {
        "day": kw["day_key"], "items": [], "counts": {"article": 0, "cve": 0},
    })
    monkeypatch.setattr(worker, "_build_feed_day_payload", build)

    def refresh(mode="dirty_only", days=None):
        return worker._refresh_feed_archive_days(None, config, logging.getLogger(__name__), mode=mode, days=days)

    return root, source, rows, build, refresh


def test_unchanged_days_are_reused_outside_hugo(archive):
    root, source, rows, build, refresh = archive
    assert refresh()["updated"] == 2
    paths = sorted((root / "days").glob("*.json"))
    before = [(p.read_bytes(), p.stat().st_mtime_ns) for p in paths]
    build.reset_mock()
    stats = refresh()
    assert stats["updated"] == 0
    assert stats["skipped"] == 2
    build.assert_not_called()
    assert [(p.read_bytes(), p.stat().st_mtime_ns) for p in paths] == before
    assert not (source / "data" / "feed").exists()
    assert not (source / "static" / "feed").exists()


@pytest.mark.parametrize("field,value", [
    ("article_count", 2), ("article_updated_at", "2026-09-18T11:00:00Z"),
    ("cve_count", 1), ("cve_updated_at", "2026-09-18T11:00:00Z"),
])
def test_only_changed_day_is_regenerated(archive, field, value):
    root, source, rows, build, refresh = archive
    refresh()
    historical = root / "days" / "2026-03-10.json"
    before = (historical.read_bytes(), historical.stat().st_mtime_ns)
    rows[1][field] = value
    build.reset_mock()
    assert refresh()["updated"] == 1
    assert [call.kwargs["day_key"] for call in build.call_args_list] == ["2026-09-18"]
    assert (historical.read_bytes(), historical.stat().st_mtime_ns) == before


def test_missing_day_is_restored_without_rebuilding_other_days(archive):
    root, source, rows, build, refresh = archive
    refresh()
    (root / "days" / rows[0]["day"]).with_suffix(".json").unlink()
    build.reset_mock()
    assert refresh("missing_only")["updated"] == 1
    assert build.call_args.kwargs["day_key"] == "2026-03-10"


def test_failed_serialization_preserves_published_day_and_manifest(archive):
    root, source, rows, build, refresh = archive
    refresh()
    path = root / "days" / "2026-09-18.json"
    before = path.read_bytes()
    manifest = (root / "day-manifest.json").read_bytes()
    rows[1]["article_count"] = 2
    build.side_effect = ValueError("invalid source data")
    with pytest.raises(ValueError, match="invalid source data"):
        refresh()
    assert path.read_bytes() == before
    assert (root / "day-manifest.json").read_bytes() == manifest
    assert json.loads(path.read_text())["day"] == "2026-09-18"


def test_content_signature_refreshes_old_day_outside_recent_window(archive):
    root, source, rows, build, refresh = archive
    for row in rows:
        row["content_signature"] = "initial"
    refresh()
    recent = root / "days" / "2026-09-18.json"
    before = (recent.read_bytes(), recent.stat().st_mtime_ns)
    rows[0]["content_signature"] = "changed-linked-product"
    build.reset_mock()
    result = refresh(days={"2026-09-18"})
    assert result["updated"] == 1
    assert result["deferred"] == 0
    assert build.call_args.kwargs["day_key"] == "2026-03-10"
    assert (recent.read_bytes(), recent.stat().st_mtime_ns) == before


def test_bookkeeping_change_does_not_rewrite_identical_content(archive):
    root, source, rows, build, refresh = archive
    for row in rows:
        row["content_signature"] = "unchanged"
    refresh()
    rows[0]["article_updated_at"] = "2026-09-18T23:00:00Z"
    build.reset_mock()
    assert refresh()["updated"] == 0
    build.assert_not_called()


def test_background_repair_is_bounded_and_resumable(archive, monkeypatch):
    root, source, rows, build, refresh = archive
    monkeypatch.setenv("SV_FEED_ARCHIVE_BACKGROUND_DAYS", "1")
    for row in rows:
        row["content_signature"] = "new-version"
    result = refresh(days=set())
    assert result["updated"] == 1
    assert result["deferred"] == 1
    assert refresh(days=set())["updated"] == 1
    build.reset_mock()
    assert refresh(days=set())["updated"] == 0
    build.assert_not_called()


def test_deleted_historical_day_is_removed_without_recent_selection(archive):
    root, source, rows, build, refresh = archive
    refresh()
    removed = rows.pop(0)["day"]
    result = refresh(days={"2026-09-18"})
    assert result["removed"] == 1
    assert not (root / "days" / f"{removed}.json").exists()


def test_background_time_budget_preserves_remaining_files(archive, monkeypatch):
    root, source, rows, build, refresh = archive
    clock = iter([0, 6])
    monkeypatch.setattr(worker.time, "monotonic", lambda: next(clock))
    result = refresh(days=set())
    assert result["updated"] == 1
    assert result["deferred"] == 1
