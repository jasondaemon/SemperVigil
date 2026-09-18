from __future__ import annotations

import json
import logging
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from sempervigil import worker

pytestmark = pytest.mark.offline


@pytest.fixture
def export_case(tmp_path, monkeypatch, request):
    day = "2026-09-08"
    root = tmp_path / "shared" / "feed"
    source = tmp_path / "source"
    monkeypatch.setenv("SV_FEED_ARCHIVE_DIR", str(root))
    stats = [dict(day=day, article_count=1, article_updated_at=day + "T12:00:00Z",
                  cve_count=1, cve_updated_at=day + "T13:00:00Z")]
    monkeypatch.setattr(worker, "list_feed_day_stats", lambda *a: stats)
    article = dict(id=7, source_id="example", source_name="Example",
                   title="Patch released", original_url="https://example.com/patch",
                   published_at=day + "T12:00:00Z", tags="security",
                   summary_llm=json.dumps({"summary": "Patch available.", "bullets": ["Update."]}))
    cve = dict(cve_id="CVE-2026-12345", published_at=day + "T13:00:00Z",
               preferred_cvss_version="3.1", preferred_base_score=8.8,
               preferred_base_severity="HIGH", description_text="Example vulnerability.",
               vendor_products_json=json.dumps([{"vendor": "Acme", "product": "Widget"}]),
               epss_score=0.0, epss_percentile=0.0, epss_date=day)
    monkeypatch.setattr(worker, "list_recent_articles", lambda *a, **k: [article])
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a, **k: [article])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **k: [cve])
    monkeypatch.setattr(worker, "search_cves", lambda *a, **k: ([cve], 1))
    monkeypatch.setattr(worker, "list_event_keys_for_articles", lambda *a: {7: ["event:example"]})
    monkeypatch.setattr(worker, "list_article_cve_tags", lambda *a: {7: [cve["cve_id"]]})
    monkeypatch.setattr(worker, "list_products_for_article", lambda *a: [{"vendor": "Acme", "product": "Widget"}])
    monkeypatch.setattr(worker, "get_article_threat_actors", lambda *a: [])
    monkeypatch.setattr(worker, "get_cve_threat_actors", lambda *a: [])
    monkeypatch.setattr(worker, "get_cve_kev", lambda *a: None)
    for name in ("_write_product_data_files", "_write_sources_data_files", "_write_cve_pages"):
        monkeypatch.setattr(worker, name, lambda *a, **k: None)
    config = SimpleNamespace(paths=SimpleNamespace(
        output_dir=str(source / "content" / "posts"), data_dir=str(tmp_path / "data")),
        app=SimpleNamespace(timezone=getattr(request, "param", "UTC")))

    def refresh():
        return worker._refresh_feed_data_files(None, config, logging.getLogger(__name__))

    return day, root / "days" / (day + ".json"), source, refresh, stats


def test_recent_and_historical_serializers_preserve_shared_contract(export_case):
    day, path, source, refresh, stats = export_case
    refresh()
    recent = json.loads(path.read_text())
    historical = worker._build_feed_day_payload(
        None, day_key=day, site_root=str(source), tz=timezone.utc, tz_name="UTC",
    )
    for item in recent["items"]:
        if item["kind"] == "article":
            assert item["id"] == item["article_id"]
    assert recent == historical


def test_recent_refresh_does_not_remove_previously_exported_article(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    refresh()
    previous = json.loads(path.read_text())
    previous["items"].append({"kind": "article", "article_id": 8, "title": "Older article"})
    previous["counts"]["article"] += 1
    path.write_text(json.dumps(previous))
    article = dict(id=7, title="Recent article", published_at=day + "T12:00:00Z")
    older = dict(id=8, title="Older article", published_at=day + "T11:00:00Z")
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a: [article, older])
    stats[0]["article_count"] = 2
    refresh()
    after = json.loads(path.read_text())
    assert {item["article_id"] for item in after["items"] if item["kind"] == "article"} == {7, 8}


def test_failed_recent_write_keeps_last_complete_public_json(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    refresh()
    previous = path.read_bytes()
    stats[0]["article_updated_at"] = day + "T14:00:00Z"
    original = Path.write_text

    def fail_day_write(self, data, *args, **kwargs):
        if self.parent == path.parent and self.name.startswith(day):
            original(self, '{"day":', *args, **kwargs)
            raise OSError("simulated interrupted write")
        return original(self, data, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", fail_day_write)
    with pytest.raises(OSError, match="interrupted write"):
        refresh()
    assert path.read_bytes() == previous


def test_recent_refresh_reuses_unchanged_day(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    refresh()
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    def unexpected(*args, **kwargs):
        raise AssertionError("An unchanged day must not be serialized")

    monkeypatch.setattr(worker, "_build_feed_day_payload", unexpected)
    refresh()
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


def test_refresh_does_not_rebuild_unrelated_history_on_version_change(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    refresh()
    old_path = path.parent / "1990-05-01.json"
    old_path.write_text('{"day":"1990-05-01","items":[],"counts":{"article":0,"cve":0}}')
    before = (old_path.read_bytes(), old_path.stat().st_mtime_ns)
    stats.append(dict(day="1990-05-01", article_count=0, article_updated_at="",
                      cve_count=1, cve_updated_at="2026-01-01T00:00:00Z"))
    monkeypatch.setattr(worker, "ARCHIVE_SCHEMA_VERSION", worker.ARCHIVE_SCHEMA_VERSION + 1)
    refresh()
    assert (old_path.read_bytes(), old_path.stat().st_mtime_ns) == before


def test_cve_only_export_does_not_require_articles(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    monkeypatch.setattr(worker, "list_recent_articles", lambda *a, **k: [])
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a, **k: [])
    stats[0]["article_count"] = 0
    refresh()
    assert json.loads(path.read_text())["counts"] == {"article": 0, "cve": 1}


@pytest.mark.parametrize("export_case", ["America/New_York"], indirect=True)
def test_midnight_cve_refreshes_database_day(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    cve = dict(cve_id="CVE-2026-12345", published_at=day + "T01:00:00Z")
    monkeypatch.setattr(worker, "list_recent_articles", lambda *a, **k: [])
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a, **k: [])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **k: [cve])
    monkeypatch.setattr(worker, "search_cves", lambda *a, **k: ([cve], 1))
    stats[0]["article_count"] = 0
    refresh()
    payload = json.loads(path.read_text())
    assert payload["day"] == day
    assert payload["counts"] == {"article": 0, "cve": 1}


def test_stored_article_day_is_refreshed(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    article = dict(id=7, title="Late publication", brief_day=day,
                   published_at="2026-09-09T01:00:00Z")
    monkeypatch.setattr(worker, "list_recent_articles", lambda *a, **k: [article])
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a, **k: [article])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **k: [])
    monkeypatch.setattr(worker, "search_cves", lambda *a, **k: ([], 0))
    stats[0]["cve_count"] = 0
    refresh()
    assert json.loads(path.read_text())["counts"] == {"article": 1, "cve": 0}


def test_empty_database_export_completes(export_case, monkeypatch):
    day, path, source, refresh, stats = export_case
    stats.clear()
    monkeypatch.setattr(worker, "list_recent_articles", lambda *a, **k: [])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **k: [])
    monkeypatch.setattr(worker, "search_cves", lambda *a, **k: ([], 0))
    result = refresh()
    assert result["today"] == 0
    assert result["recent"] == 0
    assert not path.exists()
