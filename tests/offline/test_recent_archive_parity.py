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
def export_case(tmp_path, monkeypatch):
    day = "2026-09-08"
    root = tmp_path / "shared" / "feed"
    source = tmp_path / "source"
    monkeypatch.setenv("SV_FEED_ARCHIVE_DIR", str(root))
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
        app=SimpleNamespace(timezone="UTC"))

    def refresh():
        return worker._refresh_feed_data_files(None, config, logging.getLogger(__name__))

    return day, root / "days" / (day + ".json"), source, refresh


def test_recent_and_historical_serializers_preserve_shared_contract(export_case):
    day, path, source, refresh = export_case
    refresh()
    recent = json.loads(path.read_text())
    historical = worker._build_feed_day_payload(
        None, day_key=day, site_root=str(source), tz=timezone.utc, tz_name="UTC",
    )
    # Historical articles currently carry an additional id alias. Do not remove
    # that downstream-visible field merely to make the serializers identical.
    for item in historical["items"]:
        if item["kind"] == "article":
            assert item["id"] == item["article_id"]
            item.pop("id")
    assert recent == historical


@pytest.mark.xfail(strict=True, reason="Recent-window refresh can replace a complete historical day")
def test_recent_refresh_does_not_remove_previously_exported_article(export_case):
    day, path, source, refresh = export_case
    refresh()
    previous = json.loads(path.read_text())
    previous["items"].append({"kind": "article", "article_id": 8, "title": "Older article"})
    previous["counts"]["article"] += 1
    path.write_text(json.dumps(previous))
    refresh()
    after = json.loads(path.read_text())
    assert {item["article_id"] for item in after["items"] if item["kind"] == "article"} == {7, 8}


def test_failed_recent_write_keeps_last_complete_public_json(export_case, monkeypatch):
    day, path, source, refresh = export_case
    refresh()
    previous = path.read_bytes()
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
