from __future__ import annotations

import json
from datetime import timezone

import pytest

from sempervigil import worker
from sempervigil import article_hosted_jobs

pytestmark = pytest.mark.offline


@pytest.fixture
def evidence(monkeypatch):
    monkeypatch.setenv("SV_REGULATORY_FEED_START_DAY", "2026-09-18")
    monkeypatch.setattr(article_hosted_jobs, "regulatory_rows", lambda *a, **kw: [])
    products = [{"vendor": "Acme", "product": "Widget"}]
    monkeypatch.setattr(worker, "list_products_for_article", lambda *a: products)
    monkeypatch.setattr(worker, "get_article_threat_actors", lambda *a: [])
    monkeypatch.setattr(worker, "get_cve_threat_actors", lambda *a: [])
    monkeypatch.setattr(worker, "get_cve_kev", lambda *a: {"due_date": "2026-10-01"})
    article = dict(id=7, source_id="example", source_name="Example",
                   title="Patch released", original_url="https://example.com/patch",
                   published_at="2026-09-18T10:00:00Z", tags="security",
                   summary_llm=json.dumps({"summary": "Patch available.", "bullets": ["Update Widget."]}))
    cve = dict(cve_id="CVE-2026-12345", published_at="2026-09-18T11:00:00Z",
               preferred_cvss_version="3.1", preferred_base_score=8.8,
               preferred_base_severity="HIGH", description_text="Example vulnerability.",
               vendor_products_json=json.dumps(products), epss_score=0.1234,
               epss_percentile=0.91, epss_date="2026-09-18")
    return article, cve


def test_article_contract_retains_identity_summary_and_relationships(evidence, tmp_path):
    article, _ = evidence
    item = worker._serialize_feed_article_item(
        None, article, tz=timezone.utc, tz_name="UTC", site_root=str(tmp_path),
        event_keys_map={7: ["event:example"]}, cve_tags_map={7: ["CVE-2026-12345"]},
    )
    assert item["kind"] == "article"
    assert item["id"] == item["article_id"] == 7
    assert item["url"] == article["original_url"]
    assert item["summary"] == "Patch available."
    assert item["summary_bullets"] == ["Update Widget."]
    assert item["event_keys"] == ["event:example"]
    assert item["tags"] == ["CVE-2026-12345", "security"]
    assert item["vendors"][0]["display_name"] == "Acme"
    assert item["product_items"][0]["display_name"] == "Widget"
    assert {f["kind"] for f in item["facets"]} == {"vendor", "product"}


@pytest.mark.parametrize("score", [None, 0.0, 0.1234, 1.0])
def test_cve_contract_preserves_score_units_and_missing_values(evidence, score):
    _, cve = evidence
    cve["epss_score"] = score
    item = worker._serialize_feed_cve_item(None, cve, tz_name="UTC")
    assert item["epss_score"] == score
    assert item["epss_percentile"] == 0.91
    assert item["epss_date"] == "2026-09-18"
    assert item["score"] == item["base_score"] == 8.8
    assert item["cvss"]["preferred"]["base_score"] == 8.8
    assert item["url"] == item["nvd_url"] == "https://nvd.nist.gov/vuln/detail/CVE-2026-12345"
    assert item["title_vendor"] == "Acme"
    assert item["title_product"] == "Widget"
    assert item["severity"] == "HIGH"
    assert item["kev_known_exploited"] is True
    assert item["kev_due_date"] == "2026-10-01"


def test_day_contract_counts_order_and_cve_deduplication(evidence, monkeypatch, tmp_path):
    article, cve = evidence
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a: [article])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **kw: [cve, cve])
    monkeypatch.setattr(worker, "list_event_keys_for_articles", lambda *a: {})
    monkeypatch.setattr(worker, "list_article_cve_tags", lambda *a: {})
    payload = worker._build_feed_day_payload(
        None, day_key="2026-09-18", site_root=str(tmp_path), tz=timezone.utc, tz_name="UTC",
    )
    assert payload["day"] == "2026-09-18"
    assert payload["counts"] == {"article": 1, "cve": 1}
    assert "regulatory" not in payload
    assert [i["kind"] for i in payload["items"]] == ["cve", "article"]
    assert [i["sections"] for i in payload["items"]] == [["cve"], ["news"]]
    assert all("_sort" not in i for i in payload["items"])
    assert json.loads(json.dumps(payload, allow_nan=False)) == payload


def test_full_day_does_not_cap_cves_at_500(evidence, monkeypatch, tmp_path):
    _, cve = evidence
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a: [])
    rows = [dict(cve, cve_id=f"CVE-2026-{10000+i}") for i in range(1031)]

    def cves_for_day(conn, day, limit=200):
        assert limit is None, "Archive exports must request the complete day"
        return rows if limit is None else rows[:limit]

    monkeypatch.setattr(worker, "list_cves_for_day", cves_for_day)
    payload = worker._build_feed_day_payload(
        None, day_key="2026-09-18", site_root=str(tmp_path), tz=timezone.utc, tz_name="UTC",
    )
    assert payload["counts"] == {"article": 0, "cve": 1031}
    assert len({item["cve_id"] for item in payload["items"]}) == 1031


def test_full_day_excludes_suppressed_articles(evidence, monkeypatch, tmp_path):
    article, _ = evidence
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a: [
        article, dict(article, id=8, meta_json=json.dumps({"suppressed": True})),
    ])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **kw: [])
    monkeypatch.setattr(worker, "list_event_keys_for_articles", lambda *a: {})
    monkeypatch.setattr(worker, "list_article_cve_tags", lambda *a: {})
    payload = worker._build_feed_day_payload(
        None, day_key="2026-09-18", site_root=str(tmp_path), tz=timezone.utc, tz_name="UTC",
    )
    assert payload["counts"] == {"article": 1, "cve": 0}
    assert [item["article_id"] for item in payload["items"]] == [7]


def test_regulatory_identifier_is_on_public_news_item_once(evidence, monkeypatch, tmp_path):
    article, _ = evidence
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a: [
        article, dict(article, id=8, source_id="web_enrich"),
    ])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **kw: [])
    monkeypatch.setattr(worker, "list_event_keys_for_articles", lambda *a: {})
    monkeypatch.setattr(worker, "list_article_cve_tags", lambda *a: {})
    calls = []
    def regulatory(_conn, *, article_ids):
        calls.append(article_ids)
        return [{"article_id": 7, "kind": "regulatory_guidance", "jurisdictions": [{"code": "EU", "role": "legal_scope"}]}]
    monkeypatch.setattr(article_hosted_jobs, "regulatory_rows", regulatory)
    payload = worker._build_feed_day_payload(
        None, day_key="2026-09-18", site_root=str(tmp_path), tz=timezone.utc, tz_name="UTC",
    )
    assert calls == [[7]]
    assert "regulatory" not in payload
    assert payload["counts"] == {"article": 1, "cve": 0}
    assert len(payload["items"]) == 1
    assert payload["items"][0]["kind"] == "article"
    assert payload["items"][0]["sections"] == ["news", "regulatory"]
    assert payload["items"][0]["regulatory"] == {
        "kind": "regulatory_guidance", "jurisdictions": [{"code": "EU", "role": "legal_scope"}],
    }


def test_regulatory_section_does_not_change_historical_json(evidence, monkeypatch, tmp_path):
    article, _ = evidence
    monkeypatch.setenv("SV_REGULATORY_FEED_START_DAY", "2026-09-19")
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a: [article])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **kw: [])
    monkeypatch.setattr(worker, "list_event_keys_for_articles", lambda *a: {})
    monkeypatch.setattr(worker, "list_article_cve_tags", lambda *a: {})
    payload = worker._build_feed_day_payload(
        None, day_key="2026-09-18", site_root=str(tmp_path), tz=timezone.utc, tz_name="UTC",
    )
    assert "regulatory" not in payload
    assert "regulatory" not in payload["items"][0]
    assert "sections" not in payload["items"][0]


def test_full_day_excludes_event_research_articles(evidence, monkeypatch, tmp_path):
    article, _ = evidence
    monkeypatch.setattr(worker, "list_articles_for_day", lambda *a: [
        article, dict(article, id=8, source_id="web_enrich", source_name="Web Enrichment"),
    ])
    monkeypatch.setattr(worker, "list_cves_for_day", lambda *a, **kw: [])
    monkeypatch.setattr(worker, "list_event_keys_for_articles", lambda *a: {})
    monkeypatch.setattr(worker, "list_article_cve_tags", lambda *a: {})
    payload = worker._build_feed_day_payload(
        None, day_key="2026-09-18", site_root=str(tmp_path), tz=timezone.utc, tz_name="UTC",
    )
    assert payload["counts"] == {"article": 1, "cve": 0}
    assert [item["article_id"] for item in payload["items"]] == [7]
