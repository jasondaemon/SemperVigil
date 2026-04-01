from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from sempervigil.cve_sync import process_cve_item
from sempervigil.storage import (
    init_db,
    link_article_product,
    link_article_threat_actor,
    link_cve_products_from_items,
    link_cve_threat_actor,
    upsert_product,
    upsert_threat_actor,
    upsert_vendor,
)
from sempervigil.utils import utc_now_iso
from sempervigil.worker import _refresh_feed_data_files


def _insert_article(conn, title: str, source_id: str, url: str, published_at: str) -> int:
    conn.execute(
        """
        INSERT INTO sources (id, name, enabled, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET name = excluded.name
        """,
        (source_id, source_id, 1, published_at, published_at),
    )
    cursor = conn.execute(
        """
        INSERT INTO articles (
            source_id, stable_id, original_url, normalized_url, title,
            published_at, published_at_source, ingested_at, is_commercial,
            created_at, updated_at, has_full_content
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            source_id,
            f"stable-{uuid4().hex}",
            url,
            url,
            title,
            published_at,
            None,
            published_at,
            0,
            published_at,
            published_at,
            1,
        ),
    )
    article_id = int(cursor.fetchone()[0])
    conn.commit()
    return article_id


def test_feed_json_keeps_all_relationship_facets(tmp_path: Path) -> None:
    conn = init_db()
    now = utc_now_iso()
    site_root = tmp_path / "site"
    output_dir = site_root / "content" / "posts"
    output_dir.mkdir(parents=True, exist_ok=True)

    article_id = _insert_article(
        conn,
        title="Example Article",
        source_id="src_test",
        url="https://example.com/article",
        published_at=now,
    )
    vendor_id = upsert_vendor(conn, "Acme")
    product_id, _ = upsert_product(conn, vendor_id, "Widget")
    link_article_product(conn, article_id, product_id, source="llm")
    vendor_id_2 = upsert_vendor(conn, "Globex")
    product_id_2, _ = upsert_product(conn, vendor_id_2, "Thing")
    link_article_product(conn, article_id, product_id_2, source="llm")
    article_actor_id = upsert_threat_actor(conn, "apt-example", "APT Example", actor_type="APT")
    link_article_threat_actor(conn, article_id, article_actor_id)
    article_actor_id_2 = upsert_threat_actor(conn, "apt-example-2", "APT Example 2", actor_type="APT")
    link_article_threat_actor(conn, article_id, article_actor_id_2)

    cve_item = {
        "id": "CVE-2026-9999",
        "published": now,
        "lastModified": now,
        "descriptions": [{"lang": "en", "value": "Example CVE description"}],
        "metrics": {
            "cvssMetricV31": [
                {
                    "cvssData": {
                        "baseScore": 8.8,
                        "baseSeverity": "HIGH",
                        "vectorString": "AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
                    }
                }
            ]
        },
    }
    process_cve_item(conn, cve_item, prefer_v4=True, filters={}, scope_min_cvss=None, watchlist_enabled=False)
    link_cve_products_from_items(
        conn,
        cve_id="CVE-2026-9999",
        items=[
            {"vendor": "Acme", "product": "Widget"},
            {"vendor": "Globex", "product": "Thing"},
        ],
    )
    cve_actor_id = upsert_threat_actor(conn, "apt-cve", "APT CVE", actor_type="APT")
    link_cve_threat_actor(conn, "CVE-2026-9999", cve_actor_id)
    cve_actor_id_2 = upsert_threat_actor(conn, "apt-cve-2", "APT CVE 2", actor_type="APT")
    link_cve_threat_actor(conn, "CVE-2026-9999", cve_actor_id_2)
    conn.commit()

    config = SimpleNamespace(
        paths=SimpleNamespace(output_dir=str(output_dir)),
        app=SimpleNamespace(timezone="UTC"),
    )
    _refresh_feed_data_files(conn, config, logging.getLogger("test"))

    day_key = now.split("T", 1)[0]
    payload = json.loads((site_root / "static" / "feed" / "days" / f"{day_key}.json").read_text(encoding="utf-8"))
    article = next(item for item in payload["items"] if item.get("kind") == "article")
    cve = next(item for item in payload["items"] if item.get("kind") == "cve")

    assert len(article["vendors"]) == 2
    assert len(article["product_items"]) == 2
    assert len(article["threat_actors"]) == 2
    assert len(article["facets"]) == 6
    assert {facet["kind"] for facet in article["facets"]} == {"vendor", "product", "threat"}
    assert cve["url"].startswith("https://nvd.nist.gov/vuln/detail/")
    assert cve["nvd_url"].startswith("https://nvd.nist.gov/vuln/detail/")
    assert len(cve["vendor_products"]) == 2
    assert len(cve["vendors"]) == 2
    assert len(cve["product_items"]) == 2
    assert len(cve["threat_actors"]) == 2
    assert len(cve["facets"]) == 6
    assert {facet["kind"] for facet in cve["facets"]} == {"vendor", "product", "threat"}
