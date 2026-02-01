from __future__ import annotations

import uuid

from sempervigil.models import Article
from sempervigil.storage import (
    count_articles_for_product,
    infer_article_products_from_cves,
    init_db,
    insert_articles,
    link_cve_product,
    list_articles_for_product,
    upsert_product,
    upsert_vendor,
)
from sempervigil.utils import utc_now_iso


def test_article_products_migration_exists():
    conn = init_db()
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'article_products'
        """
    ).fetchone()
    assert row is not None


def test_infer_article_products_from_cves_links_products():
    conn = init_db()
    unique = uuid.uuid4().hex[:8]
    cve_id = f"CVE-2999-{unique}"
    vendor_id = upsert_vendor(conn, f"Vendor {unique}")
    product_id, _ = upsert_product(conn, vendor_id, f"Product {unique}")
    conn.execute(
        """
        INSERT INTO cves (cve_id, published_at, last_modified_at, description_text, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (cve_id, utc_now_iso(), utc_now_iso(), "test", utc_now_iso()),
    )
    conn.commit()
    link_cve_product(conn, cve_id, product_id, source="test")

    article = Article(
        id=None,
        stable_id=f"stable-{unique}",
        original_url=f"https://example.com/{unique}",
        normalized_url=f"https://example.com/{unique}",
        title=f"Test Article {unique}",
        source_id=f"source-{unique}",
        published_at=utc_now_iso(),
        published_at_source=None,
        ingested_at=utc_now_iso(),
        summary=None,
        tags=[],
    )
    insert_articles(conn, [article])
    article_id = conn.execute(
        "SELECT id FROM articles WHERE stable_id = %s",
        (article.stable_id,),
    ).fetchone()[0]

    stats = infer_article_products_from_cves(conn, int(article_id), [cve_id])
    assert stats["links"] >= 1

    row = conn.execute(
        "SELECT source FROM article_products WHERE article_id = %s AND product_id = %s",
        (article_id, product_id),
    ).fetchone()
    assert row is not None
    assert row[0] == "cve_inferred"


def test_list_articles_for_product_returns_items():
    conn = init_db()
    unique = uuid.uuid4().hex[:8]
    vendor_id = upsert_vendor(conn, f"Vendor {unique}")
    product_id, _ = upsert_product(conn, vendor_id, f"Product {unique}")
    article = Article(
        id=None,
        stable_id=f"stable-{unique}",
        original_url=f"https://example.com/{unique}",
        normalized_url=f"https://example.com/{unique}",
        title=f"Test Article {unique}",
        source_id=f"source-{unique}",
        published_at=utc_now_iso(),
        published_at_source=None,
        ingested_at=utc_now_iso(),
        summary=None,
        tags=[],
    )
    insert_articles(conn, [article])
    article_id = conn.execute(
        "SELECT id FROM articles WHERE stable_id = %s",
        (article.stable_id,),
    ).fetchone()[0]
    # link manually
    conn.execute(
        """
        INSERT INTO article_products (article_id, product_id, source, evidence_json, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (article_id, product_id, "manual", None, utc_now_iso()),
    )
    conn.commit()

    items, total = list_articles_for_product(conn, int(product_id), page=1, page_size=10)
    assert total >= 1
    assert any(item["id"] == article_id for item in items)
    assert count_articles_for_product(conn, int(product_id)) >= 1
