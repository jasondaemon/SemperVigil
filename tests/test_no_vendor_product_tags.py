from __future__ import annotations

import uuid

from sempervigil.models import Article
from sempervigil.storage import (
    init_db,
    insert_articles,
    link_article_product,
    upsert_product,
    upsert_vendor,
)
from sempervigil.utils import utc_now_iso


def test_vendor_product_links_do_not_write_tags():
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
    link_article_product(conn, int(article_id), int(product_id), source="llm")
    row = conn.execute(
        "SELECT COUNT(*) FROM article_tags WHERE tag LIKE 'vendor:%' OR tag LIKE 'product:%'"
    ).fetchone()
    assert int(row[0] or 0) == 0
