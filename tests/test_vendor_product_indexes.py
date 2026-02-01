import logging
from uuid import uuid4
from pathlib import Path

from sempervigil.storage import init_db, upsert_product, upsert_vendor
from sempervigil.utils import utc_now_iso
from sempervigil.worker import _write_vendor_product_indexes


def _seed_vendor_product_article(conn) -> int:
    now = utc_now_iso()
    stable_id = f"stable-{uuid4().hex}"
    url = f"https://example.com/{stable_id}"
    conn.execute(
        """
        INSERT INTO sources (id, name, enabled, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET name = excluded.name
        """,
        ("src_test", "Test Source", 1, now, now),
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
            "src_test",
            stable_id,
            url,
            url,
            "Example Article",
            now,
            None,
            now,
            0,
            now,
            now,
            1,
        ),
    )
    article_id = int(cursor.fetchone()[0])
    vendor_id = upsert_vendor(conn, "Acme")
    product_id, _ = upsert_product(conn, vendor_id, "Widget")
    conn.execute(
        """
        INSERT INTO article_products (article_id, product_id, source, evidence_json, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (article_id, product_id, "llm", "{}", now),
    )
    conn.commit()
    return article_id


def _ensure_article_tags_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS article_tags (
            article_id BIGINT NOT NULL REFERENCES articles(id),
            tag TEXT NOT NULL,
            tag_type TEXT NULL,
            PRIMARY KEY(article_id, tag)
        )
        """
    )
    conn.commit()


def test_rebuild_vendor_products_without_article_tags(tmp_path: Path) -> None:
    conn = init_db()
    conn.execute("DROP TABLE IF EXISTS article_tags")
    conn.commit()
    try:
        _seed_vendor_product_article(conn)
        stats = _write_vendor_product_indexes(conn, str(tmp_path), "UTC", logging.getLogger("test"))
        assert stats["vendors"] >= 1
        assert stats["products"] >= 1
        assert (tmp_path / "data" / "vendors.json").exists()
        assert (tmp_path / "data" / "products.json").exists()
    finally:
        _ensure_article_tags_table(conn)


def test_rebuild_vendor_products_with_article_tags(tmp_path: Path) -> None:
    conn = init_db()
    _ensure_article_tags_table(conn)
    article_id = _seed_vendor_product_article(conn)
    conn.execute(
        "INSERT INTO article_tags (article_id, tag, tag_type) VALUES (%s, %s, %s)",
        (article_id, "demo-tag", None),
    )
    conn.commit()
    stats = _write_vendor_product_indexes(conn, str(tmp_path), "UTC", logging.getLogger("test"))
    assert stats["vendors"] >= 1
    assert stats["products"] >= 1
