from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from uuid import uuid4
from pathlib import Path

from sempervigil.storage import init_db
from sempervigil.utils import utc_now_iso
from sempervigil.worker import _write_article_data_files


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


def test_article_data_ignores_vendor_product_tags(tmp_path: Path) -> None:
    conn = init_db()
    _ensure_article_tags_table(conn)
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
    conn.execute(
        "INSERT INTO article_tags (article_id, tag, tag_type) VALUES (%s, %s, %s)",
        (article_id, "vendor:Acme", None),
    )
    conn.execute(
        "INSERT INTO article_tags (article_id, tag, tag_type) VALUES (%s, %s, %s)",
        (article_id, "product:Widget", None),
    )
    conn.commit()

    output_dir = tmp_path / "site" / "content" / "posts"
    output_dir.mkdir(parents=True, exist_ok=True)
    config = SimpleNamespace(
        paths=SimpleNamespace(output_dir=str(output_dir)),
        app=SimpleNamespace(timezone="UTC"),
    )
    _write_article_data_files(conn, config, logging.getLogger("test"))
    recent_path = tmp_path / "site" / "data" / "articles" / "recent.json"
    payload = json.loads(recent_path.read_text(encoding="utf-8"))
    assert payload
    item = payload[0]
    assert item.get("vendors") == []
    assert item.get("product_items") == []
