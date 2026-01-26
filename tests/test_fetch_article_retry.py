import logging

from sempervigil.storage import (
    init_db,
    enqueue_job,
    claim_next_job,
)
from sempervigil.worker import _handle_fetch_article_content
from sempervigil.config import load_runtime_config


def test_fetch_article_missing_url_fails(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    conn = init_db(str(data_dir / "state.sqlite3"))
    config = load_runtime_config(conn)

    conn.execute(
        """
        INSERT INTO articles
            (source_id, stable_id, original_url, normalized_url, title, published_at,
             published_at_source, ingested_at, brief_day, is_commercial, content_fingerprint,
             extracted_text_path, extracted_text_hash, raw_html_path, raw_html_hash,
             meta_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "source-1",
            "stable-1",
            None,
            None,
            "Title",
            None,
            None,
            "2025-01-01T00:00:00Z",
            "2025-01-01",
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            "2025-01-01T00:00:00Z",
            "2025-01-01T00:00:00Z",
        ),
    )
    conn.commit()
    article_id = conn.execute("SELECT id FROM articles").fetchone()[0]

    enqueue_job(
        conn,
        "fetch_article_content",
        {"article_id": article_id, "source_id": "source-1"},
    )
    job = claim_next_job(conn, "worker-1", allowed_types=["fetch_article_content"])
    logger = logging.getLogger("test")
    try:
        _handle_fetch_article_content(conn, config, job, job.payload, logger=logger)
    except ValueError as exc:
        assert str(exc) == "article_not_found"
    else:
        raise AssertionError("expected article_not_found")
