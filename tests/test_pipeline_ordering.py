import copy
import logging

from sempervigil.config import DEFAULT_CONFIG, load_runtime_config, set_runtime_config
from sempervigil.ingest import SourceResult
from sempervigil.models import Article
from sempervigil.storage import init_db, upsert_source
from sempervigil import worker


def _seed_runtime_config(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    conn = init_db()
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["paths"]["data_dir"] = str(data_dir)
    config["paths"]["output_dir"] = str(tmp_path / "site" / "content" / "posts")
    config["paths"]["run_reports_dir"] = str(data_dir / "reports")
    config["publishing"]["json_index_path"] = str(
        tmp_path / "site" / "static" / "sempervigil" / "index.json"
    )
    set_runtime_config(conn, config)
    return conn


def _seed_source(conn):
    upsert_source(
        conn,
        {
            "id": "source-1",
            "name": "Source One",
            "type": "rss",
            "url": "https://example.com/feed",
            "enabled": True,
            "interval_minutes": 60,
        },
    )


def _stub_result(article: Article) -> SourceResult:
    return SourceResult(
        source_id=article.source_id,
        status="ok",
        http_status=200,
        found_count=1,
        accepted_count=1,
        skipped_duplicates=0,
        skipped_filters=0,
        skipped_missing_url=0,
        already_seen_count=0,
        error=None,
        articles=[article],
        decisions=[],
        raw_entry=None,
        notes=None,
    )


def test_ingest_does_not_enqueue_publish_when_fetch_enabled(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    _seed_source(conn)
    config = load_runtime_config(conn)
    article = Article(
        id=None,
        stable_id="abc123",
        original_url="https://example.com/a",
        normalized_url="https://example.com/a",
        title="Test",
        source_id="source-1",
        published_at=None,
        published_at_source=None,
        ingested_at="2025-01-01T00:00:00Z",
        summary="stub",
        tags=["test"],
    )

    def _fake_process_source(*_args, **_kwargs):
        return _stub_result(article)

    monkeypatch.setattr(worker, "process_source", _fake_process_source)
    logger = logging.getLogger("test")
    worker._handle_ingest_source(conn, config, {"source_id": "source-1"}, logger)

    rows = conn.execute("SELECT job_type FROM jobs").fetchall()
    types = [row[0] for row in rows]
    assert "fetch_article_content" in types
    assert "write_article_markdown" not in types


def test_ingest_enqueues_publish_when_fetch_disabled(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    _seed_source(conn)
    config = load_runtime_config(conn)
    monkeypatch.setenv("SV_FETCH_FULL_CONTENT", "0")
    article = Article(
        id=None,
        stable_id="abc124",
        original_url="https://example.com/b",
        normalized_url="https://example.com/b",
        title="Test2",
        source_id="source-1",
        published_at=None,
        published_at_source=None,
        ingested_at="2025-01-01T00:00:00Z",
        summary="stub",
        tags=["test"],
    )

    def _fake_process_source(*_args, **_kwargs):
        return _stub_result(article)

    monkeypatch.setattr(worker, "process_source", _fake_process_source)
    logger = logging.getLogger("test")
    worker._handle_ingest_source(conn, config, {"source_id": "source-1"}, logger)

    rows = conn.execute("SELECT job_type FROM jobs").fetchall()
    types = [row[0] for row in rows]
    assert "write_article_markdown" in types
