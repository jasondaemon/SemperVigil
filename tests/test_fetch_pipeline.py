import logging

from sempervigil.config import load_runtime_config
from sempervigil.storage import claim_next_job, enqueue_job, init_db
from sempervigil.services import ai_service
from sempervigil import worker


def _insert_article(conn, url: str):
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
            url,
            url,
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
    return conn.execute("SELECT id FROM articles").fetchone()[0]


def _seed_summarize_profile(conn):
    provider = ai_service.create_provider(
        conn,
        {"name": "Test Provider", "type": "openai_compatible", "is_enabled": True},
    )
    model = ai_service.create_model(
        conn,
        {
            "provider_id": provider["id"],
            "model_name": "test-model",
            "is_enabled": True,
        },
    )
    prompt = ai_service.create_prompt(
        conn,
        {
            "name": "Summarize",
            "version": "v1",
            "system_template": "{{input}}",
            "user_template": "{{input}}",
        },
    )
    profile = ai_service.create_profile(
        conn,
        {
            "name": "Summarize Profile",
            "primary_provider_id": provider["id"],
            "primary_model_id": model["id"],
            "prompt_id": prompt["id"],
            "is_enabled": True,
        },
    )
    ai_service.set_pipeline_routing(conn, "summarize_article", profile["id"])
    return profile


def test_fetch_enqueues_summarize_when_llm_configured(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SV_LLM_BASE_URL", "http://llm")
    monkeypatch.setenv("SV_LLM_API_KEY", "sk-test")
    conn = init_db(str(data_dir / "state.sqlite3"))
    config = load_runtime_config(conn)
    _seed_summarize_profile(conn)

    article_id = _insert_article(conn, "https://example.com/a")
    enqueue_job(conn, "fetch_article_content", {"article_id": article_id, "source_id": "source-1"})
    job = claim_next_job(conn, "worker-1", allowed_types=["fetch_article_content"])

    def _fake_fetch(*_args, **_kwargs):
        return {"content_text": "content", "content_html": "<p>content</p>"}

    monkeypatch.setattr(worker, "fetch_article_content", _fake_fetch)
    logger = logging.getLogger("test")
    worker._handle_fetch_article_content(conn, config, job, job.payload, logger=logger)

    rows = conn.execute("SELECT job_type FROM jobs").fetchall()
    types = [row[0] for row in rows]
    assert "summarize_article_llm" in types
    assert "write_article_markdown" not in types


def test_fetch_enqueues_publish_when_llm_disabled(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    conn = init_db(str(data_dir / "state.sqlite3"))
    config = load_runtime_config(conn)

    article_id = _insert_article(conn, "https://example.com/b")
    enqueue_job(conn, "fetch_article_content", {"article_id": article_id, "source_id": "source-1"})
    job = claim_next_job(conn, "worker-1", allowed_types=["fetch_article_content"])

    def _fake_fetch(*_args, **_kwargs):
        return {"content_text": "content", "content_html": "<p>content</p>"}

    monkeypatch.setattr(worker, "fetch_article_content", _fake_fetch)
    logger = logging.getLogger("test")
    worker._handle_fetch_article_content(conn, config, job, job.payload, logger=logger)

    rows = conn.execute("SELECT job_type FROM jobs").fetchall()
    types = [row[0] for row in rows]
    assert "write_article_markdown" in types


def test_summarize_enqueues_publish(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SV_LLM_BASE_URL", "http://llm")
    monkeypatch.setenv("SV_LLM_API_KEY", "sk-test")
    conn = init_db(str(data_dir / "state.sqlite3"))
    config = load_runtime_config(conn)
    _seed_summarize_profile(conn)
    article_id = _insert_article(conn, "https://example.com/c")

    def _fake_summarize(**_kwargs):
        return {"summary": "short", "model": "test"}

    monkeypatch.setattr(worker, "summarize_with_llm", _fake_summarize)
    logger = logging.getLogger("test")
    worker._handle_summarize_article_llm(
        conn, config, {"article_id": article_id, "source_id": "source-1"}, logger=logger
    )

    rows = conn.execute("SELECT job_type FROM jobs").fetchall()
    types = [row[0] for row in rows]
    assert "write_article_markdown" in types
