import logging

from sempervigil.config import load_runtime_config
from sempervigil.storage import claim_next_job, enqueue_job, get_job, init_db
from sempervigil import worker


def test_write_article_markdown_job_skipped_when_disabled(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SV_ENABLE_ARTICLE_MARKDOWN", "0")
    conn = init_db()
    config = load_runtime_config(conn)

    job_id = enqueue_job(conn, "write_article_markdown", {"source_id": "source-1"})
    job = claim_next_job(conn, "worker-1", allowed_types=["write_article_markdown"])
    assert job is not None

    logger = logging.getLogger("test")
    worker._process_claimed_job(conn, config, job, logger)

    updated = get_job(conn, job_id)
    assert updated is not None
    assert updated.status == "succeeded"
    assert (updated.result or {}).get("status") == "skipped"
