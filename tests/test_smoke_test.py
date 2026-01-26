import copy
import logging

from sempervigil import worker
from sempervigil.config import DEFAULT_CONFIG, load_runtime_config, set_runtime_config
from sempervigil.storage import enqueue_job, get_job, init_db, upsert_source


def _seed_runtime_config(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    conn = init_db(str(data_dir / "state.sqlite3"))
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["paths"]["data_dir"] = str(data_dir)
    config["paths"]["state_db"] = str(data_dir / "state.sqlite3")
    config["paths"]["output_dir"] = str(tmp_path / "site" / "content" / "posts")
    config["paths"]["run_reports_dir"] = str(data_dir / "reports")
    config["publishing"]["json_index_path"] = str(
        tmp_path / "site" / "static" / "sempervigil" / "index.json"
    )
    set_runtime_config(conn, config)
    return conn


def test_smoke_test_uses_jobs_not_direct_writer(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    upsert_source(
        conn,
        {
            "id": "source-1",
            "name": "Source One",
            "type": "rss",
            "url": "https://example.com/feed.xml",
            "enabled": True,
            "tags": [],
        },
    )
    config = load_runtime_config(conn)
    job_id = enqueue_job(
        conn,
        "smoke_test",
        {
            "sources_limit": 1,
            "per_source_limit": 1,
            "skip_cve_sync": True,
            "skip_events": True,
            "skip_build": True,
        },
    )
    job = get_job(conn, job_id)
    assert job is not None

    calls = {"inline": 0}

    def _noop_inline(*args, **kwargs):
        calls["inline"] += 1
        allowed = kwargs.get("allowed_types") or []
        if calls["inline"] == 1:
            assert "ingest_source" in allowed
        if calls["inline"] == 2:
            assert "write_article_markdown" in allowed

    def _writer_called(*args, **kwargs):
        raise AssertionError("write_article_markdown should be called via jobs, not directly")

    monkeypatch.setattr(worker, "_run_jobs_inline", _noop_inline)
    monkeypatch.setattr(worker, "write_article_markdown", _writer_called)

    logger = logging.getLogger("test")
    result = worker._handle_smoke_test(conn, config, job, logger)
    assert result["steps"][0]["step"] == "ingest_sources"
    assert calls["inline"] >= 1
