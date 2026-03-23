import copy
import json

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.storage import init_db
from sempervigil.utils import parse_log_line


def _seed_runtime_config(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "log"
    monkeypatch.setenv("SV_DATA_DIR", str(data_dir))
    monkeypatch.setenv("SV_LOG_FILE", str(log_dir / "admin.log"))
    conn = init_db()
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["paths"]["data_dir"] = str(data_dir)
    config["paths"]["logs_dir"] = str(log_dir)
    config["paths"]["output_dir"] = str(tmp_path / "site" / "content" / "posts")
    config["paths"]["run_reports_dir"] = str(data_dir / "reports")
    config["publishing"]["json_index_path"] = str(
        tmp_path / "site" / "static" / "sempervigil" / "index.json"
    )
    set_runtime_config(conn, config)
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def test_parse_log_line_supports_json_and_key_value():
    parsed_json = parse_log_line(
        '{"event":"job_claimed","job_type":"ingest_source","runner_type":"fetch"}'
    )
    assert parsed_json["event"] == "job_claimed"
    assert parsed_json["job_type"] == "ingest_source"
    assert parsed_json["runner_type"] == "fetch"

    parsed_kv = parse_log_line(
        "event=job_succeeded job_type=fetch_article_content job_id=job_123 count=2"
    )
    assert parsed_kv["event"] == "job_succeeded"
    assert parsed_kv["job_type"] == "fetch_article_content"
    assert parsed_kv["job_id"] == "job_123"
    assert parsed_kv["count"] == 2


def test_logs_query_returns_filtered_entries(tmp_path, monkeypatch):
    log_dir = _seed_runtime_config(tmp_path, monkeypatch)
    admin_log = log_dir / "admin.log"
    worker_log = log_dir / "worker_fetch.log"
    admin_log.write_text(
        json.dumps(
            {
                "ts": "2026-03-23T19:05:00+00:00",
                "level": "INFO",
                "event": "admin_loaded",
                "message": "admin_loaded",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    worker_log.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": "2026-03-23T19:05:01+00:00",
                        "level": "INFO",
                        "event": "job_claimed",
                        "job_type": "ingest_source",
                        "runner_type": "fetch",
                        "job_id": "job_ingest",
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-03-23T19:05:02+00:00",
                        "level": "INFO",
                        "event": "job_succeeded",
                        "job_type": "fetch_article_content",
                        "runner_type": "fetch",
                        "job_id": "job_fetch",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    client = TestClient(app)

    response = client.get(
        "/admin/api/logs/query",
        params={"service": "worker", "runner": "fetch", "job_type": "ingest_source", "lines": 50},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["service"] == "worker"
    assert len(payload["entries"]) == 1
    assert payload["entries"][0]["event"] == "job_claimed"
    assert payload["entries"][0]["job_type"] == "ingest_source"
    assert payload["entries"][0]["runner_type"] == "fetch"


def test_logs_query_supports_all_service_and_non_worker_logs(tmp_path, monkeypatch):
    log_dir = _seed_runtime_config(tmp_path, monkeypatch)
    (log_dir / "orchestrator.log").write_text(
        json.dumps(
            {
                "ts": "2026-03-23T19:06:00+00:00",
                "level": "INFO",
                "event": "orchestrator_tick",
                "message": "orchestrator_tick",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (log_dir / "openai_http.log").write_text(
        json.dumps(
            {
                "ts": "2026-03-23T19:06:01+00:00",
                "level": "INFO",
                "event": "openai_request",
                "job_type": "build_daily_brief",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    client = TestClient(app)

    openai_response = client.get("/admin/api/logs/query", params={"service": "openai_prompts"})
    assert openai_response.status_code == 200
    openai_entries = openai_response.json()["entries"]
    assert len(openai_entries) == 1
    assert openai_entries[0]["event"] == "openai_request"

    all_response = client.get("/admin/api/logs/query", params={"service": "all", "lines": 50})
    assert all_response.status_code == 200
    events = {entry["event"] for entry in all_response.json()["entries"]}
    assert "orchestrator_tick" in events
    assert "openai_request" in events
