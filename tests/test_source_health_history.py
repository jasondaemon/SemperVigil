import yaml
from pathlib import Path

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.storage import init_db, insert_source_health_event


def _write_config(tmp_path: Path) -> Path:
    config = {
        "app": {"name": "SemperVigil", "timezone": "UTC"},
        "paths": {
            "data_dir": str(tmp_path / "data"),
            "output_dir": str(tmp_path / "site" / "content" / "posts"),
            "state_db": str(tmp_path / "data" / "state.sqlite3"),
            "run_reports_dir": str(tmp_path / "data" / "reports"),
        },
        "publishing": {
            "format": "hugo_markdown",
            "hugo_section": "posts",
            "write_json_index": False,
            "json_index_path": str(tmp_path / "site" / "static" / "sempervigil" / "index.json"),
        },
        "ingest": {
            "http": {
                "timeout_seconds": 10,
                "user_agent": "SemperVigil/Test",
                "max_retries": 0,
                "backoff_seconds": 1,
            },
            "dedupe": {"enabled": True, "strategy": "canonical_url_hash"},
            "filters": {"allow_keywords": [], "deny_keywords": []},
            "scheduling": {"default_run_interval_minutes": 60},
        },
        "jobs": {"lock_timeout_seconds": 120},
        "cve": {"enabled": False},
        "llm": {"enabled": False},
        "per_source_tweaks": {
            "url_normalization": {"strip_tracking_params": True, "tracking_params": []},
            "date_parsing": {"prefer_updated_if_published_missing": True},
        },
    }
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return cfg_path


def test_source_health_history_endpoint(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    monkeypatch.setenv("SV_CONFIG_PATH", str(cfg_path))
    client = TestClient(app)

    conn = init_db(str(tmp_path / "data" / "state.sqlite3"))
    conn.execute(
        "INSERT INTO sources (id, name, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        ("test-source", "Test Source", 1, "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    insert_source_health_event(
        conn,
        source_id="test-source",
        ts="2024-01-02T00:00:00+00:00",
        ok=True,
        found_count=5,
        accepted_count=2,
        seen_count=1,
        filtered_count=1,
        error_count=1,
        last_error=None,
        duration_ms=1200,
    )

    response = client.get("/sources/test-source/health?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert data[0]["source_id"] == "test-source"
