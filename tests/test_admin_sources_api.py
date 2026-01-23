import os
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from sempervigil.admin import app


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


def test_sources_crud(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    monkeypatch.setenv("SV_CONFIG_PATH", str(cfg_path))
    monkeypatch.delenv("SV_ADMIN_TOKEN", raising=False)
    client = TestClient(app)

    payload = {
        "id": "test-source",
        "name": "Test Source",
        "kind": "rss",
        "url": "https://example.com/feed",
        "enabled": True,
        "interval_minutes": 30,
        "tags": ["security"],
    }
    response = client.post("/sources", json=payload)
    assert response.status_code == 200

    response = client.get("/sources")
    assert response.status_code == 200
    sources = response.json()
    assert any(item["id"] == "test-source" for item in sources)

    response = client.get("/sources/test-source")
    assert response.status_code == 200
    assert response.json()["name"] == "Test Source"

    response = client.put("/sources/test-source", json={"enabled": False})
    assert response.status_code == 200
    assert response.json()["enabled"] is False

    response = client.delete("/sources/test-source")
    assert response.status_code == 200


def test_sources_requires_cookie_when_token_set(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    monkeypatch.setenv("SV_CONFIG_PATH", str(cfg_path))
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    client = TestClient(app)

    payload = {
        "id": "test-source",
        "name": "Test Source",
        "kind": "rss",
        "url": "https://example.com/feed",
        "enabled": True,
        "interval_minutes": 30,
    }
    response = client.post("/sources", json=payload)
    assert response.status_code == 401

    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.post("/sources", json=payload)
    assert response.status_code == 200
