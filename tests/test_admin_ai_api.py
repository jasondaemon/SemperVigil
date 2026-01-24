import base64
from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.security.secrets import MASTER_KEY_ENV, KEY_ID_ENV


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


def test_admin_ai_provider_secret_flow(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    monkeypatch.setenv("SV_CONFIG_PATH", str(cfg_path))
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    master = base64.urlsafe_b64encode(b"c" * 32).decode("utf-8")
    monkeypatch.setenv(MASTER_KEY_ENV, master)
    monkeypatch.setenv(KEY_ID_ENV, "v1")

    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    provider_payload = {
        "name": "OpenAI",
        "type": "openai_compatible",
        "base_url": "https://api.openai.com/v1",
    }
    response = client.post("/admin/ai/providers", json=provider_payload)
    assert response.status_code == 200
    provider_id = response.json()["id"]

    secret_response = client.post(
        f"/admin/ai/providers/{provider_id}/secret", json={"api_key": "supersecret"}
    )
    assert secret_response.status_code == 200
    assert "api_key" not in secret_response.text

    list_response = client.get("/admin/ai/providers")
    assert list_response.status_code == 200
    providers = list_response.json()
    assert providers[0]["api_key_last4"] == "cret"
