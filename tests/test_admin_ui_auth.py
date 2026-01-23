import yaml
from pathlib import Path

from fastapi.testclient import TestClient

from sempervigil.admin import ADMIN_COOKIE_NAME, app


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


def test_ui_login_cookie_flow(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    monkeypatch.setenv("SV_CONFIG_PATH", str(cfg_path))
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")

    client = TestClient(app)
    response = client.get("/ui", allow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/ui/login"

    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200
    assert ADMIN_COOKIE_NAME in login.cookies

    page = client.get("/ui")
    assert page.status_code == 200

    static = client.get("/ui/static/admin/admin.css")
    assert static.status_code == 200


def test_ui_redirects_to_trailing_slash_without_token(tmp_path, monkeypatch):
    cfg_path = _write_config(tmp_path)
    monkeypatch.setenv("SV_CONFIG_PATH", str(cfg_path))
    monkeypatch.delenv("SV_ADMIN_TOKEN", raising=False)

    client = TestClient(app)
    response = client.get("/ui", allow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/ui/"

    page = client.get("/ui/")
    assert page.status_code == 200
    assert "text/html" in page.headers.get("content-type", "")
