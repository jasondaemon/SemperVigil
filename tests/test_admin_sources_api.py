import copy
from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.storage import init_db


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


def test_sources_crud(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
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
    _seed_runtime_config(tmp_path, monkeypatch)
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
