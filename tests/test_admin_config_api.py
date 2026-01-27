import copy

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.storage import init_db


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


def test_admin_runtime_config_get_put(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    client = TestClient(app)

    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.get("/admin/config/runtime")
    assert response.status_code == 200
    payload = response.json()
    assert payload["config"]["app"]["name"] == DEFAULT_CONFIG["app"]["name"]

    updated = copy.deepcopy(payload["config"])
    updated["app"]["name"] = "NewName"
    response = client.put("/admin/config/runtime", json={"config": updated})
    assert response.status_code == 200

    response = client.get("/admin/config/runtime")
    assert response.status_code == 200
    assert response.json()["config"]["app"]["name"] == "NewName"


def test_admin_runtime_config_rejects_invalid(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    client = TestClient(app)

    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.put("/admin/config/runtime", json={"config": {"app": {"name": "Bad"}}})
    assert response.status_code == 400
