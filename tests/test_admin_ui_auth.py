import copy

from fastapi.testclient import TestClient

from sempervigil.admin import ADMIN_COOKIE_NAME, app
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


def test_ui_login_cookie_flow(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")

    client = TestClient(app)
    response = client.get("/ui", follow_redirects=False)
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
    _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.delenv("SV_ADMIN_TOKEN", raising=False)

    client = TestClient(app)
    response = client.get("/ui", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/ui/"

    page = client.get("/ui/")
    assert page.status_code == 200
    assert "text/html" in page.headers.get("content-type", "")
