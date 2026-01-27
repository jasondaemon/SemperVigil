import base64
import copy
from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.security.secrets import MASTER_KEY_ENV, KEY_ID_ENV
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


def test_admin_ai_provider_secret_flow(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
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
    assert '"api_key"' not in secret_response.text
    assert "key_last4" in secret_response.text

    list_response = client.get("/admin/ai/providers")
    assert list_response.status_code == 200
    providers = list_response.json()
    assert providers[0]["key_last4"] == "cret"
