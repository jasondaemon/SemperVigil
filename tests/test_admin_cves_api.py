import base64
import copy

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, DEFAULT_CVE_SETTINGS, set_cve_settings, set_runtime_config
from sempervigil.security.secrets import MASTER_KEY_ENV, KEY_ID_ENV
from sempervigil.storage import init_db, upsert_cve


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
    set_cve_settings(conn, copy.deepcopy(DEFAULT_CVE_SETTINGS))
    return conn


def test_cve_settings_api(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    master = base64.urlsafe_b64encode(b"c" * 32).decode("utf-8")
    monkeypatch.setenv(MASTER_KEY_ENV, master)
    monkeypatch.setenv(KEY_ID_ENV, "v1")

    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.get("/admin/api/cves/settings")
    assert response.status_code == 200
    settings = response.json()["settings"]
    assert settings["enabled"] is True

    settings["enabled"] = False
    response = client.put("/admin/api/cves/settings", json={"settings": settings})
    assert response.status_code == 200

    response = client.get("/admin/api/cves/settings")
    assert response.status_code == 200
    assert response.json()["settings"]["enabled"] is False


def test_cve_search_api(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    upsert_cve(
        conn,
        cve_id="CVE-2025-0001",
        published_at="2025-01-01T00:00:00Z",
        last_modified_at="2025-01-02T00:00:00Z",
        preferred_cvss_version="3.1",
        preferred_base_score=7.5,
        preferred_base_severity="HIGH",
        preferred_vector="AV:N/AC:L",
        cvss_v40_json=None,
        cvss_v31_json=None,
        description_text="Test CVE description",
        affected_products=["widget"],
        affected_cpes=["cpe:2.3:a:vendor:widget:*:*:*:*:*:*:*:*"],
        reference_domains=["example.com"],
    )

    response = client.get("/admin/api/cves?query=CVE-2025-0001")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    assert any(item["cve_id"] == "CVE-2025-0001" for item in data["items"])
