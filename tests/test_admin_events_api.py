import base64
import copy

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import (
    DEFAULT_CONFIG,
    DEFAULT_CVE_SETTINGS,
    DEFAULT_EVENTS_SETTINGS,
    set_cve_settings,
    set_events_settings,
    set_runtime_config,
)
from sempervigil.security.secrets import MASTER_KEY_ENV, KEY_ID_ENV
from sempervigil.storage import (
    init_db,
    link_cve_product,
    upsert_cve,
    upsert_event_for_cve,
    upsert_product,
    upsert_vendor,
)


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
    set_events_settings(conn, copy.deepcopy(DEFAULT_EVENTS_SETTINGS))
    return conn


def test_events_api_list_get_rebuild(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    master = base64.urlsafe_b64encode(b"c" * 32).decode("utf-8")
    monkeypatch.setenv(MASTER_KEY_ENV, master)
    monkeypatch.setenv(KEY_ID_ENV, "v1")

    vendor_id = upsert_vendor(conn, "Acme")
    product_id, _ = upsert_product(conn, vendor_id, "Widget")
    upsert_cve(
        conn,
        cve_id="CVE-2025-9999",
        published_at="2025-01-01T00:00:00Z",
        last_modified_at="2025-01-01T00:00:00Z",
        preferred_cvss_version="3.1",
        preferred_base_score=9.0,
        preferred_base_severity="CRITICAL",
        preferred_vector=None,
        cvss_v40_json=None,
        cvss_v31_json=None,
        description_text="Test CVE",
        affected_products=None,
        affected_cpes=None,
        reference_domains=None,
    )
    link_cve_product(conn, "CVE-2025-9999", product_id)
    event_id, _ = upsert_event_for_cve(
        conn,
        cve_id="CVE-2025-9999",
        published_at="2025-01-01T00:00:00Z",
        window_days=14,
        min_shared_products=1,
    )

    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.get("/admin/api/events")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 1

    detail = client.get(f"/admin/api/events/{event_id}")
    assert detail.status_code == 200
    assert detail.json()["id"] == event_id

    rebuild = client.post("/admin/api/events/rebuild", json={})
    assert rebuild.status_code == 200
    payload = rebuild.json()
    assert payload["status"] == "queued"
    assert payload["job_id"]
