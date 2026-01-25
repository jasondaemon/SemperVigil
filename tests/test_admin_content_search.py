import copy

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, DEFAULT_CVE_SETTINGS, set_cve_settings, set_runtime_config
from sempervigil.models import Article
from sempervigil.storage import init_db, insert_articles, upsert_cve, upsert_source


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
    set_cve_settings(conn, copy.deepcopy(DEFAULT_CVE_SETTINGS))
    return conn


def test_content_search_mixed_results(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")

    upsert_source(
        conn,
        {
            "id": "source-1",
            "name": "Example Source",
            "enabled": True,
            "url": "https://example.com/feed.xml",
        },
    )
    article = Article(
        id=None,
        stable_id="stable-1",
        original_url="https://example.com/article-1",
        normalized_url="https://example.com/article-1",
        title="Widget advisory roundup",
        source_id="source-1",
        published_at="2025-01-03T00:00:00Z",
        published_at_source="published",
        ingested_at="2025-01-03T01:00:00Z",
        summary="Widget summary",
        tags=["widget"],
    )
    insert_articles(conn, [article])
    upsert_cve(
        conn,
        cve_id="CVE-2025-1000",
        published_at="2025-01-01T00:00:00Z",
        last_modified_at="2025-01-02T00:00:00Z",
        preferred_cvss_version="3.1",
        preferred_base_score=7.5,
        preferred_base_severity="HIGH",
        preferred_vector="AV:N/AC:L",
        cvss_v40_json=None,
        cvss_v31_json=None,
        description_text="Widget vulnerability",
        affected_products=["widget"],
        affected_cpes=["cpe:2.3:a:vendor:widget:*:*:*:*:*:*:*:*"],
        reference_domains=["example.com"],
    )

    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.get("/admin/api/content/search?query=widget&type=all")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 2
    types = {item["type"] for item in data["items"]}
    assert "article" in types
    assert "cve" in types
