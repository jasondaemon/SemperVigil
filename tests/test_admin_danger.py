import copy

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.models import Article
from sempervigil.storage import (
    create_event,
    delete_all_articles,
    delete_all_cves,
    delete_all_events,
    init_db,
    insert_articles,
    insert_cve_snapshot,
    upsert_event_item,
    upsert_cve,
    upsert_source,
)


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
    return conn


def _insert_article(conn):
    upsert_source(conn, {"id": "source-1", "name": "Example", "enabled": True})
    article = Article(
        id=None,
        stable_id="stable-1",
        original_url="https://example.com/1",
        normalized_url="https://example.com/1",
        title="Example title",
        source_id="source-1",
        published_at="2025-01-01T00:00:00Z",
        published_at_source="published",
        ingested_at="2025-01-01T01:00:00Z",
        summary=None,
        tags=["tag-a", "tag-b"],
    )
    insert_articles(conn, [article])


def _insert_cve(conn):
    upsert_cve(
        conn,
        cve_id="CVE-2025-0001",
        published_at="2025-01-01T00:00:00Z",
        last_modified_at="2025-01-02T00:00:00Z",
        preferred_cvss_version="3.1",
        preferred_base_score=7.5,
        preferred_base_severity="HIGH",
        preferred_vector="AV:N",
        cvss_v40_json=None,
        cvss_v31_json=None,
        description_text="Test CVE",
        affected_products=[],
        affected_cpes=[],
        reference_domains=[],
    )
    insert_cve_snapshot(
        conn,
        cve_id="CVE-2025-0001",
        observed_at="2025-01-02T00:00:00Z",
        nvd_last_modified_at="2025-01-02T00:00:00Z",
        preferred_cvss_version="3.1",
        preferred_base_score=7.5,
        preferred_base_severity="HIGH",
        preferred_vector="AV:N",
        cvss_v40_json=None,
        cvss_v31_json=None,
        snapshot_hash="hash-1",
    )


def _insert_event(conn):
    event_id = create_event(
        conn,
        kind="cve_cluster",
        title="Test event",
        severity="HIGH",
        first_seen_at="2025-01-01T00:00:00Z",
        last_seen_at="2025-01-02T00:00:00Z",
    )
    upsert_event_item(conn, event_id, "cve", "CVE-2025-0001")
    return event_id


def test_delete_all_articles(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    _insert_article(conn)
    stats = delete_all_articles(conn)
    assert stats["tables"]["articles"] == 1
    assert stats["tables"]["article_tags"] >= 1


def test_delete_all_cves(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    _insert_cve(conn)
    stats = delete_all_cves(conn)
    assert stats["tables"]["cves"] == 1
    assert stats["tables"]["cve_snapshots"] >= 1


def test_delete_all_events(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    _insert_cve(conn)
    _insert_event(conn)
    stats = delete_all_events(conn)
    assert stats["tables"]["events"] == 1
    assert stats["tables"]["event_items"] >= 1


def test_admin_clear_requires_confirm(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.post("/admin/api/admin/clear/articles", json={"confirm": "nope"})
    assert response.status_code == 400


def test_admin_clear_articles(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    _insert_article(conn)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.post(
        "/admin/api/admin/clear/articles",
        json={"confirm": "DELETE_ALL_ARTICLES", "delete_files": False},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["stats"]["tables"]["articles"] == 1


def test_admin_clear_events(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    _insert_cve(conn)
    _insert_event(conn)
    monkeypatch.setenv("SV_ADMIN_TOKEN", "secret")
    client = TestClient(app)
    login = client.post("/ui/login", json={"token": "secret"})
    assert login.status_code == 200

    response = client.post(
        "/admin/api/admin/clear/events",
        json={"confirm": "DELETE_ALL_EVENTS"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["stats"]["tables"]["events"] == 1
