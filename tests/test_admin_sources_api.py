import copy
import uuid
from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.models import SourceTactic
from sempervigil.storage import init_db, list_tactics, upsert_source, upsert_tactic


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


def test_create_source_creates_tactic(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    conn = init_db()
    source_id = f"test-source-{uuid.uuid4().hex}"
    payload = {
        "id": source_id,
        "name": "Test Source",
        "kind": "rss",
        "url": "https://example.com/feed",
        "enabled": True,
        "interval_minutes": 30,
    }
    from sempervigil.services.sources_service import create_source
    from sempervigil.storage import list_tactics_for_source

    create_source(conn, payload)
    tactics = list_tactics_for_source(conn, source_id)
    assert len(tactics) == 1
    assert tactics[0].tactic_type == "rss"
    conn.close()


def test_upsert_tactic_replaces_existing(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    conn = init_db()
    source_id = f"test-source-{uuid.uuid4().hex}"
    upsert_source(
        conn,
        {
            "id": source_id,
            "name": "Test Source",
            "enabled": True,
            "base_url": "https://www.wired.com/feed/category/security/latest/rss",
            "topic_key": None,
            "default_frequency_minutes": 30,
            "pause_until": None,
            "paused_reason": None,
            "robots_notes": None,
        },
    )
    upsert_tactic(
        conn,
        SourceTactic(
            id=None,
            source_id=source_id,
            tactic_type="rss",
            enabled=True,
            priority=0,
            config={"feed_url": "https://www.wired.com/about/rss-feeds/"},
            last_success_at=None,
            last_error_at=None,
            error_streak=0,
        ),
    )
    existing_id = list_tactics(conn, source_id)[0].id
    upsert_tactic(
        conn,
        SourceTactic(
            id=existing_id,
            source_id=source_id,
            tactic_type="rss",
            enabled=True,
            priority=0,
            config={"feed_url": "https://www.wired.com/feed/category/security/latest/rss"},
            last_success_at=None,
            last_error_at=None,
            error_streak=0,
        ),
    )
    tactics = list_tactics(conn, source_id)
    assert len(tactics) == 1
    assert tactics[0].config.get("feed_url") == "https://www.wired.com/feed/category/security/latest/rss"
    conn.close()
