import copy

from fastapi.testclient import TestClient

from sempervigil.admin import app
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.storage import init_db, insert_source_health_event


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


def test_source_health_history_endpoint(tmp_path, monkeypatch):
    _seed_runtime_config(tmp_path, monkeypatch)
    client = TestClient(app)

    conn = init_db(str(tmp_path / "data" / "state.sqlite3"))
    conn.execute(
        "INSERT INTO sources (id, name, enabled, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        ("test-source", "Test Source", 1, "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
    )
    conn.commit()
    insert_source_health_event(
        conn,
        source_id="test-source",
        ts="2024-01-02T00:00:00+00:00",
        ok=True,
        found_count=5,
        accepted_count=2,
        seen_count=1,
        filtered_count=1,
        error_count=1,
        last_error=None,
        duration_ms=1200,
    )

    response = client.get("/sources/test-source/health?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert data[0]["source_id"] == "test-source"
