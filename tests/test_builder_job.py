import copy

from sempervigil import builder
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.storage import enqueue_job, get_build_state, init_db, mark_build_dirty


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
    return conn


def test_build_job_writes_result(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    mark_build_dirty(conn, reason="test_build")
    enqueue_job(conn, "build_site", None)

    def fake_run(conn, job_id, builder_id, log_paths, lease_seconds):
        return 0, "stdout line", "stderr line", False, ["/bin/sh", "/tools/hugo-build.sh"]

    monkeypatch.setattr(builder, "_run_hugo_until_done", fake_run)
    builder.run_once("builder-test")

    row = conn.execute("SELECT result_json FROM jobs WHERE job_type = 'build_site'").fetchone()
    assert row is not None
    assert "stdout_tail" in row[0]
    assert "stderr_tail" in row[0]
    state = get_build_state(conn)
    assert state["dirty"] is False
    assert state["last_build_job_id"]


def test_build_job_heartbeat_keeps_lease_fresh(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    enqueue_job(conn, "build_site", None)

    heartbeat_calls = []

    def fake_run(conn, job_id, builder_id, log_paths, lease_seconds):
        heartbeat_calls.append((job_id, builder_id, lease_seconds))
        return 0, "stdout", "stderr", False, ["/bin/sh", "/tools/hugo-build.sh"]

    monkeypatch.setattr(builder, "_run_hugo_until_done", fake_run)
    builder.run_once("builder-test")
    assert heartbeat_calls
    assert heartbeat_calls[0][1] == "builder-test"
    assert heartbeat_calls[0][2] >= 1
