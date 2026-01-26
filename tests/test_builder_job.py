import copy

from sempervigil import builder
from sempervigil.config import DEFAULT_CONFIG, set_runtime_config
from sempervigil.storage import enqueue_job, init_db


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


def test_build_job_writes_result(tmp_path, monkeypatch):
    conn = _seed_runtime_config(tmp_path, monkeypatch)
    enqueue_job(conn, "build_site", None)

    def fake_run(conn, job_id):
        return 0, "stdout line", "stderr line", False

    monkeypatch.setattr(builder, "_run_hugo_until_done", fake_run)
    builder.run_once("builder-test")

    row = conn.execute("SELECT result_json FROM jobs WHERE job_type = 'build_site'").fetchone()
    assert row is not None
    assert "stdout_tail" in row[0]
    assert "stderr_tail" in row[0]
