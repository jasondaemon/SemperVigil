import copy

from sempervigil.config import DEFAULT_CONFIG, bootstrap_runtime_config, get_runtime_config, set_runtime_config
from sempervigil.storage import init_db


def test_bootstrap_creates_runtime_config(tmp_path):
    conn = init_db()
    cfg = bootstrap_runtime_config(conn)
    assert cfg == DEFAULT_CONFIG


def test_get_runtime_config_after_set(tmp_path):
    conn = init_db()
    custom = copy.deepcopy(DEFAULT_CONFIG)
    custom["app"]["name"] = "Test"
    set_runtime_config(conn, custom)
    cfg = get_runtime_config(conn)
    assert cfg["app"]["name"] == "Test"


def test_set_runtime_config_rejects_invalid(tmp_path):
    conn = init_db()
    invalid = {"app": {"name": "Bad"}}
    try:
        set_runtime_config(conn, invalid)
    except Exception as exc:  # noqa: BLE001
        assert "Invalid config.runtime" in str(exc)
    else:
        raise AssertionError("Expected validation error")
