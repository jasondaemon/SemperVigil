import copy

from sempervigil.config import DEFAULT_CVE_SETTINGS, bootstrap_cve_settings, get_cve_settings, set_cve_settings
from sempervigil.storage import init_db


def test_cve_settings_bootstrap(tmp_path):
    conn = init_db()
    settings = bootstrap_cve_settings(conn)
    assert settings == DEFAULT_CVE_SETTINGS


def test_cve_settings_update(tmp_path):
    conn = init_db()
    settings = copy.deepcopy(DEFAULT_CVE_SETTINGS)
    settings["enabled"] = False
    set_cve_settings(conn, settings)
    loaded = get_cve_settings(conn)
    assert loaded["enabled"] is False
