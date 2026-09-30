import pytest

from sempervigil import config

pytestmark = pytest.mark.offline


def test_legacy_settings_gain_bounded_research_defaults(monkeypatch):
    values = {config.EVENTS_SETTINGS_KEY: {"enabled": True}}
    monkeypatch.setattr(config, "get_setting", lambda _conn, key, _default: values.get(key))
    monkeypatch.setattr(config, "set_setting", lambda _conn, key, value: values.__setitem__(key, value))

    result = config.bootstrap_events_settings(None)

    assert result["enrich_min_articles"] == 2
    assert result["enrich_min_articles_max_results"] == 6
    assert values[config.EVENTS_SETTINGS_KEY] == result


def test_explicit_research_opt_out_is_preserved(monkeypatch):
    values = {config.EVENTS_SETTINGS_KEY: {
        "enabled": True, "enrich_min_articles": 0,
        "enrich_min_articles_max_results": 4,
    }}
    monkeypatch.setattr(config, "get_setting", lambda _conn, key, _default: values.get(key))
    monkeypatch.setattr(config, "set_setting", lambda *_: (_ for _ in ()).throw(AssertionError("no write")))

    assert config.bootstrap_events_settings(None) == values[config.EVENTS_SETTINGS_KEY]
