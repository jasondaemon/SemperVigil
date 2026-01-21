import yaml

from sempervigil.config import load_config
from sempervigil.ingest import evaluate_entry
from sempervigil.models import Source
from sempervigil.storage import init_db


def _make_config(tmp_path, deny_keywords=None):
    config_path = tmp_path / "config.yml"
    payload = {}
    if deny_keywords is not None:
        payload["ingest"] = {"filters": {"deny_keywords": deny_keywords}}
    config_path.write_text(yaml.safe_dump(payload))
    return load_config(str(config_path))


def test_decision_missing_url(tmp_path):
    config = _make_config(tmp_path)
    conn = init_db(str(tmp_path / "state.sqlite3"))
    source = Source(
        id="s1",
        name="Source",
        type="rss",
        url="https://example.com/feed",
        enabled=True,
        tags=[],
        overrides={},
    )
    entry = {"title": "No link"}

    decision, article = evaluate_entry(entry, source, config, conn, set(), \"2024-01-01T00:00:00+00:00\")

    assert decision.decision == "SKIP"
    assert "missing_url" in decision.reasons
    assert article is None


def test_decision_deny_keyword(tmp_path):
    config = _make_config(tmp_path, deny_keywords=["blocked"])
    conn = init_db(str(tmp_path / "state.sqlite3"))
    source = Source(
        id="s1",
        name="Source",
        type="rss",
        url="https://example.com/feed",
        enabled=True,
        tags=[],
        overrides={},
    )
    entry = {"title": "Blocked item", "link": "https://example.com/1"}

    decision, article = evaluate_entry(entry, source, config, conn, set(), \"2024-01-01T00:00:00+00:00\")

    assert decision.decision == "SKIP"
    assert any(reason.startswith("deny_keywords") for reason in decision.reasons)
    assert article is None
