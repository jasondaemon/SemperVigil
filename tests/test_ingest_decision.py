import copy

from sempervigil.config import DEFAULT_CONFIG, load_runtime_config, set_runtime_config
from sempervigil.ingest import evaluate_entry
from sempervigil.models import Source
from sempervigil.storage import init_db


def _make_config(tmp_path, deny_keywords=None):
    payload = copy.deepcopy(DEFAULT_CONFIG)
    if deny_keywords is not None:
        payload["ingest"]["filters"]["deny_keywords"] = deny_keywords
    conn = init_db()
    set_runtime_config(conn, payload)
    return load_runtime_config(conn)


def test_decision_missing_url(tmp_path):
    config = _make_config(tmp_path)
    conn = init_db()
    source = Source(
        id="s1",
        name="Source",
        enabled=True,
        base_url="https://example.com",
        topic_key=None,
        default_frequency_minutes=60,
        pause_until=None,
        paused_reason=None,
        robots_notes=None,
    )
    entry = {"title": "No link"}

    decision, article = evaluate_entry(
        entry,
        source,
        {},
        config,
        conn,
        set(),
        "2024-01-01T00:00:00+00:00",
    )

    assert decision.decision == "SKIP"
    assert "missing_url" in decision.reasons
    assert article is None


def test_decision_deny_keyword(tmp_path):
    config = _make_config(tmp_path, deny_keywords=["blocked"])
    conn = init_db()
    source = Source(
        id="s1",
        name="Source",
        enabled=True,
        base_url="https://example.com",
        topic_key=None,
        default_frequency_minutes=60,
        pause_until=None,
        paused_reason=None,
        robots_notes=None,
    )
    entry = {"title": "Blocked item", "link": "https://example.com/1"}

    decision, article = evaluate_entry(
        entry,
        source,
        {},
        config,
        conn,
        set(),
        "2024-01-01T00:00:00+00:00",
    )

    assert decision.decision == "SKIP"
    assert any(reason.startswith("deny_keywords") for reason in decision.reasons)
    assert article is None
