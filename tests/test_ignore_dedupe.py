import yaml

from sempervigil.config import load_config
from sempervigil.ingest import evaluate_entry
from sempervigil.models import Article, Source
from sempervigil.storage import init_db, insert_articles
from sempervigil.utils import stable_id_from_url


def _make_config(tmp_path):
    config_path = tmp_path / "config.yml"
    config_path.write_text(yaml.safe_dump({}))
    return load_config(str(config_path))


def test_ignore_dedupe_accepts_duplicate(tmp_path):
    config = _make_config(tmp_path)
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

    url = "https://example.com/item"
    article_id = stable_id_from_url(url)
    insert_articles(
        conn,
        [
            Article(
                id=None,
                source_id="s1",
                stable_id=article_id,
                original_url=url,
                normalized_url=url,
                title="Existing",
                published_at="2024-01-01T00:00:00+00:00",
                published_at_source="published",
                ingested_at="2024-01-01T00:00:00+00:00",
                summary=None,
                tags=[],
            )
        ],
    )

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
    entry = {"title": "Existing", "link": url}

    decision, _ = evaluate_entry(
        entry,
        source,
        {},
        config,
        conn,
        set(),
        "2024-01-01T00:00:00+00:00",
    )
    assert decision.decision == "SKIP"
    assert "duplicate" in decision.reasons

    decision, _ = evaluate_entry(
        entry,
        source,
        {},
        config,
        conn,
        set(),
        "2024-01-01T00:00:00+00:00",
        ignore_dedupe=True,
    )
    assert decision.decision == "ACCEPT"
    assert "already_seen" in decision.reasons


def test_ignore_dedupe_counts_preview(tmp_path):
    config = _make_config(tmp_path)
    db_path = tmp_path / "state.sqlite3"
    conn = init_db(str(db_path))

    url = "https://example.com/item"
    article_id = stable_id_from_url(url)
    insert_articles(
        conn,
        [
            Article(
                id=None,
                source_id="s1",
                stable_id=article_id,
                original_url=url,
                normalized_url=url,
                title="Existing",
                published_at="2024-01-01T00:00:00+00:00",
                published_at_source="published",
                ingested_at="2024-01-01T00:00:00+00:00",
                summary=None,
                tags=[],
            )
        ],
    )

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
    entry = {"title": "Existing", "link": url}
    decision, article = evaluate_entry(
        entry,
        source,
        {},
        config,
        conn,
        set(),
        "2024-01-01T00:00:00+00:00",
        ignore_dedupe=True,
    )
    assert decision.decision == "ACCEPT"
    assert "already_seen" in decision.reasons
    assert article is not None
