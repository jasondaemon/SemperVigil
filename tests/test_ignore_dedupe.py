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
                id=article_id,
                title="Existing",
                url=url,
                source_id="s1",
                published_at="2024-01-01T00:00:00+00:00",
                published_at_source="published",
                fetched_at="2024-01-01T00:00:00+00:00",
                summary=None,
                tags=[],
            )
        ],
    )

    source = Source(
        id="s1",
        name="Source",
        kind="rss",
        url="https://example.com/feed",
        enabled=True,
        section="posts",
        policy={},
    )
    entry = {"title": "Existing", "link": url}

    decision, _ = evaluate_entry(
        entry,
        source,
        source.policy,
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
        source.policy,
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
                id=article_id,
                title="Existing",
                url=url,
                source_id="s1",
                published_at="2024-01-01T00:00:00+00:00",
                published_at_source="published",
                fetched_at="2024-01-01T00:00:00+00:00",
                summary=None,
                tags=[],
            )
        ],
    )

    source = Source(
        id="s1",
        name="Source",
        kind="rss",
        url="https://example.com/feed",
        enabled=True,
        section="posts",
        policy={},
    )
    entry = {"title": "Existing", "link": url}
    decision, article = evaluate_entry(
        entry,
        source,
        source.policy,
        config,
        conn,
        set(),
        "2024-01-01T00:00:00+00:00",
        ignore_dedupe=True,
    )
    assert decision.decision == "ACCEPT"
    assert "already_seen" in decision.reasons
    assert article is not None
