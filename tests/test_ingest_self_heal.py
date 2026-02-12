from __future__ import annotations

from sempervigil.config import DEFAULT_CONFIG
from sempervigil.config import _build_config
from sempervigil.ingest import SourceResult, process_source
from sempervigil.models import Source, SourceTactic


def test_ingest_self_heals_empty_tactics(monkeypatch):
    source = Source(
        id="test-source",
        name="Test Source",
        enabled=True,
        base_url="https://example.com/feed",
        topic_key=None,
        default_frequency_minutes=60,
        pause_until=None,
        paused_reason=None,
        robots_notes=None,
        overrides=None,
        kind="rss",
        url="https://example.com/feed",
    )

    class DummyConn:
        pass

    created: list[SourceTactic] = []

    def fake_list_tactics(conn, source_id):
        return []

    def fake_list_tactics_for_source(conn, source_id):
        return []

    def fake_upsert_tactic(conn, tactic):
        created.append(tactic)

    def fake_run_tactic(*args, **kwargs):
        result = SourceResult(
            source_id=source.id,
            status="ok",
            http_status=200,
            found_count=1,
            accepted_count=1,
            skipped_duplicates=0,
            skipped_filters=0,
            skipped_missing_url=0,
            already_seen_count=0,
            error=None,
            articles=[],
            decisions=[],
            raw_entry=None,
        )
        return result, {"tactic_type": "rss", "status": "ok"}

    monkeypatch.setattr("sempervigil.ingest.list_tactics", fake_list_tactics)
    monkeypatch.setattr("sempervigil.ingest.list_tactics_for_source", fake_list_tactics_for_source)
    monkeypatch.setattr("sempervigil.ingest.upsert_tactic", fake_upsert_tactic)
    monkeypatch.setattr("sempervigil.ingest._run_tactic", fake_run_tactic)

    config = _build_config(DEFAULT_CONFIG)
    result = process_source(
        source=source,
        config=config,
        logger=__import__("logging").getLogger("test"),
        conn=DummyConn(),
        test_mode=True,
    )
    assert result.status == "ok"
    assert created
    assert created[0].tactic_type == "rss"
