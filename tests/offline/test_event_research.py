import logging
from unittest.mock import Mock

import pytest

from sempervigil import event_composition_publication as publication
from sempervigil import worker
from sempervigil.enrichment.query import build_event_enrich_query

pytestmark = pytest.mark.offline


def test_waterplum_query_uses_distinctive_actor_not_geography():
    event = {
        "title": "North Korean WaterPlum hackers infected 30,000 devices worldwide",
        "kind": "campaign",
    }
    query = build_event_enrich_query(event)
    assert query.startswith('"WaterPlum"')
    assert '"North"' not in query
    validation = worker._validate_event_source_fallback(
        event,
        {"title": "WaterPlum campaign targets developers", "snippet": ""},
        "Researchers described the WaterPlum campaign and its malware.",
    )
    assert validation["related"] is True
    assert "entity:waterplum" in validation["matched_facts"]


def test_one_source_composition_queues_research_instead_of_publication(monkeypatch):
    monkeypatch.setattr(
        "sempervigil.config.get_events_settings",
        lambda conn: {"publish_min_articles": 2, "enrich_min_articles_max_results": 12},
    )
    enqueue = Mock(return_value="research-job")
    monkeypatch.setattr(publication, "enqueue_job", enqueue)
    material = {"event_id": "evt_one", "sources": [{"article_id": 1}]}
    assert publication.queue_research_if_needed(Mock(), material) == ("research-job", 2)
    assert enqueue.call_args.args[1:] == (
        "enrich_event_from_web",
        {"event_id": "evt_one", "max_results": 12, "replace_existing": False},
    )
    assert enqueue.call_args.kwargs == {"debounce": True, "dedupe": True}


def test_two_source_composition_can_advance_without_research(monkeypatch):
    monkeypatch.setattr(
        "sempervigil.config.get_events_settings",
        lambda conn: {"publish_min_articles": 2, "enrich_min_articles_max_results": 12},
    )
    enqueue = Mock(side_effect=AssertionError("must not enqueue"))
    monkeypatch.setattr(publication, "enqueue_job", enqueue)
    material = {"event_id": "evt_two", "sources": [{"article_id": 1}, {"article_id": 2}]}
    assert publication.queue_research_if_needed(Mock(), material) is None


def test_one_source_submit_never_opens_publication_authority(monkeypatch):
    material = {"event_id": "evt_one", "sources": [{"article_id": 1}]}
    monkeypatch.setattr(publication, "materialize_event", lambda conn, composition_id: material)
    monkeypatch.setattr(publication, "queue_research_if_needed",
                        lambda conn, value: ("research-job", 2))
    monkeypatch.setattr(
        publication,
        "connection_factory",
        Mock(side_effect=AssertionError("publication authority must remain closed")),
    )
    result = publication.submit(Mock(), "elc_" + "a" * 64,
                                confirmation=publication.CONFIRMATION)
    assert result == {
        "event_id": "evt_one",
        "job_id": "research-job",
        "status": "research_queued",
        "source_count": 1,
        "minimum_sources": 2,
        "public_eligible": False,
    }


def test_researched_article_enters_evidence_only_after_normal_enrichment(monkeypatch):
    article = {
        "id": 17,
        "source_id": "web_enrich",
        "summary_generated_at": "2026-09-20T00:00:00+00:00",
        "context_generated_at": "2026-09-20T00:01:00+00:00",
        "summary_error": None,
        "context_error": None,
    }
    monkeypatch.setattr(worker, "get_article_by_id", lambda conn, article_id: article)
    submit = Mock(return_value="evidence-job")
    monkeypatch.setattr("sempervigil.article_review_jobs.submit", submit)
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = (1,)
    assert worker._maybe_enqueue_event_article_review(
        conn, 17, logging.getLogger("test")
    ) == "evidence-job"
    submit.assert_called_once_with(conn, [17])


def test_unfinished_or_unmanaged_research_never_enters_evidence(monkeypatch):
    incomplete = {
        "id": 17,
        "source_id": "web_enrich",
        "summary_generated_at": "2026-09-20T00:00:00+00:00",
        "context_generated_at": None,
        "summary_error": None,
        "context_error": None,
    }
    monkeypatch.setattr(worker, "get_article_by_id", lambda conn, article_id: incomplete)
    conn = Mock()
    assert worker._maybe_enqueue_event_article_review(conn, 17, logging.getLogger("test")) is None
    conn.execute.assert_not_called()
