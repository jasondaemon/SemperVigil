from datetime import date
from types import SimpleNamespace

import pytest

from sempervigil.ingest import evaluate_entry
from sempervigil.models import Source
from sempervigil.source_overrides import normalize_source_overrides


pytestmark = pytest.mark.offline


@pytest.fixture
def source():
    return Source(
        id="test", name="Test", enabled=True, base_url="https://example.com",
        topic_key=None, default_frequency_minutes=60, pause_until=None,
        paused_reason=None, robots_notes=None,
    )


@pytest.fixture
def config():
    filters = SimpleNamespace(deny_keywords=[], allow_keywords=[])
    return SimpleNamespace(ingest=SimpleNamespace(filters=filters))


def test_start_date_validation():
    assert normalize_source_overrides({"discovery": {"not_before": "2026-11-14"}})["discovery"]["not_before"] == "2026-11-14"
    with pytest.raises(ValueError, match="discovery.not_before"):
        normalize_source_overrides({"discovery": {"not_before": "11/14/2026"}})


@pytest.mark.parametrize(
    ("published", "expected"),
    [("2026-11-13T23:59:00Z", "SKIP"), (None, "SKIP"), ("2026-11-14T00:00:00Z", "ACCEPT")],
)
def test_start_date_filters_entries(source, config, published, expected, monkeypatch):
    monkeypatch.setattr("sempervigil.ingest.article_exists", lambda *_: False)
    entry = {"title": "Story", "link": "https://example.com/story"}
    if published:
        entry["published"] = published
    decision, article = evaluate_entry(
        entry, source, {}, config, None, set(), "2026-11-15T00:00:00+00:00",
        not_before=date(2026, 11, 14),
    )
    assert decision.decision == expected
    assert (article is not None) == (expected == "ACCEPT")
    assert ("before_source_start" in decision.reasons) == (expected == "SKIP")
