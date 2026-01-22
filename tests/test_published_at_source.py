import time

from sempervigil.utils import extract_published_at


def test_published_parsed_source():
    entry = {"published_parsed": time.gmtime(0)}
    published_at, source = extract_published_at(
        entry, "2024-01-01T00:00:00+00:00", strategy="published_then_updated"
    )
    assert source == "published"
    assert published_at.startswith("1970-01-01T00:00:00")


def test_updated_source():
    entry = {"updated_parsed": time.gmtime(10)}
    published_at, source = extract_published_at(
        entry, "2024-01-01T00:00:00+00:00", strategy="published_then_updated"
    )
    assert source == "modified"
    assert published_at.startswith("1970-01-01T00:00:10")


def test_fallback_source():
    entry = {}
    published_at, source = extract_published_at(
        entry, "2024-01-01T00:00:00+00:00", strategy="published_then_updated"
    )
    assert source == "guessed"
    assert published_at == "2024-01-01T00:00:00+00:00"
