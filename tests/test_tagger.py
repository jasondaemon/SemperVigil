from sempervigil.models import Source
from sempervigil.tagger import derive_tags, normalize_tag


def test_tag_normalization_alias():
    source = Source(
        id="s1",
        name="Source",
        type="rss",
        url="https://example.com/feed",
        enabled=True,
        tags=["0 day"],
        overrides={"tag_normalize": {"0-day": "zero-day"}},
    )
    tags = derive_tags(source, "", "")
    assert "zero-day" in tags


def test_include_rule_adds_tag():
    source = Source(
        id="s1",
        name="Source",
        type="rss",
        url="https://example.com/feed",
        enabled=True,
        tags=[],
        overrides={"tag_rules": {"include_if": {"ransomware": ["Ransomware"]}}},
    )
    tags = derive_tags(source, "New ransomware campaign", "")
    assert "ransomware" in tags


def test_exclude_rule_removes_tag():
    source = Source(
        id="s1",
        name="Source",
        type="rss",
        url="https://example.com/feed",
        enabled=True,
        tags=["microsoft"],
        overrides={"tag_rules": {"exclude_if": {"windows": ["microsoft"]}}},
    )
    tags = derive_tags(source, "Windows update", "")
    assert "microsoft" not in tags


def test_normalize_tag_hyphenates():
    assert normalize_tag("Zero Day") == "zero-day"
