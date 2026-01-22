from sempervigil.models import Source
from sempervigil.tagger import derive_tags, normalize_tag


def test_tag_normalization_alias():
    source = Source(
        id="s1",
        name="Source",
        kind="rss",
        url="https://example.com/feed",
        enabled=True,
        section="posts",
        policy={"tags": {"tag_defaults": ["0 day"], "tag_normalize": {"0-day": "zero-day"}}},
    )
    tags = derive_tags(source, source.policy, "", "")
    assert "zero-day" in tags


def test_include_rule_adds_tag():
    source = Source(
        id="s1",
        name="Source",
        kind="rss",
        url="https://example.com/feed",
        enabled=True,
        section="posts",
        policy={"tags": {"tag_rules": {"include_if": {"ransomware": ["Ransomware"]}}}},
    )
    tags = derive_tags(source, source.policy, "New ransomware campaign", "")
    assert "ransomware" in tags


def test_exclude_rule_removes_tag():
    source = Source(
        id="s1",
        name="Source",
        kind="rss",
        url="https://example.com/feed",
        enabled=True,
        section="posts",
        policy={
            "tags": {
                "tag_defaults": ["microsoft"],
                "tag_rules": {"exclude_if": {"windows": ["microsoft"]}},
            }
        },
    )
    tags = derive_tags(source, source.policy, "Windows update", "")
    assert "microsoft" not in tags


def test_normalize_tag_hyphenates():
    assert normalize_tag("Zero Day") == "zero-day"
