from sempervigil.tagger import derive_tags, normalize_tag


def test_tag_normalization_alias():
    tags_cfg = {"tag_defaults": ["0 day"], "tag_normalize": {"0-day": "zero-day"}}
    tags = derive_tags(tags_cfg, "", "")
    assert "zero-day" in tags


def test_include_rule_adds_tag():
    tags_cfg = {"tag_rules": {"include_if": {"ransomware": ["Ransomware"]}}}
    tags = derive_tags(tags_cfg, "New ransomware campaign", "")
    assert "ransomware" in tags


def test_exclude_rule_removes_tag():
    tags_cfg = {
        "tag_defaults": ["microsoft"],
        "tag_rules": {"exclude_if": {"windows": ["microsoft"]}},
    }
    tags = derive_tags(tags_cfg, "Windows update", "")
    assert "microsoft" not in tags


def test_normalize_tag_hyphenates():
    assert normalize_tag("Zero Day") == "zero-day"
