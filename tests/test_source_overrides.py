from __future__ import annotations

import logging
import re

from sempervigil.pipelines.content_fetch import extract_content_from_html
from sempervigil.source_overrides import normalize_source_overrides, should_allow_url


def test_override_allowlist_blocklist() -> None:
    allow = re.compile(r"^https://example\\.com/story/")
    block = re.compile(r"/tag/")
    ok, reason = should_allow_url("https://example.com/story/ok", allow, block)
    assert ok is True
    assert reason is None

    ok, reason = should_allow_url("https://example.com/tag/something", allow, block)
    assert ok is False
    assert reason == "blocklist_match"

    ok, reason = should_allow_url("https://example.com/other", allow, block)
    assert ok is False
    assert reason == "allowlist_miss"


def test_jsonld_articlebody_extraction() -> None:
    html = """
    <html><head>
    <script type="application/ld+json">
    {"@context":"https://schema.org","@type":"NewsArticle","articleBody":"Hello world body."}
    </script>
    </head><body><p>fallback</p></body></html>
    """
    overrides = {"content": {"mode": "jsonld_articlebody", "min_chars": 1}}
    result = extract_content_from_html(
        html, overrides=overrides, logger=logging.getLogger("test")
    )
    assert result["method"].startswith("jsonld_articlebody")
    assert "Hello world body." in result["content_text"]


def test_discovery_mode_rss_only_defaults() -> None:
    overrides = normalize_source_overrides({"discovery": {"mode": "rss_only"}})
    assert overrides["discovery"]["mode"] == "default"


def test_discovery_mode_unknown_defaults() -> None:
    overrides = normalize_source_overrides({"discovery": {"mode": "typo_value"}})
    assert overrides["discovery"]["mode"] == "default"
