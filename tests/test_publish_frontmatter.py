from pathlib import Path

import yaml

from sempervigil.models import Article
from sempervigil.publish import write_hugo_markdown


def _extract_frontmatter(path):
    content = path.read_text(encoding="utf-8")
    parts = content.split("---\n")
    assert len(parts) >= 3
    return yaml.safe_load(parts[1])


def test_frontmatter_uses_source_url(tmp_path):
    article = Article(
        id=None,
        source_id="source",
        stable_id="abc123",
        original_url="https://example.com/story",
        normalized_url="https://example.com/story",
        title="Test Article",
        published_at="2024-01-01T00:00:00+00:00",
        published_at_source="published",
        ingested_at="2024-01-01T00:00:00+00:00",
        summary="Summary",
        tags=["news"],
    )

    paths = write_hugo_markdown([article], str(tmp_path))
    frontmatter = _extract_frontmatter(Path(paths[0]))

    assert "url" not in frontmatter
    assert frontmatter.get("source_url") == article.normalized_url
