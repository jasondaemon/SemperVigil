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
        id="abc123",
        title="Test Article",
        url="https://example.com/story",
        source_id="source",
        published_at="2024-01-01T00:00:00+00:00",
        published_at_source="published",
        fetched_at="2024-01-01T00:00:00+00:00",
        summary="Summary",
        tags=["news"],
    )

    paths = write_hugo_markdown([article], str(tmp_path))
    frontmatter = _extract_frontmatter(Path(paths[0]))

    assert "url" not in frontmatter
    assert frontmatter.get("source_url") == article.url
