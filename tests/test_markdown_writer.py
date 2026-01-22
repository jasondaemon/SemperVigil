import yaml

from sempervigil.models import Article
from sempervigil.publish import write_article_markdown


def test_write_article_markdown(tmp_path):
    output_dir = tmp_path / "posts"
    article = Article(
        id=None,
        stable_id="abcdef123456",
        original_url="https://example.com/cve-1",
        normalized_url="https://example.com/cve-1",
        title="Test Title",
        source_id="source-1",
        published_at="2025-01-01T00:00:00Z",
        published_at_source="published",
        ingested_at="2025-01-01T00:00:00Z",
        summary="Summary text",
        tags=["tag1", "tag2"],
    )
    path = write_article_markdown(article, str(output_dir))
    content = (output_dir / path.split("/")[-1]).read_text(encoding="utf-8")
    frontmatter = content.split("---")[1]
    data = yaml.safe_load(frontmatter)
    assert data["title"] == "Test Title"
    assert data["summary"] == "Summary text"
    assert data["draft"] is False
    assert data["source_url"] == "https://example.com/cve-1"
    assert data["categories"] == ["tag1", "tag2"]
