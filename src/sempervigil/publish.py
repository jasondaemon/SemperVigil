from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable

import yaml

from .models import Article
from .utils import slugify


def _safe_filename(article: Article) -> str:
    date_part = (article.published_at or article.ingested_at).split("T")[0]
    slug = slugify(article.title)
    return f"{date_part}-{slug}-{article.stable_id[:8]}.md"


def write_hugo_markdown(articles: Iterable[Article], output_dir: str) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    written: list[str] = []
    for article in articles:
        filename = _safe_filename(article)
        path = os.path.join(output_dir, filename)
        frontmatter = {
            "title": article.title,
            "date": article.published_at or article.ingested_at,
            "source": article.source_id,
            "source_url": article.normalized_url,
            "tags": article.tags,
            "published_at_source": article.published_at_source,
        }
        summary = article.summary or ""
        body = "\n".join(
            [
                summary.strip(),
                "",
                f"[Read more]({article.normalized_url})",
                "",
            ]
        )
        content = "---\n"
        content += yaml.safe_dump(
            frontmatter, sort_keys=False, allow_unicode=False, default_flow_style=False
        )
        content += "---\n\n"
        content += body
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        written.append(path)
    return written


def write_json_index(articles: Iterable[Article], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = [
        {
            "id": article.id,
            "stable_id": article.stable_id,
            "title": article.title,
            "original_url": article.original_url,
            "normalized_url": article.normalized_url,
            "source_id": article.source_id,
            "published_at": article.published_at,
            "ingested_at": article.ingested_at,
            "tags": article.tags,
        }
        for article in articles
    ]
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def write_tag_indexes(articles: Iterable[Article], output_dir: str, section: str) -> list[str]:
    content_root = Path(output_dir).parent
    tags_root = content_root / "tags"
    tags_root.mkdir(parents=True, exist_ok=True)

    tag_map: dict[str, list[Article]] = {}
    for article in articles:
        for tag in article.tags:
            tag_map.setdefault(tag, []).append(article)

    written: list[str] = []
    for tag in sorted(tag_map):
        tag_dir = tags_root / tag
        tag_dir.mkdir(parents=True, exist_ok=True)
        path = tag_dir / "_index.md"
        frontmatter = {"title": f"Tag: {tag}"}
        lines = ["---"]
        lines.append(yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=False).strip())
        lines.append("---")
        lines.append("")

        articles_sorted = sorted(
            tag_map[tag],
            key=lambda item: item.published_at or item.ingested_at,
            reverse=True,
        )
        for article in articles_sorted:
            filename = _safe_filename(article)
            slug = Path(filename).stem
            date_part = (article.published_at or article.ingested_at).split("T")[0]
            lines.append(f"- [{article.title}](/{section}/{slug}/) ({date_part})")

        content = "\n".join(lines) + "\n"
        path.write_text(content, encoding="utf-8")
        written.append(str(path))

    return written
