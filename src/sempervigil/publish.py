from __future__ import annotations

import os
from typing import Iterable

import yaml
import json

from .models import Article
from .utils import slugify


def _safe_filename(article: Article) -> str:
    date_part = (article.published_at or article.fetched_at).split("T")[0]
    slug = slugify(article.title)
    return f"{date_part}-{slug}-{article.id[:8]}.md"


def write_hugo_markdown(articles: Iterable[Article], output_dir: str) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    written: list[str] = []
    for article in articles:
        filename = _safe_filename(article)
        path = os.path.join(output_dir, filename)
        frontmatter = {
            "title": article.title,
            "date": article.published_at or article.fetched_at,
            "source": article.source_id,
            "source_url": article.url,
            "tags": article.tags,
        }
        summary = article.summary or ""
        body = "\n".join(
            [
                summary.strip(),
                "",
                f"[Read more]({article.url})",
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
            "title": article.title,
            "url": article.url,
            "source_id": article.source_id,
            "published_at": article.published_at,
            "fetched_at": article.fetched_at,
            "tags": article.tags,
        }
        for article in articles
    ]
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
