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


def write_article_markdown(
    article: Article, output_dir: str, extra_frontmatter: dict[str, object] | None = None
) -> str:
    os.makedirs(output_dir, exist_ok=True)
    filename = _safe_filename(article)
    path = os.path.join(output_dir, filename)
    frontmatter = {
        "title": article.title,
        "date": article.published_at or article.ingested_at,
        "tags": article.tags,
        "categories": article.tags,
        "summary": article.summary or "",
        "draft": False,
        "source_url": article.normalized_url,
    }
    if extra_frontmatter:
        frontmatter.update(extra_frontmatter)
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
    return path


def write_hugo_markdown(articles: Iterable[Article], output_dir: str) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    written: list[str] = []
    for article in articles:
        written.append(write_article_markdown(article, output_dir))
    return written


def write_json_index(
    articles: Iterable[Article],
    path: str,
    extra_by_stable_id: dict[str, dict[str, object]] | None = None,
) -> None:
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
            **(extra_by_stable_id.get(article.stable_id, {}) if extra_by_stable_id else {}),
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


def write_events_index(events: Iterable[dict[str, object]], base_static_dir: str) -> str:
    index_dir = os.path.join(base_static_dir, "index")
    os.makedirs(index_dir, exist_ok=True)
    payload = []
    for event in events:
        items = event.get("items") or {}
        cves = items.get("cves") or []
        products = items.get("products") or []
        articles = items.get("articles") or []
        payload.append(
            {
                "event_id": event.get("id"),
                "title": event.get("title"),
                "summary": event.get("summary"),
                "severity": event.get("severity"),
                "kind": event.get("kind"),
                "status": event.get("status"),
                "first_seen_at": event.get("first_seen_at"),
                "last_seen_at": event.get("last_seen_at"),
                "cves": cves[:50],
                "products": products[:50],
                "articles": articles[:50],
                "counts": {
                    "cves": len(cves),
                    "products": len(products),
                    "articles": len(articles),
                },
            }
        )
    path = os.path.join(index_dir, "events.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def write_events_markdown(
    events: Iterable[dict[str, object]], base_content_dir: str
) -> list[str]:
    output_dir = os.path.join(base_content_dir, "events")
    os.makedirs(output_dir, exist_ok=True)
    written: list[str] = []
    for event in events:
        event_id = str(event.get("id") or "")
        if not event_id:
            continue
        frontmatter = {
            "title": event.get("title") or event_id,
            "severity": event.get("severity") or "UNKNOWN",
            "kind": event.get("kind"),
            "status": event.get("status"),
            "first_seen_at": event.get("first_seen_at"),
            "last_seen_at": event.get("last_seen_at"),
        }
        summary = (event.get("summary") or "").strip()
        items = event.get("items") or {}
        cves = items.get("cves") or []
        products = items.get("products") or []
        articles = items.get("articles") or []
        lines = ["---"]
        lines.append(yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=False).strip())
        lines.append("---")
        lines.append("")
        if summary:
            lines.append(summary)
            lines.append("")
        if cves:
            lines.append("## CVEs")
            for cve in cves[:50]:
                cve_id = cve.get("cve_id") or ""
                severity = cve.get("preferred_base_severity") or "UNKNOWN"
                score = cve.get("preferred_base_score")
                score_text = f" ({score})" if score is not None else ""
                lines.append(f"- {cve_id} [{severity}]{score_text}")
            lines.append("")
        if products:
            lines.append("## Products")
            for product in products[:50]:
                vendor = product.get("vendor_name") or ""
                name = product.get("product_name") or ""
                lines.append(f"- {vendor} {name}".strip())
            lines.append("")
        if articles:
            lines.append("## Articles")
            for article in articles[:50]:
                title = article.get("title") or ""
                url = article.get("url") or ""
                if url:
                    lines.append(f"- [{title}]({url})")
                else:
                    lines.append(f"- {title}")
            lines.append("")
        content = "\n".join(lines).strip() + "\n"
        path = os.path.join(output_dir, f"{event_id}.md")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        written.append(path)
    return written
