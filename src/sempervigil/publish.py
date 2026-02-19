from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable

import yaml

from .models import Article
from .utils import atomic_write_json, atomic_write_text, slugify


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
    atomic_write_text(path, content)
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
    atomic_write_json(path, payload, indent=2)


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
        tag_slug = slugify(tag)
        tag_dir = tags_root / tag_slug
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
        atomic_write_text(path, content)
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
    atomic_write_json(path, payload, indent=2)
    return path


def write_events_markdown(
    events: Iterable[dict[str, object]], base_content_dir: str
) -> list[str]:
    output_dir = os.path.join(base_content_dir, "events")
    os.makedirs(output_dir, exist_ok=True)
    for existing in Path(output_dir).glob("*.md"):
        if existing.name in {"_index.md"}:
            continue
        try:
            existing.unlink()
        except OSError:
            pass
    written: list[str] = []
    for event in events:
        event_id = str(event.get("id") or "")
        if not event_id:
            continue
        site_slug = str(event.get("site_slug") or "").strip()
        if not site_slug:
            site_slug = event_id
        frontmatter = {
            "title": event.get("title") or event_id,
            "severity": event.get("severity") or "UNKNOWN",
            "event_kind": event.get("kind"),
            "status": event.get("status"),
            "publish_state": event.get("publish_state"),
            "published_at": event.get("published_at"),
            "first_seen_at": event.get("first_seen_at"),
            "last_seen_at": event.get("last_seen_at"),
            "slug": site_slug,
        }
        summary = (event.get("summary") or "").strip()
        narrative = event.get("narrative") or {}
        report = event.get("report") or {}
        timeline = event.get("timeline") or []
        items = event.get("items") or {}
        cves = items.get("cves") or []
        products = items.get("products") or []
        articles = items.get("articles") or []
        lines = ["---"]
        lines.append(yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=False).strip())
        lines.append("---")
        lines.append("")
        narrative_summary = ""
        narrative_bullets: list[str] = []
        narrative_sections: dict[str, object] = {}
        if isinstance(narrative, dict):
            narrative_summary = str(narrative.get("summary") or "").strip()
            bullets_raw = narrative.get("bullets") or []
            if isinstance(bullets_raw, list):
                narrative_bullets = [str(item).strip() for item in bullets_raw if str(item).strip()][:10]
            sections_raw = narrative.get("sections") or {}
            if isinstance(sections_raw, dict):
                narrative_sections = sections_raw
        report_overview = ""
        report_timeline: list[dict[str, object]] = []
        report_sections: dict[str, list[str]] = {}
        report_attribution: dict[str, object] = {}
        if isinstance(report, dict):
            report_overview = str(report.get("overview") or "").strip()
            timeline_raw = report.get("timeline") or []
            if isinstance(timeline_raw, list):
                report_timeline = [row for row in timeline_raw if isinstance(row, dict)]
            attribution_raw = report.get("attribution") or {}
            if isinstance(attribution_raw, dict):
                report_attribution = attribution_raw
            for key in (
                "impact",
                "compromise_path",
                "investigation_findings",
                "legal_regulatory_outcomes",
                "response_recovery",
                "lessons_learned",
                "confidence_notes",
            ):
                values = report.get(key) or []
                if isinstance(values, list):
                    report_sections[key] = [str(v).strip() for v in values if str(v).strip()][:20]
        if report_overview:
            lines.append(report_overview)
            lines.append("")
        elif narrative_summary:
            lines.append(narrative_summary)
        elif summary:
            lines.append(summary)
            lines.append("")
        if narrative_bullets:
            for bullet in narrative_bullets:
                lines.append(f"- {bullet}")
            lines.append("")
        section_order = [
            "breach_compromise",
            "impact",
            "response_recovery",
            "lessons_learned",
        ]
        for section_key in section_order:
            section = narrative_sections.get(section_key) if isinstance(narrative_sections, dict) else None
            if not isinstance(section, dict):
                continue
            title = str(section.get("title") or "").strip()
            points = section.get("points") or []
            if not title or not isinstance(points, list) or not points:
                continue
            lines.append(f"## {title}")
            for point in points[:8]:
                clean = str(point).strip()
                if clean:
                    lines.append(f"- {clean}")
            lines.append("")
        report_section_order = [
            ("attribution", "Attribution"),
            ("compromise_path", "Compromise Path"),
            ("investigation_findings", "Investigation Findings"),
            ("legal_regulatory_outcomes", "Legal and Regulatory Outcomes"),
            ("impact", "Impact"),
            ("response_recovery", "Response and Recovery"),
            ("lessons_learned", "Lessons Learned"),
            ("confidence_notes", "Confidence Notes"),
        ]
        for key, title in report_section_order:
            if key == "attribution":
                actor = str(report_attribution.get("responsible_actor") or "").strip()
                actor_type = str(report_attribution.get("actor_type") or "").strip()
                confidence = str(report_attribution.get("confidence") or "").strip()
                rationale = report_attribution.get("rationale") or []
                disputed = report_attribution.get("disputed_claims") or []
                if actor or actor_type or confidence or rationale or disputed:
                    lines.append(f"## {title}")
                    if actor:
                        lines.append(f"- Responsible actor: {actor}")
                    if actor_type:
                        lines.append(f"- Actor type: {actor_type}")
                    if confidence:
                        lines.append(f"- Attribution confidence: {confidence}")
                    if isinstance(rationale, list):
                        for point in rationale[:8]:
                            clean = str(point).strip()
                            if clean:
                                lines.append(f"- Rationale: {clean}")
                    if isinstance(disputed, list):
                        for point in disputed[:8]:
                            clean = str(point).strip()
                            if clean:
                                lines.append(f"- Disputed claim: {clean}")
                    lines.append("")
                continue
            points = report_sections.get(key) or []
            if not points:
                continue
            lines.append(f"## {title}")
            for point in points[:20]:
                lines.append(f"- {point}")
            lines.append("")
        if report_timeline:
            lines.append("## Timeline")
            for entry in report_timeline[:60]:
                date_text = str(entry.get("date") or "Unknown date").strip() or "Unknown date"
                event_text = str(entry.get("event") or "").strip()
                if not event_text:
                    continue
                lines.append(f"- **{date_text}**: {event_text}")
                evidence = entry.get("evidence") or []
                if isinstance(evidence, list):
                    for fact in evidence[:6]:
                        clean = str(fact).strip()
                        if clean:
                            lines.append(f"  - {clean}")
            lines.append("")
        if timeline and not report_timeline:
            lines.append("## Timeline")
            for entry in timeline[:50]:
                ts = entry.get("date") or "Unknown date"
                title = entry.get("title") or "Untitled"
                summary_text = str(entry.get("summary") or "").strip()
                facts = entry.get("facts") or []
                url = str(entry.get("url") or "").strip()
                if url:
                    lines.append(f"- **{ts}**: [{title}]({url})")
                else:
                    lines.append(f"- **{ts}**: {title}")
                if summary_text:
                    lines.append(f"  - {summary_text}")
                if isinstance(facts, list):
                    for fact in facts[:6]:
                        clean = str(fact).strip()
                        if clean:
                            lines.append(f"  - {clean}")
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
        path = os.path.join(output_dir, f"{site_slug}.md")
        atomic_write_text(path, content)
        written.append(path)
    return written
