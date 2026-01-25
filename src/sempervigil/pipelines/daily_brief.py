from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_daily_brief(
    *,
    base_content_dir: str,
    base_static_dir: str,
    day: str,
    items: list[dict[str, Any]],
) -> dict[str, str]:
    content_dir = Path(base_content_dir) / "briefs"
    static_dir = Path(base_static_dir) / "briefs"
    content_dir.mkdir(parents=True, exist_ok=True)
    static_dir.mkdir(parents=True, exist_ok=True)

    md_path = content_dir / f"{day}.md"
    json_path = static_dir / f"{day}.json"

    lines = [
        "---",
        f"title: Daily Brief {day}",
        f"date: {day}",
        "draft: false",
        "---",
        "",
        "## Top stories",
        "",
    ]
    for item in items:
        summary = item["summary_data"].get("summary", "").strip()
        bullets = item["summary_data"].get("bullets", [])
        why = item["summary_data"].get("why", "").strip()
        lines.append(f"### {item['title']}")
        lines.append(f"- Source: {item['source_id']}")
        lines.append(f"- Link: {item['original_url']}")
        if summary:
            lines.append(f"- Summary: {summary}")
        if why:
            lines.append(f"- Why it matters: {why}")
        if bullets:
            lines.append("- Key points:")
            for bullet in bullets:
                lines.append(f"  - {bullet}")
        lines.append("")

    md_path.write_text("\n".join(lines), encoding="utf-8")
    json_path.write_text(json.dumps(items, indent=2), encoding="utf-8")
    return {"markdown_path": str(md_path), "json_path": str(json_path)}
