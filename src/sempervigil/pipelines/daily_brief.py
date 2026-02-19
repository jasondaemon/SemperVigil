from __future__ import annotations

from pathlib import Path
from typing import Any

from ..utils import atomic_write_json, atomic_write_text


def write_daily_brief(
    *,
    base_site_dir: str,
    day: str,
    payload: dict[str, Any],
    data_root: str | None = None,
    content_root: str | None = None,
) -> dict[str, str]:
    data_dir = (
        Path(data_root) / "briefs"
        if data_root
        else Path(base_site_dir) / "data" / "briefs"
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    json_path = data_dir / f"{day}.json"
    atomic_write_json(json_path, payload, indent=2)
    content_dir = (
        Path(content_root) / "daily"
        if content_root
        else Path(base_site_dir) / "content" / "daily"
    )
    content_dir.mkdir(parents=True, exist_ok=True)
    md_path = content_dir / f"{day}.md"
    desired_url = f"/daily-briefs/{day}/"
    if not md_path.exists():
        atomic_write_text(
            md_path,
            "\n".join(
                [
                    "---",
                    f'title: "Daily Brief – {day}"',
                    f"date: {day}",
                    "type: daily",
                    f'url: "{desired_url}"',
                    "---",
                    "",
                ]
            ),
        )
    else:
        # Backfill legacy brief files that were created without explicit URL frontmatter.
        existing = md_path.read_text(encoding="utf-8")
        if existing.startswith("---\n"):
            end_idx = existing.find("\n---", 4)
            if end_idx != -1:
                frontmatter = existing[4:end_idx]
                if "url:" not in frontmatter:
                    updated = existing[:end_idx] + f'\nurl: "{desired_url}"' + existing[end_idx:]
                    atomic_write_text(md_path, updated)
    return {"json_path": str(json_path), "content_path": str(md_path)}
