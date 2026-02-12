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
        Path(content_root) / "briefs"
        if content_root
        else Path(base_site_dir) / "content" / "briefs"
    )
    content_dir.mkdir(parents=True, exist_ok=True)
    md_path = content_dir / f"{day}.md"
    if not md_path.exists():
        atomic_write_text(
            md_path,
            "\n".join(
                [
                    "---",
                    f'title: "Daily Brief – {day}"',
                    f"date: {day}",
                    "type: briefs",
                    "---",
                    "",
                ]
            ),
        )
    return {"json_path": str(json_path), "content_path": str(md_path)}
