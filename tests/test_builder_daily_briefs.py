from __future__ import annotations

import json
import logging
from sempervigil.builder import _publish_daily_brief_assets
from sempervigil.storage import init_db, upsert_daily_brief


def _brief_payload(day: str) -> dict[str, object]:
    return {
        "brief_day": day,
        "profile_id": "profile-a",
        "tldr": ["one", "two"],
        "technical_synthesis": {"text": f"Synthesis for {day}", "citations": []},
        "actions": [{"label": "review", "url": "https://example.com"}],
        "families": ["family-a"],
        "low_value": [],
        "citations": [{"url": "https://example.com/article", "title": "Example"}],
        "meta": {
            "brief_day": day,
            "profile_id": "profile-a",
            "generated_at": f"{day}T12:00:00+00:00",
        },
        "created_at": f"{day}T12:00:00+00:00",
        "updated_at": f"{day}T12:00:00+00:00",
    }


def test_publish_daily_brief_assets_backfills_source_tree(tmp_path):
    conn = init_db()
    day = "2026-03-31"
    upsert_daily_brief(conn, _brief_payload(day))

    source_dir = tmp_path / "site-src"
    data_root = tmp_path / "runtime-data"
    (source_dir / "content" / "daily").mkdir(parents=True)
    (source_dir / "data" / "briefs").mkdir(parents=True)
    (data_root / "briefs").mkdir(parents=True)
    (source_dir / "content" / "daily" / f"{day}.md").write_text("stale markdown", encoding="utf-8")
    (source_dir / "data" / "briefs" / f"{day}.json").write_text("{}", encoding="utf-8")
    (data_root / "briefs" / f"{day}.json").write_text("{}", encoding="utf-8")

    written = _publish_daily_brief_assets(conn, str(source_dir), str(data_root), logging.getLogger("test"))

    assert written == 1
    md_path = source_dir / "content" / "daily" / f"{day}.md"
    json_path = data_root / "briefs" / f"{day}.json"

    assert md_path.exists()
    assert json_path.exists()
    assert md_path.read_text(encoding="utf-8") == (
        "---\n"
        f'title: "Daily Brief – {day}"\n'
        f"date: {day}\n"
        "type: daily\n"
        f'url: "/daily-briefs/{day}/"\n'
        "---\n\n"
    )
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["meta"]["brief_day"] == day
    assert payload["technical_synthesis"]["text"] == f"Synthesis for {day}"
