from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from typing import Any

from ..models import SourceTactic
from ..storage import upsert_tactic
from ..utils import json_dumps, utc_now_iso


def list_sources(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "sources")
    select_cols = [
        "id",
        "name",
        "enabled",
        "kind",
        "url",
        "interval_minutes",
        "tags_json",
        "created_at",
        "updated_at",
        "last_checked_at",
        "last_ok_at",
        "last_error",
        "base_url",
        "default_frequency_minutes",
        "pause_until",
        "paused_reason",
    ]
    cols = [col for col in select_cols if col in columns]
    cursor = conn.execute(f"SELECT {', '.join(cols)} FROM sources ORDER BY id")
    rows = []
    for row in cursor.fetchall():
        data = dict(zip(cols, row))
        data["enabled"] = bool(data.get("enabled", 0))
        data["interval_minutes"] = _int_or_default(
            data.get("interval_minutes"), data.get("default_frequency_minutes"), 60
        )
        data["url"] = data.get("url") or data.get("base_url")
        data["kind"] = data.get("kind")
        data["tags"] = _parse_tags(data.get("tags_json"))
        rows.append(data)
    return rows


def get_source(conn: sqlite3.Connection, source_id: str) -> dict[str, Any] | None:
    for source in list_sources(conn):
        if source.get("id") == source_id:
            return source
    return None


def create_source(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    # Allow UI to omit id when creating a new source
    source_id = str(payload.get("id") or "").strip()

    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")

    if not source_id:
        source_id = _generate_source_id(conn, name)

    kind = str(payload.get("kind") or "rss").strip()
    url = str(payload.get("url") or "").strip()
    if not url:
        raise ValueError("url is required")

    enabled = bool(payload.get("enabled", True))
    interval = int(payload.get("interval_minutes", 60))
    tags = _parse_tags(payload.get("tags"))
    now = utc_now_iso()

    conn.execute(
        """
        INSERT INTO sources
            (id, name, enabled, kind, url, interval_minutes, tags_json,
             base_url, default_frequency_minutes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            name,
            1 if enabled else 0,
            kind,
            url,
            interval,
            json_dumps(tags) if tags else None,
            url,
            interval,
            now,
            now,
        ),
    )
    conn.commit()
    _ensure_tactic(conn, source_id, kind, url, enabled)
    return get_source(conn, source_id) or {}


def _slugify(value: str) -> str:
    # Simple, dependency-free slugify
    value = value.strip().lower()
    out = []
    dash = False
    for ch in value:
        if ch.isalnum():
            out.append(ch)
            dash = False
        else:
            if not dash:
                out.append("-")
                dash = True
    slug = "".join(out).strip("-")
    return slug or "source"


def _generate_source_id(conn: sqlite3.Connection, name: str) -> str:
    base = _slugify(name)
    candidate = base
    i = 2
    while get_source(conn, candidate) is not None:
        candidate = f"{base}-{i}"
        i += 1
    return candidate


def update_source(conn: sqlite3.Connection, source_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_source(conn, source_id)
    if not current:
        raise ValueError("source_not_found")

    name = str(payload.get("name") or current["name"]).strip()
    kind = str(payload.get("kind") or current.get("kind") or "rss").strip()
    url = str(payload.get("url") or current.get("url") or "").strip()
    enabled = bool(payload.get("enabled", current.get("enabled", True)))
    interval = int(payload.get("interval_minutes", current.get("interval_minutes", 60)))
    tags = _parse_tags(payload.get("tags", current.get("tags")))
    now = utc_now_iso()

    conn.execute(
        """
        UPDATE sources
        SET name = ?, enabled = ?, kind = ?, url = ?, interval_minutes = ?,
            tags_json = ?, base_url = ?, default_frequency_minutes = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            name,
            1 if enabled else 0,
            kind,
            url,
            interval,
            json_dumps(tags) if tags else None,
            url,
            interval,
            now,
            source_id,
        ),
    )
    conn.commit()
    _ensure_tactic(conn, source_id, kind, url, enabled)
    return get_source(conn, source_id) or {}


def delete_source(conn: sqlite3.Connection, source_id: str) -> None:
    conn.execute("DELETE FROM source_tactics WHERE source_id = ?", (source_id,))
    conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
    conn.commit()


def record_test_result(
    conn: sqlite3.Connection, source_id: str, ok: bool, error: str | None
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE sources
        SET last_checked_at = ?, last_ok_at = ?, last_error = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            now,
            now if ok else None,
            None if ok else error,
            now,
            source_id,
        ),
    )
    conn.commit()


def _ensure_tactic(conn: sqlite3.Connection, source_id: str, kind: str, url: str, enabled: bool) -> None:
    tactic_type = "rss" if kind == "rss" else "html_index"
    config: dict[str, Any] = {"feed_url": url}
    tactic = SourceTactic(
        id=None,
        source_id=source_id,
        tactic_type=tactic_type,
        enabled=enabled,
        priority=100,
        config=config,
        last_success_at=None,
        last_error_at=None,
        error_streak=0,
    )
    upsert_tactic(conn, tactic)


def _parse_tags(tags: Any) -> list[str]:
    if tags is None:
        return []
    if isinstance(tags, list):
        return [str(tag).strip() for tag in tags if str(tag).strip()]
    if isinstance(tags, str):
        try:
            parsed = json.loads(tags)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(tag).strip() for tag in parsed if str(tag).strip()]
        return [item.strip() for item in tags.split(",") if item.strip()]
    return []


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _int_or_default(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 60
