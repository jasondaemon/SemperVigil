from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from typing import Any

from ..models import SourceTactic
from ..storage import get_article_state_counts_by_source, upsert_tactic
from ..utils import json_dumps, utc_now_iso


def list_sources(conn: Any) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "sources")
    acquire_map = _active_acquire_jobs(conn)
    state_counts = get_article_state_counts_by_source(conn)
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
        "overrides",
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
        data["overrides"] = _parse_overrides(data.get("overrides"))
        if data.get("id") in acquire_map:
            data["acquire_status"] = acquire_map[data["id"]]["status"]
            data["acquire_job_id"] = acquire_map[data["id"]]["job_id"]
        counts = state_counts.get(data.get("id") or "", {})
        data["new_count"] = counts.get("new_count", 0)
        data["gathered_count"] = counts.get("gathered_count", 0)
        data["summarized_count"] = counts.get("summarized_count", 0)
        data["last_successful_poll_at"] = _last_successful_poll(conn, data.get("id"))
        data["last_article_at"] = _last_article_ingested(conn, data.get("id"))
        rows.append(data)
    return rows


def get_source(conn: Any, source_id: str) -> dict[str, Any] | None:
    for source in list_sources(conn):
        if source.get("id") == source_id:
            return source
    return None


def create_source(conn: Any, payload: dict[str, Any]) -> dict[str, Any]:
    # Allow UI to omit id when creating a new source
    source_id = str(payload.get("id") or "").strip()

    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")

    if not source_id:
        source_id = str(uuid.uuid4())

    kind = str(payload.get("kind") or "rss").strip()
    url = str(payload.get("url") or "").strip()
    if not url:
        raise ValueError("url is required")

    enabled = bool(payload.get("enabled", True))
    interval = int(payload.get("interval_minutes", 60))
    tags = _parse_tags(payload.get("tags"))
    overrides = _normalize_overrides_payload(payload.get("overrides"))
    now = utc_now_iso()
    columns = _table_columns(conn, "sources")

    if "overrides" in columns:
        conn.execute(
            """
            INSERT INTO sources
                (id, name, enabled, kind, url, interval_minutes, tags_json,
                 base_url, default_frequency_minutes, overrides, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
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
                json_dumps(overrides) if overrides is not None else None,
                now,
                now,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO sources
                (id, name, enabled, kind, url, interval_minutes, tags_json,
                 base_url, default_frequency_minutes, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def _generate_source_id(conn: Any, name: str) -> str:
    base = _slugify(name)
    candidate = base
    i = 2
    while get_source(conn, candidate) is not None:
        candidate = f"{base}-{i}"
        i += 1
    return candidate


def update_source(conn: Any, source_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    current = get_source(conn, source_id)
    if not current:
        raise ValueError("source_not_found")

    name = str(payload.get("name") or current["name"]).strip()
    kind = str(payload.get("kind") or current.get("kind") or "rss").strip()
    url = str(payload.get("url") or current.get("url") or "").strip()
    enabled = bool(payload.get("enabled", current.get("enabled", True)))
    interval = int(payload.get("interval_minutes", current.get("interval_minutes", 60)))
    tags = _parse_tags(payload.get("tags", current.get("tags")))
    overrides = _normalize_overrides_payload(payload.get("overrides", current.get("overrides")))
    now = utc_now_iso()
    columns = _table_columns(conn, "sources")

    if "overrides" in columns:
        conn.execute(
            """
            UPDATE sources
            SET name = %s, enabled = %s, kind = %s, url = %s, interval_minutes = %s,
                tags_json = %s, base_url = %s, default_frequency_minutes = %s,
                overrides = %s::jsonb, updated_at = %s
            WHERE id = %s
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
                json_dumps(overrides) if overrides is not None else None,
                now,
                source_id,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE sources
            SET name = %s, enabled = %s, kind = %s, url = %s, interval_minutes = %s,
                tags_json = %s, base_url = %s, default_frequency_minutes = %s, updated_at = %s
            WHERE id = %s
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


def delete_source(conn: Any, source_id: str) -> None:
    conn.execute("DELETE FROM source_tactics WHERE source_id = %s", (source_id,))
    conn.execute("DELETE FROM source_health_history WHERE source_id = %s", (source_id,))
    conn.execute("DELETE FROM source_runs WHERE source_id = %s", (source_id,))
    conn.execute("DELETE FROM health_alerts WHERE source_id = %s", (source_id,))
    conn.execute("DELETE FROM sources WHERE id = %s", (source_id,))
    conn.commit()


def record_test_result(
    conn: Any, source_id: str, ok: bool, error: str | None
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE sources
        SET last_checked_at = %s, last_ok_at = %s, last_error = %s, updated_at = %s
        WHERE id = %s
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


def _ensure_tactic(conn: Any, source_id: str, kind: str, url: str, enabled: bool) -> None:
    tactic_type = "rss" if kind == "rss" else "html_index"
    config: dict[str, Any] = {"feed_url": url}
    tactic = SourceTactic(
        id=None,
        source_id=source_id,
        tactic_type=tactic_type,
        enabled=enabled,
        priority=0,
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


def _parse_overrides(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _normalize_overrides_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _table_columns(conn: Any, table: str) -> set[str]:
    cursor = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {row[0] for row in cursor.fetchall()}


def _table_exists(conn: Any, table: str) -> bool:
    cursor = conn.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    row = cursor.fetchone()
    return bool(row and row[0])


def _active_acquire_jobs(conn: Any) -> dict[str, dict[str, str]]:
    if not _table_exists(conn, "jobs"):
        return {}
    cursor = conn.execute(
        """
        SELECT id, status, payload_json, requested_at
        FROM jobs
        WHERE job_type = 'source_acquire' AND status IN ('queued', 'running')
        ORDER BY requested_at DESC
        """
    )
    mapping: dict[str, dict[str, str]] = {}
    for job_id, status, payload_json, _ in cursor.fetchall():
        try:
            payload = json.loads(payload_json) if payload_json else {}
        except json.JSONDecodeError:
            payload = {}
        source_id = payload.get("source_id")
        if not source_id or source_id in mapping:
            continue
        mapping[str(source_id)] = {"job_id": job_id, "status": status}
    return mapping


def _int_or_default(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 60


def _last_successful_poll(conn: Any, source_id: str | None) -> str | None:
    if not source_id or not _table_columns(conn, "source_health_history"):
        return None
    row = conn.execute(
        """
        SELECT ts
        FROM source_health_history
        WHERE source_id = %s AND ok = 1
        ORDER BY ts DESC
        LIMIT 1
        """,
        (source_id,),
    ).fetchone()
    return row[0] if row else None


def _last_article_ingested(conn: Any, source_id: str | None) -> str | None:
    if not source_id or not _table_columns(conn, "articles"):
        return None
    row = conn.execute(
        """
        SELECT MAX(ingested_at)
        FROM articles
        WHERE source_id = %s
        """,
        (source_id,),
    ).fetchone()
    return row[0] if row else None
