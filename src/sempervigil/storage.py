from __future__ import annotations

import json
import os
import sqlite3
from typing import Iterable

from .migrations import apply_migrations
from .models import Article, Source, SourceTactic
from .utils import utc_now_iso


def init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    apply_migrations(conn)
    return conn


def upsert_source(conn: sqlite3.Connection, source_dict: dict[str, object]) -> None:
    source = _source_from_dict(source_dict)
    cursor = conn.execute("SELECT created_at FROM sources WHERE id = ?", (source.id,))
    row = cursor.fetchone()
    created_at = row[0] if row else utc_now_iso()
    updated_at = utc_now_iso()
    conn.execute(
        """
        INSERT INTO sources
            (id, name, enabled, base_url, topic_key, default_frequency_minutes,
             pause_until, paused_reason, robots_notes, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            enabled=excluded.enabled,
            base_url=excluded.base_url,
            topic_key=excluded.topic_key,
            default_frequency_minutes=excluded.default_frequency_minutes,
            pause_until=excluded.pause_until,
            paused_reason=excluded.paused_reason,
            robots_notes=excluded.robots_notes,
            updated_at=excluded.updated_at
        """,
        (
            source.id,
            source.name,
            1 if source.enabled else 0,
            source.base_url,
            source.topic_key,
            source.default_frequency_minutes,
            source.pause_until,
            source.paused_reason,
            source.robots_notes,
            created_at,
            updated_at,
        ),
    )
    conn.commit()


def set_source_enabled(conn: sqlite3.Connection, source_id: str, enabled: bool) -> None:
    conn.execute(
        "UPDATE sources SET enabled = ?, updated_at = ? WHERE id = ?",
        (1 if enabled else 0, utc_now_iso(), source_id),
    )
    conn.commit()


def get_source(conn: sqlite3.Connection, source_id: str) -> Source | None:
    cursor = conn.execute(
        """
        SELECT id, name, enabled, base_url, topic_key, default_frequency_minutes,
               pause_until, paused_reason, robots_notes
        FROM sources
        WHERE id = ?
        """,
        (source_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return _row_to_source(row)


def list_sources(conn: sqlite3.Connection, enabled_only: bool = True) -> list[Source]:
    if enabled_only:
        cursor = conn.execute(
            """
            SELECT id, name, enabled, base_url, topic_key, default_frequency_minutes,
                   pause_until, paused_reason, robots_notes
            FROM sources
            WHERE enabled = 1
            ORDER BY id
            """
        )
    else:
        cursor = conn.execute(
            """
            SELECT id, name, enabled, base_url, topic_key, default_frequency_minutes,
                   pause_until, paused_reason, robots_notes
            FROM sources
            ORDER BY id
            """
        )
    return [_row_to_source(row) for row in cursor.fetchall()]


def list_tactics(conn: sqlite3.Connection, source_id: str) -> list[SourceTactic]:
    cursor = conn.execute(
        """
        SELECT id, source_id, tactic_type, enabled, priority, config_json,
               last_success_at, last_error_at, error_streak
        FROM source_tactics
        WHERE source_id = ? AND enabled = 1
        ORDER BY priority ASC
        """,
        (source_id,),
    )
    rows = cursor.fetchall()
    return [_row_to_tactic(row) for row in rows]


def upsert_tactic(conn: sqlite3.Connection, tactic: SourceTactic) -> None:
    updated_at = utc_now_iso()
    created_at = utc_now_iso()
    conn.execute(
        """
        INSERT INTO source_tactics
            (source_id, tactic_type, enabled, priority, config_json,
             last_success_at, last_error_at, error_streak, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id, tactic_type, priority) DO UPDATE SET
            enabled=excluded.enabled,
            config_json=excluded.config_json,
            last_success_at=excluded.last_success_at,
            last_error_at=excluded.last_error_at,
            error_streak=excluded.error_streak,
            updated_at=excluded.updated_at
        """,
        (
            tactic.source_id,
            tactic.tactic_type,
            1 if tactic.enabled else 0,
            tactic.priority,
            json.dumps(tactic.config),
            tactic.last_success_at,
            tactic.last_error_at,
            tactic.error_streak,
            created_at,
            updated_at,
        ),
    )
    conn.commit()


def article_exists(conn: sqlite3.Connection, source_id: str, stable_id: str) -> bool:
    cursor = conn.execute(
        "SELECT 1 FROM articles WHERE source_id = ? AND stable_id = ?",
        (source_id, stable_id),
    )
    return cursor.fetchone() is not None


def insert_articles(conn: sqlite3.Connection, articles: Iterable[Article]) -> int:
    rows = [
        (
            article.source_id,
            article.stable_id,
            article.original_url,
            article.normalized_url,
            article.title,
            article.published_at,
            article.published_at_source,
            article.ingested_at,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            article.ingested_at,
            article.ingested_at,
        )
        for article in articles
    ]
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT OR IGNORE INTO articles
            (source_id, stable_id, original_url, normalized_url, title, published_at,
             published_at_source, ingested_at, is_commercial, content_fingerprint,
             extracted_text_path, extracted_text_hash, raw_html_path, raw_html_hash,
             meta_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()

    for article in articles:
        article_id = _get_article_id(conn, article.source_id, article.stable_id)
        if article_id is None:
            continue
        _insert_article_tags(conn, article_id, article.tags)

    return len(rows)


def record_source_run(
    conn: sqlite3.Connection,
    source_id: str,
    started_at: str,
    finished_at: str | None,
    status: str,
    http_status: int | None,
    items_found: int,
    items_accepted: int,
    skipped_duplicates: int,
    skipped_filters: int,
    skipped_missing_url: int,
    error: str | None,
    notes: dict[str, object] | None,
) -> None:
    conn.execute(
        """
        INSERT INTO source_runs
            (source_id, started_at, finished_at, status, http_status, items_found,
             items_accepted, skipped_duplicates, skipped_filters, skipped_missing_url,
             error, notes_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            started_at,
            finished_at,
            status,
            http_status,
            items_found,
            items_accepted,
            skipped_duplicates,
            skipped_filters,
            skipped_missing_url,
            error,
            json.dumps(notes) if notes else None,
            started_at,
        ),
    )
    conn.commit()


def _row_to_source(row: tuple) -> Source:
    (
        source_id,
        name,
        enabled,
        base_url,
        topic_key,
        default_frequency_minutes,
        pause_until,
        paused_reason,
        robots_notes,
    ) = row
    return Source(
        id=source_id,
        name=name,
        enabled=bool(enabled),
        base_url=base_url,
        topic_key=topic_key,
        default_frequency_minutes=int(default_frequency_minutes),
        pause_until=pause_until,
        paused_reason=paused_reason,
        robots_notes=robots_notes,
    )


def _row_to_tactic(row: tuple) -> SourceTactic:
    (
        tactic_id,
        source_id,
        tactic_type,
        enabled,
        priority,
        config_json,
        last_success_at,
        last_error_at,
        error_streak,
    ) = row
    try:
        config = json.loads(config_json) if config_json else {}
    except json.JSONDecodeError:
        config = {}
    return SourceTactic(
        id=tactic_id,
        source_id=source_id,
        tactic_type=tactic_type,
        enabled=bool(enabled),
        priority=int(priority),
        config=config,
        last_success_at=last_success_at,
        last_error_at=last_error_at,
        error_streak=int(error_streak),
    )


def _get_article_id(conn: sqlite3.Connection, source_id: str, stable_id: str) -> int | None:
    cursor = conn.execute(
        "SELECT id FROM articles WHERE source_id = ? AND stable_id = ?",
        (source_id, stable_id),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _insert_article_tags(conn: sqlite3.Connection, article_id: int, tags: list[str]) -> None:
    if not tags:
        return
    rows = [(article_id, tag, None) for tag in tags]
    conn.executemany(
        """
        INSERT OR IGNORE INTO article_tags (article_id, tag, tag_type)
        VALUES (?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def _source_from_dict(source_dict: dict[str, object]) -> Source:
    source_id = source_dict.get("id")
    name = source_dict.get("name") or source_id
    enabled = source_dict.get("enabled", True)
    base_url = source_dict.get("base_url") or source_dict.get("url")
    topic_key = source_dict.get("topic_key")
    default_frequency_minutes = source_dict.get("default_frequency_minutes", 60)
    pause_until = source_dict.get("pause_until")
    paused_reason = source_dict.get("paused_reason")
    robots_notes = source_dict.get("robots_notes")

    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source.id is required")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("source.name is required")
    if base_url is not None and not isinstance(base_url, str):
        raise ValueError("source.base_url must be a string")
    if not isinstance(default_frequency_minutes, int):
        raise ValueError("source.default_frequency_minutes must be an integer")

    return Source(
        id=source_id,
        name=str(name),
        enabled=bool(enabled),
        base_url=str(base_url) if base_url else None,
        topic_key=str(topic_key) if topic_key else None,
        default_frequency_minutes=int(default_frequency_minutes),
        pause_until=str(pause_until) if pause_until else None,
        paused_reason=str(paused_reason) if paused_reason else None,
        robots_notes=str(robots_notes) if robots_notes else None,
    )
