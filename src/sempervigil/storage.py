from __future__ import annotations

import json
import os
import sqlite3
from typing import Iterable

from .models import Article, Source
from .utils import utc_now_iso


def init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            source_id TEXT NOT NULL,
            published_at TEXT,
            fetched_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            status TEXT NOT NULL,
            http_status INTEGER,
            found_count INTEGER,
            accepted_count INTEGER,
            error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            url TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            tags_json TEXT NOT NULL,
            overrides_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
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
            (id, name, type, url, enabled, tags_json, overrides_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            type=excluded.type,
            url=excluded.url,
            enabled=excluded.enabled,
            tags_json=excluded.tags_json,
            overrides_json=excluded.overrides_json,
            created_at=excluded.created_at,
            updated_at=excluded.updated_at
        """,
        (
            source.id,
            source.name,
            source.type,
            source.url,
            1 if source.enabled else 0,
            json.dumps(source.tags),
            json.dumps(source.overrides),
            created_at,
            updated_at,
        ),
    )
    conn.commit()


def get_source(conn: sqlite3.Connection, source_id: str) -> Source | None:
    cursor = conn.execute(
        """
        SELECT id, name, type, url, enabled, tags_json, overrides_json
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
            SELECT id, name, type, url, enabled, tags_json, overrides_json
            FROM sources
            WHERE enabled = 1
            ORDER BY id
            """
        )
    else:
        cursor = conn.execute(
            """
            SELECT id, name, type, url, enabled, tags_json, overrides_json
            FROM sources
            ORDER BY id
            """
        )
    return [_row_to_source(row) for row in cursor.fetchall()]


def article_exists(conn: sqlite3.Connection, article_id: str) -> bool:
    cursor = conn.execute("SELECT 1 FROM articles WHERE id = ?", (article_id,))
    return cursor.fetchone() is not None


def insert_articles(conn: sqlite3.Connection, articles: Iterable[Article]) -> int:
    rows = [
        (
            article.id,
            article.url,
            article.title,
            article.source_id,
            article.published_at,
            article.fetched_at,
        )
        for article in articles
    ]
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT OR IGNORE INTO articles
            (id, url, title, source_id, published_at, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def record_source_run(
    conn: sqlite3.Connection,
    source_id: str,
    started_at: str,
    status: str,
    http_status: int | None,
    found_count: int,
    accepted_count: int,
    error: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO source_runs
            (source_id, started_at, status, http_status, found_count, accepted_count, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            started_at,
            status,
            http_status,
            found_count,
            accepted_count,
            error,
        ),
    )
    conn.commit()


def _row_to_source(row: sqlite3.Row | tuple) -> Source:
    source_id, name, source_type, url, enabled, tags_json, overrides_json = row
    try:
        tags = json.loads(tags_json) if tags_json else []
    except json.JSONDecodeError:
        tags = []
    try:
        overrides = json.loads(overrides_json) if overrides_json else {}
    except json.JSONDecodeError:
        overrides = {}
    return Source(
        id=source_id,
        name=name,
        type=source_type,
        url=url,
        enabled=bool(enabled),
        tags=tags if isinstance(tags, list) else [],
        overrides=overrides if isinstance(overrides, dict) else {},
    )


def _source_from_dict(source_dict: dict[str, object]) -> Source:
    source_id = source_dict.get("id")
    source_type = source_dict.get("type")
    url = source_dict.get("url")
    name = source_dict.get("name") or source_id
    enabled = source_dict.get("enabled", True)
    tags = source_dict.get("tags") or []
    overrides = source_dict.get("overrides") or {}
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source.id is required")
    if source_type not in {"rss", "atom", "html"}:
        raise ValueError("source.type must be one of rss, atom, html")
    if not isinstance(url, str) or not url.strip():
        raise ValueError("source.url is required")
    if not isinstance(tags, list):
        raise ValueError("source.tags must be a list")
    if not isinstance(overrides, dict):
        raise ValueError("source.overrides must be a mapping")
    return Source(
        id=source_id,
        name=str(name),
        type=str(source_type),
        url=str(url),
        enabled=bool(enabled),
        tags=[str(tag) for tag in tags],
        overrides={str(k): v for k, v in overrides.items()},
    )
