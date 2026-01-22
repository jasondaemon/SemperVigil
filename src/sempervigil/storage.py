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
            published_at_source TEXT,
            tags_json TEXT,
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
            kind TEXT NOT NULL,
            url TEXT NOT NULL,
            enabled INTEGER NOT NULL,
            section TEXT NOT NULL,
            policy_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    _ensure_articles_schema(conn)
    _ensure_sources_schema(conn)
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
            (id, name, kind, url, enabled, section, policy_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            kind=excluded.kind,
            url=excluded.url,
            enabled=excluded.enabled,
            section=excluded.section,
            policy_json=excluded.policy_json,
            created_at=excluded.created_at,
            updated_at=excluded.updated_at
        """,
        (
            source.id,
            source.name,
            source.kind,
            source.url,
            1 if source.enabled else 0,
            source.section,
            json.dumps(source.policy),
            created_at,
            updated_at,
        ),
    )
    conn.commit()


def get_source(conn: sqlite3.Connection, source_id: str) -> Source | None:
    cursor = conn.execute(
        """
        SELECT id, name, kind, url, enabled, section, policy_json, tags_json, overrides_json, type
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
            SELECT id, name, kind, url, enabled, section, policy_json, tags_json, overrides_json, type
            FROM sources
            WHERE enabled = 1
            ORDER BY id
            """
        )
    else:
        cursor = conn.execute(
            """
            SELECT id, name, kind, url, enabled, section, policy_json, tags_json, overrides_json, type
            FROM sources
            ORDER BY id
            """
        )
    return [_row_to_source(row) for row in cursor.fetchall()]


def article_exists(conn: sqlite3.Connection, article_id: str) -> bool:
    cursor = conn.execute("SELECT 1 FROM articles WHERE id = ?", (article_id,))
    return cursor.fetchone() is not None


def insert_articles(conn: sqlite3.Connection, articles: Iterable[Article]) -> int:
    columns = _articles_columns(conn)
    has_published_source = "published_at_source" in columns
    has_tags = "tags_json" in columns
    rows = []
    for article in articles:
        row = [
            article.id,
            article.url,
            article.title,
            article.source_id,
            article.published_at,
        ]
        if has_published_source:
            row.append(article.published_at_source)
        if has_tags:
            row.append(json.dumps(article.tags))
        row.append(article.fetched_at)
        rows.append(tuple(row))
    if not rows:
        return 0
    if has_published_source and has_tags:
        conn.executemany(
            """
            INSERT OR IGNORE INTO articles
                (id, url, title, source_id, published_at, published_at_source, tags_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    elif has_published_source:
        conn.executemany(
            """
            INSERT OR IGNORE INTO articles
                (id, url, title, source_id, published_at, published_at_source, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    elif has_tags:
        conn.executemany(
            """
            INSERT OR IGNORE INTO articles
                (id, url, title, source_id, published_at, tags_json, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    else:
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
    (
        source_id,
        name,
        kind,
        url,
        enabled,
        section,
        policy_json,
        tags_json,
        overrides_json,
        legacy_type,
    ) = row
    policy = _policy_from_row(policy_json, tags_json, overrides_json)
    return Source(
        id=source_id,
        name=name,
        kind=kind or legacy_type or "rss",
        url=url,
        enabled=bool(enabled),
        section=section or "posts",
        policy=policy,
    )


def _articles_columns(conn: sqlite3.Connection) -> set[str]:
    cursor = conn.execute("PRAGMA table_info(articles)")
    return {row[1] for row in cursor.fetchall()}


def _ensure_articles_schema(conn: sqlite3.Connection) -> None:
    columns = _articles_columns(conn)
    if "published_at_source" not in columns:
        conn.execute("ALTER TABLE articles ADD COLUMN published_at_source TEXT")
    if "tags_json" not in columns:
        conn.execute("ALTER TABLE articles ADD COLUMN tags_json TEXT")
    conn.commit()


def _source_from_dict(source_dict: dict[str, object]) -> Source:
    source_id = source_dict.get("id")
    source_kind = source_dict.get("kind") or source_dict.get("type")
    url = source_dict.get("url")
    name = source_dict.get("name") or source_id
    enabled = source_dict.get("enabled", True)
    section = source_dict.get("section") or "posts"
    policy = source_dict.get("policy_json") or source_dict.get("policy") or {}
    tags = source_dict.get("tags") or []
    overrides = source_dict.get("overrides") or {}
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError("source.id is required")
    if source_kind not in {"rss", "atom", "html"}:
        raise ValueError("source.kind must be one of rss, atom, html")
    if not isinstance(url, str) or not url.strip():
        raise ValueError("source.url is required")
    if not isinstance(section, str) or not section.strip():
        raise ValueError("source.section must be a non-empty string")
    if not isinstance(policy, dict):
        raise ValueError("source.policy_json must be a mapping")
    if tags and not isinstance(tags, list):
        raise ValueError("source.tags must be a list")
    if overrides and not isinstance(overrides, dict):
        raise ValueError("source.overrides must be a mapping")
    return Source(
        id=source_id,
        name=str(name),
        kind=str(source_kind),
        url=str(url),
        enabled=bool(enabled),
        section=str(section),
        policy=_policy_from_row(json.dumps(policy), json.dumps(tags), json.dumps(overrides)),
    )


def _policy_from_row(
    policy_json: str | None,
    tags_json: str | None,
    overrides_json: str | None,
) -> dict[str, object]:
    try:
        policy = json.loads(policy_json) if policy_json else {}
    except json.JSONDecodeError:
        policy = {}
    if policy:
        return policy if isinstance(policy, dict) else {}
    try:
        tags = json.loads(tags_json) if tags_json else []
    except json.JSONDecodeError:
        tags = []
    try:
        overrides = json.loads(overrides_json) if overrides_json else {}
    except json.JSONDecodeError:
        overrides = {}
    policy: dict[str, object] = {}
    if isinstance(tags, list) and tags:
        policy["tags"] = {"tag_defaults": tags}
    if isinstance(overrides, dict) and overrides:
        if overrides.get("parse", {}).get("prefer_entry_summary") is not None:
            policy.setdefault("parse", {})["prefer_entry_summary"] = overrides["parse"][
                "prefer_entry_summary"
            ]
        if overrides.get("http_headers"):
            policy.setdefault("fetch", {})["headers"] = overrides["http_headers"]
    return policy


def _ensure_sources_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(sources)")
    columns = {row[1] for row in cursor.fetchall()}
    if "kind" not in columns:
        conn.execute("ALTER TABLE sources ADD COLUMN kind TEXT")
    if "section" not in columns:
        conn.execute("ALTER TABLE sources ADD COLUMN section TEXT")
    if "policy_json" not in columns:
        conn.execute("ALTER TABLE sources ADD COLUMN policy_json TEXT")
    if "tags_json" not in columns:
        conn.execute("ALTER TABLE sources ADD COLUMN tags_json TEXT")
    if "overrides_json" not in columns:
        conn.execute("ALTER TABLE sources ADD COLUMN overrides_json TEXT")
    if "type" not in columns:
        conn.execute("ALTER TABLE sources ADD COLUMN type TEXT")
    conn.commit()
