from __future__ import annotations

import json
import sqlite3
from typing import Callable

from .utils import utc_now_iso

Migration = Callable[[sqlite3.Connection], None]


def apply_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )
    migrations = [
        ("001_initial_schema", _migration_initial_schema),
    ]
    applied = {
        row[0]
        for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
    }
    for version, migration in migrations:
        if version in applied:
            continue
        migration(conn)
        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
            (version, utc_now_iso()),
        )
        conn.commit()


def _migration_initial_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            base_url TEXT NULL,
            topic_key TEXT NULL,
            default_frequency_minutes INTEGER NOT NULL DEFAULT 60,
            pause_until TEXT NULL,
            paused_reason TEXT NULL,
            robots_notes TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_tactics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL REFERENCES sources(id),
            tactic_type TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            config_json TEXT NULL,
            last_success_at TEXT NULL,
            last_error_at TEXT NULL,
            error_streak INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_id, tactic_type, priority)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL REFERENCES sources(id),
            started_at TEXT NOT NULL,
            finished_at TEXT NULL,
            status TEXT NOT NULL,
            http_status INTEGER NULL,
            items_found INTEGER NOT NULL DEFAULT 0,
            items_accepted INTEGER NOT NULL DEFAULT 0,
            skipped_duplicates INTEGER NOT NULL DEFAULT 0,
            skipped_filters INTEGER NOT NULL DEFAULT 0,
            skipped_missing_url INTEGER NOT NULL DEFAULT 0,
            error TEXT NULL,
            notes_json TEXT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL REFERENCES sources(id),
            stable_id TEXT NOT NULL,
            original_url TEXT NOT NULL,
            normalized_url TEXT NOT NULL,
            title TEXT NOT NULL,
            published_at TEXT NULL,
            published_at_source TEXT NULL,
            ingested_at TEXT NOT NULL,
            is_commercial INTEGER NOT NULL DEFAULT 0,
            content_fingerprint TEXT NULL,
            extracted_text_path TEXT NULL,
            extracted_text_hash TEXT NULL,
            raw_html_path TEXT NULL,
            raw_html_hash TEXT NULL,
            meta_json TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_id, stable_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS article_tags (
            article_id INTEGER NOT NULL REFERENCES articles(id),
            tag TEXT NOT NULL,
            tag_type TEXT NULL,
            PRIMARY KEY(article_id, tag)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    _migrate_legacy_sources(conn)
    _migrate_legacy_source_runs(conn)
    _migrate_legacy_articles(conn)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)
    )
    return cursor.fetchone() is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate_legacy_sources(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "sources"):
        return
    columns = _table_columns(conn, "sources")
    if "base_url" in columns:
        return

    conn.execute("ALTER TABLE sources RENAME TO sources_legacy")
    conn.execute(
        """
        CREATE TABLE sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            base_url TEXT NULL,
            topic_key TEXT NULL,
            default_frequency_minutes INTEGER NOT NULL DEFAULT 60,
            pause_until TEXT NULL,
            paused_reason TEXT NULL,
            robots_notes TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    rows = conn.execute("SELECT id, name, enabled, url, created_at, updated_at FROM sources_legacy").fetchall()
    now = utc_now_iso()
    for row in rows:
        source_id, name, enabled, url, created_at, updated_at = row
        conn.execute(
            """
            INSERT INTO sources
                (id, name, enabled, base_url, topic_key, default_frequency_minutes,
                 pause_until, paused_reason, robots_notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                name,
                enabled,
                url,
                None,
                60,
                None,
                None,
                None,
                created_at or now,
                updated_at or now,
            ),
        )
        _seed_tactic_from_legacy(conn, source_id)


def _seed_tactic_from_legacy(conn: sqlite3.Connection, source_id: str) -> None:
    legacy = conn.execute(
        "SELECT kind, url, policy_json FROM sources_legacy WHERE id = ?",
        (source_id,),
    ).fetchone()
    if not legacy:
        return
    kind, url, policy_json = legacy
    if not kind or not url:
        return
    try:
        policy = json.loads(policy_json) if policy_json else {}
    except json.JSONDecodeError:
        policy = {}
    config = {"feed_url": url}
    if policy:
        config["policy"] = policy
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO source_tactics
            (source_id, tactic_type, enabled, priority, config_json,
             last_success_at, last_error_at, error_streak, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (source_id, kind, 1, 100, json.dumps(config), None, None, 0, now, now),
    )


def _migrate_legacy_source_runs(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "source_runs"):
        return
    columns = _table_columns(conn, "source_runs")
    if "items_found" in columns:
        return

    conn.execute("ALTER TABLE source_runs RENAME TO source_runs_legacy")
    conn.execute(
        """
        CREATE TABLE source_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL REFERENCES sources(id),
            started_at TEXT NOT NULL,
            finished_at TEXT NULL,
            status TEXT NOT NULL,
            http_status INTEGER NULL,
            items_found INTEGER NOT NULL DEFAULT 0,
            items_accepted INTEGER NOT NULL DEFAULT 0,
            skipped_duplicates INTEGER NOT NULL DEFAULT 0,
            skipped_filters INTEGER NOT NULL DEFAULT 0,
            skipped_missing_url INTEGER NOT NULL DEFAULT 0,
            error TEXT NULL,
            notes_json TEXT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    rows = conn.execute(
        "SELECT source_id, started_at, status, http_status, found_count, accepted_count, error FROM source_runs_legacy"
    ).fetchall()
    for row in rows:
        source_id, started_at, status, http_status, found_count, accepted_count, error = row
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
                None,
                status,
                http_status,
                found_count or 0,
                accepted_count or 0,
                0,
                0,
                0,
                error,
                None,
                started_at,
            ),
        )


def _migrate_legacy_articles(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "articles"):
        return
    columns = _table_columns(conn, "articles")
    if "stable_id" in columns:
        return

    conn.execute("ALTER TABLE articles RENAME TO articles_legacy")
    conn.execute(
        """
        CREATE TABLE articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL REFERENCES sources(id),
            stable_id TEXT NOT NULL,
            original_url TEXT NOT NULL,
            normalized_url TEXT NOT NULL,
            title TEXT NOT NULL,
            published_at TEXT NULL,
            published_at_source TEXT NULL,
            ingested_at TEXT NOT NULL,
            is_commercial INTEGER NOT NULL DEFAULT 0,
            content_fingerprint TEXT NULL,
            extracted_text_path TEXT NULL,
            extracted_text_hash TEXT NULL,
            raw_html_path TEXT NULL,
            raw_html_hash TEXT NULL,
            meta_json TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_id, stable_id)
        )
        """
    )
    rows = conn.execute(
        "SELECT id, url, title, source_id, published_at, published_at_source, tags_json, fetched_at FROM articles_legacy"
    ).fetchall()
    for row in rows:
        stable_id, url, title, source_id, published_at, published_at_source, tags_json, fetched_at = row
        ingested_at = fetched_at
        conn.execute(
            """
            INSERT INTO articles
                (source_id, stable_id, original_url, normalized_url, title, published_at,
                 published_at_source, ingested_at, is_commercial, content_fingerprint,
                 extracted_text_path, extracted_text_hash, raw_html_path, raw_html_hash,
                 meta_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                stable_id,
                url,
                url,
                title,
                published_at,
                published_at_source,
                ingested_at,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                ingested_at,
                ingested_at,
            ),
        )
        article_id = conn.execute(
            "SELECT id FROM articles WHERE source_id = ? AND stable_id = ?",
            (source_id, stable_id),
        ).fetchone()
        if not article_id:
            continue
        tags = []
        if tags_json:
            try:
                tags = json.loads(tags_json)
            except json.JSONDecodeError:
                tags = []
        for tag in tags or []:
            conn.execute(
                "INSERT OR IGNORE INTO article_tags (article_id, tag, tag_type) VALUES (?, ?, ?)",
                (article_id[0], str(tag), None),
            )
