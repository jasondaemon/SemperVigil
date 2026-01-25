from __future__ import annotations

import json
import logging
import sqlite3
from typing import Callable

from .utils import json_dumps, utc_now_iso

Migration = Callable[[sqlite3.Connection], None]

def apply_migrations(conn: sqlite3.Connection) -> None:
    # Schema stable as of v0.1 — future changes via migrations only.
    logger = logging.getLogger("sempervigil.migrations")
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )
    applied = {
        row[0]
        for row in conn.execute("SELECT version FROM schema_migrations").fetchall()
    }
    try:
        for version, migration in _get_migrations():
            if version in applied:
                logger.debug("migration_skipped version=%s", version)
                continue
            migration(conn)
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, utc_now_iso()),
            )
            logger.info("migration_applied version=%s", version)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


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


def _migration_jobs_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NULL,
            result_json TEXT NULL,
            requested_at TEXT NOT NULL,
            started_at TEXT NULL,
            finished_at TEXT NULL,
            locked_by TEXT NULL,
            locked_at TEXT NULL,
            error TEXT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_status_requested ON jobs(status, requested_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_locked ON jobs(locked_by, locked_at)"
    )


def _migration_jobs_result_json(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "jobs"):
        return
    columns = _table_columns(conn, "jobs")
    if "result_json" in columns:
        return
    conn.execute("ALTER TABLE jobs ADD COLUMN result_json TEXT NULL")


def _migration_health_alerts(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS health_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL REFERENCES sources(id),
            alert_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_health_alerts_source ON health_alerts(source_id, created_at)"
    )


def _migration_cve_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cves (
            cve_id TEXT PRIMARY KEY,
            published_at TEXT NULL,
            last_modified_at TEXT NULL,
            preferred_cvss_version TEXT NULL,
            preferred_base_score REAL NULL,
            preferred_base_severity TEXT NULL,
            preferred_vector TEXT NULL,
            cvss_v40_json TEXT NULL,
            cvss_v31_json TEXT NULL,
            cwe_ids_json TEXT NULL,
            vuln_tags_json TEXT NULL,
            affected_products_json TEXT NULL,
            affected_cpes_json TEXT NULL,
            reference_domains_json TEXT NULL,
            description_text TEXT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cve_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cve_id TEXT NOT NULL REFERENCES cves(cve_id),
            observed_at TEXT NOT NULL,
            nvd_last_modified_at TEXT NULL,
            preferred_cvss_version TEXT NULL,
            preferred_base_score REAL NULL,
            preferred_base_severity TEXT NULL,
            preferred_vector TEXT NULL,
            cvss_v40_json TEXT NULL,
            cvss_v31_json TEXT NULL,
            snapshot_hash TEXT NOT NULL,
            UNIQUE(cve_id, snapshot_hash)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cve_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cve_id TEXT NOT NULL REFERENCES cves(cve_id),
            change_at TEXT NOT NULL,
            cvss_version TEXT NULL,
            change_type TEXT NOT NULL,
            from_score REAL NULL,
            to_score REAL NULL,
            from_severity TEXT NULL,
            to_severity TEXT NULL,
            vector_from TEXT NULL,
            vector_to TEXT NULL,
            metrics_changed_json TEXT NULL,
            note TEXT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cves_last_modified ON cves(last_modified_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cve_snapshots_cve ON cve_snapshots(cve_id, observed_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cve_changes_cve ON cve_changes(cve_id, change_at DESC)"
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)
    )
    return cursor.fetchone() is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _get_migrations() -> list[tuple[str, Migration]]:
    return [
        ("001_initial_schema", _migration_initial_schema),
        ("002_jobs_table", _migration_jobs_table),
        ("003_jobs_result_json", _migration_jobs_result_json),
        ("004_health_alerts", _migration_health_alerts),
        ("005_cve_tables", _migration_cve_tables),
        ("006_sources_admin_fields", _migration_sources_admin_fields),
        ("007_llm_config", _migration_llm_config),
        ("008_article_content_and_health", _migration_article_content_and_health),
        ("009_products_catalog", _migration_products_catalog),
        ("010_events", _migration_events),
        ("011_product_key_normalize", _migration_product_key_normalize),
    ]


def _migration_sources_admin_fields(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "sources"):
        return
    columns = _table_columns(conn, "sources")
    to_add = {
        "kind": "TEXT NULL",
        "url": "TEXT NULL",
        "interval_minutes": "INTEGER NOT NULL DEFAULT 60",
        "tags_json": "TEXT NULL",
        "last_checked_at": "TEXT NULL",
        "last_ok_at": "TEXT NULL",
        "last_error": "TEXT NULL",
    }
    for column, definition in to_add.items():
        if column in columns:
            continue
        conn.execute(f"ALTER TABLE sources ADD COLUMN {column} {definition}")


def _migration_llm_config(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_providers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            type TEXT NOT NULL,
            base_url TEXT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            timeout_s INTEGER NOT NULL DEFAULT 30,
            retries INTEGER NOT NULL DEFAULT 2,
            last_test_status TEXT NULL,
            last_test_at TEXT NULL,
            last_test_error TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_provider_secrets (
            provider_id TEXT PRIMARY KEY REFERENCES llm_providers(id),
            key_id TEXT NOT NULL,
            api_key_enc TEXT NOT NULL,
            api_key_last4 TEXT NOT NULL,
            headers_enc TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_models (
            id TEXT PRIMARY KEY,
            provider_id TEXT NOT NULL REFERENCES llm_providers(id),
            model_name TEXT NOT NULL,
            max_context INTEGER NULL,
            default_params_json TEXT NULL,
            tags_json TEXT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_prompts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            system_template TEXT NOT NULL,
            user_template TEXT NOT NULL,
            notes TEXT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_schemas (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            version TEXT NOT NULL,
            json_schema TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_profiles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            primary_provider_id TEXT NOT NULL REFERENCES llm_providers(id),
            primary_model_id TEXT NOT NULL REFERENCES llm_models(id),
            prompt_id TEXT NOT NULL REFERENCES llm_prompts(id),
            schema_id TEXT NULL REFERENCES llm_schemas(id),
            params_json TEXT NULL,
            fallback_json TEXT NULL,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_stage_config (
            stage_name TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL REFERENCES llm_profiles(id),
            rules_json TEXT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_llm_models_provider ON llm_models(provider_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_llm_profiles_provider ON llm_profiles(primary_provider_id)"
    )


def _migration_article_content_and_health(conn: sqlite3.Connection) -> None:
    if _table_exists(conn, "articles"):
        columns = _table_columns(conn, "articles")
        to_add = {
            "content_text": "TEXT NULL",
            "content_html": "TEXT NULL",
            "content_fetched_at": "TEXT NULL",
            "content_error": "TEXT NULL",
            "summary_llm": "TEXT NULL",
            "summary_model": "TEXT NULL",
            "summary_generated_at": "TEXT NULL",
            "summary_error": "TEXT NULL",
            "brief_day": "TEXT NULL",
            "has_full_content": "INTEGER NOT NULL DEFAULT 0",
        }
        for column, definition in to_add.items():
            if column in columns:
                continue
            conn.execute(f"ALTER TABLE articles ADD COLUMN {column} {definition}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_source_published ON articles(source_id, published_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_articles_brief_day ON articles(brief_day)"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_health_history (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL REFERENCES sources(id),
            ts TEXT NOT NULL,
            ok INTEGER NOT NULL,
            found_count INTEGER NOT NULL DEFAULT 0,
            accepted_count INTEGER NOT NULL DEFAULT 0,
            seen_count INTEGER NOT NULL DEFAULT 0,
            filtered_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NULL,
            duration_ms INTEGER NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_source_health_source_ts ON source_health_history(source_id, ts DESC)"
    )


def _migration_products_catalog(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_norm TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id INTEGER NOT NULL REFERENCES vendors(id),
            name_norm TEXT NOT NULL,
            display_name TEXT NOT NULL,
            product_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            UNIQUE(vendor_id, name_norm)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cve_products (
            cve_id TEXT NOT NULL REFERENCES cves(cve_id),
            product_id INTEGER NOT NULL REFERENCES products(id),
            source TEXT NOT NULL DEFAULT 'nvd',
            evidence_json TEXT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(cve_id, product_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_key ON products(product_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_name ON products(name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_vendor ON products(vendor_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_products_product ON cve_products(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_products_cve ON cve_products(cve_id)")


def _migration_events(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NULL,
            severity TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            meta_json TEXT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_last_seen ON events(last_seen_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_items (
            event_id TEXT NOT NULL REFERENCES events(id),
            item_type TEXT NOT NULL,
            item_key TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (event_id, item_type, item_key)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_items_type_key ON event_items(item_type, item_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_items_event ON event_items(event_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_signals (
            event_id TEXT NOT NULL REFERENCES events(id),
            signal_type TEXT NOT NULL,
            signal_value TEXT NOT NULL,
            weight REAL NOT NULL DEFAULT 1.0,
            created_at TEXT NOT NULL,
            PRIMARY KEY (event_id, signal_type, signal_value)
        )
        """
    )


def _migration_product_key_normalize(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "products") or not _table_exists(conn, "vendors"):
        return
    cursor = conn.execute(
        """
        SELECT p.id, p.name_norm, v.name_norm
        FROM products p
        JOIN vendors v ON v.id = p.vendor_id
        """
    )
    for product_id, product_norm, vendor_norm in cursor.fetchall():
        product_key = f"{vendor_norm}:{product_norm}"
        conn.execute(
            "UPDATE products SET product_key = ? WHERE id = ?",
            (product_key, product_id),
        )


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
        (source_id, kind, 1, 100, json_dumps(config), None, None, 0, now, now),
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
