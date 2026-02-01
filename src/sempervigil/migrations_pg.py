from __future__ import annotations

import logging

from .utils import utc_now_iso


def _has_column(conn, table: str, column: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table, column),
    ).fetchone()
    return row is not None


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = %s
        """,
        (table,),
    ).fetchone()
    return row is not None


def _events_visibility_ready(conn) -> bool:
    return _has_column(conn, "events", "candidate") and _has_column(conn, "events", "evidence")


def apply_migrations_pg(conn) -> None:
    logger = logging.getLogger("sempervigil.migrations")
    conn.execute("BEGIN")
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
    if "pg_bootstrap_001" in applied:
        if "pg_events_005" in applied and not _events_visibility_ready(conn):
            _migrate_events_visibility(conn)
            logger.info("migration_reapplied version=pg_events_005")

        if "pg_events_002" not in applied:
            _migrate_events_v2(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_events_002", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_events_002")
            applied.add("pg_events_002")
        if "pg_events_003" not in applied:
            _migrate_events_articles(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_events_003", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_events_003")
            applied.add("pg_events_003")
        if "pg_events_004" not in applied:
            _migrate_events_manual(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_events_004", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_events_004")
            applied.add("pg_events_004")
        if "pg_events_005" not in applied:
            _migrate_events_visibility(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_events_005", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_events_005")
            applied.add("pg_events_005")
        if "pg_event_enrich_006" not in applied:
            _migrate_event_web_sources(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_event_enrich_006", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_event_enrich_006")
            applied.add("pg_event_enrich_006")
        if "pg_article_products_007" not in applied:
            _migrate_article_products(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_article_products_007", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_article_products_007")
            applied.add("pg_article_products_007")
        if "pg_threat_actors_008" not in applied:
            _migrate_threat_actors(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_threat_actors_008", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_threat_actors_008")
            applied.add("pg_threat_actors_008")
        if "pg_cve_prompt_009" not in applied:
            _migrate_cve_enrich_prompt(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_cve_prompt_009", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_cve_prompt_009")
            applied.add("pg_cve_prompt_009")
        if "pg_vendor_product_tag_cleanup_010" not in applied:
            _migrate_vendor_product_tag_cleanup(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_vendor_product_tag_cleanup_010", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_vendor_product_tag_cleanup_010")
            applied.add("pg_vendor_product_tag_cleanup_010")
        if "pg_llm_vendor_product_prompts_011" not in applied:
            _migrate_llm_vendor_product_prompts(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_llm_vendor_product_prompts_011", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_llm_vendor_product_prompts_011")
            applied.add("pg_llm_vendor_product_prompts_011")
        if "pg_llm_event_classify_012" not in applied:
            _migrate_llm_event_classify_prompts(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_llm_event_classify_012", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_llm_event_classify_012")
            applied.add("pg_llm_event_classify_012")
        if "pg_source_overrides_013" not in applied:
            _migrate_source_overrides(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s) ON CONFLICT (version) DO NOTHING",
                ("pg_source_overrides_013", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_source_overrides_013")
            applied.add("pg_source_overrides_013")
        else:
            conn.commit()
        return
    _bootstrap_schema(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_bootstrap_001", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_bootstrap_001")

    conn.execute("BEGIN")
    _migrate_events_v2(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_events_002", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_events_002")

    conn.execute("BEGIN")
    _migrate_events_articles(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_events_003", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_events_003")

    conn.execute("BEGIN")
    _migrate_events_manual(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_events_004", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_events_004")

    conn.execute("BEGIN")
    _migrate_events_visibility(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_events_005", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_events_005")

    conn.execute("BEGIN")
    _migrate_event_web_sources(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_event_enrich_006", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_event_enrich_006")

    conn.execute("BEGIN")
    _migrate_article_products(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_article_products_007", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_article_products_007")

    conn.execute("BEGIN")
    _migrate_threat_actors(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_threat_actors_008", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_threat_actors_008")

    _migrate_cve_enrich_prompt(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_cve_prompt_009", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_cve_prompt_009")

    _migrate_vendor_product_tag_cleanup(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_vendor_product_tag_cleanup_010", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_vendor_product_tag_cleanup_010")

    _migrate_llm_vendor_product_prompts(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_llm_vendor_product_prompts_011", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_llm_vendor_product_prompts_011")

    _migrate_llm_event_classify_prompts(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_llm_event_classify_012", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_llm_event_classify_012")

def _bootstrap_schema(conn) -> None:
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
            updated_at TEXT NOT NULL,
            kind TEXT NULL,
            url TEXT NULL,
            interval_minutes INTEGER NOT NULL DEFAULT 60,
            tags_json TEXT NULL,
            overrides JSONB NULL,
            last_checked_at TEXT NULL,
            last_ok_at TEXT NULL,
            last_error TEXT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_tactics (
            id BIGSERIAL PRIMARY KEY,
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
            id BIGSERIAL PRIMARY KEY,
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
            id BIGSERIAL PRIMARY KEY,
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
            content_text TEXT NULL,
            content_html TEXT NULL,
            content_fetched_at TEXT NULL,
            content_error TEXT NULL,
            summary_llm TEXT NULL,
            summary_model TEXT NULL,
            summary_generated_at TEXT NULL,
            summary_error TEXT NULL,
            brief_day TEXT NULL,
            has_full_content INTEGER NOT NULL DEFAULT 0,
            UNIQUE(source_id, stable_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS article_tags (
            article_id BIGINT NOT NULL REFERENCES articles(id),
            tag TEXT NOT NULL,
            tag_type TEXT NULL,
            PRIMARY KEY(article_id, tag)
        )
        """
    )
    _create_article_product_tables(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
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
        """
        CREATE TABLE IF NOT EXISTS health_alerts (
            id BIGSERIAL PRIMARY KEY,
            source_id TEXT NOT NULL REFERENCES sources(id),
            alert_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
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
            cvss_v31_list_json TEXT NULL,
            cvss_v40_list_json TEXT NULL,
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
            id BIGSERIAL PRIMARY KEY,
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
            id BIGSERIAL PRIMARY KEY,
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
        """
        CREATE TABLE IF NOT EXISTS vendors (
            id BIGSERIAL PRIMARY KEY,
            name_norm TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id BIGSERIAL PRIMARY KEY,
            vendor_id BIGINT NOT NULL REFERENCES vendors(id),
            name_norm TEXT NOT NULL,
            display_name TEXT NOT NULL,
            product_key TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            UNIQUE(vendor_id, name_norm)
        )
        """
    )
    _create_threat_actor_tables(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cve_products (
            cve_id TEXT NOT NULL REFERENCES cves(cve_id),
            product_id BIGINT NOT NULL REFERENCES products(id),
            source TEXT NOT NULL DEFAULT 'nvd',
            evidence_json TEXT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(cve_id, product_id)
        )
        """
    )
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
            meta_json TEXT NULL,
            event_key TEXT NULL,
            occurred_at TEXT NULL,
            summary_updated_at TEXT NULL,
            confidence REAL NULL,
            manual INTEGER NOT NULL DEFAULT 0,
            is_manual INTEGER NOT NULL DEFAULT 0,
            visibility TEXT NOT NULL DEFAULT 'active',
            confidence_tier TEXT NOT NULL DEFAULT 'watch',
            reasons JSONB NOT NULL DEFAULT '[]'::jsonb
        )
        """
    )
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_articles (
            event_id TEXT NOT NULL REFERENCES events(id),
            article_id BIGINT NOT NULL REFERENCES articles(id),
            added_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (event_id, article_id)
        )
        """
    )
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_runs (
            id TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            job_id TEXT NULL,
            provider_id TEXT NULL,
            model_id TEXT NULL,
            prompt_name TEXT NULL,
            input_chars INTEGER NOT NULL DEFAULT 0,
            output_chars INTEGER NOT NULL DEFAULT 0,
            latency_ms INTEGER NOT NULL DEFAULT 0,
            ok INTEGER NOT NULL DEFAULT 0,
            error TEXT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cve_product_versions (
            cve_id TEXT NOT NULL REFERENCES cves(cve_id),
            product_id BIGINT NOT NULL REFERENCES products(id),
            version TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'nvd',
            created_at TEXT NOT NULL,
            PRIMARY KEY (cve_id, product_id, version)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watched_vendors (
            id TEXT PRIMARY KEY,
            vendor_norm TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watched_products (
            id TEXT PRIMARY KEY,
            vendor_norm TEXT NULL,
            product_norm TEXT NOT NULL,
            display_name TEXT NOT NULL,
            match_mode TEXT NOT NULL DEFAULT 'exact',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cve_scope (
            id TEXT PRIMARY KEY,
            cve_id TEXT NOT NULL UNIQUE,
            in_scope INTEGER NOT NULL,
            reasons_json TEXT NOT NULL,
            computed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_leases (
            lease_name TEXT PRIMARY KEY,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            holder TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_requested ON jobs(status, requested_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_locked ON jobs(locked_by, locked_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_health_alerts_source ON health_alerts(source_id, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cves_last_modified ON cves(last_modified_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_snapshots_cve ON cve_snapshots(cve_id, observed_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_changes_cve ON cve_changes(cve_id, change_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_models_provider ON llm_models(provider_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_profiles_provider ON llm_profiles(primary_provider_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_source_published ON articles(source_id, published_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_brief_day ON articles(brief_day)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_source_health_source_ts ON source_health_history(source_id, ts DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_key ON products(product_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_name ON products(name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_vendor ON products(vendor_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_products_product ON cve_products(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_products_cve ON cve_products(cve_id)")
    _create_threat_actor_indexes(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_last_seen ON events(last_seen_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_items_type_key ON event_items(item_type, item_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_items_event ON event_items(event_id)")


def _migrate_events_v2(conn) -> None:
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS event_key TEXT")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS occurred_at TEXT")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS summary_updated_at TEXT")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS confidence REAL")
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_key ON events(event_key)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_product_versions_product ON cve_product_versions(product_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_product_versions_cve ON cve_product_versions(cve_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_watched_vendors_norm ON watched_vendors(vendor_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_watched_products_norm ON watched_products(product_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_scope_cve ON cve_scope(cve_id)")


def _migrate_events_articles(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_articles (
            event_id TEXT NOT NULL REFERENCES events(id),
            article_id BIGINT NOT NULL REFERENCES articles(id),
            added_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (event_id, article_id)
        )
        """
    )


def _migrate_events_manual(conn) -> None:
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS manual INTEGER NOT NULL DEFAULT 0")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_manual ON events(manual)")


def _migrate_events_visibility(conn) -> None:
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS is_manual INTEGER NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'active'")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS confidence_tier TEXT NOT NULL DEFAULT 'watch'")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS candidate BOOLEAN NOT NULL DEFAULT false")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS entity TEXT")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS incident_date TEXT")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS evidence JSONB NOT NULL DEFAULT '[]'::jsonb")
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS reasons JSONB NOT NULL DEFAULT '[]'::jsonb")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_visibility ON events(visibility)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_kind_visibility ON events(kind, visibility)")


def _create_article_product_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS article_products (
            article_id BIGINT NOT NULL REFERENCES articles(id),
            product_id BIGINT NOT NULL REFERENCES products(id),
            source TEXT NOT NULL DEFAULT 'llm',
            evidence_json TEXT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(article_id, product_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_article_products_product_id ON article_products(product_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_article_products_article_id ON article_products(article_id)"
    )


def _create_threat_actor_tables(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS threat_actors (
            id BIGSERIAL PRIMARY KEY,
            actor_key TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            country TEXT NULL,
            confidence INTEGER NULL,
            first_seen TEXT NULL,
            last_seen TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS threat_actor_aliases (
            actor_id BIGINT NOT NULL REFERENCES threat_actors(id) ON DELETE CASCADE,
            alias TEXT NOT NULL,
            UNIQUE(actor_id, alias)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS article_threat_actors (
            article_id BIGINT NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
            actor_id BIGINT NOT NULL REFERENCES threat_actors(id) ON DELETE CASCADE,
            UNIQUE(article_id, actor_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cve_threat_actors (
            cve_id TEXT NOT NULL REFERENCES cves(cve_id) ON DELETE CASCADE,
            actor_id BIGINT NOT NULL REFERENCES threat_actors(id) ON DELETE CASCADE,
            UNIQUE(cve_id, actor_id)
        )
        """
    )


def _create_threat_actor_indexes(conn) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_threat_actors_key ON threat_actors(actor_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_threat_actors_display ON threat_actors(display_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_article_threat_actors_article ON article_threat_actors(article_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_article_threat_actors_actor ON article_threat_actors(actor_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_threat_actors_cve ON cve_threat_actors(cve_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cve_threat_actors_actor ON cve_threat_actors(actor_id)")


def _migrate_event_web_sources(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_web_sources (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
            url TEXT NOT NULL,
            url_hash TEXT NOT NULL,
            title TEXT,
            snippet TEXT,
            domain TEXT,
            published_at TEXT NULL,
            engine TEXT NULL,
            category TEXT NULL,
            score INTEGER NOT NULL DEFAULT 0,
            score_reasons JSONB NOT NULL DEFAULT '{}'::jsonb,
            status TEXT NOT NULL DEFAULT 'new',
            discovered_at TEXT NOT NULL DEFAULT now(),
            promoted_article_id BIGINT NULL REFERENCES articles(id) ON DELETE SET NULL,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            UNIQUE (event_id, url_hash)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_web_sources_event ON event_web_sources(event_id, discovered_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_web_sources_status ON event_web_sources(event_id, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_event_web_sources_domain ON event_web_sources(domain)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS article_candidates (
            id TEXT PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            url_hash TEXT NOT NULL UNIQUE,
            title TEXT,
            snippet TEXT,
            domain TEXT,
            discovered_at TEXT NOT NULL DEFAULT now(),
            status TEXT NOT NULL DEFAULT 'new',
            score INTEGER NOT NULL DEFAULT 0,
            score_reasons JSONB NOT NULL DEFAULT '{}'::jsonb,
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )


def _migrate_article_products(conn) -> None:
    _create_article_product_tables(conn)


def _migrate_threat_actors(conn) -> None:
    _create_threat_actor_tables(conn)
    _create_threat_actor_indexes(conn)


def _migrate_cve_enrich_prompt(conn) -> None:
    if not (
        _table_exists(conn, "pipeline_stage_config")
        and _table_exists(conn, "llm_profiles")
        and _table_exists(conn, "llm_prompts")
    ):
        return
    row = conn.execute(
        "SELECT profile_id FROM pipeline_stage_config WHERE stage_name = %s",
        ("cve_enrich_products",),
    ).fetchone()
    if not row:
        return
    profile_id = row[0]
    prompt_row = conn.execute(
        "SELECT prompt_id FROM llm_profiles WHERE id = %s",
        (profile_id,),
    ).fetchone()
    if not prompt_row:
        return
    prompt_id = prompt_row[0]
    if not prompt_id:
        return
    system_template = "\n".join(
        [
            "You extract affected software vendor/product/version from CVE text.",
            "Return JSON only.",
            "",
            "Rules:",
            "- Only return proper-noun product names.",
            "- Do NOT use generic nouns as products (e.g., software, application, system, service, library) unless part of the proper name.",
            "- Prefer evidence in this order:",
            "  1) Explicit CPEs or affected product lists",
            "  2) Sentences like \"affects X\" or \"vulnerability in X\"",
            "  3) Proper nouns near words like software/product/library/RMM",
            "- If vendor is unknown, set vendor to null.",
            "- If product is unknown or missing, omit the item.",
            "- Versions: keep raw strings; do not parse.",
            "",
            "Output schema:",
            "{\"items\": [{\"vendor\": null|\"Vendor\", \"product\": \"Product\", \"versions\": [\"<string>\"]}]}",
        ]
    )
    user_template = "{{input}}"
    conn.execute(
        """
        UPDATE llm_prompts
        SET system_template = %s,
            user_template = %s,
            version = %s,
            notes = %s
        WHERE id = %s
        """,
        (
            system_template,
            user_template,
            "2026-01-31",
            "Hardened CVE vendor/product extraction prompt for smaller models.",
            prompt_id,
        ),
    )


def _migrate_vendor_product_tag_cleanup(conn) -> None:
    if not _table_exists(conn, "article_tags"):
        return
    conn.execute(
        """
        DELETE FROM article_tags
        WHERE tag LIKE 'vendor:%%' OR tag LIKE 'product:%%'
        """
    )


def _upsert_llm_schema(conn, schema_id: str, name: str, version: str, json_schema: str) -> None:
    if not _table_exists(conn, "llm_schemas"):
        return
    conn.execute(
        """
        INSERT INTO llm_schemas (id, name, version, json_schema, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            version = excluded.version,
            json_schema = excluded.json_schema
        """,
        (schema_id, name, version, json_schema, utc_now_iso()),
    )


def _upsert_llm_prompt(
    conn, prompt_id: str, name: str, version: str, system_template: str, user_template: str, notes: str
) -> None:
    if not _table_exists(conn, "llm_prompts"):
        return
    conn.execute(
        """
        INSERT INTO llm_prompts (id, name, version, system_template, user_template, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            version = excluded.version,
            system_template = excluded.system_template,
            user_template = excluded.user_template,
            notes = excluded.notes
        """,
        (prompt_id, name, version, system_template, user_template, notes, utc_now_iso()),
    )


def _update_stage_profile_prompt_schema(
    conn, stage_name: str, prompt_id: str, schema_id: str | None
) -> None:
    if not _table_exists(conn, "pipeline_stage_config") or not _table_exists(conn, "llm_profiles"):
        return
    row = conn.execute(
        "SELECT profile_id FROM pipeline_stage_config WHERE stage_name = %s",
        (stage_name,),
    ).fetchone()
    if not row:
        return
    profile_id = row[0]
    if schema_id:
        conn.execute(
            """
            UPDATE llm_profiles
            SET prompt_id = %s, schema_id = %s, updated_at = %s
            WHERE id = %s
            """,
            (prompt_id, schema_id, utc_now_iso(), profile_id),
        )
    else:
        conn.execute(
            """
            UPDATE llm_profiles
            SET prompt_id = %s, updated_at = %s
            WHERE id = %s
            """,
            (prompt_id, utc_now_iso(), profile_id),
        )


def _migrate_llm_vendor_product_prompts(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    schema_article_products = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["items"],
      "properties": {
        "items": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["product"],
            "properties": {
              "vendor": { "type": ["string", "null"] },
              "product": { "type": "string" }
            }
          }
        }
      }
    }
    """.strip()
    schema_cve_products = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["items"],
      "properties": {
        "items": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["product", "versions"],
            "properties": {
              "vendor": { "type": ["string", "null"] },
              "product": { "type": "string" },
              "versions": {
                "type": "array",
                "items": { "type": "string" }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_threat_actors = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["items"],
      "properties": {
        "items": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["name", "type", "aliases", "confidence"],
            "properties": {
              "name": { "type": "string" },
              "type": { "type": "string", "enum": ["apt", "crimeware", "nation_state", "unknown"] },
              "country": { "type": ["string", "null"] },
              "aliases": { "type": "array", "items": { "type": "string" } },
              "confidence": { "type": "integer", "minimum": 0, "maximum": 100 }
            }
          }
        }
      }
    }
    """.strip()

    _upsert_llm_schema(conn, "schema_article_enrich_products_v1", "Article Enrich Products", "v1", schema_article_products)
    _upsert_llm_schema(conn, "schema_cve_enrich_products_v1", "CVE Enrich Products", "v1", schema_cve_products)
    _upsert_llm_schema(conn, "schema_threat_actors_v1", "Threat Actors", "v1", schema_threat_actors)

    _upsert_llm_prompt(
        conn,
        "prompt_article_enrich_products_v1",
        "Article Enrich Products",
        "v1",
        "\n".join(
            [
                "Extract affected vendor/product from the article content.",
                "Return JSON only.",
                "",
                "Rules:",
                "- Output must be: {\"items\":[{\"vendor\":\"...\",\"product\":\"...\"}]}",
                "- If vendor or product is not explicit, return an empty items list.",
                "- product MUST be explicit; omit items without a clear product.",
                "- Do not include tags, categories, NIST, or commentary.",
                "- Do not hallucinate.",
            ]
        ),
        "{{input}}",
        "Strict JSON-only vendor/product extraction for articles.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_cve_enrich_products_v1",
        "CVE Enrich Products",
        "v1",
        "\n".join(
            [
                "Extract affected vendor/product/version from CVE text.",
                "Return JSON only.",
                "",
                "Rules:",
                "- Output must be: {\"items\":[{\"vendor\":\"...\",\"product\":\"...\",\"versions\":[\"...\"]}]}",
                "- versions must be [] if none. Do not guess.",
                "- If vendor or product is not explicit, return an empty items list.",
                "- product MUST be explicit; omit items without a clear product.",
                "- Do not include tags, categories, NIST, or commentary.",
                "- Do not hallucinate.",
            ]
        ),
        "{{input}}",
        "Strict JSON-only vendor/product extraction for CVEs.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_threat_actor_extract_v1",
        "Threat Actor Extraction",
        "v1",
        "\n".join(
            [
                "Extract threat actors (APTs, crimeware groups, nation-state actors) from the input.",
                "Return JSON only.",
                "",
                "Output must be:",
                "{\"items\":[{\"name\":\"...\",\"type\":\"apt|crimeware|nation_state|unknown\",\"country\":\"..\",\"aliases\":[\"...\"],\"confidence\":0-100}]}",
                "",
                "Rules:",
                "- Use type 'unknown' if unclear.",
                "- country may be null.",
                "- aliases must be [] if none.",
                "- If no specific actor is named, return an empty items list.",
                "- Do not add commentary.",
            ]
        ),
        "{{input}}",
        "Strict JSON-only threat actor extraction.",
    )

    _update_stage_profile_prompt_schema(
        conn,
        "article_enrich_products",
        "prompt_article_enrich_products_v1",
        "schema_article_enrich_products_v1",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "cve_enrich_products",
        "prompt_cve_enrich_products_v1",
        "schema_cve_enrich_products_v1",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "article_enrich_threat_actors",
        "prompt_threat_actor_extract_v1",
        "schema_threat_actors_v1",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "cve_enrich_threat_actors",
        "prompt_threat_actor_extract_v1",
        "schema_threat_actors_v1",
    )


def _migrate_llm_event_classify_prompts(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    _migrate_llm_vendor_product_prompts(conn)
    schema_event_classify = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["is_event", "event_type", "victim", "headline", "summary", "confidence"],
      "properties": {
        "is_event": { "type": "boolean" },
        "event_type": { "type": "string" },
        "victim": { "type": "string" },
        "headline": { "type": "string" },
        "summary": { "type": "string" },
        "confidence": { "type": "integer", "minimum": 0, "maximum": 100 }
      }
    }
    """.strip()
    _upsert_llm_schema(
        conn,
        "schema_event_classify_v1",
        "Event Classify",
        "v1",
        schema_event_classify,
    )
    _upsert_llm_prompt(
        conn,
        "prompt_event_classify_v1",
        "Event Classify",
        "v1",
        "\n".join(
            [
                "Classify whether the article describes a cybersecurity incident/event.",
                "Return JSON only.",
                "",
                "Output must be:",
                "{\"is_event\":true|false,\"event_type\":\"...\",\"victim\":\"...\",\"headline\":\"...\",\"summary\":\"...\",\"confidence\":0-100}",
                "",
                "Rules:",
                "- If not an event, set is_event=false and leave other fields empty strings.",
                "- Use event_type values like: breach, ransomware, intrusion, ddos, malware_campaign, exploit_in_the_wild, advisory, vuln_disclosure, outage, other.",
                "- victim should be the primary affected organization/entity if explicit.",
                "- Do not invent victims or event types.",
                "- summary should be 1-2 concise sentences.",
            ]
        ),
        "{{input}}",
        "Strict JSON-only event classification.",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "derive_events_from_articles",
        "prompt_event_classify_v1",
        "schema_event_classify_v1",
    )


def _migrate_source_overrides(conn) -> None:
    conn.execute("ALTER TABLE sources ADD COLUMN IF NOT EXISTS overrides JSONB NULL")
