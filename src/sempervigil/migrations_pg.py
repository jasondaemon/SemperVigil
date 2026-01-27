from __future__ import annotations

import logging

from .utils import utc_now_iso


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
    conn.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS reasons JSONB NOT NULL DEFAULT '[]'::jsonb")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_visibility ON events(visibility)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_kind_visibility ON events(kind, visibility)")


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
