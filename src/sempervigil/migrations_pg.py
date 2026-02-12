from __future__ import annotations

import logging
import json

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
        if "pg_articles_002_context_pack" not in applied:
            _migrate_articles_context_pack(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_articles_002_context_pack", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_articles_002_context_pack")
            applied.add("pg_articles_002_context_pack")
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
        if "pg_daily_briefs_014" not in applied:
            _migrate_daily_briefs(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_briefs_014", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_briefs_014")
            applied.add("pg_daily_briefs_014")
        if "pg_daily_brief_prompts_015" not in applied:
            _migrate_daily_brief_prompt_updates(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_015", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_015")
            applied.add("pg_daily_brief_prompts_015")
        if "pg_daily_brief_prompts_016" not in applied:
            _migrate_daily_brief_prompt_updates_v3(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_016", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_016")
            applied.add("pg_daily_brief_prompts_016")
        if "pg_daily_brief_prompts_017" not in applied:
            _migrate_daily_brief_prompt_updates_v4(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_017", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_017")
            applied.add("pg_daily_brief_prompts_017")
        if "pg_daily_brief_prompts_018" not in applied:
            _migrate_daily_brief_prompt_updates_v5(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_018", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_018")
            applied.add("pg_daily_brief_prompts_018")
        if "pg_daily_brief_prompts_019" not in applied:
            _migrate_daily_brief_prompt_updates_v6(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_019", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_019")
            applied.add("pg_daily_brief_prompts_019")
        if "pg_daily_brief_prompts_020" not in applied:
            _migrate_daily_brief_prompt_updates_v7(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_020", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_020")
            applied.add("pg_daily_brief_prompts_020")
        if "pg_daily_brief_cluster_openai_021" not in applied:
            _migrate_daily_brief_cluster_use_openai(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_cluster_openai_021", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_cluster_openai_021")
            applied.add("pg_daily_brief_cluster_openai_021")
        if "pg_daily_brief_nist_openai_022" not in applied:
            _migrate_daily_brief_nist_use_openai(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_nist_openai_022", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_nist_openai_022")
            applied.add("pg_daily_brief_nist_openai_022")
        if "pg_article_context_pack_prompt_023" not in applied:
            _migrate_article_context_pack_prompt_v1(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_article_context_pack_prompt_023", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_article_context_pack_prompt_023")
            applied.add("pg_article_context_pack_prompt_023")
        if "pg_daily_brief_prompts_024" not in applied:
            _migrate_daily_brief_overall_prompt_updates_v8(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_024", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_024")
            applied.add("pg_daily_brief_prompts_024")
        if "pg_daily_brief_prompts_025" not in applied:
            _migrate_daily_brief_overall_prompt_updates_v9(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_025", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_025")
            applied.add("pg_daily_brief_prompts_025")
        if "pg_daily_brief_prompts_026" not in applied:
            _migrate_daily_brief_overall_prompt_updates_v10(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_prompts_026", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_prompts_026")
            applied.add("pg_daily_brief_prompts_026")
        if "pg_daily_brief_overall_input_027" not in applied:
            _migrate_daily_brief_overall_input_limits_v1(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_daily_brief_overall_input_027", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_daily_brief_overall_input_027")
            applied.add("pg_daily_brief_overall_input_027")
        if "pg_jobs_priority_028" not in applied:
            _migrate_jobs_priority(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
                ("pg_jobs_priority_028", utc_now_iso()),
            )
            conn.commit()
            logger.info("migration_applied version=pg_jobs_priority_028")
            applied.add("pg_jobs_priority_028")
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

    _migrate_source_overrides(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_source_overrides_013", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_source_overrides_013")

    _migrate_daily_briefs(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_briefs_014", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_briefs_014")

    conn.execute("BEGIN")
    _migrate_daily_brief_prompt_updates(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_prompts_015", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_prompts_015")

    conn.execute("BEGIN")
    _migrate_daily_brief_prompt_updates_v3(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_prompts_016", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_prompts_016")

    conn.execute("BEGIN")
    _migrate_daily_brief_prompt_updates_v4(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_prompts_017", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_prompts_017")

    conn.execute("BEGIN")
    _migrate_daily_brief_prompt_updates_v5(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_prompts_018", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_prompts_018")

    conn.execute("BEGIN")
    _migrate_daily_brief_prompt_updates_v6(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_prompts_019", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_prompts_019")

    conn.execute("BEGIN")
    _migrate_daily_brief_prompt_updates_v7(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_prompts_020", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_prompts_020")

    conn.execute("BEGIN")
    _migrate_daily_brief_cluster_use_openai(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_cluster_openai_021", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_cluster_openai_021")

    conn.execute("BEGIN")
    _migrate_daily_brief_nist_use_openai(conn)
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (%s, %s)",
        ("pg_daily_brief_nist_openai_022", utc_now_iso()),
    )
    conn.commit()
    logger.info("migration_applied version=pg_daily_brief_nist_openai_022")

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
            UNIQUE(source_id, tactic_type)
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
            priority INTEGER NOT NULL DEFAULT 0,
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
    _migrate_daily_briefs(conn)
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


def _migrate_articles_context_pack(conn) -> None:
    if not _table_exists(conn, "articles"):
        return
    if not _has_column(conn, "articles", "context_llm"):
        conn.execute("ALTER TABLE articles ADD COLUMN context_llm TEXT NULL")
    if not _has_column(conn, "articles", "context_model"):
        conn.execute("ALTER TABLE articles ADD COLUMN context_model TEXT NULL")
    if not _has_column(conn, "articles", "context_generated_at"):
        conn.execute("ALTER TABLE articles ADD COLUMN context_generated_at TEXT NULL")
    if not _has_column(conn, "articles", "context_error"):
        conn.execute("ALTER TABLE articles ADD COLUMN context_error TEXT NULL")


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


def _update_stage_profile_params(conn, stage_name: str, updates: dict[str, object]) -> None:
    if not _table_exists(conn, "pipeline_stage_config") or not _table_exists(conn, "llm_profiles"):
        return
    row = conn.execute(
        "SELECT profile_id FROM pipeline_stage_config WHERE stage_name = %s",
        (stage_name,),
    ).fetchone()
    if not row:
        return
    profile_id = row[0]
    params_row = conn.execute(
        "SELECT params_json FROM llm_profiles WHERE id = %s",
        (profile_id,),
    ).fetchone()
    params: dict[str, object] = {}
    if params_row and params_row[0]:
        try:
            parsed = json.loads(params_row[0])
            if isinstance(parsed, dict):
                params = parsed
        except Exception:
            params = {}
    params.update(updates)
    conn.execute(
        """
        UPDATE llm_profiles
        SET params_json = %s, updated_at = %s
        WHERE id = %s
        """,
        (json.dumps(params), utc_now_iso(), profile_id),
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


def _migrate_daily_briefs(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_briefs (
            brief_day TEXT PRIMARY KEY,
            profile_id TEXT NULL,
            tldr_json TEXT NULL,
            highlights_json TEXT NULL,
            families_json TEXT NULL,
            urls_json TEXT NULL,
            topics_json TEXT NULL,
            meta_json TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    if _table_exists(conn, "llm_schemas"):
        _migrate_daily_brief_schemas(conn)


def _migrate_daily_brief_schemas(conn) -> None:
    schema_cluster = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics", "article_topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "label"],
            "properties": {
              "topic_key": {"type": "string"},
              "label": {"type": "string"},
              "importance": {"type": ["number", "integer"]},
              "confidence": {"type": ["number", "integer"]},
              "why": {"type": "string"}
            }
          }
        },
        "article_topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["id", "topic_key"],
            "properties": {
              "id": {"type": ["integer", "string"]},
              "topic_key": {"type": "string"},
              "confidence": {"type": ["number", "integer"]}
            }
          }
        }
      }
    }
    """.strip()
    schema_summarize = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "topic_tldr", "topic_summary", "recommended_actions"],
            "properties": {
              "topic_key": {"type": "string"},
              "topic_tldr": {"type": "array", "items": {"type": "string"}},
              "topic_summary": {"type": "string"},
              "recommended_actions": {"type": "array", "items": {"type": "string"}},
              "uncertainty": {"type": ["string", "null"]}
            }
          }
        }
      }
    }
    """.strip()
    schema_map = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "families"],
            "properties": {
              "topic_key": {"type": "string"},
              "families": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["family"],
                  "properties": {
                    "family": {"type": "string"},
                    "title": {"type": "string"},
                    "justification": {"type": "string"}
                  }
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_overall = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["tldr_overall", "highlights_reel", "recommended_actions_overall"],
      "properties": {
        "tldr_overall": {"type": "array", "items": {"type": "string"}},
        "highlights_reel": {"type": "string"},
        "recommended_actions_overall": {"type": "array", "items": {"type": "string"}}
      }
    }
    """.strip()
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_cluster_topics_v1",
        "Daily Brief Topic Clustering",
        "v1",
        schema_cluster,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_summarize_topics_v1",
        "Daily Brief Topic Synthesis",
        "v1",
        schema_summarize,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_map_nist_v1",
        "Daily Brief NIST Mapping",
        "v1",
        schema_map,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_overall_v1",
        "Daily Brief Overall Synthesis",
        "v1",
        schema_overall,
    )


def _migrate_daily_brief_prompt_updates(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    schema_cluster = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics", "article_topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "label", "topic_type", "importance", "confidence", "why"],
            "properties": {
              "topic_key": {"type": "string"},
              "label": {"type": "string"},
              "topic_type": {"type": "string"},
              "importance": {"type": "number"},
              "confidence": {"type": "number"},
              "why": {"type": "string"}
            }
          }
        },
        "article_topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["id", "topic_key", "confidence"],
            "properties": {
              "id": {"type": "number"},
              "topic_key": {"type": "string"},
              "confidence": {"type": "number"}
            }
          }
        }
      }
    }
    """.strip()
    schema_summarize = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": [
              "topic_key",
              "topic_tldr",
              "what_happened",
              "why_today",
              "attack_surface",
              "likely_impact",
              "observed_tactics",
              "immediate_checks",
              "mitigations",
              "caveats"
            ],
            "properties": {
              "topic_key": {"type": "string"},
              "topic_tldr": {"type": "array", "items": {"type": "string"}},
              "what_happened": {"type": "string"},
              "why_today": {"type": "string"},
              "attack_surface": {"type": "string"},
              "likely_impact": {"type": "string"},
              "observed_tactics": {"type": "array", "items": {"type": "string"}},
              "immediate_checks": {"type": "array", "items": {"type": "string"}},
              "mitigations": {"type": "array", "items": {"type": "string"}},
              "caveats": {"type": "array", "items": {"type": "string"}},
              "detection_iocs": {"type": "array", "items": {"type": "string"}}
            }
          }
        }
      }
    }
    """.strip()
    schema_map = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "families"],
            "properties": {
              "topic_key": {"type": "string"},
              "families": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["family", "title", "justification"],
                  "properties": {
                    "family": {"type": "string"},
                    "title": {"type": "string"},
                    "justification": {"type": "string"}
                  }
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_overall = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["tldr_narrative", "technical_synthesis", "key_items", "recommended_actions_overall"],
      "properties": {
        "tldr_narrative": {"type": "string"},
        "technical_synthesis": {"type": "string"},
        "key_items": {"type": "array", "items": {"type": "string"}},
        "recommended_actions_overall": {"type": "array", "items": {"type": "string"}},
        "tldr_overall": {"type": "string"},
        "highlights_reel": {"type": "string"}
      }
    }
    """.strip()
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_cluster_topics_v2",
        "Daily Brief Topic Clustering",
        "v2",
        schema_cluster,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_summarize_topics_v2",
        "Daily Brief Topic Synthesis",
        "v2",
        schema_summarize,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_map_nist_v2",
        "Daily Brief NIST Mapping",
        "v2",
        schema_map,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_overall_v2",
        "Daily Brief Overall Synthesis",
        "v2",
        schema_overall,
    )

    system_common = (
        "You are a senior security analyst. Output strict JSON only. "
        "No markdown, no code fences, no extra keys."
    )
    cluster_user = """
You will receive JSON input in {{input}} with articles.
Cluster articles into operationally meaningful topics.
Rules:
- Merge near-duplicates aggressively.
- Do not create topics from navigation, categories, or URLs.
- topic_key must be stable, lowercase, and machine-safe.
- topic_type must be one of: operational, campaign, vulnerability, misconfiguration, research, policy, contextual.
Return JSON only with this shape:
{
  "topics": [
    {
      "topic_key": "stable_machine_key",
      "label": "Human readable topic name",
      "topic_type": "operational",
      "importance": 0.9,
      "confidence": 1.0,
      "why": "Why this topic matters today"
    }
  ],
  "article_topics": [
    { "id": 123, "topic_key": "stable_machine_key", "confidence": 1.0 }
  ]
}
""".strip()
    summarize_user = """
You will receive JSON input in {{input}} with topics and contributing articles.
Write analyst-grade, technical summaries. No hype, no marketing, no generic advice.
Each topic must include: what_happened, why_today, attack_surface, likely_impact,
observed_tactics, immediate_checks, mitigations, caveats, and topic_tldr.
Return JSON only with this shape:
{
  "topics": [
    {
      "topic_key": "stable_machine_key",
      "topic_tldr": ["One-sentence technical takeaway"],
      "what_happened": "...",
      "why_today": "...",
      "attack_surface": "...",
      "likely_impact": "...",
      "observed_tactics": ["..."],
      "immediate_checks": ["..."],
      "mitigations": ["..."],
      "caveats": ["..."]
    }
  ]
}
""".strip()
    map_user = """
You will receive JSON input in {{input}} with topics and summaries.
Map each topic to NIST 800-53 families that meaningfully apply.
Include justification tied to the topic mechanics. Do not over-assign families.
Return JSON only with this shape:
{
  "topics": [
    {
      "topic_key": "stable_machine_key",
      "families": [
        {
          "family": "CM",
          "title": "Configuration Management",
          "justification": "Why this control family applies"
        }
      ]
    }
  ]
}
""".strip()
    overall_user = """
You will receive JSON input in {{input}} with synthesized topics.
Write a narrative daily brief for senior practitioners.
Output JSON only with:
- tldr_narrative: 4–7 sentence paragraph covering the whole day
- technical_synthesis: 1–2 short paragraphs, operational focus
- key_items: 4–8 concise bullets
- recommended_actions_overall: concrete, prioritized actions
Also include tldr_overall and highlights_reel for backward compatibility:
  tldr_overall = tldr_narrative
  highlights_reel = technical_synthesis
Shape:
{
  "tldr_narrative": "...",
  "technical_synthesis": "...",
  "key_items": ["..."],
  "recommended_actions_overall": ["..."],
  "tldr_overall": "...",
  "highlights_reel": "..."
}
""".strip()

    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_cluster_topics_v2",
        "Daily Brief Topic Clustering",
        "v2",
        system_common,
        cluster_user,
        "Cluster articles into operational topics; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_summarize_topics_v2",
        "Daily Brief Topic Synthesis",
        "v2",
        system_common,
        summarize_user,
        "Technical topic summaries; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_map_nist_v2",
        "Daily Brief NIST Mapping",
        "v2",
        system_common,
        map_user,
        "Map topics to NIST families with justification; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v2",
        "Daily Brief Overall Synthesis",
        "v2",
        system_common,
        overall_user,
        "Narrative daily synthesis; JSON only.",
    )

    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_cluster_topics",
        "prompt_daily_brief_cluster_topics_v2",
        "schema_daily_brief_cluster_topics_v2",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_summarize_topics",
        "prompt_daily_brief_summarize_topics_v2",
        "schema_daily_brief_summarize_topics_v2",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_map_nist_families",
        "prompt_daily_brief_map_nist_v2",
        "schema_daily_brief_map_nist_v2",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v2",
        "schema_daily_brief_overall_v2",
    )


def _migrate_daily_brief_prompt_updates_v3(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    schema_cluster = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics", "article_topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "label", "topic_type", "importance", "confidence", "why"],
            "properties": {
              "topic_key": {"type": "string"},
              "label": {"type": "string"},
              "topic_type": {"type": "string"},
              "importance": {"type": "number"},
              "confidence": {"type": "number"},
              "why": {"type": "string"},
              "anchors": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                  "cves": {"type": "array", "items": {"type": "string"}},
                  "actors": {"type": "array", "items": {"type": "string"}},
                  "products": {"type": "array", "items": {"type": "string"}},
                  "orgs": {"type": "array", "items": {"type": "string"}},
                  "keywords": {"type": "array", "items": {"type": "string"}}
                }
              }
            }
          }
        },
        "article_topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["id", "topic_key", "confidence"],
            "properties": {
              "id": {"type": "number"},
              "topic_key": {"type": "string"},
              "confidence": {"type": "number"}
            }
          }
        }
      }
    }
    """.strip()
    schema_summarize = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": [
              "topic_key",
              "topic_tldr",
              "what_happened",
              "why_today",
              "attack_surface",
              "likely_impact",
              "observed_tactics",
              "immediate_checks",
              "mitigations",
              "caveats",
              "evidence"
            ],
            "properties": {
              "topic_key": {"type": "string"},
              "topic_tldr": {"type": "array", "items": {"type": "string"}},
              "what_happened": {"type": "string"},
              "why_today": {"type": "string"},
              "attack_surface": {"type": "string"},
              "likely_impact": {"type": "string"},
              "observed_tactics": {"type": "array", "items": {"type": "string"}},
              "immediate_checks": {"type": "array", "items": {"type": "string"}},
              "mitigations": {"type": "array", "items": {"type": "string"}},
              "caveats": {"type": "array", "items": {"type": "string"}},
              "iocs_or_detection": {"type": "array", "items": {"type": "string"}},
              "evidence": {
                "type": "object",
                "additionalProperties": false,
                "required": ["article_ids", "concrete_facts"],
                "properties": {
                  "article_ids": {"type": "array", "items": {"type": "integer"}},
                  "concrete_facts": {"type": "array", "items": {"type": "string"}}
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_map = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "families"],
            "properties": {
              "topic_key": {"type": "string"},
              "families": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["family", "title", "justification"],
                  "properties": {
                    "family": {"type": "string"},
                    "title": {"type": "string"},
                    "justification": {"type": "string"}
                  }
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_overall = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["tldr_narrative", "technical_synthesis", "key_items", "recommended_actions_overall", "links"],
      "properties": {
        "tldr_narrative": {"type": "string"},
        "technical_synthesis": {"type": "string"},
        "key_items": {"type": "array", "items": {"type": "string"}},
        "recommended_actions_overall": {"type": "array", "items": {"type": "string"}},
        "tldr_overall": {"type": "string"},
        "highlights_reel": {"type": "string"},
        "links": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "label", "articles"],
            "properties": {
              "topic_key": {"type": "string"},
              "label": {"type": "string"},
              "articles": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["title", "url", "source", "article_id"],
                  "properties": {
                    "title": {"type": "string"},
                    "url": {"type": "string"},
                    "source": {"type": "string"},
                    "article_id": {"type": "integer"}
                  }
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_cluster_topics_v3",
        "Daily Brief Topic Clustering",
        "v3",
        schema_cluster,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_summarize_topics_v3",
        "Daily Brief Topic Synthesis",
        "v3",
        schema_summarize,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_map_nist_v3",
        "Daily Brief NIST Mapping",
        "v3",
        schema_map,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_overall_v3",
        "Daily Brief Overall Synthesis",
        "v3",
        schema_overall,
    )

    system_common = (
        "You are a senior security analyst. Output strict JSON only. "
        "No markdown, no code fences, no extra keys."
    )
    cluster_user = """
You will receive JSON input in {{input}} with articles.
Cluster articles into operationally meaningful topics.
Rules:
- Merge near-duplicates aggressively. Target 8–15 topics (hard cap 20).
- Each topic must be grounded in at least one article. Do NOT invent facts.
- Do not create topics from navigation, categories, URLs, or marketing content.
- Exclude webinars, sponsored/promotional content, or opinion pieces. If uncertain, mark topic_type="contextual" with importance <=0.2.
- topic_key must be stable, lowercase, and machine-safe. Prefer: cve:CVE-YYYY-NNNN, campaign:slug, incident:slug, misconfig:slug, supplychain:slug.
- article_topics must be many-to-one; do NOT create one-off topics unless absolutely necessary.
- Each topic's "why" must cite concrete anchors (CVE/actor/product/vector/impact) present in the input.
Return JSON only with this shape:
{
  "topics": [
    {
      "topic_key": "stable_machine_key",
      "label": "Human readable topic name",
      "topic_type": "operational",
      "importance": 0.9,
      "confidence": 1.0,
      "why": "Why this topic matters today (cite concrete anchors like CVE, actor, product, vector, impact)",
      "anchors": {
        "cves": ["CVE-2025-1234"],
        "actors": ["Example actor"],
        "products": ["Product"],
        "orgs": ["Org"],
        "keywords": ["vector", "impact"]
      }
    }
  ],
  "article_topics": [
    { "id": 123, "topic_key": "stable_machine_key", "confidence": 1.0 }
  ]
}
""".strip()
    summarize_user = """
You will receive JSON input in {{input}} with topics and contributing articles.
Write analyst-grade, technical summaries. No hype, no marketing, no generic advice.
ONLY use facts present in the input; if not stated, say "unknown" and add to caveats.
Each topic must include: what_happened, why_today, attack_surface, likely_impact,
observed_tactics, iocs_or_detection, immediate_checks, mitigations, caveats, topic_tldr.
Add evidence with article_ids and concrete_facts that are explicitly present in the input.
Concrete facts must be short anchors (CVE, actor, product, vector, impact, timeframe).
Return JSON only with this shape:
{
  "topics": [
    {
      "topic_key": "stable_machine_key",
      "topic_tldr": ["One-sentence technical takeaway"],
      "what_happened": "...",
      "why_today": "...",
      "attack_surface": "...",
      "likely_impact": "...",
      "observed_tactics": ["..."],
      "iocs_or_detection": ["..."],
      "immediate_checks": ["..."],
      "mitigations": ["..."],
      "caveats": ["..."],
      "evidence": {
        "article_ids": [123, 456],
        "concrete_facts": ["CVE-2025-1234", "Actor X", "Affected Product Y"]
      }
    }
  ]
}
""".strip()
    map_user = """
You will receive JSON input in {{input}} with topics and summaries.
Map each topic to NIST 800-53 families that meaningfully apply.
Include justification tied to topic anchors. Do not over-assign families.
Return JSON only with this shape:
{
  "topics": [
    {
      "topic_key": "stable_machine_key",
      "families": [
        {
          "family": "CM",
          "title": "Configuration Management",
          "justification": "Why this control family applies"
        }
      ]
    }
  ]
}
""".strip()
    overall_user = """
You will receive JSON input in {{input}} with synthesized topics and mappings.
Write a grounded, technical daily brief. DO NOT BE GENERIC.
Only use facts present in the input; if not stated, say "unknown".
Output JSON only with:
- tldr_narrative: 8–12 sentences, mention top 3–6 topics with concrete anchors
- technical_synthesis: 2–3 short paragraphs, cite repeated mechanisms/patterns and 4–8 anchors
- key_items: 5–10 bullets, each with at least one concrete anchor
- recommended_actions_overall: 6–10 prioritized actions, each with "(topics: ...)" listing related topic_keys
- links: grouped articles by topic with title/url/source/article_id
Also include tldr_overall and highlights_reel for backward compatibility:
  tldr_overall = tldr_narrative
  highlights_reel = technical_synthesis
Shape:
{
  "tldr_narrative": "...",
  "technical_synthesis": "...",
  "key_items": ["..."],
  "recommended_actions_overall": ["..."],
  "tldr_overall": "...",
  "highlights_reel": "...",
  "links": [
    {
      "topic_key": "...",
      "label": "...",
      "articles": [
        {"title":"...","url":"...","source":"...","article_id":123}
      ]
    }
  ]
}
""".strip()

    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_cluster_topics_v3",
        "Daily Brief Topic Clustering",
        "v3",
        system_common,
        cluster_user,
        "Cluster articles into operational topics; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_summarize_topics_v3",
        "Daily Brief Topic Synthesis",
        "v3",
        system_common,
        summarize_user,
        "Technical topic summaries; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_map_nist_v3",
        "Daily Brief NIST Mapping",
        "v3",
        system_common,
        map_user,
        "Map topics to NIST families with justification; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v3",
        "Daily Brief Overall Synthesis",
        "v3",
        system_common,
        overall_user,
        "Narrative daily synthesis; JSON only.",
    )

    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_cluster_topics",
        "prompt_daily_brief_cluster_topics_v3",
        "schema_daily_brief_cluster_topics_v3",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_summarize_topics",
        "prompt_daily_brief_summarize_topics_v3",
        "schema_daily_brief_summarize_topics_v3",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_map_nist_families",
        "prompt_daily_brief_map_nist_v3",
        "schema_daily_brief_map_nist_v3",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v3",
        "schema_daily_brief_overall_v3",
    )


def _migrate_daily_brief_prompt_updates_v4(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    schema_cluster = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics", "article_topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "label", "topic_type", "importance", "confidence", "why", "anchors"],
            "properties": {
              "topic_key": {"type": "string"},
              "label": {"type": "string"},
              "topic_type": {"type": "string"},
              "importance": {"type": "number"},
              "confidence": {"type": "number"},
              "why": {"type": "string"},
              "anchors": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                  "cves": {"type": "array", "items": {"type": "string"}},
                  "actors": {"type": "array", "items": {"type": "string"}},
                  "products": {"type": "array", "items": {"type": "string"}},
                  "orgs": {"type": "array", "items": {"type": "string"}},
                  "keywords": {"type": "array", "items": {"type": "string"}}
                }
              }
            }
          }
        },
        "article_topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["id", "topic_key", "confidence"],
            "properties": {
              "id": {"type": "number"},
              "topic_key": {"type": "string"},
              "confidence": {"type": "number"}
            }
          }
        }
      }
    }
    """.strip()
    schema_summarize = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": [
              "topic_key",
              "topic_tldr",
              "what_happened",
              "why_today",
              "attack_surface",
              "likely_impact",
              "observed_tactics",
              "iocs_or_detection",
              "immediate_checks",
              "mitigations",
              "caveats"
            ],
            "properties": {
              "topic_key": {"type": "string"},
              "topic_tldr": {"type": "array", "items": {"type": "string"}},
              "what_happened": {"type": "string"},
              "why_today": {"type": "string"},
              "attack_surface": {"type": "string"},
              "likely_impact": {"type": "string"},
              "observed_tactics": {"type": "array", "items": {"type": "string"}},
              "iocs_or_detection": {"type": "array", "items": {"type": "string"}},
              "immediate_checks": {"type": "array", "items": {"type": "string"}},
              "mitigations": {"type": "array", "items": {"type": "string"}},
              "caveats": {"type": "array", "items": {"type": "string"}}
            }
          }
        }
      }
    }
    """.strip()
    schema_map = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "families"],
            "properties": {
              "topic_key": {"type": "string"},
              "families": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["family", "title", "justification"],
                  "properties": {
                    "family": {"type": "string"},
                    "title": {"type": "string"},
                    "justification": {"type": "string"}
                  }
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_overall = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["tldr_narrative", "technical_synthesis", "key_items", "recommended_actions_overall", "podcast_script"],
      "properties": {
        "tldr_narrative": {"type": "string"},
        "technical_synthesis": {"type": "string"},
        "key_items": {"type": "array", "items": {"type": "string"}},
        "recommended_actions_overall": {"type": "array", "items": {"type": "string"}},
        "podcast_script": {"type": "string"}
      }
    }
    """.strip()
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_cluster_topics_v4",
        "Daily Brief Topic Clustering",
        "v4",
        schema_cluster,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_summarize_topics_v4",
        "Daily Brief Topic Synthesis",
        "v4",
        schema_summarize,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_map_nist_v4",
        "Daily Brief NIST Mapping",
        "v4",
        schema_map,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_overall_v4",
        "Daily Brief Overall Synthesis",
        "v4",
        schema_overall,
    )

    system_common = (
        "You are a senior security analyst. Output strict JSON only. "
        "No markdown, no code fences, no extra keys."
    )
    cluster_user = """
You cluster cybersecurity news articles into a compact set of topics that deduplicate the day’s coverage. You must be specific, grounded in the provided articles, and output strict JSON only.

Input JSON: {{input}}

Input includes a JSON array of articles. Each article has:
- id (int), title (string), source_name (string), url (string), published_at (string)
- tags (array of strings), cves (array of strings), summary_text (string)

TASK
1) Create 8–20 topic clusters that best represent the day’s cyber news.
2) Deduplicate near-duplicates: multiple articles about the same CVE/campaign/vendor incident must map to one topic_key.
3) Prefer these topic_key styles (choose the best fit):
- "cve:CVE-YYYY-NNNNN" (if CVE present)
- "campaign:<slug>" (APT/campaign)
- "incident:<slug>" (breach/outage/arrest)
- "vuln:<product_slug>" (no CVE but clear product vuln)
- "research:<slug>" (analysis/guidance)
- "trend:<slug>" (broad trend, but keep rare)

RULES
- Use article tags and CVEs as strong signals.
- If a title is clearly non-security / off-topic, set a topic_key of "noise:<slug>" AND assign importance <= 0.2 so it can be dropped later.
- Importance: 1.0 = active exploitation / KEV / mass exploitation / major breach; 0.7 = serious vuln or high-impact incident; 0.4 = guidance/research; 0.2 = background/noise.
- Confidence is 0.5–1.0 based on how clearly the articles support it.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "label": "string",
      "topic_type": "operational|contextual|noise",
      "importance": 0.0-1.0,
      "confidence": 0.0-1.0,
      "why": "1-2 sentences grounded in the articles"
    }
  ],
  "article_topics": [
    { "id": 123, "topic_key": "string", "confidence": 0.0-1.0 }
  ]
}

Return JSON only.
""".strip()
    summarize_user = """
You are writing a technical daily cyber brief for practitioners. Be concrete and actionable. You must not invent facts. Use only what is supported by the provided articles. Output strict JSON only.

Input JSON: {{input}}

Input format:
{
  "topics": [...],
  "topic_articles": {
     "<topic_key>": [
        {
          "id": int,
          "title": string,
          "source_name": string,
          "url": string,
          "published_at": string,
          "tags": [string],
          "cves": [string],
          "summary_text": string
        }, ...
     ]
  }
}

TASK
For each topic:
- Produce a practitioner-grade brief that explains what happened, why it matters today, and what defenders should do next.
- If CVE is involved: include affected product/version if stated; exploit status if stated (active exploited, KEV, PoC, etc); and a detection hint.
- Avoid generic advice. Each action must be tied to THIS topic and include a rationale ("why now").
- Keep it concise but information-dense.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "topic_tldr": ["1 short bullet", "optional second bullet"],
      "what_happened": "3-6 sentences, specific",
      "why_today": "1-2 sentences",
      "attack_surface": "1-2 sentences",
      "likely_impact": "1-2 sentences",
      "observed_tactics": ["bullet", "..."],
      "iocs_or_detection": ["bullet", "..."],
      "immediate_checks": ["action - why now: ...", "..."],
      "mitigations": ["action - why now: ...", "..."],
      "caveats": ["uncertainty / missing info", "..."]
    }
  ]
}

QUALITY BAR
- If you cannot substantiate something from the summaries, say so in caveats.
- Prefer precise nouns: product names, components, auth flows, protocol names, misconfig types.
- Include at least one detection-oriented item per operational topic when possible.

Return JSON only.
""".strip()
    map_user = """
Map each topic to relevant NIST SP 800-53 Rev.5 control families. Be conservative and justify mappings. Output strict JSON only.

Input JSON: {{input}}
{
  "topics": [
    { "topic_key": "string", "label": "string", "topic_type": "string", "importance": number }
  ],
  "topic_summaries": {
    "<topic_key>": {
      "what_happened": "...",
      "attack_surface": "...",
      "immediate_checks": [...],
      "mitigations": [...]
    }
  }
}

TASK
For each topic_key, choose 1–3 NIST 800-53 families that best fit the defensive work implied by the topic.
Use only these families:
AC, AU, AT, CA, CM, CP, IA, IR, MA, MP, PE, PL, PM, PS, RA, SA, SC, SI, SR

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "families": [
        { "family": "CM", "title": "Configuration Management", "justification": "1 sentence" }
      ]
    }
  ]
}

Return JSON only.
""".strip()
    overall_user = """
You synthesize a full-day technical cyber brief from per-topic summaries. It must read well as a narrative and include non-generic, justified actions. Output strict JSON only.

Input JSON: {{input}}
{
  "topics_ranked": [
    {
      "topic_key": "string",
      "label": "string",
      "importance": number,
      "topic_type": "string",
      "summary": {
        "topic_tldr": [...],
        "what_happened": "...",
        "why_today": "...",
        "immediate_checks": [...],
        "mitigations": [...]
      },
      "families": ["AC","IR"]
    }
  ],
  "links": [
    { "title":"...", "url":"...", "source_name":"...", "summary_text":"..." }
  ]
}

TASK
Write:
1) TLDR (Narrative): 1–2 paragraphs that summarize the DAY (not just 4 bullets). Must connect topics, note clusters (e.g., “supply chain + identity + KEV”), and name the top 2–4 concrete items (CVE, product, actor, incident).
2) Technical Synthesis: 1–2 paragraphs with technical emphasis (attack surfaces, common failure modes, what defenders should instrument/validate).
3) Key Items: 5–8 bullets, each concrete.
4) Recommended Actions: 6–10 bullets that are NOT generic. Each must include “why now:” and reference which topic_key(s) it applies to.
5) Podcast script: 150–400 words, spoken-friendly, technical but clear. Intro + 3–5 segments + close. Mention at least 2 CVEs/products/actors and at least 2 defensive actions with rationale.

OUTPUT JSON SCHEMA (strict)
{
  "tldr_narrative": "string",
  "technical_synthesis": "string",
  "key_items": ["string", "..."],
  "recommended_actions_overall": ["action (topics: <topic_key,...>) - why now: ...", "..."],
  "podcast_script": "string"
}

Return JSON only.
""".strip()

    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_cluster_topics_v4",
        "Daily Brief Topic Clustering",
        "v4",
        system_common,
        cluster_user,
        "Cluster articles into operational topics; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_summarize_topics_v4",
        "Daily Brief Topic Synthesis",
        "v4",
        system_common,
        summarize_user,
        "Technical topic summaries; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_map_nist_v4",
        "Daily Brief NIST Mapping",
        "v4",
        system_common,
        map_user,
        "Map topics to NIST families with justification; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v4",
        "Daily Brief Overall Synthesis",
        "v4",
        system_common,
        overall_user,
        "Narrative daily synthesis; JSON only.",
    )

    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_cluster_topics",
        "prompt_daily_brief_cluster_topics_v4",
        "schema_daily_brief_cluster_topics_v4",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_summarize_topics",
        "prompt_daily_brief_summarize_topics_v4",
        "schema_daily_brief_summarize_topics_v4",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_map_nist_families",
        "prompt_daily_brief_map_nist_v4",
        "schema_daily_brief_map_nist_v4",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v4",
        "schema_daily_brief_overall_v4",
    )
    for stage in (
        "daily_brief_cluster_topics",
        "daily_brief_summarize_topics",
        "daily_brief_map_nist_families",
        "daily_brief_overall_synthesis",
    ):
        _update_stage_profile_params(conn, stage, {"max_input_chars": 120000})


def _migrate_daily_brief_prompt_updates_v5(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    schema_cluster = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics", "article_topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "label", "topic_type", "importance", "confidence", "why"],
            "properties": {
              "topic_key": {"type": "string"},
              "label": {"type": "string"},
              "topic_type": {"type": "string"},
              "importance": {"type": "number"},
              "confidence": {"type": "number"},
              "why": {"type": "string"}
            }
          }
        },
        "article_topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["id", "topic_key", "confidence"],
            "properties": {
              "id": {"type": "number"},
              "topic_key": {"type": "string"},
              "confidence": {"type": "number"}
            }
          }
        }
      }
    }
    """.strip()
    schema_summarize = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "headline", "narrative", "key_facts", "evidence", "caveats"],
            "properties": {
              "topic_key": {"type": "string"},
              "headline": {"type": "string"},
              "narrative": {"type": "string"},
              "key_facts": {"type": "array", "items": {"type": "string"}},
              "evidence": {
                "type": "object",
                "additionalProperties": false,
                "required": ["article_ids", "concrete_facts"],
                "properties": {
                  "article_ids": {"type": "array", "items": {"type": "number"}},
                  "concrete_facts": {"type": "array", "items": {"type": "string"}}
                }
              },
              "caveats": {"type": "array", "items": {"type": "string"}}
            }
          }
        }
      }
    }
    """.strip()
    schema_map = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["families"],
      "properties": {
        "families": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["family_id", "family_title", "summary", "subtopics"],
            "properties": {
              "family_id": {"type": "string"},
              "family_title": {"type": "string"},
              "summary": {"type": "string"},
              "subtopics": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["subtopic_id", "title", "severity", "narrative", "citations"],
                  "properties": {
                    "subtopic_id": {"type": "string"},
                    "title": {"type": "string"},
                    "severity": {"type": "string"},
                    "narrative": {"type": "string"},
                    "citations": {"type": "array", "items": {"type": "number"}}
                  }
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_overall = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["tldr", "technical_synthesis", "actions"],
      "properties": {
        "tldr": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["text", "citations"],
            "properties": {
              "text": {"type": "string"},
              "citations": {"type": "array", "items": {"type": "number"}}
            }
          }
        },
        "technical_synthesis": {
          "type": "object",
          "additionalProperties": false,
          "required": ["text", "citations"],
          "properties": {
            "text": {"type": "string"},
            "citations": {"type": "array", "items": {"type": "number"}}
          }
        },
        "actions": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["action", "why", "priority", "time_horizon", "citations"],
            "properties": {
              "action": {"type": "string"},
              "why": {"type": "string"},
              "priority": {"type": "string"},
              "time_horizon": {"type": "string"},
              "citations": {"type": "array", "items": {"type": "number"}}
            }
          }
        },
        "low_value": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["citation_id", "reason"],
            "properties": {
              "citation_id": {"type": "number"},
              "reason": {"type": "string"}
            }
          }
        }
      }
    }
    """.strip()

    _upsert_llm_schema(
        conn,
        "schema_daily_brief_cluster_topics_v5",
        "Daily Brief Topic Clustering",
        "v5",
        schema_cluster,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_summarize_topics_v5",
        "Daily Brief Topic Summaries",
        "v5",
        schema_summarize,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_map_nist_v5",
        "Daily Brief NIST Mapping",
        "v5",
        schema_map,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_overall_v5",
        "Daily Brief Overall Synthesis",
        "v5",
        schema_overall,
    )

    system_common = (
        "You are a senior cyber threat intelligence analyst. "
        "Output strict JSON only. No markdown, no code fences, no extra keys."
    )
    cluster_user = """
You cluster cybersecurity news articles into a compact set of topics that deduplicate the day’s coverage. You must be specific, grounded in the provided articles, and output strict JSON only.

Input JSON: {{input}}

Input includes a JSON array of articles. Each article has:
- id (int), citation_id (int), title (string), source_name (string), url (string), published_at (string)
- tags (array of strings), cves (array of strings), summary_text (string)

TASK
1) Create 8–20 topic clusters that best represent the day’s cyber news.
2) Deduplicate near-duplicates: multiple articles about the same CVE/campaign/vendor incident must map to one topic_key.
3) Prefer these topic_key styles (choose the best fit):
- "cve:CVE-YYYY-NNNNN" (if CVE present)
- "campaign:<slug>" (APT/campaign)
- "incident:<slug>" (breach/outage/arrest)
- "vuln:<product_slug>" (no CVE but clear product vuln)
- "research:<slug>" (analysis/guidance)
- "trend:<slug>" (broad trend, but keep rare)

RULES
- Use article tags and CVEs as strong signals.
- If a title is clearly non-security / off-topic, set a topic_key of "noise:<slug>" AND assign importance <= 0.2 so it can be dropped later.
- Importance: 1.0 = active exploitation / KEV / mass exploitation / major breach; 0.7 = serious vuln or high-impact incident; 0.4 = guidance/research; 0.2 = background/noise.
- Confidence is 0.5–1.0 based on how clearly the articles support it.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "label": "string",
      "topic_type": "operational|contextual|noise",
      "importance": 0.0-1.0,
      "confidence": 0.0-1.0,
      "why": "1-2 sentences grounded in the articles"
    }
  ],
  "article_topics": [
    { "id": 123, "topic_key": "string", "confidence": 0.0-1.0 }
  ]
}

Return JSON only.
""".strip()
    summarize_user = """
You are writing a technical daily cyber brief for practitioners. Be concrete and factual. You must not invent facts. Use only what is supported by the provided articles. Output strict JSON only.

Input JSON: {{input}}

Input format:
{
  "topics": [...],
  "topic_articles": {
     "<topic_key>": [
        {
          "id": int,
          "citation_id": int,
          "title": string,
          "source_name": string,
          "url": string,
          "published_at": string,
          "tags": [string],
          "cves": [string],
          "summary_text": string
        }, ...
     ]
  }
}

TASK
For each topic, write a concise analyst narrative that can be read aloud.
- Do NOT use checklist sections like “What happened / Attack surface / Mitigations”.
- Anchor the narrative to concrete details in the summaries and citation IDs.
- If key details are missing, say so explicitly in caveats. Do NOT invent.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "headline": "short analyst headline",
      "narrative": "2–5 sentences, grounded in evidence",
      "key_facts": ["short fact", "..."],
      "evidence": {
        "article_ids": [123, 456],
        "concrete_facts": ["fact with anchor", "..."]
      },
      "caveats": ["missing detail", "..."]
    }
  ]
}

QUALITY BAR
- Prefer precise nouns: product names, components, auth flows, protocol names, misconfig types.
- Avoid generic advice. This stage is about facts and narrative only.

Return JSON only.
""".strip()
    map_user = """
You map the day’s topics into NIST 800-53 families and write the family-level narrative and subtopics. Output strict JSON only.

Input JSON: {{input}}
{
  "topics": [...],
  "topic_summaries": {...},
  "topic_articles": {...},
  "citations": [
    { "id": 1, "title": "...", "source_name": "...", "url": "...", "summary": "..." }
  ]
}

TASK
- Families are the primary organization. Only include families that appear today.
- For each family:
  - family_id, family_title
  - summary paragraph with inline citations like (1)
  - subtopics: each with subtopic_id, title, severity (High|Medium|Low), narrative with inline citations, and citations array.
- Nearly all citations should be included in at least one family subtopic unless they are clearly low-value.

Use only these families:
AC, AU, AT, CA, CM, CP, IA, IR, MA, MP, PE, PL, PM, PS, PT, RA, SA, SC, SI, SR

OUTPUT JSON SCHEMA (strict)
{
  "families": [
    {
      "family_id": "CM",
      "family_title": "Configuration Management",
      "summary": "string",
      "subtopics": [
        {
          "subtopic_id": "stable_slug",
          "title": "string",
          "severity": "High|Medium|Low",
          "narrative": "string",
          "citations": [1, 2, 3]
        }
      ]
    }
  ]
}

Return JSON only.
""".strip()
    overall_user = """
You are writing a daily cyber threat intelligence brief for senior practitioners. The brief must read like a human analyst report and be suitable for spoken-word delivery. Output strict JSON only.

Input JSON: {{input}}
{
  "day": "YYYY-MM-DD",
  "topics": [...],
  "citations": [...],
  "families": [...]
}

OUTPUT STRUCTURE (strict)
1) TLDR: 5–7 bullets. Each bullet is “Read this because…”. Must include concrete identifiers and inline citations like (1)(2).
2) Technical Synthesis: 2–3 paragraphs. Succinct, anchored to concrete facts. Use inline citations.
3) Actions: 3–7 items. Each action must include WHAT, WHY, and WHEN/priority (P0/P1/P2 with time horizon). Each action must include citations.

RULES
- Do NOT produce outlines or checklists.
- Do NOT use generic advice. If you recommend an action, explain why it is urgent now and cite sources.
- Do NOT include raw URLs inline; links appear only in the citations list.
- Do NOT invent facts. If details are missing, acknowledge uncertainty.
- Inline citations must match the citations list IDs.

OUTPUT JSON SCHEMA (strict)
{
  "tldr": [
    { "text": "bullet text", "citations": [1,2] }
  ],
  "technical_synthesis": { "text": "string", "citations": [1,2,3] },
  "actions": [
    {
      "action": "imperative sentence",
      "why": "tight rationale tied to today’s items",
      "priority": "P0|P1|P2",
      "time_horizon": "0-24h|72h|7d|30d",
      "citations": [1,2]
    }
  ]
}

Return JSON only.
""".strip()

    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_cluster_topics_v5",
        "Daily Brief Topic Clustering",
        "v5",
        system_common,
        cluster_user,
        "Cluster articles into operational topics; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_summarize_topics_v5",
        "Daily Brief Topic Summaries",
        "v5",
        system_common,
        summarize_user,
        "Analyst narratives per topic; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_map_nist_v5",
        "Daily Brief NIST Mapping",
        "v5",
        system_common,
        map_user,
        "Map topics to NIST families with justification; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v5",
        "Daily Brief Overall Synthesis",
        "v5",
        system_common,
        overall_user,
        "Narrative daily synthesis; JSON only.",
    )

    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_cluster_topics",
        "prompt_daily_brief_cluster_topics_v5",
        "schema_daily_brief_cluster_topics_v5",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_summarize_topics",
        "prompt_daily_brief_summarize_topics_v5",
        "schema_daily_brief_summarize_topics_v5",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_map_nist_families",
        "prompt_daily_brief_map_nist_v5",
        "schema_daily_brief_map_nist_v5",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v5",
        "schema_daily_brief_overall_v5",
    )
    for stage in (
        "daily_brief_cluster_topics",
        "daily_brief_summarize_topics",
        "daily_brief_map_nist_families",
        "daily_brief_overall_synthesis",
    ):
        _update_stage_profile_params(conn, stage, {"max_input_chars": 120000})


def _migrate_daily_brief_prompt_updates_v6(conn) -> None:
    schema_cluster = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics", "article_topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "label", "topic_type", "importance", "confidence", "why"],
            "properties": {
              "topic_key": {"type": "string"},
              "label": {"type": "string"},
              "topic_type": {"type": "string"},
              "importance": {"type": "number"},
              "confidence": {"type": "number"},
              "why": {"type": "string"}
            }
          }
        },
        "article_topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["id", "topic_key", "confidence"],
            "properties": {
              "id": {"type": "number"},
              "topic_key": {"type": "string"},
              "confidence": {"type": "number"}
            }
          }
        }
      }
    }
    """.strip()
    schema_summarize = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["topics"],
      "properties": {
        "topics": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["topic_key", "headline", "narrative", "key_facts", "evidence", "caveats"],
            "properties": {
              "topic_key": {"type": "string"},
              "headline": {"type": "string"},
              "narrative": {"type": "string"},
              "key_facts": {"type": "array", "items": {"type": "string"}},
              "evidence": {
                "type": "object",
                "additionalProperties": false,
                "required": ["article_ids", "concrete_facts"],
                "properties": {
                  "article_ids": {"type": "array", "items": {"type": "number"}},
                  "concrete_facts": {"type": "array", "items": {"type": "string"}}
                }
              },
              "caveats": {"type": "array", "items": {"type": "string"}}
            }
          }
        }
      }
    }
    """.strip()
    schema_map = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["families"],
      "properties": {
        "families": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["family_id", "family_title", "summary", "subtopics"],
            "properties": {
              "family_id": {"type": "string"},
              "family_title": {"type": "string"},
              "summary": {"type": "string"},
              "subtopics": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["subtopic_id", "title", "severity", "narrative", "citations"],
                  "properties": {
                    "subtopic_id": {"type": "string"},
                    "title": {"type": "string"},
                    "severity": {"type": "string"},
                    "narrative": {"type": "string"},
                    "citations": {"type": "array", "items": {"type": "number"}}
                  }
                }
              }
            }
          }
        }
      }
    }
    """.strip()
    schema_overall = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["tldr", "technical_synthesis", "actions"],
      "properties": {
        "tldr": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["text", "citations"],
            "properties": {
              "text": {"type": "string"},
              "citations": {"type": "array", "items": {"type": "number"}}
            }
          }
        },
        "technical_synthesis": {
          "type": "object",
          "additionalProperties": false,
          "required": ["text", "citations"],
          "properties": {
            "text": {"type": "string"},
            "citations": {"type": "array", "items": {"type": "number"}}
          }
        },
        "actions": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["action", "why", "priority", "time_horizon", "citations"],
            "properties": {
              "action": {"type": "string"},
              "why": {"type": "string"},
              "priority": {"type": "string"},
              "time_horizon": {"type": "string"},
              "citations": {"type": "array", "items": {"type": "number"}}
            }
          }
        },
        "low_value": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["citation_id", "reason"],
            "properties": {
              "citation_id": {"type": "number"},
              "reason": {"type": "string"}
            }
          }
        },
        "podcast_script": {"type": "string"}
      }
    }
    """.strip()

    _upsert_llm_schema(
        conn,
        "schema_daily_brief_cluster_topics_v6",
        "Daily Brief Topic Clustering",
        "v6",
        schema_cluster,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_summarize_topics_v6",
        "Daily Brief Topic Summaries",
        "v6",
        schema_summarize,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_map_nist_v6",
        "Daily Brief NIST Mapping",
        "v6",
        schema_map,
    )
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_overall_v6",
        "Daily Brief Overall Synthesis",
        "v6",
        schema_overall,
    )

    system_common = (
        "You are a senior cyber threat intelligence analyst. "
        "Output strict JSON only. No markdown, no code fences, no extra keys."
    )
    cluster_user = """
You cluster cybersecurity news articles into a compact set of topics that deduplicate the day’s coverage. You must be specific, grounded in the provided articles, and output strict JSON only.

Input JSON: {{input}}

Input includes a JSON array of articles. Each article has:
- id (int), citation_id (int), title (string), source_name (string), url (string), published_at (string)
- tags (array of strings), cves (array of strings), summary_text (string)

TASK
1) Create 8–20 topic clusters that best represent the day’s cyber news.
2) Deduplicate near-duplicates: multiple articles about the same CVE/campaign/vendor incident must map to one topic_key.
3) Prefer these topic_key styles (choose the best fit):
- "cve:CVE-YYYY-NNNNN" (if CVE present)
- "campaign:<slug>" (APT/campaign)
- "incident:<slug>" (breach/outage/arrest)
- "vuln:<product_slug>" (no CVE but clear product vuln)
- "research:<slug>" (analysis/guidance)
- "trend:<slug>" (broad trend, but keep rare)

RULES
- Use article tags and CVEs as strong signals.
- If a title is clearly non-security / off-topic, set a topic_key of "noise:<slug>" AND assign importance <= 0.2 so it can be dropped later.
- Importance: 1.0 = active exploitation / KEV / mass exploitation / major breach; 0.7 = serious vuln or high-impact incident; 0.4 = guidance/research; 0.2 = background/noise.
- Confidence is 0.5–1.0 based on how clearly the articles support it.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "label": "string",
      "topic_type": "operational|contextual|noise",
      "importance": 0.0-1.0,
      "confidence": 0.0-1.0,
      "why": "1-2 sentences grounded in the articles"
    }
  ],
  "article_topics": [
    { "id": 123, "topic_key": "string", "confidence": 0.0-1.0 }
  ]
}

Return JSON only.
""".strip()
    summarize_user = """
You are writing a technical daily cyber brief for practitioners. Be concrete and factual. You must not invent facts. Use only what is supported by the provided articles. Output strict JSON only.

Input JSON: {{input}}

Input format:
{
  "topics": [...],
  "topic_articles": {
     "<topic_key>": [
        {
          "id": int,
          "citation_id": int,
          "title": string,
          "source_name": string,
          "url": string,
          "published_at": string,
          "tags": [string],
          "cves": [string],
          "summary_text": string
        }, ...
     ]
  }
}

TASK
For each topic, write a concise analyst narrative that can be read aloud.
- Anchor the narrative to concrete details in the summaries and citation IDs.
- Include concrete facts and evidence IDs so downstream stages can cite accurately.
- If key details are missing, say so explicitly in caveats. Do NOT invent.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "headline": "short analyst headline",
      "narrative": "2–5 sentences, grounded in evidence",
      "key_facts": ["short fact", "..."],
      "evidence": {
        "article_ids": [123, 456],
        "concrete_facts": ["fact with anchor", "..."]
      },
      "caveats": ["missing detail", "..."]
    }
  ]
}

QUALITY BAR
- Prefer precise nouns: product names, components, auth flows, protocol names, misconfig types.
- Avoid generic advice. This stage is about facts and narrative only.

Return JSON only.
""".strip()
    map_user = """
You map the day’s topics into NIST 800-53 families and write the family-level narrative and subtopics. Output strict JSON only.

Input JSON: {{input}}
{
  "topics": [...],
  "topic_summaries": {...},
  "topic_articles": {...},
  "citations": [
    { "id": 1, "title": "...", "source_name": "...", "url": "...", "summary": "..." }
  ],
  "nist_families": [
    { "code": "CM", "title": "Configuration Management", "description": "..." }
  ]
}

TASK
- Families are the primary organization. Only include families that appear today.
- For each family:
  - family_id, family_title
  - summary paragraph with inline citations like (1)
  - subtopics: each with subtopic_id, title, severity (High|Medium|Low), narrative with inline citations, and citations array.
- Use the provided NIST family descriptions to stay grounded; do NOT invent what a family means.
- Each citation ID should appear in exactly one family (best-fit). Do not duplicate across families.
- Nearly all citations should be included in at least one family subtopic unless they are clearly low-value.
- Family summaries must be specific and cite at least one citation ID; avoid generic boilerplate.

CRITICAL
- Output MUST be a top-level {"families":[...]} list. Do NOT output topic->families mapping.
- Citations MUST use the citation IDs provided in the input (1..N). Do NOT use article IDs.

Use only these families:
AC, AU, AT, CA, CM, CP, IA, IR, MA, MP, PE, PL, PM, PS, PT, RA, SA, SC, SI, SR

OUTPUT JSON SCHEMA (strict)
{
  "families": [
    {
      "family_id": "CM",
      "family_title": "Configuration Management",
      "summary": "string",
      "subtopics": [
        {
          "subtopic_id": "stable_slug",
          "title": "string",
          "severity": "High|Medium|Low",
          "narrative": "string",
          "citations": [1, 2, 3]
        }
      ]
    }
  ]
}

Return JSON only.
""".strip()
    overall_user = """
You are writing a daily cyber threat intelligence brief for senior practitioners. The brief must read like a human analyst report and be suitable for spoken-word delivery. Output strict JSON only.

Input JSON: {{input}}
{
  "day": "YYYY-MM-DD",
  "topics": [...],
  "citations": [...],
  "families": [...]
}

OUTPUT STRUCTURE (strict)
1) TLDR: 5–7 bullets. Each bullet is “Read this because…”. Must include concrete identifiers and inline citations like (1)(2).
2) Technical Synthesis: 2–4 paragraphs. Detailed, anchored to concrete facts from the day. Avoid repeating TLDR text; instead expand with additional cited detail. Use inline citations.
3) Actions: 3–5 items (use 5 when justified by the day). Each action must include WHAT, WHY, and WHEN/priority (P0/P1/P2 with time horizon). Each action must include citations.
4) Podcast Script: 150–400 words, spoken-friendly, technical but clear. Intro + 3–5 segments + close. Mention at least 2 CVEs/products/actors and at least 2 defensive actions with rationale.

RULES
- Do NOT produce outlines or checklists.
- Do NOT use generic advice. If you recommend an action, explain why it is urgent now and cite sources.
- Do NOT include raw URLs inline; links appear only in the citations list.
- Do NOT invent facts. If details are missing, acknowledge uncertainty.
- Inline citations must match the citations list IDs (1..N). Do NOT use article IDs.
- If you emit low_value items, use a reason enum from: webinar|sponsored|press_release|promo|advertisement|marketing|whitepaper|ebook|roundup|opinion|survey|podcast|url_only.

OUTPUT JSON SCHEMA (strict)
{
  "tldr": [
    { "text": "bullet text", "citations": [1,2] }
  ],
  "technical_synthesis": { "text": "string", "citations": [1,2,3] },
  "actions": [
    {
      "action": "imperative sentence",
      "why": "tight rationale tied to today’s items",
      "priority": "P0|P1|P2",
      "time_horizon": "0-24h|72h|7d|30d",
      "citations": [1,2]
    }
  ],
  "podcast_script": "string"
}

Return JSON only.
""".strip()

    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_cluster_topics_v6",
        "Daily Brief Topic Clustering",
        "v6",
        system_common,
        cluster_user,
        "Cluster articles into operational topics; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_summarize_topics_v6",
        "Daily Brief Topic Summaries",
        "v6",
        system_common,
        summarize_user,
        "Analyst narratives per topic; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_map_nist_v6",
        "Daily Brief NIST Mapping",
        "v6",
        system_common,
        map_user,
        "Map topics to NIST families; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v6",
        "Daily Brief Overall Synthesis",
        "v6",
        system_common,
        overall_user,
        "Narrative daily synthesis; JSON only.",
    )

    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_cluster_topics",
        "prompt_daily_brief_cluster_topics_v6",
        "schema_daily_brief_cluster_topics_v6",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_summarize_topics",
        "prompt_daily_brief_summarize_topics_v6",
        "schema_daily_brief_summarize_topics_v6",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_map_nist_families",
        "prompt_daily_brief_map_nist_v6",
        "schema_daily_brief_map_nist_v6",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v6",
        "schema_daily_brief_overall_v6",
    )
    for stage in (
        "daily_brief_cluster_topics",
        "daily_brief_summarize_topics",
        "daily_brief_map_nist_families",
        "daily_brief_overall_synthesis",
    ):
        _update_stage_profile_params(conn, stage, {"max_input_chars": 120000})


def _migrate_daily_brief_prompt_updates_v7(conn) -> None:
    system_common = "You are a precise cybersecurity intelligence analyst. Output strict JSON only."
    cluster_user = """
Date: {date}

Input: a JSON array of articles. Each article has:
- id (int), citation_id (int), title (string), source_name (string), url (string), published_at (string)
- tags (array of strings), cves (array of strings), summary_text (string)

TASK
1) Create 8–20 topic clusters that best represent the day’s cyber news.
2) Deduplicate near-duplicates: multiple articles about the same CVE/campaign/vendor incident must map to one topic_key.
3) Prefer these topic_key styles (choose the best fit):
  - "cve:CVE-YYYY-NNNNN" (if CVE present)
  - "campaign:<slug>" (APT/campaign)
  - "incident:<slug>" (breach/outage/arrest)
  - "vuln:<product_slug>" (no CVE but clear product vuln)
  - "research:<slug>" (analysis/guidance)
  - "trend:<slug>" (broad trend, but keep rare)

RULES
- Use article tags and CVEs as strong signals.
- If a title is clearly non-security / off-topic, set topic_key "noise:<slug>" and importance <= 0.2.
- Importance: 1.0 = active exploitation / KEV / mass exploitation / major breach; 0.7 = serious vuln or high-impact incident; 0.4 = guidance/research; 0.2 = background/noise.
- Confidence is 0.5–1.0 based on how clearly the articles support it.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "label": "string",
      "topic_type": "operational|contextual|noise",
      "importance": 0.0-1.0,
      "confidence": 0.0-1.0,
      "why": "1-2 sentences grounded in the articles"
    }
  ],
  "article_topics": [
    { "id": 123, "topic_key": "string", "confidence": 0.0-1.0 }
  ]
}

Return JSON only.
""".strip()
    summarize_user = """
You are writing a technical daily cyber brief for practitioners. Be concrete and factual. You must not invent facts. Use only what is supported by the provided articles. Output strict JSON only.

Input JSON: {{input}}

Input format:
{
  "topics": [...],
  "topic_articles": {
     "<topic_key>": [
        {
          "id": int,
          "citation_id": int,
          "title": string,
          "source_name": string,
          "url": string,
          "published_at": string,
          "tags": [string],
          "cves": [string],
          "summary_text": string
        }, ...
     ]
  }
}

TASK
For each topic, write a concise analyst narrative that can be read aloud.
- Anchor the narrative to concrete details in the summaries and citation IDs.
- Include concrete facts and evidence IDs so downstream stages can cite accurately.
- If key details are missing, say so explicitly in caveats. Do NOT invent.

OUTPUT JSON SCHEMA (strict)
{
  "topics": [
    {
      "topic_key": "string",
      "headline": "short analyst headline",
      "narrative": "2–5 sentences, grounded in evidence",
      "key_facts": ["short fact", "..."],
      "evidence": {
        "article_ids": [123, 456],
        "concrete_facts": ["fact with anchor", "..."]
      },
      "caveats": ["missing detail", "..."]
    }
  ]
}

QUALITY BAR
- Prefer precise nouns: product names, components, auth flows, protocol names, misconfig types.
- Avoid generic advice. This stage is about facts and narrative only.

Return JSON only.
""".strip()
    map_user = """
You map the day’s topics into NIST 800-53 families and write the family-level narrative and subtopics. Output strict JSON only.

Input JSON: {{input}}
{
  "topics": [...],
  "topic_summaries": {...},
  "topic_articles": {...},
  "citations": [
    { "id": 1, "title": "...", "source_name": "...", "url": "...", "summary": "..." }
  ],
  "nist_families": [
    { "code": "CM", "title": "Configuration Management", "description": "..." }
  ]
}

TASK
- Families are the primary organization. Only include families that appear today.
- For each family:
  - family_id, family_title
  - summary paragraph that synthesizes what impacted THIS family today (not a glossary). Use inline citations like (1).
  - subtopics: each with subtopic_id, title, severity (High|Medium|Low), narrative with inline citations, and citations array.
- Use the provided NIST family descriptions to stay grounded; do NOT invent what a family means.
- Each citation ID should appear in exactly one family (best-fit). Do not duplicate across families.
- Nearly all citations should be included in at least one family subtopic unless they are clearly low-value.
- Family summaries must be specific and cite at least one citation ID.
- Avoid internal repetition inside a family: do not restate the same facts in both summary and subtopics.
- The family summary should describe the scope/patterns across the subtopics, not re-list the subtopics.

CRITICAL
- Output MUST be a top-level {"families":[...]} list. Do NOT output topic->families mapping.
- Citations MUST use the citation IDs provided in the input (1..N). Do NOT use article IDs.

Use only these families:
AC, AU, AT, CM, CP, IA, IR, MA, PE, PL, PM, PS, RA, SA, SC, SI, SR

OUTPUT JSON SCHEMA (strict)
{
  "families": [
    {
      "family_id": "CM",
      "family_title": "Configuration Management",
      "summary": "string",
      "subtopics": [
        {
          "subtopic_id": "stable_slug",
          "title": "string",
          "severity": "High|Medium|Low",
          "narrative": "string",
          "citations": [1, 2, 3]
        }
      ]
    }
  ]
}

Return JSON only.
""".strip()
    overall_user = """
You are writing a daily cyber threat intelligence brief for senior practitioners. The brief must read like a human analyst report and be suitable for spoken-word delivery. Output strict JSON only.

Input JSON: {{input}}
{
  "day": "YYYY-MM-DD",
  "topics": [...],
  "citations": [...],
  "families": [...]
}

OUTPUT STRUCTURE (strict)
1) TLDR: 5–7 bullets. Each bullet is a top item summary (no prefatory phrase). Include concrete identifiers and inline citations like (1)(2). Bullets must be distinct; do not restate the same story twice.
2) Technical Synthesis: 3–5 paragraphs. Detailed, anchored to concrete facts from the day. Avoid repeating TLDR text; instead expand with additional cited detail and connective context. Use inline citations.
3) Actions: 3–5 items (use 5 when justified by the day). Each action must include WHAT, WHY, and WHEN/priority (P0/P1/P2 with time horizon). Each action must include citations.
4) Podcast Script: 150–400 words, spoken-friendly, technical but clear. Intro + 3–5 segments + close. Mention at least 2 CVEs/products/actors and at least 2 defensive actions with rationale.

RULES
- Do NOT produce outlines or checklists.
- Do NOT begin TLDR bullets with "Read this because" or similar phrasing.
- Do NOT use generic advice. If you recommend an action, explain why it is urgent now and cite sources.
- Do NOT include raw URLs inline; links appear only in the citations list.
- Do NOT invent facts. If details are missing, acknowledge uncertainty.
- Inline citations must match the citations list IDs (1..N). Do NOT use article IDs.
- If you emit low_value items, use a reason enum from: webinar|sponsored|press_release|promo|advertisement|marketing|whitepaper|ebook|roundup|opinion|survey|podcast|url_only.

OUTPUT JSON SCHEMA (strict)
{
  "tldr": [
    { "text": "bullet text", "citations": [1,2] }
  ],
  "technical_synthesis": { "text": "string", "citations": [1,2,3] },
  "actions": [
    {
      "action": "imperative sentence",
      "why": "tight rationale tied to today’s items",
      "priority": "P0|P1|P2",
      "time_horizon": "0-24h|72h|7d|30d",
      "citations": [1,2]
    }
  ],
  "podcast_script": "string"
}

Return JSON only.
""".strip()

    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_cluster_topics_v7",
        "Daily Brief Topic Clustering",
        "v7",
        system_common,
        cluster_user,
        "Cluster articles into operational topics; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_summarize_topics_v7",
        "Daily Brief Topic Summaries",
        "v7",
        system_common,
        summarize_user,
        "Analyst narratives per topic; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_map_nist_v7",
        "Daily Brief NIST Mapping",
        "v7",
        system_common,
        map_user,
        "Map topics to NIST families; JSON only.",
    )
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v7",
        "Daily Brief Overall Synthesis",
        "v7",
        system_common,
        overall_user,
        "Narrative daily synthesis; JSON only.",
    )

    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_cluster_topics",
        "prompt_daily_brief_cluster_topics_v7",
        "schema_daily_brief_cluster_topics_v6",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_summarize_topics",
        "prompt_daily_brief_summarize_topics_v7",
        "schema_daily_brief_summarize_topics_v6",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_map_nist_families",
        "prompt_daily_brief_map_nist_v7",
        "schema_daily_brief_map_nist_v6",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v7",
        "schema_daily_brief_overall_v6",
    )


def _migrate_article_context_pack_prompt_v1(conn) -> None:
    if not _table_exists(conn, "llm_prompts"):
        return
    schema_context = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["facts", "entities", "numbers", "iocs", "cves", "timeline", "uncertainties"],
      "properties": {
        "facts": { "type": "array", "items": { "type": "string" } },
        "entities": {
          "type": "object",
          "additionalProperties": false,
          "required": ["orgs","people","products","vendors","threat_actors","countries"],
          "properties": {
            "orgs": { "type": "array", "items": { "type": "string" } },
            "people": { "type": "array", "items": { "type": "string" } },
            "products": { "type": "array", "items": { "type": "string" } },
            "vendors": { "type": "array", "items": { "type": "string" } },
            "threat_actors": { "type": "array", "items": { "type": "string" } },
            "countries": { "type": "array", "items": { "type": "string" } }
          }
        },
        "numbers": { "type": "array", "items": { "type": "string" } },
        "iocs": { "type": "array", "items": { "type": "string" } },
        "cves": { "type": "array", "items": { "type": "string" } },
        "timeline": { "type": "array", "items": { "type": "string" } },
        "uncertainties": { "type": "array", "items": { "type": "string" } }
      }
    }
    """.strip()
    _upsert_llm_schema(
        conn,
        "schema_article_context_pack_v1",
        "Article Context Pack",
        "v1",
        schema_context,
    )
    system_template = """
You extract dense factual context from a cybersecurity news article.
Output strict JSON only. No markdown, no prose, no recommendations.
""".strip()
    user_template = """
Input JSON: {{input}}

TASK
Extract a dense, factual context pack from the article.
Rules:
- Use only facts stated or clearly implied by the article.
- Keep items atomic and concrete.
- No narrative prose, no opinions, no recommendations.
- If uncertain or missing, note it in uncertainties.

OUTPUT JSON SCHEMA (strict)
{
  "facts": ["..."],
  "entities": {
    "orgs": [],
    "people": [],
    "products": [],
    "vendors": [],
    "threat_actors": [],
    "countries": []
  },
  "numbers": ["..."],
  "iocs": ["..."],
  "cves": ["CVE-YYYY-NNNNN"],
  "timeline": ["..."],
  "uncertainties": ["..."]
}

Return JSON only.
""".strip()
    _upsert_llm_prompt(
        conn,
        "prompt_article_context_pack_v1",
        "Article Context Pack (dense facts)",
        "v1",
        system_template,
        user_template,
        "Per-article dense facts for daily brief input.",
    )
    if _table_exists(conn, "pipeline_stage_config") and _table_exists(conn, "llm_profiles"):
        row = conn.execute(
            "SELECT profile_id FROM pipeline_stage_config WHERE stage_name = %s",
            ("article_context_pack",),
        ).fetchone()
        if not row:
            base_row = conn.execute(
                "SELECT profile_id FROM pipeline_stage_config WHERE stage_name = %s",
                ("summarize_article",),
            ).fetchone()
            if base_row:
                conn.execute(
                    """
                    INSERT INTO pipeline_stage_config (stage_name, profile_id, rules_json, updated_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    ("article_context_pack", base_row[0], None, utc_now_iso()),
                )


def _migrate_daily_brief_overall_prompt_updates_v8(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    system_common = (
        "You are a senior cyber threat intelligence analyst. Output strict JSON only. "
        "No markdown, no code fences, no extra keys."
    )
    overall_user = """
You are writing a daily cyber threat intelligence brief for senior practitioners. The brief must read like a human analyst report and be suitable for spoken-word delivery. Output strict JSON only.

Input JSON: {{input}}
This input can be one of:
1) Single-stage mode:
{
  "day": "YYYY-MM-DD",
  "citations": [{ "id": 1, "title": "...", "source_name": "...", "url": "...", "summary": "..." }],
  "articles": [
    {
      "citation_id": 1,
      "title": "...",
      "source": "...",
      "url": "...",
      "published_at": "...",
      "cves": [...],
      "tags": [...],
      "context_pack": { ... }
    }
  ]
}
2) Multi-stage mode:
{
  "day": "YYYY-MM-DD",
  "topics": [...],
  "citations": [...],
  "families": [...]
}

OUTPUT STRUCTURE (strict)
1) TLDR: 5–7 bullets. Each bullet is a top item summary (no prefatory phrase). Include concrete identifiers and inline citations like (1)(2). Bullets must be distinct; do not restate the same story twice.
2) Technical Synthesis: 3–5 paragraphs. Detailed, anchored to concrete facts from the day. Avoid repeating TLDR text; instead expand with additional cited detail and connective context. Use inline citations.
3) Actions: 3–5 items (use 5 when justified by the day). Each action must include WHAT, WHY, and WHEN/priority (P0/P1/P2 with time horizon). Each action must include citations.
4) Podcast Script: 150–400 words, spoken-friendly, technical but clear. Intro + 3–5 segments + close. Mention at least 2 CVEs/products/actors and at least 2 defensive actions with rationale.

RULES
- Do NOT produce outlines or checklists.
- Do NOT begin TLDR bullets with "Read this because" or similar phrasing.
- Do NOT use generic advice. If you recommend an action, explain why it is urgent now and cite sources.
- Do NOT include raw URLs inline; links appear only in the citations list.
- Do NOT invent facts. If details are missing, acknowledge uncertainty.
- Inline citations must match the citations list IDs (1..N). Do NOT use article IDs.
- If you emit low_value items, use a reason enum from: webinar|sponsored|press_release|promo|advertisement|marketing|whitepaper|ebook|roundup|opinion|survey|podcast|url_only.

OUTPUT JSON SCHEMA (strict)
{
  "tldr": [
    { "text": "bullet text", "citations": [1,2] }
  ],
  "technical_synthesis": { "text": "string", "citations": [1,2,3] },
  "actions": [
    {
      "action": "imperative sentence",
      "why": "tight rationale tied to today’s items",
      "priority": "P0|P1|P2",
      "time_horizon": "0-24h|72h|7d|30d",
      "citations": [1,2]
    }
  ],
  "podcast_script": "string"
}

Return JSON only.
""".strip()
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v8",
        "Daily Brief Overall Synthesis",
        "v8",
        system_common,
        overall_user,
        "Narrative daily synthesis; JSON only; supports single and multi-stage input.",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v8",
        "schema_daily_brief_overall_v6",
    )


def _migrate_daily_brief_overall_prompt_updates_v9(conn) -> None:
    if not (_table_exists(conn, "llm_prompts") and _table_exists(conn, "llm_schemas")):
        return
    system_common = (
        "You are a senior cyber threat intelligence analyst. Output strict JSON only. "
        "No markdown, no code fences, no extra keys."
    )
    schema_overall = """
    {
      "type": "object",
      "additionalProperties": false,
      "required": ["tldr", "technical_synthesis", "actions", "families", "low_value"],
      "properties": {
        "tldr": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["text", "citations"],
            "properties": {
              "text": {"type": "string"},
              "citations": {"type": "array", "items": {"type": "number"}}
            }
          }
        },
        "technical_synthesis": {
          "type": "object",
          "additionalProperties": false,
          "required": ["text", "citations"],
          "properties": {
            "text": {"type": "string"},
            "citations": {"type": "array", "items": {"type": "number"}}
          }
        },
        "actions": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["action", "why", "priority", "time_horizon", "citations"],
            "properties": {
              "action": {"type": "string"},
              "why": {"type": "string"},
              "priority": {"type": "string"},
              "time_horizon": {"type": "string"},
              "citations": {"type": "array", "items": {"type": "number"}}
            }
          }
        },
        "families": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["family_id", "family_title", "summary", "subtopics"],
            "properties": {
              "family_id": {"type": "string"},
              "family_title": {"type": "string"},
              "summary": {"type": "string"},
              "subtopics": {
                "type": "array",
                "items": {
                  "type": "object",
                  "additionalProperties": false,
                  "required": ["subtopic_id", "title", "severity", "narrative", "citations"],
                  "properties": {
                    "subtopic_id": {"type": "string"},
                    "title": {"type": "string"},
                    "severity": {"type": "string"},
                    "narrative": {"type": "string"},
                    "citations": {"type": "array", "items": {"type": "number"}}
                  }
                }
              }
            }
          }
        },
        "low_value": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["citation_id", "reason"],
            "properties": {
              "citation_id": {"type": "number"},
              "reason": {"type": "string"}
            }
          }
        },
        "podcast_script": {"type": "string"}
      }
    }
    """.strip()
    _upsert_llm_schema(
        conn,
        "schema_daily_brief_overall_v7",
        "Daily Brief Overall Synthesis",
        "v7",
        schema_overall,
    )
    overall_user = """
You are writing a daily cyber threat intelligence brief for senior practitioners. The brief must read like a human analyst report and be suitable for spoken-word delivery. Output strict JSON only.

Input JSON: {{input}}
Single-stage mode input:
{
  "day": "YYYY-MM-DD",
  "citations": [{ "id": 1, "title": "...", "source_name": "...", "url": "...", "summary": "..." }],
  "articles": [
    {
      "citation_id": 1,
      "title": "...",
      "source": "...",
      "url": "...",
      "published_at": "...",
      "cves": [...],
      "tags": [...],
      "context_pack": { ... }
    }
  ],
  "nist_families": [
    { "code": "AC", "title": "Access Control", "description": "..." }
  ]
}

REQUIRED SECTIONS
1) TLDR: 5–7 bullets. No prefatory phrase. Each bullet must include concrete identifiers + inline citations like (1)(2).
2) Technical Synthesis: 3–5 paragraphs. Expand on the day’s most impactful stories; do not restate TLDR. Use inline citations.
3) Actions: 3–7 items. Each action must include WHAT, WHY, and WHEN/priority (P0/P1/P2 with time horizon). Include citations.
4) Full Summary by NIST Family (families):
   - Assign every NON‑low‑value citation to exactly one NIST family.
   - Use the provided nist_families list (code/title/description) to decide placement.
   - For each family used today: write a news‑first family summary (1–4 paragraphs) and 1–3 subtopics with short narrative paragraphs (3–6 sentences), all with inline citations.
   - Each citation ID must appear in exactly one family subtopic (no duplicates across families).
5) Low‑Value: only items that are clearly non‑security, promos, or opinion. Use reason enum:
   webinar|sponsored|press_release|promo|advertisement|marketing|whitepaper|ebook|roundup|opinion|survey|podcast|url_only
6) Podcast Script: 150–400 words, spoken‑friendly, technical but clear. Intro + 3–5 segments + close. Mention at least 2 CVEs/products/actors and at least 2 defensive actions with rationale.

RULES
- Do NOT include raw URLs inline; links appear only in the citations list.
- Do NOT invent facts. Use only provided context packs and summaries.
- Inline citations must match the citations list IDs (1..N). Do NOT use article IDs.
- If details are missing, acknowledge uncertainty rather than guessing.
- Families must be actual NIST 800-53 families and must use the provided list.

OUTPUT JSON SCHEMA (strict)
{
  "tldr": [
    { "text": "bullet text", "citations": [1,2] }
  ],
  "technical_synthesis": { "text": "string", "citations": [1,2,3] },
  "actions": [
    {
      "action": "imperative sentence",
      "why": "tight rationale tied to today’s items",
      "priority": "P0|P1|P2",
      "time_horizon": "0-24h|72h|7d|30d",
      "citations": [1,2]
    }
  ],
  "families": [
    {
      "family_id": "CM",
      "family_title": "Configuration Management",
      "summary": "string",
      "subtopics": [
        {
          "subtopic_id": "stable_slug",
          "title": "string",
          "severity": "High|Medium|Low",
          "narrative": "string",
          "citations": [1, 2, 3]
        }
      ]
    }
  ],
  "low_value": [
    { "citation_id": 44, "reason": "opinion" }
  ],
  "podcast_script": "string"
}

Return JSON only.
""".strip()
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v9",
        "Daily Brief Overall Synthesis",
        "v9",
        system_common,
        overall_user,
        "Single-stage overall synthesis with full NIST family summary; JSON only.",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v9",
        "schema_daily_brief_overall_v7",
    )


def _migrate_daily_brief_overall_prompt_updates_v10(conn) -> None:
    if not _table_exists(conn, "llm_prompts"):
        return
    system_common = (
        "You are a senior cyber threat intelligence analyst. Output strict JSON only. "
        "No markdown, no code fences, no extra keys."
    )
    overall_user = """
You are writing a daily cyber threat intelligence brief for senior practitioners. The brief must read like a human analyst report and be suitable for spoken-word delivery. Output strict JSON only.

Input JSON: {{input}}
Single-stage mode input:
{
  "day": "YYYY-MM-DD",
  "citations": [{ "id": 1, "title": "...", "source_name": "...", "url": "...", "summary": "..." }],
  "articles": [
    {
      "citation_id": 1,
      "title": "...",
      "source": "...",
      "url": "...",
      "published_at": "...",
      "cves": [...],
      "tags": [...],
      "context_pack": { ... }
    }
  ],
  "nist_families": [
    { "code": "AC", "title": "Access Control", "description": "..." }
  ]
}

REQUIRED SECTIONS
1) TLDR: 5–7 distinct bullets. No prefatory phrase. Each bullet must include concrete identifiers + inline citations like (1)(2).
2) Technical Synthesis: 3–5 paragraphs, each 3–5 sentences. Add connective tissue and context, do not restate TLDR verbatim. Use inline citations and cover at least 8 distinct citations.
3) Actions: 3–7 items. Each action must include WHAT, WHY, and WHEN/priority (P0/P1/P2 with time horizon). Include citations.
4) Full Summary by NIST Family (families):
   - Assign every NON‑low‑value citation to exactly one NIST family.
   - Use the provided nist_families list (code/title/description) to decide placement. The descriptions are guidance only; do NOT explain them.
   - For each family used today: write a news‑first family summary (1–2 sentences) describing what happened in that control area TODAY.
   - Then include 1–3 subtopics under that family; each subtopic is a short narrative paragraph (3–6 sentences) with inline citations.
   - Each citation ID must appear in exactly one family subtopic (no duplicates across families).
5) Low‑Value: only items that are clearly non‑security, promos, or opinion. Use reason enum:
   webinar|sponsored|press_release|promo|advertisement|marketing|whitepaper|ebook|roundup|opinion|survey|podcast|url_only
6) Podcast Script: 150–400 words, spoken‑friendly, technical but clear. Intro + 3–5 segments + close. Mention at least 2 CVEs/products/actors and at least 2 defensive actions with rationale.

RULES
- Do NOT begin TLDR bullets with “Read this because” or similar phrasing.
- Do NOT include raw URLs inline; links appear only in the citations list.
- Do NOT invent facts. Use only provided context packs and summaries.
- Inline citations must match the citations list IDs (1..N). Do NOT use article IDs.
- If details are missing, acknowledge uncertainty rather than guessing.
- No duplicate subtopics within a family. Avoid repeating the family summary in subtopics.

OUTPUT JSON SCHEMA (strict)
{
  "tldr": [
    { "text": "bullet text", "citations": [1,2] }
  ],
  "technical_synthesis": { "text": "string", "citations": [1,2,3] },
  "actions": [
    {
      "action": "imperative sentence",
      "why": "tight rationale tied to today’s items",
      "priority": "P0|P1|P2",
      "time_horizon": "0-24h|72h|7d|30d",
      "citations": [1,2]
    }
  ],
  "families": [
    {
      "family_id": "CM",
      "family_title": "Configuration Management",
      "summary": "string",
      "subtopics": [
        {
          "subtopic_id": "stable_slug",
          "title": "string",
          "severity": "High|Medium|Low",
          "narrative": "string",
          "citations": [1, 2, 3]
        }
      ]
    }
  ],
  "low_value": [
    { "citation_id": 44, "reason": "opinion" }
  ],
  "podcast_script": "string"
}

Return JSON only.
""".strip()
    _upsert_llm_prompt(
        conn,
        "prompt_daily_brief_overall_v10",
        "Daily Brief Overall Synthesis",
        "v10",
        system_common,
        overall_user,
        "Single-stage overall synthesis with news-first NIST family summary; JSON only.",
    )
    _update_stage_profile_prompt_schema(
        conn,
        "daily_brief_overall_synthesis",
        "prompt_daily_brief_overall_v10",
        "schema_daily_brief_overall_v7",
    )


def _migrate_daily_brief_overall_input_limits_v1(conn) -> None:
    if not _table_exists(conn, "pipeline_stage_config"):
        return
    _update_stage_profile_params(
        conn,
        "daily_brief_overall_synthesis",
        {"max_input_chars": 400000},
    )


def _migrate_daily_brief_cluster_use_openai(conn) -> None:
    row = conn.execute(
        "SELECT profile_id FROM pipeline_stage_config WHERE stage_name = %s",
        ("daily_brief_cluster_topics",),
    ).fetchone()
    if not row:
        return
    profile_id = row[0]
    provider_row = conn.execute(
        """
        SELECT id, name, base_url
        FROM llm_providers
        WHERE lower(type) = 'openai_compatible'
        ORDER BY
            CASE
                WHEN position('openai' in lower(name)) > 0 THEN 0
                WHEN position('openai' in lower(base_url)) > 0 THEN 1
                ELSE 2
            END,
            name
        LIMIT 1
        """,
        (),
    ).fetchone()
    if not provider_row:
        return
    provider_id = provider_row[0]
    model_row = conn.execute(
        """
        SELECT id
        FROM llm_models
        WHERE provider_id = %s AND is_enabled = 1
        ORDER BY model_name
        LIMIT 1
        """,
        (provider_id,),
    ).fetchone()
    if not model_row:
        return
    model_id = model_row[0]
    conn.execute(
        """
        UPDATE llm_profiles
        SET primary_provider_id = %s,
            primary_model_id = %s,
            updated_at = %s
        WHERE id = %s
        """,
        (provider_id, model_id, utc_now_iso(), profile_id),
    )
    conn.commit()


def _migrate_daily_brief_nist_use_openai(conn) -> None:
    row = conn.execute(
        "SELECT profile_id FROM pipeline_stage_config WHERE stage_name = %s",
        ("daily_brief_map_nist_families",),
    ).fetchone()
    if not row:
        return
    profile_id = row[0]
    provider_row = conn.execute(
        """
        SELECT id, name, base_url
        FROM llm_providers
        WHERE lower(type) = 'openai_compatible'
        ORDER BY
            CASE
                WHEN position('openai' in lower(name)) > 0 THEN 0
                WHEN position('openai' in lower(base_url)) > 0 THEN 1
                ELSE 2
            END,
            name
        LIMIT 1
        """,
        (),
    ).fetchone()
    if not provider_row:
        return
    provider_id = provider_row[0]
    model_row = conn.execute(
        """
        SELECT id
        FROM llm_models
        WHERE provider_id = %s AND is_enabled = 1
        ORDER BY model_name
        LIMIT 1
        """,
        (provider_id,),
    ).fetchone()
    if not model_row:
        return
    model_id = model_row[0]
    conn.execute(
        """
        UPDATE llm_profiles
        SET primary_provider_id = %s,
            primary_model_id = %s,
            updated_at = %s
        WHERE id = %s
        """,
        (provider_id, model_id, utc_now_iso(), profile_id),
    )
    conn.commit()

def _migrate_source_overrides(conn) -> None:
    conn.execute("ALTER TABLE sources ADD COLUMN IF NOT EXISTS overrides JSONB NULL")


def _migrate_jobs_priority(conn) -> None:
    if not _table_exists(conn, "jobs"):
        return
    if _has_column(conn, "jobs", "priority"):
        return
    conn.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
