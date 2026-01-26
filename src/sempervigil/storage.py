from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from typing import Iterable

from .migrations import apply_migrations
from .models import Article, Job, Source, SourceTactic
from .normalize import cpe_to_vendor_product, normalize_name
from .utils import json_dumps, utc_now_iso, utc_now_iso_offset


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


def list_due_sources(conn: sqlite3.Connection, now_iso: str) -> list[Source]:
    sources = list_sources(conn, enabled_only=True)
    due: list[Source] = []
    last_runs = _last_run_map(conn)
    now_dt = _parse_iso(now_iso)
    for source in sources:
        if source.pause_until and _parse_iso(source.pause_until) > now_dt:
            continue
        last_run = last_runs.get(source.id)
        if not last_run:
            due.append(source)
            continue
        last_dt = _parse_iso(last_run)
        if last_dt + timedelta(minutes=source.default_frequency_minutes) <= now_dt:
            due.append(source)
    return due


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
            json_dumps(tactic.config),
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


def get_article_id(conn: sqlite3.Connection, source_id: str, stable_id: str) -> int | None:
    return _get_article_id(conn, source_id, stable_id)


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
            _brief_day_from(article.published_at or article.ingested_at),
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
             published_at_source, ingested_at, brief_day, is_commercial, content_fingerprint,
             extracted_text_path, extracted_text_hash, raw_html_path, raw_html_hash,
             meta_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def list_articles_for_day(conn: sqlite3.Connection, day: str) -> list[dict[str, object]]:
    cursor = conn.execute(
        """
        SELECT id, source_id, title, original_url, published_at, ingested_at, summary, brief_day,
               summary_llm, summary_model, summary_generated_at
        FROM articles
        WHERE brief_day = ?
        ORDER BY published_at DESC
        """,
        (day,),
    )
    rows = []
    for row in cursor.fetchall():
        (
            article_id,
            source_id,
            title,
            original_url,
            published_at,
            ingested_at,
            summary,
            brief_day,
            summary_llm,
            summary_model,
            summary_generated_at,
        ) = row
        rows.append(
            {
                "id": article_id,
                "source_id": source_id,
                "title": title,
                "original_url": original_url,
                "published_at": published_at,
                "ingested_at": ingested_at,
                "summary": summary,
                "brief_day": brief_day,
                "summary_llm": summary_llm,
                "summary_model": summary_model,
                "summary_generated_at": summary_generated_at,
            }
        )
    return rows


def list_summaries_for_day(conn: sqlite3.Connection, day: str) -> list[dict[str, object]]:
    articles = list_articles_for_day(conn, day)
    rows: list[dict[str, object]] = []
    for article in articles:
        if not article.get("summary_llm"):
            continue
        try:
            summary_data = json.loads(article["summary_llm"])
        except json.JSONDecodeError:
            summary_data = {"summary": article["summary_llm"], "bullets": [], "why": "", "cves": []}
        rows.append({**article, "summary_data": summary_data})
    return rows


def upsert_cve_links(
    conn: sqlite3.Connection,
    article_id: int,
    cve_ids: list[str],
    evidence: dict[str, object],
) -> None:
    if not cve_ids:
        return
    now = utc_now_iso()
    if _table_exists(conn, "cves"):
        cve_columns = _table_columns(conn, "cves")
        for cve_id in cve_ids:
            if "created_at" in cve_columns and "last_seen_at" in cve_columns:
                conn.execute(
                    """
                    INSERT INTO cves (cve_id, created_at, last_seen_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(cve_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
                    """,
                    (cve_id, now, now),
                )
            elif "updated_at" in cve_columns:
                conn.execute(
                    """
                    INSERT INTO cves (cve_id, updated_at)
                    VALUES (?, ?)
                    ON CONFLICT(cve_id) DO UPDATE SET updated_at = excluded.updated_at
                    """,
                    (cve_id, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO cves (cve_id) VALUES (?)",
                    (cve_id,),
                )
    if _table_exists(conn, "article_cves"):
        columns = _table_columns(conn, "article_cves")
        for cve_id in cve_ids:
            payload = {
                "article_id": article_id,
                "cve_id": cve_id,
                "confidence": 1.0,
                "confidence_band": "linked",
                "reasons_json": json_dumps(["rule.cve.explicit"]),
                "evidence_json": json_dumps(evidence),
                "created_at": now,
                "matched_by": "explicit",
                "inference_level": "explicit",
            }
            cols = [key for key in payload if key in columns]
            values = [payload[col] for col in cols]
            placeholders = ", ".join("?" for _ in cols)
            conn.execute(
                f"""
                INSERT OR IGNORE INTO article_cves ({", ".join(cols)})
                VALUES ({placeholders})
                """,
                values,
            )
        conn.commit()
        return
    _append_article_cves_meta(conn, article_id, cve_ids, evidence)


def _append_article_cves_meta(
    conn: sqlite3.Connection,
    article_id: int,
    cve_ids: list[str],
    evidence: dict[str, object],
) -> None:
    cursor = conn.execute("SELECT meta_json FROM articles WHERE id = ?", (article_id,))
    row = cursor.fetchone()
    meta = {}
    if row and row[0]:
        try:
            meta = json.loads(row[0])
        except json.JSONDecodeError:
            meta = {}
    links = {item.get("cve_id"): item for item in meta.get("cve_links", []) if item}
    for cve_id in cve_ids:
        links[cve_id] = {
            "cve_id": cve_id,
            "confidence": 1.0,
            "confidence_band": "linked",
            "matched_by": "explicit",
            "inference_level": "explicit",
            "reasons": ["rule.cve.explicit"],
            "evidence": evidence,
        }
    meta["cve_links"] = list(links.values())
    conn.execute(
        "UPDATE articles SET meta_json = ?, updated_at = ? WHERE id = ?",
        (json_dumps(meta), utc_now_iso(), article_id),
    )
    conn.commit()


def get_setting(conn: sqlite3.Connection, key: str, default: object) -> object:
    cursor = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    if not row:
        return default
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return default


def set_setting(conn: sqlite3.Connection, key: str, value: object) -> None:
    payload = json_dumps(value)
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, payload, now),
    )
    conn.commit()


def upsert_cve(
    conn: sqlite3.Connection,
    cve_id: str,
    published_at: str | None,
    last_modified_at: str | None,
    preferred_cvss_version: str | None,
    preferred_base_score: float | None,
    preferred_base_severity: str | None,
    preferred_vector: str | None,
    cvss_v40_json: dict[str, object] | None,
    cvss_v31_json: dict[str, object] | None,
    description_text: str | None,
    affected_products: list[str] | None = None,
    affected_cpes: list[str] | None = None,
    reference_domains: list[str] | None = None,
    cvss_v40_list_json: list[dict[str, object]] | None = None,
    cvss_v31_list_json: list[dict[str, object]] | None = None,
) -> None:
    columns = _table_columns(conn, "cves") if _table_exists(conn, "cves") else set()
    has_v40_list = "cvss_v40_list_json" in columns
    has_v31_list = "cvss_v31_list_json" in columns
    extra_cols = []
    extra_vals = []
    extra_updates = []
    if has_v40_list:
        extra_cols.append("cvss_v40_list_json")
        extra_vals.append(json_dumps(cvss_v40_list_json) if cvss_v40_list_json else None)
        extra_updates.append("cvss_v40_list_json=excluded.cvss_v40_list_json")
    if has_v31_list:
        extra_cols.append("cvss_v31_list_json")
        extra_vals.append(json_dumps(cvss_v31_list_json) if cvss_v31_list_json else None)
        extra_updates.append("cvss_v31_list_json=excluded.cvss_v31_list_json")

    conn.execute(
        f"""
        INSERT INTO cves
            (cve_id, published_at, last_modified_at, preferred_cvss_version,
             preferred_base_score, preferred_base_severity, preferred_vector,
             cvss_v40_json, cvss_v31_json, description_text, affected_products_json,
             affected_cpes_json, reference_domains_json, updated_at
             {"," if extra_cols else ""} {", ".join(extra_cols)})
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
             {"," if extra_cols else ""} {", ".join("?" for _ in extra_cols)})
        ON CONFLICT(cve_id) DO UPDATE SET
            published_at=excluded.published_at,
            last_modified_at=excluded.last_modified_at,
            preferred_cvss_version=excluded.preferred_cvss_version,
            preferred_base_score=excluded.preferred_base_score,
            preferred_base_severity=excluded.preferred_base_severity,
            preferred_vector=excluded.preferred_vector,
            cvss_v40_json=excluded.cvss_v40_json,
            cvss_v31_json=excluded.cvss_v31_json,
            {", ".join(extra_updates) + "," if extra_updates else ""}
            description_text=excluded.description_text,
            affected_products_json=excluded.affected_products_json,
            affected_cpes_json=excluded.affected_cpes_json,
            reference_domains_json=excluded.reference_domains_json,
            updated_at=excluded.updated_at
        """,
        (
            cve_id,
            published_at,
            last_modified_at,
            preferred_cvss_version,
            preferred_base_score,
            preferred_base_severity,
            preferred_vector,
            json_dumps(cvss_v40_json) if cvss_v40_json else None,
            json_dumps(cvss_v31_json) if cvss_v31_json else None,
            description_text,
            json_dumps(affected_products) if affected_products else None,
            json_dumps(affected_cpes) if affected_cpes else None,
            json_dumps(reference_domains) if reference_domains else None,
            utc_now_iso(),
            *extra_vals,
        ),
    )
    conn.commit()


def link_cve_products_from_signals(
    conn: sqlite3.Connection,
    *,
    cve_id: str,
    products: list[str],
    cpes: list[str],
    product_versions: list[str] | None = None,
    source: str = "nvd",
) -> dict[str, int]:
    pairs: list[tuple[str, str]] = []
    for cpe in cpes:
        vendor, product = cpe_to_vendor_product(cpe)
        if vendor and product:
            pairs.append((vendor, product))
    if not pairs:
        for product in products:
            if product:
                pairs.append(("unknown", product))
    created = 0
    for vendor_display, product_display in pairs:
        vendor_id = upsert_vendor(conn, vendor_display)
        product_id, _ = upsert_product(conn, vendor_id, product_display)
        link_cve_product(
            conn,
            cve_id,
            product_id,
            source=source,
            evidence={"cpes": cpes[:25]},
        )
        created += 1
    if product_versions:
        for entry in product_versions:
            parts = entry.split(":")
            if len(parts) != 3:
                continue
            vendor_display, product_display, version = parts
            vendor_id = upsert_vendor(conn, vendor_display)
            product_id, _ = upsert_product(conn, vendor_id, product_display)
            _link_cve_product_version(conn, cve_id, product_id, version, source)
    return {"links": created}


def _link_cve_product_version(
    conn: sqlite3.Connection, cve_id: str, product_id: int, version: str, source: str
) -> None:
    if not _table_exists(conn, "cve_product_versions"):
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO cve_product_versions
            (cve_id, product_id, version, source, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (cve_id, product_id, version, source, utc_now_iso()),
    )


def insert_cve_snapshot(
    conn: sqlite3.Connection,
    cve_id: str,
    observed_at: str,
    nvd_last_modified_at: str | None,
    preferred_cvss_version: str | None,
    preferred_base_score: float | None,
    preferred_base_severity: str | None,
    preferred_vector: str | None,
    cvss_v40_json: dict[str, object] | None,
    cvss_v31_json: dict[str, object] | None,
    snapshot_hash: str,
) -> bool:
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO cve_snapshots
            (cve_id, observed_at, nvd_last_modified_at, preferred_cvss_version,
             preferred_base_score, preferred_base_severity, preferred_vector,
             cvss_v40_json, cvss_v31_json, snapshot_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cve_id,
            observed_at,
            nvd_last_modified_at,
            preferred_cvss_version,
            preferred_base_score,
            preferred_base_severity,
            preferred_vector,
            json_dumps(cvss_v40_json) if cvss_v40_json else None,
            json_dumps(cvss_v31_json) if cvss_v31_json else None,
            snapshot_hash,
        ),
    )
    conn.commit()
    return cursor.rowcount == 1


def get_latest_cve_snapshot(conn: sqlite3.Connection, cve_id: str) -> dict[str, object] | None:
    cursor = conn.execute(
        """
        SELECT preferred_cvss_version, preferred_base_score, preferred_base_severity,
               preferred_vector, cvss_v40_json, cvss_v31_json, nvd_last_modified_at
        FROM cve_snapshots
        WHERE cve_id = ?
        ORDER BY observed_at DESC
        LIMIT 1
        """,
        (cve_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    cvss_v40 = json.loads(row[4]) if row[4] else None
    cvss_v31 = json.loads(row[5]) if row[5] else None
    return {
        "preferred_cvss_version": row[0],
        "preferred_base_score": row[1],
        "preferred_base_severity": row[2],
        "preferred_vector": row[3],
        "cvss_v40_json": cvss_v40,
        "cvss_v31_json": cvss_v31,
        "nvd_last_modified_at": row[6],
    }


def insert_cve_change(
    conn: sqlite3.Connection,
    cve_id: str,
    change_at: str,
    cvss_version: str | None,
    change_type: str,
    from_score: float | None,
    to_score: float | None,
    from_severity: str | None,
    to_severity: str | None,
    vector_from: str | None,
    vector_to: str | None,
    metrics_changed_json: dict[str, object] | None,
    note: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO cve_changes
            (cve_id, change_at, cvss_version, change_type, from_score, to_score,
             from_severity, to_severity, vector_from, vector_to, metrics_changed_json, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cve_id,
            change_at,
            cvss_version,
            change_type,
            from_score,
            to_score,
            from_severity,
            to_severity,
            vector_from,
            vector_to,
            json_dumps(metrics_changed_json) if metrics_changed_json else None,
            note,
        ),
    )
    conn.commit()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)
    )
    return cursor.fetchone() is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


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
            json_dumps(notes) if notes else None,
            started_at,
        ),
    )
    conn.commit()


def pause_source(
    conn: sqlite3.Connection, source_id: str, reason: str, pause_minutes: int
) -> None:
    pause_until = utc_now_iso_offset(seconds=pause_minutes * 60)
    conn.execute(
        """
        UPDATE sources
        SET enabled = 0,
            pause_until = ?,
            paused_reason = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (pause_until, reason, utc_now_iso(), source_id),
    )
    conn.commit()


def record_health_alert(conn: sqlite3.Connection, source_id: str, alert_type: str, message: str) -> None:
    conn.execute(
        """
        INSERT INTO health_alerts (source_id, alert_type, message, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (source_id, alert_type, message, utc_now_iso()),
    )
    conn.commit()


def get_source_run_streaks(conn: sqlite3.Connection, source_id: str, limit: int = 20) -> dict[str, int]:
    cursor = conn.execute(
        """
        SELECT status, items_accepted
        FROM source_runs
        WHERE source_id = ?
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (source_id, limit),
    )
    consecutive_errors = 0
    consecutive_zero = 0
    for status, items_accepted in cursor.fetchall():
        if status == "error":
            consecutive_errors += 1
            continue
        break
    cursor = conn.execute(
        """
        SELECT status, items_accepted
        FROM source_runs
        WHERE source_id = ?
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (source_id, limit),
    )
    for status, items_accepted in cursor.fetchall():
        if status == "ok" and int(items_accepted) == 0:
            consecutive_zero += 1
            continue
        break
    return {"consecutive_errors": consecutive_errors, "consecutive_zero": consecutive_zero}


def enqueue_job(
    conn: sqlite3.Connection,
    job_type: str,
    payload: dict[str, object] | None,
    debounce: bool = False,
) -> str:
    if debounce and _has_pending_job(conn, job_type):
        return _get_latest_job_id(conn, job_type)
    job_id = _new_job_id()
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO jobs
            (id, job_type, status, payload_json, result_json, requested_at, started_at,
             finished_at, locked_by, locked_at, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            job_type,
            "queued",
            json_dumps(payload) if payload else None,
            None,
            now,
            None,
            None,
            None,
            None,
            None,
        ),
    )
    conn.commit()
    return job_id


def list_jobs(conn: sqlite3.Connection, limit: int = 50) -> list[Job]:
    cursor = conn.execute(
        """
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error
        FROM jobs
        ORDER BY requested_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [_row_to_job(row) for row in cursor.fetchall()]


def get_schema_version(conn: sqlite3.Connection) -> str | None:
    if not _table_exists(conn, "schema_migrations"):
        return None
    row = conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def count_table(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] or 0)


def get_last_job_by_type(conn: sqlite3.Connection, job_type: str) -> Job | None:
    if not _table_exists(conn, "jobs"):
        return None
    row = conn.execute(
        """
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error
        FROM jobs
        WHERE job_type = ?
        ORDER BY requested_at DESC
        LIMIT 1
        """,
        (job_type,),
    ).fetchone()
    return _row_to_job(row) if row else None


def get_job(conn: sqlite3.Connection, job_id: str) -> Job | None:
    if not _table_exists(conn, "jobs"):
        return None
    row = conn.execute(
        """
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error
        FROM jobs
        WHERE id = ?
        """,
        (job_id,),
    ).fetchone()
    return _row_to_job(row) if row else None


def list_jobs_by_types_since(
    conn: sqlite3.Connection, *, types: list[str], since: str
) -> list[Job]:
    if not _table_exists(conn, "jobs") or not types:
        return []
    placeholders = ",".join("?" for _ in types)
    cursor = conn.execute(
        f"""
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error
        FROM jobs
        WHERE requested_at >= ? AND job_type IN ({placeholders})
        ORDER BY requested_at ASC
        """,
        (since, *types),
    )
    return [_row_to_job(row) for row in cursor.fetchall()]


def insert_llm_run(
    conn: sqlite3.Connection,
    *,
    job_id: str | None,
    provider_id: str | None,
    model_id: str | None,
    prompt_name: str | None,
    input_chars: int | None,
    output_chars: int | None,
    latency_ms: int | None,
    ok: bool,
    error: str | None,
) -> str:
    run_id = f"llm_{uuid.uuid4().hex}"
    conn.execute(
        """
        INSERT INTO llm_runs
            (id, ts, job_id, provider_id, model_id, prompt_name,
             input_chars, output_chars, latency_ms, ok, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            utc_now_iso(),
            job_id,
            provider_id,
            model_id,
            prompt_name,
            input_chars,
            output_chars,
            latency_ms,
            1 if ok else 0,
            error,
        ),
    )
    conn.commit()
    return run_id


def list_llm_runs(conn: sqlite3.Connection, limit: int = 10) -> list[dict[str, object]]:
    if not _table_exists(conn, "llm_runs"):
        return []
    cursor = conn.execute(
        """
        SELECT id, ts, job_id, provider_id, model_id, prompt_name,
               input_chars, output_chars, latency_ms, ok, error
        FROM llm_runs
        ORDER BY ts DESC
        LIMIT ?
        """,
        (limit,),
    )
    items = []
    for row in cursor.fetchall():
        (
            run_id,
            ts,
            job_id,
            provider_id,
            model_id,
            prompt_name,
            input_chars,
            output_chars,
            latency_ms,
            ok,
            error,
        ) = row
        items.append(
            {
                "id": run_id,
                "ts": ts,
                "job_id": job_id,
                "provider_id": provider_id,
                "model_id": model_id,
                "prompt_name": prompt_name,
                "input_chars": input_chars,
                "output_chars": output_chars,
                "latency_ms": latency_ms,
                "ok": bool(ok),
                "error": error,
            }
        )
    return items


def update_job_result(conn: sqlite3.Connection, job_id: str, result: dict[str, object]) -> bool:
    cursor = conn.execute(
        """
        UPDATE jobs
        SET result_json = ?
        WHERE id = ? AND status = 'running'
        """,
        (json_dumps(result), job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def cancel_job(conn: sqlite3.Connection, job_id: str, reason: str = "canceled_by_admin") -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'canceled',
            finished_at = ?,
            error = ?,
            locked_by = NULL,
            locked_at = NULL
        WHERE id = ? AND status IN ('queued', 'running')
        """,
        (now, reason, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def cancel_all_jobs(conn: sqlite3.Connection, reason: str = "canceled_by_admin") -> int:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'canceled',
            finished_at = ?,
            error = ?,
            locked_by = NULL,
            locked_at = NULL
        WHERE status IN ('queued', 'running')
        """,
        (now, reason),
    )
    conn.commit()
    return int(cursor.rowcount or 0)


def is_job_canceled(conn: sqlite3.Connection, job_id: str) -> bool:
    row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return bool(row and row[0] == "canceled")


def has_pending_job(
    conn: sqlite3.Connection, job_type: str, exclude_job_id: str | None = None
) -> bool:
    if exclude_job_id:
        cursor = conn.execute(
            """
            SELECT 1 FROM jobs
            WHERE job_type = ? AND status IN ('queued', 'running') AND id != ?
            LIMIT 1
            """,
            (job_type, exclude_job_id),
        )
    else:
        cursor = conn.execute(
            """
            SELECT 1 FROM jobs
            WHERE job_type = ? AND status IN ('queued', 'running')
            LIMIT 1
            """,
            (job_type,),
        )
    return cursor.fetchone() is not None


def get_source_name(conn: sqlite3.Connection, source_id: str) -> str | None:
    row = conn.execute("SELECT name FROM sources WHERE id = ?", (source_id,)).fetchone()
    return row[0] if row else None


def get_batch_job_counts(conn: sqlite3.Connection, batch_id: str) -> dict[str, int]:
    pattern = f'%\"batch_id\":\"{batch_id}\"%'
    cursor = conn.execute(
        """
        SELECT status, COUNT(*)
        FROM jobs
        WHERE job_type = 'write_article_markdown' AND payload_json LIKE ?
        GROUP BY status
        """,
        (pattern,),
    )
    counts = {"total": 0, "queued": 0, "running": 0, "succeeded": 0, "failed": 0}
    for status, count in cursor.fetchall():
        counts["total"] += count
        counts[status] = count
    return counts


def count_articles_total(conn: sqlite3.Connection, source_id: str) -> int:
    if not _table_exists(conn, "articles"):
        return 0
    cursor = conn.execute("SELECT COUNT(*) FROM articles WHERE source_id = ?", (source_id,))
    return int(cursor.fetchone()[0] or 0)


def insert_source_health_event(
    conn: sqlite3.Connection,
    source_id: str,
    ts: str,
    ok: bool,
    found_count: int,
    accepted_count: int,
    seen_count: int,
    filtered_count: int,
    error_count: int,
    last_error: str | None,
    duration_ms: int | None,
) -> None:
    conn.execute(
        """
        INSERT INTO source_health_history
            (id, source_id, ts, ok, found_count, accepted_count, seen_count,
             filtered_count, error_count, last_error, duration_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            source_id,
            ts,
            1 if ok else 0,
            int(found_count),
            int(accepted_count),
            int(seen_count),
            int(filtered_count),
            int(error_count),
            last_error,
            duration_ms,
        ),
    )
    conn.commit()


def list_source_health_events(
    conn: sqlite3.Connection, source_id: str, limit: int = 50
) -> list[dict[str, object]]:
    cursor = conn.execute(
        """
        SELECT id, source_id, ts, ok, found_count, accepted_count, seen_count,
               filtered_count, error_count, last_error, duration_ms
        FROM source_health_history
        WHERE source_id = ?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (source_id, limit),
    )
    rows = []
    for row in cursor.fetchall():
        (
            event_id,
            source_id,
            ts,
            ok,
            found_count,
            accepted_count,
            seen_count,
            filtered_count,
            error_count,
            last_error,
            duration_ms,
        ) = row
        rows.append(
            {
                "id": event_id,
                "source_id": source_id,
                "ts": ts,
                "ok": bool(ok),
                "found_count": found_count,
                "accepted_count": accepted_count,
                "seen_count": seen_count,
                "filtered_count": filtered_count,
                "error_count": error_count,
                "last_error": last_error,
                "duration_ms": duration_ms,
            }
        )
    return rows


def count_articles_since(conn: sqlite3.Connection, source_id: str, since_iso: str) -> int:
    cursor = conn.execute(
        """
        SELECT COUNT(*)
        FROM articles
        WHERE source_id = ? AND published_at >= ?
        """,
        (source_id, since_iso),
    )
    return int(cursor.fetchone()[0])


def get_last_source_run(conn: sqlite3.Connection, source_id: str) -> dict[str, object] | None:
    cursor = conn.execute(
        """
        SELECT started_at, items_accepted, status, error
        FROM source_runs
        WHERE source_id = ?
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (source_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "started_at": row[0],
        "items_accepted": row[1],
        "status": row[2],
        "error": row[3],
    }


def list_articles_per_day(conn: sqlite3.Connection, since_day: str) -> list[dict[str, object]]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "brief_day" in columns:
        date_expr = "brief_day"
    else:
        published_expr = "substr(published_at, 1, 10)" if "published_at" in columns else "NULL"
        ingested_expr = "substr(ingested_at, 1, 10)" if "ingested_at" in columns else "NULL"
        created_expr = "substr(created_at, 1, 10)" if "created_at" in columns else "NULL"
        date_expr = f"COALESCE({published_expr}, {ingested_expr}, {created_expr})"
    cursor = conn.execute(
        f"""
        SELECT {date_expr} as day, COUNT(*)
        FROM articles
        WHERE {date_expr} >= ?
        GROUP BY day
        ORDER BY day
        """,
        (since_day,),
    )
    return [{"day": row[0], "count": row[1]} for row in cursor.fetchall() if row[0]]


def get_source_stats(
    conn: sqlite3.Connection, days: int, runs: int
) -> list[dict[str, object]]:
    since_day = (datetime.now(tz=timezone.utc) - timedelta(days=days)).date().isoformat()
    article_columns = _table_columns(conn, "articles") if _table_exists(conn, "articles") else set()
    has_full_content_col = "has_full_content" in article_columns
    has_summary_col = "summary_llm" in article_columns
    brief_day_col = "brief_day" in article_columns
    extracted_text_col = "extracted_text_path" in article_columns
    rows = []
    sources = conn.execute(
        "SELECT id, name, enabled, interval_minutes FROM sources ORDER BY name"
    ).fetchall()
    for source_id, name, enabled, interval_minutes in sources:
        total_articles = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE source_id = ?",
            (source_id,),
        ).fetchone()[0]
        if has_full_content_col:
            full_content = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = ? AND has_full_content = 1",
                (source_id,),
            ).fetchone()[0]
        elif extracted_text_col:
            full_content = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = ? AND extracted_text_path IS NOT NULL",
                (source_id,),
            ).fetchone()[0]
        else:
            full_content = 0
        summaries = (
            conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = ? AND summary_llm IS NOT NULL",
                (source_id,),
            ).fetchone()[0]
            if has_summary_col
            else 0
        )
        if brief_day_col:
            recent_articles = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = ? AND brief_day >= ?",
                (source_id, since_day),
            ).fetchone()[0]
        else:
            recent_articles = conn.execute(
                """
                SELECT COUNT(*)
                FROM articles
                WHERE source_id = ? AND COALESCE(substr(published_at, 1, 10), substr(ingested_at, 1, 10)) >= ?
                """,
                (source_id, since_day),
            ).fetchone()[0]
        if _table_exists(conn, "source_health_history"):
            health = conn.execute(
                """
                SELECT COUNT(*), SUM(ok)
                FROM (
                    SELECT ok
                    FROM source_health_history
                    WHERE source_id = ?
                    ORDER BY ts DESC
                    LIMIT ?
                )
                """,
                (source_id, runs),
            ).fetchone()
            run_count = health[0] or 0
            ok_count = health[1] or 0
        else:
            run_count = 0
            ok_count = 0
        last_run_row = None
        if _table_exists(conn, "source_runs"):
            last_run_row = conn.execute(
                "SELECT started_at FROM source_runs WHERE source_id = ? ORDER BY started_at DESC LIMIT 1",
                (source_id,),
            ).fetchone()
        last_run_at = last_run_row[0] if last_run_row else None

        last_ok_row = None
        last_error_row = None
        if _table_exists(conn, "source_health_history"):
            last_ok_row = conn.execute(
                """
                SELECT ts
                FROM source_health_history
                WHERE source_id = ? AND ok = 1
                ORDER BY ts DESC
                LIMIT 1
                """,
                (source_id,),
            ).fetchone()
            last_error_row = conn.execute(
                """
                SELECT last_error
                FROM source_health_history
                WHERE source_id = ? AND ok = 0 AND last_error IS NOT NULL
                ORDER BY ts DESC
                LIMIT 1
                """,
                (source_id,),
            ).fetchone()
        last_ok_at = last_ok_row[0] if last_ok_row else None
        last_error = last_error_row[0] if last_error_row else None

        rows.append(
            {
                "source_id": source_id,
                "source_name": name,
                "enabled": bool(enabled),
                "interval_minutes": interval_minutes,
                "last_run_at": last_run_at,
                "articles_per_day_avg": round(recent_articles / max(days, 1), 2),
                "last_ok_at": last_ok_at,
                "last_error": last_error,
                "ok_rate": round((ok_count / run_count) * 100, 1) if run_count else 0.0,
                "total_articles": total_articles,
                "pct_full_content": round((full_content / total_articles) * 100, 1)
                if total_articles
                else 0.0,
                "pct_summaries": round((summaries / total_articles) * 100, 1)
                if total_articles
                else 0.0,
            }
        )
    return rows


def claim_next_job(
    conn: sqlite3.Connection,
    worker_id: str,
    allowed_types: list[str] | None = None,
    lock_timeout_seconds: int | None = None,
) -> Job | None:
    conn.execute("BEGIN IMMEDIATE")
    if lock_timeout_seconds is not None:
        cutoff = utc_now_iso_offset(seconds=-lock_timeout_seconds)
        conn.execute(
            """
            UPDATE jobs
            SET status = 'queued',
                locked_by = NULL,
                locked_at = NULL,
                started_at = NULL,
                error = 'stale_lock_requeued'
            WHERE status = 'running' AND locked_at IS NOT NULL AND locked_at < ?
            """,
            (cutoff,),
        )
    for _ in range(20):
        if allowed_types:
            placeholders = ",".join("?" for _ in allowed_types)
            cursor = conn.execute(
                f"""
                SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
                       finished_at, locked_by, locked_at, error
                FROM jobs
                WHERE status = 'queued' AND locked_by IS NULL AND job_type IN ({placeholders})
                ORDER BY requested_at ASC
                LIMIT 1
                """,
                tuple(allowed_types),
            )
        else:
            cursor = conn.execute(
                """
                SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
                       finished_at, locked_by, locked_at, error
                FROM jobs
                WHERE status = 'queued' AND locked_by IS NULL
                ORDER BY requested_at ASC
                LIMIT 1
                """
            )
        row = cursor.fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        payload_json = row[3]
        try:
            payload = json.loads(payload_json) if payload_json else {}
        except json.JSONDecodeError:
            payload = {}
        not_before = payload.get("not_before")
        if not_before and isinstance(not_before, str) and not_before > utc_now_iso():
            conn.execute(
                """
                UPDATE jobs
                SET requested_at = ?
                WHERE id = ?
                """,
                (not_before, row[0]),
            )
            continue
        break
    job_id = row[0]
    now = utc_now_iso()
    result = conn.execute(
        """
        UPDATE jobs
        SET status = 'running', started_at = ?, locked_by = ?, locked_at = ?
        WHERE id = ? AND status = 'queued' AND locked_by IS NULL
        """,
        (now, worker_id, now, job_id),
    )
    if result.rowcount == 0:
        conn.execute("COMMIT")
        return None
    conn.execute("COMMIT")
    job = _row_to_job(row)
    return Job(
        id=job.id,
        job_type=job.job_type,
        status="running",
        payload=job.payload,
        result=job.result,
        requested_at=job.requested_at,
        started_at=now,
        finished_at=job.finished_at,
        locked_by=worker_id,
        locked_at=now,
        error=job.error,
    )



def complete_job(
    conn: sqlite3.Connection, job_id: str, result: dict[str, object] | None = None
) -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'succeeded', finished_at = ?, error = NULL, result_json = ?
        WHERE id = ? AND status = 'running'
        """,
        (now, json_dumps(result) if result else None, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def fail_job(conn: sqlite3.Connection, job_id: str, error: str) -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'failed', finished_at = ?, error = ?
        WHERE id = ? AND status = 'running'
        """,
        (now, error, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def requeue_job(
    conn: sqlite3.Connection,
    job_id: str,
    payload: dict[str, object],
    requested_at: str,
) -> bool:
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'queued',
            requested_at = ?,
            payload_json = ?,
            result_json = NULL,
            started_at = NULL,
            finished_at = NULL,
            locked_by = NULL,
            locked_at = NULL,
            error = NULL
        WHERE id = ? AND status = 'running'
        """,
        (requested_at, json_dumps(payload), job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


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


def _row_to_job(row: tuple) -> Job:
    (
        job_id,
        job_type,
        status,
        payload_json,
        result_json,
        requested_at,
        started_at,
        finished_at,
        locked_by,
        locked_at,
        error,
    ) = row
    try:
        payload = json.loads(payload_json) if payload_json else {}
    except json.JSONDecodeError:
        payload = {}
    try:
        result = json.loads(result_json) if result_json else None
    except json.JSONDecodeError:
        result = None
    return Job(
        id=job_id,
        job_type=job_type,
        status=status,
        payload=payload,
        result=result,
        requested_at=requested_at,
        started_at=started_at,
        finished_at=finished_at,
        locked_by=locked_by,
        locked_at=locked_at,
        error=error,
    )


def _has_pending_job(conn: sqlite3.Connection, job_type: str) -> bool:
    cursor = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE job_type = ? AND status IN ('queued', 'running')
        LIMIT 1
        """,
        (job_type,),
    )
    return cursor.fetchone() is not None


def _get_latest_job_id(conn: sqlite3.Connection, job_type: str) -> str:
    cursor = conn.execute(
        """
        SELECT id FROM jobs
        WHERE job_type = ?
        ORDER BY requested_at DESC
        LIMIT 1
        """,
        (job_type,),
    )
    row = cursor.fetchone()
    return row[0] if row else _new_job_id()


def _new_job_id() -> str:
    return f"job_{uuid.uuid4().hex}"


def _get_article_id(conn: sqlite3.Connection, source_id: str, stable_id: str) -> int | None:
    cursor = conn.execute(
        "SELECT id FROM articles WHERE source_id = ? AND stable_id = ?",
        (source_id, stable_id),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _brief_day_from(value: str) -> str:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value).date().isoformat()
    except ValueError:
        return utc_now_iso().split("T")[0]


def get_article_by_id(conn: sqlite3.Connection, article_id: int) -> dict[str, object] | None:
    if not _table_exists(conn, "articles"):
        return None
    columns = _table_columns(conn, "articles")
    wanted = [
        "id",
        "source_id",
        "original_url",
        "normalized_url",
        "title",
        "published_at",
        "ingested_at",
        "summary",
        "content_text",
        "content_html",
        "content_fetched_at",
        "content_error",
        "summary_llm",
        "summary_model",
        "summary_generated_at",
        "summary_error",
        "brief_day",
        "has_full_content",
        "extracted_text_path",
        "raw_html_path",
        "meta_json",
        "created_at",
        "updated_at",
    ]
    selected = [name for name in wanted if name in columns]
    if "id" not in selected:
        return None
    cursor = conn.execute(
        f"SELECT {', '.join(selected)} FROM articles WHERE id = ?",
        (article_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    article = dict(zip(selected, row))
    content_text = article.get("content_text")
    extracted_path = article.get("extracted_text_path")
    if not content_text and extracted_path:
        content_text = _load_text_file(extracted_path)
    content_html = article.get("content_html")
    html_excerpt = None
    if content_html:
        html_excerpt = content_html[:2000]
    has_full_content = bool(content_text) or bool(extracted_path)
    return {
        "id": article.get("id"),
        "source_id": article.get("source_id"),
        "original_url": article.get("original_url"),
        "normalized_url": article.get("normalized_url"),
        "title": article.get("title"),
        "published_at": article.get("published_at"),
        "ingested_at": article.get("ingested_at"),
        "summary": article.get("summary"),
        "content_text": content_text,
        "content_html_excerpt": html_excerpt,
        "content_fetched_at": article.get("content_fetched_at"),
        "content_error": article.get("content_error"),
        "summary_llm": article.get("summary_llm"),
        "summary_model": article.get("summary_model"),
        "summary_generated_at": article.get("summary_generated_at"),
        "summary_error": article.get("summary_error"),
        "brief_day": article.get("brief_day"),
        "has_full_content": has_full_content,
        "meta_json": article.get("meta_json"),
        "created_at": article.get("created_at"),
        "updated_at": article.get("updated_at"),
    }


def _load_text_file(path: str, limit: int = 250_000) -> str | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = handle.read(limit + 1)
            return data[:limit]
    except OSError:
        return None


def list_article_tags(conn: sqlite3.Connection) -> list[dict[str, object]]:
    if not _table_exists(conn, "article_tags") or not _table_exists(conn, "articles"):
        return []
    cursor = conn.execute(
        """
        SELECT t.tag, COUNT(*)
        FROM article_tags t
        JOIN articles a ON a.id = t.article_id
        GROUP BY t.tag
        ORDER BY COUNT(*) DESC, t.tag ASC
        """
    )
    return [{"tag": row[0], "count": row[1]} for row in cursor.fetchall()]


def upsert_vendor(conn: sqlite3.Connection, vendor_display: str) -> int:
    vendor_norm = normalize_name(vendor_display)
    if not vendor_norm:
        vendor_norm = "unknown"
    display = vendor_display.strip() or vendor_norm.replace("_", " ").title()
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO vendors (name_norm, display_name, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(name_norm) DO UPDATE SET display_name = excluded.display_name
        """,
        (vendor_norm, display, now),
    )
    row = conn.execute(
        "SELECT id FROM vendors WHERE name_norm = ?",
        (vendor_norm,),
    ).fetchone()
    conn.commit()
    return int(row[0])


def upsert_product(
    conn: sqlite3.Connection, vendor_id: int, product_display: str
) -> tuple[int, str]:
    product_norm = normalize_name(product_display)
    if not product_norm:
        product_norm = "unknown"
    vendor_row = conn.execute(
        "SELECT name_norm FROM vendors WHERE id = ?",
        (vendor_id,),
    ).fetchone()
    vendor_norm = vendor_row[0] if vendor_row else "unknown"
    product_key = f"{vendor_norm}:{product_norm}"
    display = product_display.strip() or product_norm.replace("_", " ").title()
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO products (vendor_id, name_norm, display_name, product_key, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(vendor_id, name_norm) DO UPDATE SET display_name = excluded.display_name
        """,
        (vendor_id, product_norm, display, product_key, now),
    )
    row = conn.execute(
        "SELECT id, product_key FROM products WHERE vendor_id = ? AND name_norm = ?",
        (vendor_id, product_norm),
    ).fetchone()
    conn.commit()
    return int(row[0]), str(row[1])


def link_cve_product(
    conn: sqlite3.Connection,
    cve_id: str,
    product_id: int,
    source: str = "nvd",
    evidence: dict[str, object] | None = None,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT OR IGNORE INTO cve_products (cve_id, product_id, source, evidence_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (cve_id, product_id, source, json_dumps(evidence) if evidence else None, now),
    )
    conn.commit()


def backfill_products_from_cves(
    conn: sqlite3.Connection, limit: int | None = None
) -> dict[str, object]:
    stats = {
        "cves_processed": 0,
        "vendors_created": 0,
        "products_created": 0,
        "links_created": 0,
    }
    if not _table_exists(conn, "cves"):
        return stats
    cursor = conn.execute(
        "SELECT cve_id, affected_products_json, affected_cpes_json FROM cves"
        + (" LIMIT ?" if limit else ""),
        (limit,) if limit else (),
    )
    for cve_id, products_json, cpes_json in cursor.fetchall():
        stats["cves_processed"] += 1
        cpes = json.loads(cpes_json) if cpes_json else []
        products = json.loads(products_json) if products_json else []
        pairs: list[tuple[str, str]] = []
        for cpe in cpes:
            vendor, product = cpe_to_vendor_product(cpe)
            if vendor and product:
                pairs.append((vendor, product))
        if not pairs:
            for product in products:
                if product:
                    pairs.append(("unknown", product))
        for vendor_display, product_display in pairs:
            vendor_id = upsert_vendor(conn, vendor_display)
            product_id, _ = upsert_product(conn, vendor_id, product_display)
            link_cve_product(
                conn,
                cve_id,
                product_id,
                evidence={"cpes": cpes[:25]},
            )
            stats["links_created"] += 1
    return stats


def query_products(
    conn: sqlite3.Connection,
    query: str | None,
    vendor: str | None,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "products"):
        return [], 0
    where: list[str] = []
    params: list[object] = []
    if query:
        like = f"%{query.lower()}%"
        where.append("(LOWER(p.display_name) LIKE ? OR LOWER(p.name_norm) LIKE ?)")
        params.extend([like, like])
    if vendor:
        like = f"%{vendor.lower()}%"
        where.append("(LOWER(v.display_name) LIKE ? OR LOWER(v.name_norm) LIKE ?)")
        params.extend([like, like])
    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql

    count_cursor = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM products p
        JOIN vendors v ON v.id = p.vendor_id
        {where_sql}
        """,
        params,
    )
    total = count_cursor.fetchone()[0]

    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        f"""
        SELECT p.id, p.product_key, p.display_name, v.display_name
        FROM products p
        JOIN vendors v ON v.id = p.vendor_id
        {where_sql}
        ORDER BY v.display_name, p.display_name
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    )
    items = [
        {
            "product_id": row[0],
            "product_key": row[1],
            "product_name": row[2],
            "vendor_name": row[3],
        }
        for row in cursor.fetchall()
    ]
    return items, total


def get_product(conn: sqlite3.Connection, product_key: str) -> dict[str, object] | None:
    if not _table_exists(conn, "products"):
        return None
    row = conn.execute(
        """
        SELECT p.id, p.product_key, p.display_name, v.display_name, v.name_norm
        FROM products p
        JOIN vendors v ON v.id = p.vendor_id
        WHERE p.product_key = ?
        """,
        (product_key,),
    ).fetchone()
    if not row:
        return None
    return {
        "product_id": row[0],
        "product_key": row[1],
        "product_name": row[2],
        "vendor_name": row[3],
        "vendor_norm": row[4],
    }


def get_product_cves(
    conn: sqlite3.Connection,
    product_id: int,
    severity_min: float | None,
    severities: list[str] | None,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "cve_products") or not _table_exists(conn, "cves"):
        return [], 0
    where: list[str] = ["cp.product_id = ?"]
    params: list[object] = [product_id]
    if severity_min is not None:
        where.append("c.preferred_base_score >= ?")
        params.append(severity_min)
    if severities:
        normalized = [value.upper() for value in severities]
        placeholders = ",".join("?" for _ in normalized)
        where.append(f"c.preferred_base_severity IN ({placeholders})")
        params.extend(normalized)
    where_sql = " AND ".join(where)
    count_cursor = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM cve_products cp
        JOIN cves c ON c.cve_id = cp.cve_id
        WHERE {where_sql}
        """,
        params,
    )
    total = count_cursor.fetchone()[0]
    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        f"""
        SELECT c.cve_id, c.published_at, c.last_modified_at, c.preferred_base_score,
               c.preferred_base_severity, c.description_text
        FROM cve_products cp
        JOIN cves c ON c.cve_id = cp.cve_id
        WHERE {where_sql}
        ORDER BY c.last_modified_at DESC
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    )
    items = [
        {
            "cve_id": row[0],
            "published_at": row[1],
            "last_modified_at": row[2],
            "preferred_base_score": row[3],
            "preferred_base_severity": row[4],
            "summary": (row[5] or "")[:240],
        }
        for row in cursor.fetchall()
    ]
    return items, total


def get_product_facets(conn: sqlite3.Connection, product_id: int) -> dict[str, int]:
    if not _table_exists(conn, "cve_products") or not _table_exists(conn, "cves"):
        return {}
    cursor = conn.execute(
        """
        SELECT COALESCE(c.preferred_base_severity, 'UNKNOWN') as severity, COUNT(*)
        FROM cve_products cp
        JOIN cves c ON c.cve_id = cp.cve_id
        WHERE cp.product_id = ?
        GROUP BY severity
        """,
        (product_id,),
    )
    return {row[0]: int(row[1]) for row in cursor.fetchall()}


def list_product_keys_for_cve(conn: sqlite3.Connection, cve_id: str) -> list[str]:
    if not _table_exists(conn, "cve_products") or not _table_exists(conn, "products"):
        return []
    cursor = conn.execute(
        """
        SELECT p.product_key
        FROM cve_products cp
        JOIN products p ON p.id = cp.product_id
        WHERE cp.cve_id = ?
        ORDER BY p.product_key
        """,
        (cve_id,),
    )
    return [row[0] for row in cursor.fetchall()]


def get_product_display_by_key(conn: sqlite3.Connection, product_key: str) -> dict[str, str] | None:
    if not _table_exists(conn, "products") or not _table_exists(conn, "vendors"):
        return None
    row = conn.execute(
        """
        SELECT p.display_name, v.display_name
        FROM products p
        JOIN vendors v ON v.id = p.vendor_id
        WHERE p.product_key = ?
        """,
        (product_key,),
    ).fetchone()
    if not row:
        return None
    return {"product": row[0], "vendor": row[1]}


def create_event(
    conn: sqlite3.Connection,
    kind: str,
    title: str,
    severity: str | None,
    first_seen_at: str,
    last_seen_at: str,
    summary: str | None = None,
    meta: dict[str, object] | None = None,
) -> str:
    event_id = f"evt_{uuid.uuid4().hex[:12]}"
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO events
            (id, kind, title, summary, severity, created_at, updated_at,
             first_seen_at, last_seen_at, status, meta_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            kind,
            title,
            summary,
            severity,
            now,
            now,
            first_seen_at,
            last_seen_at,
            "open",
            json_dumps(meta) if meta else None,
        ),
    )
    conn.commit()
    return event_id


def upsert_event_item(
    conn: sqlite3.Connection, event_id: str, item_type: str, item_key: str
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO event_items (event_id, item_type, item_key, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (event_id, item_type, item_key, utc_now_iso()),
    )
    conn.commit()


def touch_event(conn: sqlite3.Connection, event_id: str, seen_at: str) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE events
        SET last_seen_at = CASE WHEN last_seen_at > ? THEN last_seen_at ELSE ? END,
            updated_at = ?
        WHERE id = ?
        """,
        (seen_at, seen_at, now, event_id),
    )
    conn.commit()


def _severity_rank(severity: str | None) -> int:
    order = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}
    if not severity:
        return -1
    return order.get(severity.upper(), 0)


def _event_title_for(conn: sqlite3.Connection, event_id: str) -> str | None:
    cursor = conn.execute(
        """
        SELECT p.product_key
        FROM event_items ei
        JOIN products p ON p.product_key = ei.item_key
        WHERE ei.event_id = ? AND ei.item_type = 'product'
        ORDER BY p.product_key
        LIMIT 1
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if row:
        display = get_product_display_by_key(conn, row[0])
        if display:
            return f"CVE activity: {display['vendor']} {display['product']}"
    cursor = conn.execute(
        """
        SELECT item_key
        FROM event_items
        WHERE event_id = ? AND item_type = 'cve'
        ORDER BY item_key
        LIMIT 1
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if row:
        return f"CVE activity: {row[0]}"
    return None


def update_event_rollups(conn: sqlite3.Connection, event_id: str) -> None:
    if not _table_exists(conn, "events"):
        return
    cursor = conn.execute(
        """
        SELECT c.preferred_base_severity
        FROM event_items ei
        JOIN cves c ON c.cve_id = ei.item_key
        WHERE ei.event_id = ? AND ei.item_type = 'cve'
        """,
        (event_id,),
    )
    severities = [row[0] for row in cursor.fetchall()]
    best = None
    best_rank = -1
    for severity in severities:
        rank = _severity_rank(severity)
        if rank > best_rank:
            best_rank = rank
            best = severity
    title_prefix = _event_title_for(conn, event_id)
    count_cursor = conn.execute(
        "SELECT COUNT(*) FROM event_items WHERE event_id = ? AND item_type = 'cve'",
        (event_id,),
    )
    cve_count = int(count_cursor.fetchone()[0])
    if title_prefix:
        title = f"{title_prefix} ({cve_count} CVEs)"
    else:
        title = f"CVE activity ({cve_count} CVEs)"
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE events
        SET severity = ?, title = ?, updated_at = ?
        WHERE id = ?
        """,
        (best or "UNKNOWN", title, now, event_id),
    )
    conn.commit()


def _find_event_for_cve(conn: sqlite3.Connection, cve_id: str) -> str | None:
    if not _table_exists(conn, "event_items"):
        return None
    row = conn.execute(
        """
        SELECT event_id
        FROM event_items
        WHERE item_type = 'cve' AND item_key = ?
        LIMIT 1
        """,
        (cve_id,),
    ).fetchone()
    return row[0] if row else None


def find_merge_candidate_event(
    conn: sqlite3.Connection,
    product_keys: list[str],
    window_days: int,
    min_shared_products: int,
) -> str | None:
    if not product_keys or not _table_exists(conn, "event_items"):
        return None
    placeholders = ",".join("?" for _ in product_keys)
    cutoff = utc_now_iso_offset(seconds=-(window_days * 86400))
    cursor = conn.execute(
        f"""
        SELECT e.id, COUNT(*) as matches, e.last_seen_at
        FROM events e
        JOIN event_items ei ON ei.event_id = e.id
        WHERE e.status = 'open'
          AND e.kind = 'cve_cluster'
          AND e.last_seen_at >= ?
          AND ei.item_type = 'product'
          AND ei.item_key IN ({placeholders})
        GROUP BY e.id
        HAVING COUNT(*) >= ?
        ORDER BY matches DESC, e.last_seen_at DESC
        LIMIT 1
        """,
        [cutoff, *product_keys, min_shared_products],
    )
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_event_for_cve(
    conn: sqlite3.Connection,
    cve_id: str,
    published_at: str | None,
    window_days: int,
    min_shared_products: int,
) -> tuple[str, str]:
    if not _table_exists(conn, "events") or not _table_exists(conn, "event_items"):
        raise ValueError("events tables not initialized")
    event_id = _find_event_for_cve(conn, cve_id)
    product_keys = list_product_keys_for_cve(conn, cve_id)
    now = utc_now_iso()
    if event_id:
        upsert_event_item(conn, event_id, "cve", cve_id)
        for product_key in product_keys:
            upsert_event_item(conn, event_id, "product", product_key)
        touch_event(conn, event_id, published_at or now)
        update_event_rollups(conn, event_id)
        return event_id, "existing"
    candidate = find_merge_candidate_event(conn, product_keys, window_days, min_shared_products)
    if candidate:
        event_id = candidate
        upsert_event_item(conn, event_id, "cve", cve_id)
        for product_key in product_keys:
            upsert_event_item(conn, event_id, "product", product_key)
        touch_event(conn, event_id, published_at or now)
        update_event_rollups(conn, event_id)
        return event_id, "merged"
    first_seen = published_at or now
    title = f"CVE activity ({1} CVEs)"
    event_id = create_event(
        conn,
        kind="cve_cluster",
        title=title,
        severity="UNKNOWN",
        first_seen_at=first_seen,
        last_seen_at=now,
        meta={"seed_cve": cve_id},
    )
    upsert_event_item(conn, event_id, "cve", cve_id)
    for product_key in product_keys:
        upsert_event_item(conn, event_id, "product", product_key)
    update_event_rollups(conn, event_id)
    return event_id, "created"


def link_article_to_events(
    conn: sqlite3.Connection,
    article_id: int,
    cve_ids: list[str],
    published_at: str | None,
) -> int:
    if not cve_ids:
        return 0
    attached = 0
    now = utc_now_iso()
    for cve_id in cve_ids:
        event_id = _find_event_for_cve(conn, cve_id)
        if not event_id:
            continue
        upsert_event_item(conn, event_id, "article", str(article_id))
        touch_event(conn, event_id, published_at or now)
        attached += 1
    return attached


def list_events(
    conn: sqlite3.Connection,
    status: str | None,
    kind: str | None,
    severity: str | None,
    query: str | None,
    after: str | None,
    before: str | None,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "events"):
        return [], 0
    where: list[str] = []
    params: list[object] = []
    if status:
        where.append("status = ?")
        params.append(status)
    if kind:
        where.append("kind = ?")
        params.append(kind)
    if severity:
        where.append("severity = ?")
        params.append(severity)
    if query:
        like = f"%{query.lower()}%"
        where.append("(LOWER(title) LIKE ? OR LOWER(summary) LIKE ?)")
        params.extend([like, like])
    if after:
        where.append("last_seen_at >= ?")
        params.append(after)
    if before:
        where.append("last_seen_at <= ?")
        params.append(before)
    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql
    count_cursor = conn.execute(
        f"SELECT COUNT(*) FROM events {where_sql}",
        params,
    )
    total = count_cursor.fetchone()[0]
    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        f"""
        SELECT id, kind, title, summary, severity, created_at, updated_at,
               first_seen_at, last_seen_at, status
        FROM events
        {where_sql}
        ORDER BY last_seen_at DESC
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    )
    items = [
        {
            "id": row[0],
            "kind": row[1],
            "title": row[2],
            "summary": row[3],
            "severity": row[4],
            "created_at": row[5],
            "updated_at": row[6],
            "first_seen_at": row[7],
            "last_seen_at": row[8],
            "status": row[9],
        }
        for row in cursor.fetchall()
    ]
    return items, total


def get_event(conn: sqlite3.Connection, event_id: str) -> dict[str, object] | None:
    if not _table_exists(conn, "events"):
        return None
    row = conn.execute(
        """
        SELECT id, kind, title, summary, severity, created_at, updated_at,
               first_seen_at, last_seen_at, status, meta_json
        FROM events
        WHERE id = ?
        """,
        (event_id,),
    ).fetchone()
    if not row:
        return None
    meta = json.loads(row[10]) if row[10] else {}
    event = {
        "id": row[0],
        "kind": row[1],
        "title": row[2],
        "summary": row[3],
        "severity": row[4],
        "created_at": row[5],
        "updated_at": row[6],
        "first_seen_at": row[7],
        "last_seen_at": row[8],
        "status": row[9],
        "meta": meta,
    }
    cves_cursor = conn.execute(
        """
        SELECT c.cve_id, c.published_at, c.preferred_base_score,
               c.preferred_base_severity, c.description_text
        FROM event_items ei
        JOIN cves c ON c.cve_id = ei.item_key
        WHERE ei.event_id = ? AND ei.item_type = 'cve'
        ORDER BY c.last_modified_at DESC
        """,
        (event_id,),
    )
    cves = [
        {
            "cve_id": row[0],
            "published_at": row[1],
            "preferred_base_score": row[2],
            "preferred_base_severity": row[3],
            "summary": (row[4] or "")[:240],
        }
        for row in cves_cursor.fetchall()
    ]
    products_cursor = conn.execute(
        """
        SELECT p.product_key, p.display_name, v.display_name
        FROM event_items ei
        JOIN products p ON p.product_key = ei.item_key
        JOIN vendors v ON v.id = p.vendor_id
        WHERE ei.event_id = ? AND ei.item_type = 'product'
        ORDER BY v.display_name, p.display_name
        """,
        (event_id,),
    )
    products = [
        {
            "product_key": row[0],
            "product_name": row[1],
            "vendor_name": row[2],
        }
        for row in products_cursor.fetchall()
    ]
    articles = []
    if _table_exists(conn, "articles"):
        article_cursor = conn.execute(
            """
            SELECT a.id, a.title, a.published_at, a.original_url
            FROM event_items ei
            JOIN articles a ON a.id = CAST(ei.item_key AS INTEGER)
            WHERE ei.event_id = ? AND ei.item_type = 'article'
            ORDER BY a.published_at DESC
            """,
            (event_id,),
        )
        articles = [
            {
                "article_id": row[0],
                "title": row[1],
                "published_at": row[2],
                "url": row[3],
            }
            for row in article_cursor.fetchall()
        ]
    event["items"] = {"cves": cves, "products": products, "articles": articles}
    return event


def list_events_for_product(
    conn: sqlite3.Connection,
    product_key: str,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "event_items"):
        return [], 0
    count_cursor = conn.execute(
        """
        SELECT COUNT(DISTINCT e.id)
        FROM event_items ei
        JOIN events e ON e.id = ei.event_id
        WHERE ei.item_type = 'product' AND ei.item_key = ?
        """,
        (product_key,),
    )
    total = count_cursor.fetchone()[0]
    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        """
        SELECT e.id, e.kind, e.title, e.severity, e.last_seen_at, e.status
        FROM event_items ei
        JOIN events e ON e.id = ei.event_id
        WHERE ei.item_type = 'product' AND ei.item_key = ?
        ORDER BY e.last_seen_at DESC
        LIMIT ? OFFSET ?
        """,
        (product_key, page_size, offset),
    )
    items = [
        {
            "id": row[0],
            "kind": row[1],
            "title": row[2],
            "severity": row[3],
            "last_seen_at": row[4],
            "status": row[5],
        }
        for row in cursor.fetchall()
    ]
    return items, total


def rebuild_events_from_cves(
    conn: sqlite3.Connection,
    window_days: int,
    min_shared_products: int,
    limit: int | None = None,
) -> dict[str, object]:
    stats = {
        "events_created": 0,
        "events_merged": 0,
        "events_existing": 0,
        "cves_processed": 0,
        "articles_linked": 0,
    }
    if _table_exists(conn, "event_items"):
        conn.execute("DELETE FROM event_items")
    if _table_exists(conn, "event_signals"):
        conn.execute("DELETE FROM event_signals")
    if _table_exists(conn, "events"):
        conn.execute("DELETE FROM events")
    conn.commit()
    if not _table_exists(conn, "cves"):
        return stats
    cursor = conn.execute(
        "SELECT cve_id, published_at FROM cves ORDER BY published_at"
        + (" LIMIT ?" if limit else ""),
        (limit,) if limit else (),
    )
    for cve_id, published_at in cursor.fetchall():
        stats["cves_processed"] += 1
        event_id, action = upsert_event_for_cve(
            conn,
            cve_id,
            published_at,
            window_days,
            min_shared_products,
        )
        if action == "created":
            stats["events_created"] += 1
        elif action == "merged":
            stats["events_merged"] += 1
        else:
            stats["events_existing"] += 1
    if _table_exists(conn, "article_cves") and _table_exists(conn, "articles"):
        article_cursor = conn.execute(
            """
            SELECT ac.article_id, ac.cve_id, a.published_at, a.ingested_at
            FROM article_cves ac
            JOIN articles a ON a.id = ac.article_id
            """
        )
        for article_id, cve_id, published_at, ingested_at in article_cursor.fetchall():
            linked = link_article_to_events(
                conn,
                int(article_id),
                [str(cve_id)],
                published_at or ingested_at,
            )
            stats["articles_linked"] += linked
    return stats


def delete_all_articles(conn: sqlite3.Connection, *, delete_files: bool = False) -> dict[str, object]:
    stats: dict[str, object] = {"tables": {}, "files_deleted": 0, "file_errors": []}
    file_paths: list[str] = []
    if delete_files and _table_exists(conn, "articles"):
        columns = _table_columns(conn, "articles")
        path_cols = [col for col in ("extracted_text_path", "raw_html_path") if col in columns]
        if path_cols:
            cursor = conn.execute(
                f"SELECT {', '.join(path_cols)} FROM articles WHERE " +
                " OR ".join(f"{col} IS NOT NULL" for col in path_cols)
            )
            for row in cursor.fetchall():
                for value in row:
                    if isinstance(value, str) and value:
                        file_paths.append(value)

    conn.execute("BEGIN IMMEDIATE")
    if _table_exists(conn, "article_tags"):
        cursor = conn.execute("DELETE FROM article_tags")
        stats["tables"]["article_tags"] = cursor.rowcount
    if _table_exists(conn, "article_cves"):
        cursor = conn.execute("DELETE FROM article_cves")
        stats["tables"]["article_cves"] = cursor.rowcount
    if _table_exists(conn, "articles"):
        cursor = conn.execute("DELETE FROM articles")
        stats["tables"]["articles"] = cursor.rowcount
    conn.execute("COMMIT")

    if delete_files:
        _delete_content_files(conn, file_paths, stats)
    return stats


def delete_all_cves(conn: sqlite3.Connection) -> dict[str, object]:
    stats: dict[str, object] = {"tables": {}}
    conn.execute("BEGIN IMMEDIATE")
    if _table_exists(conn, "article_cves"):
        cursor = conn.execute("DELETE FROM article_cves")
        stats["tables"]["article_cves"] = cursor.rowcount
    if _table_exists(conn, "cve_products"):
        cursor = conn.execute("DELETE FROM cve_products")
        stats["tables"]["cve_products"] = cursor.rowcount
    if _table_exists(conn, "cve_changes"):
        cursor = conn.execute("DELETE FROM cve_changes")
        stats["tables"]["cve_changes"] = cursor.rowcount
    if _table_exists(conn, "cve_snapshots"):
        cursor = conn.execute("DELETE FROM cve_snapshots")
        stats["tables"]["cve_snapshots"] = cursor.rowcount
    if _table_exists(conn, "cves"):
        cursor = conn.execute("DELETE FROM cves")
        stats["tables"]["cves"] = cursor.rowcount
    conn.execute("COMMIT")
    return stats


def delete_all_events(conn: sqlite3.Connection) -> dict[str, object]:
    stats: dict[str, object] = {"tables": {}}
    conn.execute("BEGIN IMMEDIATE")
    if _table_exists(conn, "event_signals"):
        cursor = conn.execute("DELETE FROM event_signals")
        stats["tables"]["event_signals"] = cursor.rowcount
    if _table_exists(conn, "event_items"):
        cursor = conn.execute("DELETE FROM event_items")
        stats["tables"]["event_items"] = cursor.rowcount
    if _table_exists(conn, "events"):
        cursor = conn.execute("DELETE FROM events")
        stats["tables"]["events"] = cursor.rowcount
    conn.execute("COMMIT")
    return stats


def delete_all_content(conn: sqlite3.Connection, *, delete_files: bool = False) -> dict[str, object]:
    articles = delete_all_articles(conn, delete_files=delete_files)
    cves = delete_all_cves(conn)
    events = delete_all_events(conn)
    return {"articles": articles, "cves": cves, "events": events}


def _delete_content_files(
    conn: sqlite3.Connection, file_paths: list[str], stats: dict[str, object]
) -> None:
    config = get_setting(conn, "config.runtime", {}) or {}
    data_dir = ((config.get("paths") or {}).get("data_dir") or "").strip()
    if not data_dir:
        stats["file_errors"].append("missing data_dir in config.runtime")
        return
    allowed_root = os.path.realpath(data_dir)
    deleted = 0
    for path in file_paths:
        try:
            real_path = os.path.realpath(path)
            if not real_path.startswith(allowed_root + os.sep):
                stats["file_errors"].append(f"skip_outside_root:{path}")
                continue
            os.remove(real_path)
            deleted += 1
        except FileNotFoundError:
            continue
        except OSError as exc:
            stats["file_errors"].append(f"{path}:{exc}")
    stats["files_deleted"] = deleted


def search_articles(
    conn: sqlite3.Connection,
    query: str | None,
    source_id: str | None,
    has_summary: bool | None,
    after: str | None,
    before: str | None,
    tags: list[str] | None,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "articles"):
        return [], 0
    columns = _table_columns(conn, "articles")
    where: list[str] = []
    params: list[object] = []
    if query:
        like = f"%{query}%"
        parts = ["a.title LIKE ?"]
        params.append(like)
        if "content_text" in columns:
            parts.append("a.content_text LIKE ?")
            params.append(like)
        if "summary_llm" in columns:
            parts.append("a.summary_llm LIKE ?")
            params.append(like)
        where.append("(" + " OR ".join(parts) + ")")
    if source_id:
        where.append("a.source_id = ?")
        params.append(source_id)
    if has_summary is True:
        if "summary_llm" in columns:
            where.append("a.summary_llm IS NOT NULL")
        else:
            return [], 0
    if has_summary is False:
        if "summary_llm" in columns:
            where.append("a.summary_llm IS NULL")
    if after:
        if "published_at" in columns:
            where.append("a.published_at >= ?")
            params.append(after)
    if before:
        if "published_at" in columns:
            where.append("a.published_at <= ?")
            params.append(before)
    if tags:
        if not _table_exists(conn, "article_tags"):
            return [], 0
        where.append(
            "EXISTS (SELECT 1 FROM article_tags t WHERE t.article_id = a.id AND t.tag IN ({}))".format(
                ",".join("?" for _ in tags)
            )
        )
        params.extend(tags)

    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql

    count_cursor = conn.execute(
        f"SELECT COUNT(1) FROM articles a {where_sql}",
        params,
    )
    total = count_cursor.fetchone()[0]

    offset = max(page - 1, 0) * page_size
    order_col = "a.published_at" if "published_at" in columns else "a.ingested_at"
    cursor = conn.execute(
        f"""
        SELECT a.id, a.title, a.original_url, a.published_at, a.ingested_at,
               { 'a.summary_llm' if 'summary_llm' in columns else 'NULL' } as summary_llm,
               a.source_id, s.name,
               GROUP_CONCAT(t.tag) as tags
        FROM articles a
        LEFT JOIN sources s ON s.id = a.source_id
        LEFT JOIN article_tags t ON t.article_id = a.id
        {where_sql}
        GROUP BY a.id
        ORDER BY {order_col} DESC
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    )
    items: list[dict[str, object]] = []
    for (
        article_id,
        title,
        original_url,
        published_at,
        ingested_at,
        summary_llm,
        source_id,
        source_name,
        tags_csv,
    ) in cursor.fetchall():
        items.append(
            {
                "id": article_id,
                "title": title,
                "url": original_url,
                "published_at": published_at,
                "ingested_at": ingested_at,
                "has_summary": summary_llm is not None,
                "source_id": source_id,
                "source_name": source_name,
                "tags": tags_csv.split(",") if tags_csv else [],
            }
        )
    return items, total


def get_cve(conn: sqlite3.Connection, cve_id: str) -> dict[str, object] | None:
    columns = _table_columns(conn, "cves") if _table_exists(conn, "cves") else set()
    selected = [
        "cve_id",
        "published_at",
        "last_modified_at",
        "preferred_cvss_version",
        "preferred_base_score",
        "preferred_base_severity",
        "preferred_vector",
        "cvss_v31_json",
        "cvss_v40_json",
        "cvss_v31_list_json",
        "cvss_v40_list_json",
        "description_text",
        "affected_products_json",
        "affected_cpes_json",
        "reference_domains_json",
        "updated_at",
    ]
    selected = [col for col in selected if col in columns]
    if not selected:
        return None
    cursor = conn.execute(
        f"""
        SELECT {", ".join(selected)}
        FROM cves
        WHERE cve_id = ?
        """,
        (cve_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    data = dict(zip(selected, row))
    cvss_v31_json = data.get("cvss_v31_json")
    cvss_v40_json = data.get("cvss_v40_json")
    cvss_v31_list_json = data.get("cvss_v31_list_json")
    cvss_v40_list_json = data.get("cvss_v40_list_json")
    cvss_v31 = json.loads(cvss_v31_json) if cvss_v31_json else None
    cvss_v40 = json.loads(cvss_v40_json) if cvss_v40_json else None
    cvss_v31_list = json.loads(cvss_v31_list_json) if cvss_v31_list_json else []
    cvss_v40_list = json.loads(cvss_v40_list_json) if cvss_v40_list_json else []
    product_versions = _list_cve_product_versions(conn, cve_id)
    return {
        "cve_id": data.get("cve_id"),
        "published_at": data.get("published_at"),
        "last_modified_at": data.get("last_modified_at"),
        "preferred_cvss_version": data.get("preferred_cvss_version"),
        "preferred_base_score": data.get("preferred_base_score"),
        "preferred_base_severity": data.get("preferred_base_severity"),
        "preferred_vector": data.get("preferred_vector"),
        "cvss_v31": cvss_v31,
        "cvss_v40": cvss_v40,
        "cvss_v31_list": cvss_v31_list,
        "cvss_v40_list": cvss_v40_list,
        "description_text": data.get("description_text"),
        "affected_products": json.loads(data.get("affected_products_json") or "[]"),
        "affected_cpes": json.loads(data.get("affected_cpes_json") or "[]"),
        "reference_domains": json.loads(data.get("reference_domains_json") or "[]"),
        "product_versions": product_versions,
        "updated_at": data.get("updated_at"),
    }


def get_cve_last_seen(conn: sqlite3.Connection, cve_id: str) -> str | None:
    cursor = conn.execute(
        "SELECT MAX(observed_at) FROM cve_snapshots WHERE cve_id = ?",
        (cve_id,),
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] else None


def search_cves(
    conn: sqlite3.Connection,
    query: str | None,
    severities: list[str] | None,
    min_cvss: float | None,
    after: str | None,
    before: str | None,
    vendor_keywords: list[str] | None,
    product_keywords: list[str] | None,
    in_scope: bool | None,
    settings: dict[str, object] | None,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    columns = _table_columns(conn, "cves") if _table_exists(conn, "cves") else set()
    where: list[str] = []
    params: list[object] = []
    if query:
        like = f"%{query}%"
        where.append(
            "(cve_id LIKE ? OR description_text LIKE ? OR LOWER(affected_products_json) LIKE ? OR LOWER(affected_cpes_json) LIKE ?)"
        )
        params.extend([like, like, like.lower(), like.lower()])
    if severities:
        normalized = [severity.upper() for severity in severities]
        include_unknown = "UNKNOWN" in normalized
        normalized = [value for value in normalized if value != "UNKNOWN"]
        condition_parts = []
        if normalized:
            condition_parts.append(
                "preferred_base_severity IN ({})".format(",".join("?" for _ in normalized))
            )
            params.extend(normalized)
        if include_unknown:
            condition_parts.append("preferred_base_severity IS NULL")
        if condition_parts:
            where.append("(" + " OR ".join(condition_parts) + ")")
    if min_cvss is not None:
        where.append("preferred_base_score >= ?")
        params.append(min_cvss)
    if after:
        where.append("published_at >= ?")
        params.append(after)
    if before:
        where.append("published_at <= ?")
        params.append(before)
    if vendor_keywords:
        for keyword in vendor_keywords:
            like = f"%{keyword.lower()}%"
            where.append(
                "(LOWER(description_text) LIKE ? OR LOWER(affected_products_json) LIKE ? OR LOWER(affected_cpes_json) LIKE ? OR LOWER(reference_domains_json) LIKE ?)"
            )
            params.extend([like, like, like, like])
    if product_keywords:
        for keyword in product_keywords:
            like = f"%{keyword.lower()}%"
            where.append(
                "(LOWER(description_text) LIKE ? OR LOWER(affected_products_json) LIKE ? OR LOWER(affected_cpes_json) LIKE ? OR LOWER(reference_domains_json) LIKE ?)"
            )
            params.extend([like, like, like, like])
    if in_scope and settings:
        filters = settings.get("filters") or {}
        scope_sevs = filters.get("severities") or []
        if scope_sevs:
            where.append(
                "preferred_base_severity IN ({})".format(",".join("?" for _ in scope_sevs))
            )
            params.extend([severity.upper() for severity in scope_sevs])
        min_score = filters.get("min_cvss")
        if min_score is not None:
            where.append("preferred_base_score >= ?")
            params.append(min_score)
        if filters.get("require_known_score"):
            where.append("preferred_base_score IS NOT NULL")
        keyword_filters = (filters.get("vendor_keywords") or []) + (
            filters.get("product_keywords") or []
        )
        if keyword_filters:
            keyword_where = []
            for keyword in keyword_filters:
                like = f"%{keyword.lower()}%"
                keyword_where.append("LOWER(description_text) LIKE ?")
                params.append(like)
                keyword_where.append("LOWER(affected_products_json) LIKE ?")
                params.append(like)
                keyword_where.append("LOWER(affected_cpes_json) LIKE ?")
                params.append(like)
                keyword_where.append("LOWER(reference_domains_json) LIKE ?")
                params.append(like)
            where.append("(" + " OR ".join(keyword_where) + ")")

    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql

    count_cursor = conn.execute(f"SELECT COUNT(1) FROM cves {where_sql}", params)
    total = count_cursor.fetchone()[0]

    offset = max(page - 1, 0) * page_size
    selected = [
        "cve_id",
        "published_at",
        "last_modified_at",
        "preferred_cvss_version",
        "preferred_base_score",
        "preferred_base_severity",
        "preferred_vector",
        "description_text",
        "updated_at",
        "affected_products_json",
        "affected_cpes_json",
        "reference_domains_json",
        "cvss_v31_list_json",
        "cvss_v40_list_json",
    ]
    selected = [col for col in selected if col in columns]
    cursor = conn.execute(
        f"""
        SELECT {", ".join(selected)}
        FROM cves
        {where_sql}
        ORDER BY last_modified_at DESC
        LIMIT ? OFFSET ?
        """,
        [*params, page_size, offset],
    )
    items = []
    for row in cursor.fetchall():
        data = dict(zip(selected, row))
        cvss_v31_list_json = data.get("cvss_v31_list_json")
        cvss_v40_list_json = data.get("cvss_v40_list_json")
        items.append(
            {
                "cve_id": data.get("cve_id"),
                "published_at": data.get("published_at"),
                "last_modified_at": data.get("last_modified_at"),
                "preferred_cvss_version": data.get("preferred_cvss_version"),
                "preferred_base_score": data.get("preferred_base_score"),
                "preferred_base_severity": data.get("preferred_base_severity"),
                "preferred_vector": data.get("preferred_vector"),
                "summary": data.get("description_text"),
                "updated_at": data.get("updated_at"),
                "affected_products": json.loads(data.get("affected_products_json") or "[]"),
                "affected_cpes": json.loads(data.get("affected_cpes_json") or "[]"),
                "reference_domains": json.loads(data.get("reference_domains_json") or "[]"),
                "cvss_v31_list": json.loads(cvss_v31_list_json) if cvss_v31_list_json else [],
                "cvss_v40_list": json.loads(cvss_v40_list_json) if cvss_v40_list_json else [],
                "product_versions": _list_cve_product_versions(conn, data.get("cve_id")),
            }
        )
    return items, total


def _list_cve_product_versions(conn: sqlite3.Connection, cve_id: str | None) -> list[str]:
    if not cve_id or not _table_exists(conn, "cve_product_versions"):
        return []
    if not _table_exists(conn, "products") or not _table_exists(conn, "vendors"):
        return []
    cursor = conn.execute(
        """
        SELECT v.display_name, p.display_name, cpv.version
        FROM cve_product_versions cpv
        JOIN products p ON p.id = cpv.product_id
        JOIN vendors v ON v.id = p.vendor_id
        WHERE cpv.cve_id = ?
        ORDER BY v.display_name, p.display_name, cpv.version
        """,
        (cve_id,),
    )
    return [
        f"{vendor}:{product}:{version}"
        for vendor, product, version in cursor.fetchall()
        if vendor and product and version
    ]


def cve_data_completeness(conn: sqlite3.Connection, limit: int = 20) -> dict[str, object]:
    if not _table_exists(conn, "cves"):
        return {"counts": {}, "missing": []}
    columns = _table_columns(conn, "cves")
    total = count_table(conn, "cves")
    def _count_where(clause: str) -> int:
        row = conn.execute(f"SELECT COUNT(*) FROM cves WHERE {clause}").fetchone()
        return int(row[0] or 0)

    counts = {"total": total}
    if "description_text" in columns:
        counts["with_description"] = _count_where("description_text IS NOT NULL AND description_text != ''")
        counts["good_description"] = _count_where("length(description_text) >= 80")
    if "reference_domains_json" in columns:
        counts["with_domains"] = _count_where("reference_domains_json IS NOT NULL AND reference_domains_json != '[]'")
    if "affected_products_json" in columns:
        counts["with_products"] = _count_where("affected_products_json IS NOT NULL AND affected_products_json != '[]'")
    cvss_any = []
    if "cvss_v31_json" in columns:
        cvss_any.append("cvss_v31_json IS NOT NULL")
        counts["has_v31"] = _count_where("cvss_v31_json IS NOT NULL")
    if "cvss_v40_json" in columns:
        cvss_any.append("cvss_v40_json IS NOT NULL")
        counts["has_v40"] = _count_where("cvss_v40_json IS NOT NULL")
    if "cvss_v31_list_json" in columns:
        cvss_any.append("cvss_v31_list_json IS NOT NULL")
        counts["has_v31_list"] = _count_where("cvss_v31_list_json IS NOT NULL")
    if "cvss_v40_list_json" in columns:
        cvss_any.append("cvss_v40_list_json IS NOT NULL")
        counts["has_v40_list"] = _count_where("cvss_v40_list_json IS NOT NULL")
    if "preferred_base_score" in columns:
        cvss_any.append("preferred_base_score IS NOT NULL")
    counts["has_any_cvss"] = _count_where(" OR ".join(cvss_any)) if cvss_any else 0

    where_missing = []
    if "description_text" in columns:
        where_missing.append("(description_text IS NULL OR description_text = '')")
    if "reference_domains_json" in columns:
        where_missing.append("(reference_domains_json IS NULL OR reference_domains_json = '[]')")
    if "affected_products_json" in columns:
        where_missing.append("(affected_products_json IS NULL OR affected_products_json = '[]')")
    if cvss_any:
        parts = []
        if "cvss_v31_json" in columns:
            parts.append("cvss_v31_json IS NULL")
        if "cvss_v40_json" in columns:
            parts.append("cvss_v40_json IS NULL")
        if "cvss_v31_list_json" in columns:
            parts.append("cvss_v31_list_json IS NULL")
        if "cvss_v40_list_json" in columns:
            parts.append("cvss_v40_list_json IS NULL")
        if "preferred_base_score" in columns:
            parts.append("preferred_base_score IS NULL")
        where_missing.append("(" + " AND ".join(parts) + ")")

    missing: list[dict[str, object]] = []
    missing_by_category: dict[str, list[str]] = {
        "description": [],
        "products": [],
        "domains": [],
        "cvss": [],
    }
    if where_missing:
        cursor = conn.execute(
            f"""
            SELECT cve_id, description_text, affected_products_json, reference_domains_json,
                   cvss_v31_json, cvss_v40_json, cvss_v31_list_json, cvss_v40_list_json,
                   preferred_base_score
            FROM cves
            WHERE {" OR ".join(where_missing)}
            ORDER BY published_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        for (
            cve_id,
            description_text,
            affected_products_json,
            reference_domains_json,
            cvss_v31_json,
            cvss_v40_json,
            cvss_v31_list_json,
            cvss_v40_list_json,
            preferred_base_score,
        ) in cursor.fetchall():
            missing_fields = []
            if "description_text" in columns and not (description_text or "").strip():
                missing_fields.append("description")
                missing_by_category["description"].append(cve_id)
            if "affected_products_json" in columns and (not affected_products_json or affected_products_json == "[]"):
                missing_fields.append("products")
                missing_by_category["products"].append(cve_id)
            if "reference_domains_json" in columns and (not reference_domains_json or reference_domains_json == "[]"):
                missing_fields.append("domains")
                missing_by_category["domains"].append(cve_id)
            has_cvss = any(
                value is not None
                for value in (
                    cvss_v31_json,
                    cvss_v40_json,
                    cvss_v31_list_json,
                    cvss_v40_list_json,
                    preferred_base_score,
                )
            )
            if not has_cvss:
                missing_fields.append("cvss")
                missing_by_category["cvss"].append(cve_id)
            missing.append({"cve_id": cve_id, "missing": missing_fields})
    return {"counts": counts, "missing": missing, "missing_by_category": missing_by_category}


def update_article_content(
    conn: sqlite3.Connection,
    article_id: int,
    *,
    content_text: str | None,
    content_html: str | None,
    content_fetched_at: str,
    content_error: str | None,
    has_full_content: bool,
) -> None:
    conn.execute(
        """
        UPDATE articles
        SET content_text = ?, content_html = ?, content_fetched_at = ?,
            content_error = ?, has_full_content = ?
        WHERE id = ?
        """,
        (
            content_text,
            content_html,
            content_fetched_at,
            content_error,
            1 if has_full_content else 0,
            article_id,
        ),
    )
    conn.commit()


def update_article_summary(
    conn: sqlite3.Connection,
    article_id: int,
    *,
    summary_llm: str | None,
    summary_model: str | None,
    summary_generated_at: str | None,
    summary_error: str | None,
) -> None:
    conn.execute(
        """
        UPDATE articles
        SET summary_llm = ?, summary_model = ?, summary_generated_at = ?,
            summary_error = ?
        WHERE id = ?
        """,
        (
            summary_llm,
            summary_model,
            summary_generated_at,
            summary_error,
            article_id,
        ),
    )
    conn.commit()


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


def _last_run_map(conn: sqlite3.Connection) -> dict[str, str]:
    cursor = conn.execute(
        """
        SELECT source_id, MAX(started_at) AS last_run
        FROM source_runs
        GROUP BY source_id
        """
    )
    return {row[0]: row[1] for row in cursor.fetchall() if row[1]}


def _parse_iso(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value).astimezone(timezone.utc)
