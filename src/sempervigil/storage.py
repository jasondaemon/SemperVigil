from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from typing import Iterable

from .migrations import apply_migrations
from .models import Article, Job, Source, SourceTactic
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
) -> None:
    conn.execute(
        """
        INSERT INTO cves
            (cve_id, published_at, last_modified_at, preferred_cvss_version,
             preferred_base_score, preferred_base_severity, preferred_vector,
             cvss_v40_json, cvss_v31_json, description_text, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cve_id) DO UPDATE SET
            published_at=excluded.published_at,
            last_modified_at=excluded.last_modified_at,
            preferred_cvss_version=excluded.preferred_cvss_version,
            preferred_base_score=excluded.preferred_base_score,
            preferred_base_severity=excluded.preferred_base_severity,
            preferred_vector=excluded.preferred_vector,
            cvss_v40_json=excluded.cvss_v40_json,
            cvss_v31_json=excluded.cvss_v31_json,
            description_text=excluded.description_text,
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
            utc_now_iso(),
        ),
    )
    conn.commit()


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
    cursor = conn.execute(
        """
        SELECT brief_day, COUNT(*)
        FROM articles
        WHERE brief_day >= ?
        GROUP BY brief_day
        ORDER BY brief_day
        """,
        (since_day,),
    )
    return [{"day": row[0], "count": row[1]} for row in cursor.fetchall()]


def get_source_stats(
    conn: sqlite3.Connection, days: int, runs: int
) -> list[dict[str, object]]:
    since_day = (datetime.now(tz=timezone.utc) - timedelta(days=days)).date().isoformat()
    rows = []
    sources = conn.execute(
        "SELECT id, name, last_ok_at, last_error FROM sources ORDER BY name"
    ).fetchall()
    for source_id, name, last_ok_at, last_error in sources:
        total_articles = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE source_id = ?",
            (source_id,),
        ).fetchone()[0]
        full_content = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE source_id = ? AND has_full_content = 1",
            (source_id,),
        ).fetchone()[0]
        summaries = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE source_id = ? AND summary_llm IS NOT NULL",
            (source_id,),
        ).fetchone()[0]
        recent_articles = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE source_id = ? AND brief_day >= ?",
            (source_id, since_day),
        ).fetchone()[0]
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
        rows.append(
            {
                "source_id": source_id,
                "source_name": name,
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
    cursor = conn.execute(
        """
        SELECT id, source_id, original_url, normalized_url, title, published_at,
               ingested_at, summary, content_text, content_html, content_fetched_at,
               content_error, summary_llm, summary_model, summary_generated_at,
               summary_error, brief_day, has_full_content
        FROM articles
        WHERE id = ?
        """,
        (article_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    (
        article_id,
        source_id,
        original_url,
        normalized_url,
        title,
        published_at,
        ingested_at,
        summary,
        content_text,
        content_html,
        content_fetched_at,
        content_error,
        summary_llm,
        summary_model,
        summary_generated_at,
        summary_error,
        brief_day,
        has_full_content,
    ) = row
    return {
        "id": article_id,
        "source_id": source_id,
        "original_url": original_url,
        "normalized_url": normalized_url,
        "title": title,
        "published_at": published_at,
        "ingested_at": ingested_at,
        "summary": summary,
        "content_text": content_text,
        "content_html": content_html,
        "content_fetched_at": content_fetched_at,
        "content_error": content_error,
        "summary_llm": summary_llm,
        "summary_model": summary_model,
        "summary_generated_at": summary_generated_at,
        "summary_error": summary_error,
        "brief_day": brief_day,
        "has_full_content": bool(has_full_content),
    }


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
