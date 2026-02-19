from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo
import logging
import os
import hashlib
import struct
import uuid
from datetime import datetime, timezone, timedelta
from typing import Iterable, Any
from urllib.parse import urlparse

from .db import connect_db
from .models import Article, Job, Source, SourceTactic
from .normalize import cpe_to_vendor_product, normalize_name
from .enrichment.url import normalize_url, url_hash
from .utils import json_dumps, log_event, utc_now_iso, utc_now_iso_offset, slugify
from psycopg import errors as pg_errors

_CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)


def init_db():
    return connect_db()


def _reset_serial_sequence(conn: Any, table: str, column: str) -> None:
    row = conn.execute(
        "SELECT pg_get_serial_sequence(%s, %s)",
        (table, column),
    ).fetchone()
    if not row or not row[0]:
        return
    seq = row[0]
    conn.execute(
        f"SELECT setval(%s::regclass, COALESCE((SELECT MAX({column}) FROM {table}), 0), true)",
        (seq,),
    )
    conn.commit()


def upsert_source(conn: Any, source_dict: dict[str, object]) -> None:
    source = _source_from_dict(source_dict)
    cursor = conn.execute("SELECT created_at FROM sources WHERE id = %s", (source.id,))
    row = cursor.fetchone()
    created_at = row[0] if row else utc_now_iso()
    updated_at = utc_now_iso()
    conn.execute(
        """
        INSERT INTO sources
            (id, name, enabled, base_url, topic_key, default_frequency_minutes,
             pause_until, paused_reason, robots_notes, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def set_source_enabled(conn: Any, source_id: str, enabled: bool) -> None:
    conn.execute(
        "UPDATE sources SET enabled = %s, updated_at = %s WHERE id = %s",
        (1 if enabled else 0, utc_now_iso(), source_id),
    )
    conn.commit()


def get_source(conn: Any, source_id: str) -> Source | None:
    cursor = conn.execute(
        """
        SELECT id, name, enabled, base_url, topic_key, default_frequency_minutes,
               kind, url,
               pause_until, paused_reason, robots_notes, overrides
        FROM sources
        WHERE id = %s
        """,
        (source_id,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return _row_to_source(row)


def list_sources(conn: Any, enabled_only: bool = True) -> list[Source]:
    if enabled_only:
        cursor = conn.execute(
            """
            SELECT id, name, enabled, base_url, topic_key, default_frequency_minutes,
                   kind, url,
                   pause_until, paused_reason, robots_notes, overrides
            FROM sources
            WHERE enabled = 1
            ORDER BY id
            """
        )
    else:
        cursor = conn.execute(
            """
            SELECT id, name, enabled, base_url, topic_key, default_frequency_minutes,
                   kind, url,
                   pause_until, paused_reason, robots_notes, overrides
            FROM sources
            ORDER BY id
            """
        )
    return [_row_to_source(row) for row in cursor.fetchall()]


def list_tactics_for_source(conn: Any, source_id: str) -> list[SourceTactic]:
    cursor = conn.execute(
        """
        SELECT id, source_id, tactic_type, enabled, priority, config_json,
               last_success_at, last_error_at, error_streak
        FROM source_tactics
        WHERE source_id = %s
        ORDER BY priority ASC
        """,
        (source_id,),
    )
    rows = cursor.fetchall()
    return [_row_to_tactic(row) for row in rows]


def list_due_sources(conn: Any, now_iso: str) -> list[Source]:
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


def list_tactics(conn: Any, source_id: str) -> list[SourceTactic]:
    cursor = conn.execute(
        """
        SELECT id, source_id, tactic_type, enabled, priority, config_json,
               last_success_at, last_error_at, error_streak
        FROM source_tactics
        WHERE source_id = %s AND enabled = 1
        ORDER BY priority ASC
        """,
        (source_id,),
    )
    rows = cursor.fetchall()
    return [_row_to_tactic(row) for row in rows]


def upsert_tactic(conn: Any, tactic: SourceTactic) -> None:
    updated_at = utc_now_iso()
    created_at = utc_now_iso()
    conn.execute(
        """
        DELETE FROM source_tactics
        WHERE source_id = %s AND tactic_type = %s
        """,
        (tactic.source_id, tactic.tactic_type),
    )
    cursor = conn.execute(
        """
        INSERT INTO source_tactics
            (source_id, tactic_type, enabled, priority, config_json,
             last_success_at, last_error_at, error_streak, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
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
    cursor.fetchone()
    conn.commit()


def article_exists(conn: Any, source_id: str, stable_id: str) -> bool:
    cursor = conn.execute(
        "SELECT 1 FROM articles WHERE source_id = %s AND stable_id = %s",
        (source_id, stable_id),
    )
    return cursor.fetchone() is not None


def get_article_id(conn: Any, source_id: str, stable_id: str) -> int | None:
    return _get_article_id(conn, source_id, stable_id)


def insert_articles(conn: Any, articles: Iterable[Article]) -> int:
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
        INSERT INTO articles
            (source_id, stable_id, original_url, normalized_url, title, published_at,
             published_at_source, ingested_at, brief_day, is_commercial, content_fingerprint,
             extracted_text_path, extracted_text_hash, raw_html_path, raw_html_hash,
             meta_json, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
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


def list_articles_for_day(conn: Any, day: str) -> list[dict[str, object]]:
    day_candidates = {day}
    try:
        base = datetime.fromisoformat(day).date()
        day_candidates = {
            base.isoformat(),
            (base - timedelta(days=1)).isoformat(),
            (base + timedelta(days=1)).isoformat(),
        }
    except ValueError:
        day_candidates = {day}
    day_values = sorted(day_candidates)
    placeholders = ", ".join(["%s"] * len(day_values))
    cursor = conn.execute(
        f"""
        SELECT id, source_id, title, original_url, published_at, ingested_at, brief_day,
               summary_llm, summary_model, summary_generated_at, meta_json,
               context_llm, context_model, context_generated_at, context_error
        FROM articles
        WHERE COALESCE(brief_day, SUBSTR(published_at, 1, 10), SUBSTR(ingested_at, 1, 10)) IN ({placeholders})
        ORDER BY COALESCE(published_at, ingested_at) DESC
        """,
        tuple(day_values),
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
            brief_day,
            summary_llm,
            summary_model,
            summary_generated_at,
            meta_json,
            context_llm,
            context_model,
            context_generated_at,
            context_error,
        ) = row
        effective_day = brief_day
        if not effective_day:
            published_at_raw = published_at or ingested_at or ""
            if published_at_raw:
                effective_day = _brief_day_from(str(published_at_raw))
        if effective_day and str(effective_day) != str(day):
            continue
        rows.append(
            {
                "id": article_id,
                "source_id": source_id,
                "title": title,
                "original_url": original_url,
                "published_at": published_at,
                "ingested_at": ingested_at,
                "summary": None,
                "brief_day": brief_day,
                "summary_llm": summary_llm,
                "summary_model": summary_model,
                "summary_generated_at": summary_generated_at,
                "meta_json": meta_json,
                "context_llm": context_llm,
                "context_model": context_model,
                "context_generated_at": context_generated_at,
                "context_error": context_error,
            }
        )
    return rows


def list_summaries_for_day(conn: Any, day: str) -> list[dict[str, object]]:
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


def upsert_daily_brief(conn: Any, payload: dict[str, object]) -> None:
    if not _table_exists(conn, "daily_briefs"):
        return
    meta = payload.get("meta") or {}
    day = str(meta.get("brief_day") or payload.get("day") or "")
    profile_id = meta.get("profile_id") or payload.get("profile_id")
    def _normalize_json_payload(value: object, fallback: object) -> object:
        if isinstance(value, (list, dict)):
            return value
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("[") or text.startswith("{"):
                try:
                    parsed = json.loads(text)
                except Exception:
                    return fallback
                if isinstance(parsed, (list, dict)):
                    return parsed
        return fallback

    tldr = json_dumps(_normalize_json_payload(payload.get("tldr"), []))
    highlights = json_dumps(_normalize_json_payload(payload.get("technical_synthesis"), {}))
    families = json_dumps(_normalize_json_payload(payload.get("families"), []))
    urls = json_dumps(_normalize_json_payload(payload.get("low_value"), []))
    topics = json_dumps(_normalize_json_payload(payload.get("citations"), []))
    meta_payload = dict(meta)
    meta_payload["actions"] = payload.get("actions") or []
    meta_json = json_dumps(meta_payload)
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO daily_briefs
            (brief_day, profile_id, tldr_json, highlights_json, families_json, urls_json,
             topics_json, meta_json, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (brief_day) DO UPDATE SET
            profile_id = excluded.profile_id,
            tldr_json = excluded.tldr_json,
            highlights_json = excluded.highlights_json,
            families_json = excluded.families_json,
            urls_json = excluded.urls_json,
            topics_json = excluded.topics_json,
            meta_json = excluded.meta_json,
            updated_at = excluded.updated_at
        """,
        (day, profile_id, tldr, highlights, families, urls, topics, meta_json, now, now),
    )
    conn.commit()


def list_daily_briefs(conn: Any, page: int = 1, page_size: int = 50) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "daily_briefs"):
        return [], 0
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 50), 200), 1)
    offset = (page - 1) * page_size
    total = 0
    count_cursor = conn.execute("SELECT COUNT(*) FROM daily_briefs")
    row = count_cursor.fetchone()
    if row:
        total = int(row[0] or 0)
    cursor = conn.execute(
        """
        SELECT brief_day, profile_id, tldr_json, highlights_json, families_json,
               urls_json, topics_json, meta_json, created_at, updated_at
        FROM daily_briefs
        ORDER BY brief_day DESC
        LIMIT %s OFFSET %s
        """,
        (page_size, offset),
    )
    items: list[dict[str, object]] = []
    for row in cursor.fetchall():
        (
            brief_day,
            profile_id,
            tldr_json,
            highlights_json,
            families_json,
            urls_json,
            topics_json,
            meta_json,
            created_at,
            updated_at,
        ) = row
        tldr = _safe_json_loads(tldr_json, [])
        families = _safe_json_loads(families_json, [])
        urls = _safe_json_loads(urls_json, [])
        topics = _safe_json_loads(topics_json, [])
        meta = _safe_json_loads(meta_json, {})
        items.append(
            {
                "brief_day": str(brief_day),
                "profile_id": profile_id,
                "created_at": created_at,
                "updated_at": updated_at,
                "generated_at": meta.get("generated_at"),
                "article_count": meta.get("article_count"),
                "topic_count": meta.get("topic_count"),
                "family_count": meta.get("family_count"),
                "url_count": len(urls),
                "tldr_count": len(tldr),
            }
        )
    return items, total


def get_daily_brief(conn: Any, day: str) -> dict[str, object] | None:
    if not _table_exists(conn, "daily_briefs"):
        return None
    cursor = conn.execute(
        """
        SELECT brief_day, profile_id, tldr_json, highlights_json, families_json,
               urls_json, topics_json, meta_json, created_at, updated_at
        FROM daily_briefs
        WHERE brief_day = %s
        """,
        (day,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    (
        brief_day,
        profile_id,
        tldr_json,
        highlights_json,
        families_json,
        urls_json,
        topics_json,
        meta_json,
        created_at,
        updated_at,
    ) = row
    meta = _safe_json_loads(meta_json, {})
    return {
        "brief_day": str(brief_day),
        "profile_id": profile_id,
        "tldr": _safe_json_loads(tldr_json, []),
        "technical_synthesis": _safe_json_loads(highlights_json, {}),
        "actions": meta.get("actions", []),
        "families": _safe_json_loads(families_json, []),
        "low_value": _safe_json_loads(urls_json, []),
        "citations": _safe_json_loads(topics_json, []),
        "meta": meta,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def _safe_json_loads(value: Any, default: object) -> object:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(value)
    except Exception:
        return default
    if isinstance(parsed, str):
        text = parsed.strip()
        if text.startswith("[") or text.startswith("{"):
            try:
                reparsed = json.loads(text)
            except Exception:
                return parsed
            return reparsed
    return parsed


def list_recent_articles(conn: Any, limit: int = 200) -> list[dict[str, object]]:
    if not _table_exists(conn, "articles"):
        return []
    tag_join = ""
    tag_select = "'' AS tags"
    if _table_exists(conn, "article_tags"):
        tag_join = "LEFT JOIN article_tags t ON t.article_id = a.id"
        tag_select = "COALESCE(string_agg(t.tag, ',' ORDER BY t.tag), '') AS tags"
    cursor = conn.execute(
        f"""
        SELECT a.id, a.title, a.original_url, a.published_at, a.ingested_at,
               a.source_id, s.name AS source_name,
               a.summary_llm,
               a.meta_json,
               {tag_select}
        FROM articles a
        LEFT JOIN sources s ON s.id = a.source_id
        {tag_join}
        WHERE COALESCE(BTRIM(a.published_at), '') <> ''
        GROUP BY a.id, s.name
        ORDER BY COALESCE(a.published_at, a.ingested_at) DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = []
    for row in cursor.fetchall():
        rows.append(
            {
                "id": row[0],
                "title": row[1],
                "original_url": row[2],
                "published_at": row[3],
                "ingested_at": row[4],
                "source_id": row[5],
                "source_name": row[6] or "",
                "summary_llm": row[7],
                "meta_json": row[8],
                "tags": row[9] or "",
            }
        )
    return rows




def _meta_is_suppressed(meta_json: object) -> bool:
    if not meta_json:
        return False
    try:
        parsed = json.loads(meta_json) if isinstance(meta_json, str) else meta_json
    except Exception:
        return False
    if isinstance(parsed, dict):
        return bool(parsed.get("suppressed"))
    return False
def list_cves_for_day(conn: Any, day: str, limit: int = 200) -> list[dict[str, object]]:

    if not _table_exists(conn, "cves"):

        return []

    cursor = conn.execute(

        """

        SELECT c.cve_id, c.description_text, c.published_at, c.last_modified_at,

               c.preferred_base_score, c.preferred_base_severity

        FROM cves c

        WHERE DATE(COALESCE(c.published_at, c.last_modified_at)) = %s

        ORDER BY COALESCE(c.published_at, c.last_modified_at) DESC

        LIMIT %s

        """,

        (day, limit),

    )

    rows = []

    for row in cursor.fetchall():

        rows.append(

            {

                "cve_id": row[0],

                "description_text": row[1] or "",

                "published_at": row[2],

                "last_modified_at": row[3],

                "preferred_base_score": row[4],

                "preferred_base_severity": row[5] or "",

            }

        )

    return rows



def list_event_keys_for_articles(conn: Any, article_ids: list[int]) -> dict[int, list[str]]:
    if not article_ids or not _table_exists(conn, "event_articles") or not _table_exists(conn, "events"):
        return {}
    cursor = conn.execute(
        """
        SELECT ea.article_id, string_agg(e.event_key, ',' ORDER BY e.event_key) AS event_keys
        FROM event_articles ea
        JOIN events e ON e.id = ea.event_id
        WHERE ea.article_id = ANY(%s) AND e.event_key IS NOT NULL AND e.event_key != ''
        GROUP BY ea.article_id
        """,
        (article_ids,),
    )
    mapping: dict[int, list[str]] = {}
    for row in cursor.fetchall():
        article_id = int(row[0])
        keys = row[1].split(",") if row[1] else []
        mapping[article_id] = keys
    return mapping



def upsert_cve_links(
    conn: Any,
    article_id: int,
    cve_ids: list[str],
    evidence: dict[str, object],
) -> None:
    if not cve_ids:
        return
    now = utc_now_iso()
    event_columns = _table_columns(conn, "events")
    event_columns = _table_columns(conn, "events")
    event_columns = _table_columns(conn, "events")
    if _table_exists(conn, "cves"):
        cve_columns = _table_columns(conn, "cves")
        for cve_id in cve_ids:
            if "created_at" in cve_columns and "last_seen_at" in cve_columns:
                conn.execute(
                    """
                    INSERT INTO cves (cve_id, created_at, last_seen_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(cve_id) DO UPDATE SET last_seen_at = excluded.last_seen_at
                    """,
                    (cve_id, now, now),
                )
            elif "updated_at" in cve_columns:
                conn.execute(
                    """
                    INSERT INTO cves (cve_id, updated_at)
                    VALUES (%s, %s)
                    ON CONFLICT(cve_id) DO UPDATE SET updated_at = excluded.updated_at
                    """,
                    (cve_id, now),
                )
            else:
                conn.execute(
                    "INSERT INTO cves (cve_id) VALUES (%s) ON CONFLICT DO NOTHING",
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
            placeholders = ", ".join("%s" for _ in cols)
            conn.execute(
                f"""
                INSERT INTO article_cves ({", ".join(cols)})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
                """,
                values,
            )
        conn.commit()
        return
    _append_article_cves_meta(conn, article_id, cve_ids, evidence)


def _append_article_cves_meta(
    conn: Any,
    article_id: int,
    cve_ids: list[str],
    evidence: dict[str, object],
) -> None:
    cursor = conn.execute("SELECT meta_json FROM articles WHERE id = %s", (article_id,))
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
        "UPDATE articles SET meta_json = %s, updated_at = %s WHERE id = %s",
        (json_dumps(meta), utc_now_iso(), article_id),
    )
    conn.commit()


def get_setting(conn: Any, key: str, default: object) -> object:
    cursor = conn.execute("SELECT value FROM settings WHERE key = %s", (key,))
    row = cursor.fetchone()
    if not row:
        return default
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return default


def set_setting(conn: Any, key: str, value: object) -> None:
    payload = json_dumps(value)
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO settings (key, value, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, payload, now),
    )
    conn.commit()


def increment_setting_counter(conn: Any, key: str, delta: int = 1) -> int:
    now = utc_now_iso()
    row = conn.execute(
        """
        INSERT INTO settings (key, value, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT(key) DO UPDATE SET
            value = (
                CASE
                    WHEN settings.value ~ '^-?[0-9]+$' THEN settings.value::bigint
                    ELSE 0
                END + EXCLUDED.value::bigint
            )::text,
            updated_at = EXCLUDED.updated_at
        RETURNING value
        """,
        (key, str(int(delta)), now),
    ).fetchone()
    conn.commit()
    try:
        return int(row[0]) if row and row[0] is not None else 0
    except (TypeError, ValueError):
        return 0


def list_settings_with_prefix(
    conn: Any, prefix: str, limit: int = 1000
) -> dict[str, object]:
    cursor = conn.execute(
        """
        SELECT key, value
        FROM settings
        WHERE key LIKE %s
        ORDER BY key ASC
        LIMIT %s
        """,
        (f"{prefix}%", int(limit)),
    )
    out: dict[str, object] = {}
    for key, value in cursor.fetchall():
        if key is None:
            continue
        if isinstance(value, str):
            try:
                out[str(key)] = json.loads(value)
                continue
            except json.JSONDecodeError:
                pass
        out[str(key)] = value
    return out


def upsert_cve(
    conn: Any,
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
             {"," if extra_cols else ""} {", ".join("%s" for _ in extra_cols)})
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
    conn: Any,
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
    created = 0
    for vendor_display, product_display in pairs:
        vendor_norm = _normalize_vendor_display(vendor_display)
        if not vendor_norm:
            continue
        if not product_display:
            continue
        vendor_id = upsert_vendor(conn, vendor_norm)
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
            vendor_norm = _normalize_vendor_display(vendor_display)
            if not vendor_norm:
                continue
            if not product_display or not version:
                continue
            vendor_id = upsert_vendor(conn, vendor_norm)
            product_id, _ = upsert_product(conn, vendor_id, product_display)
            _link_cve_product_version(conn, cve_id, product_id, version, source)
    return {"links": created}


def link_cve_products_from_items(
    conn: Any,
    *,
    cve_id: str,
    items: list[dict[str, object]],
    source: str = "llm",
) -> dict[str, int]:
    vendors_created = 0
    products_created = 0
    links_created = 0
    versions_created = 0
    for item in items:
        vendor_display = _normalize_vendor_display(item.get("vendor"))
        product_display = str(item.get("product") or "").strip()
        versions = item.get("versions") or []
        if not vendor_display or not product_display:
            continue
        existing_vendor_id = get_vendor_id_by_name(conn, vendor_display)
        vendor_id = existing_vendor_id or upsert_vendor(conn, vendor_display)
        if existing_vendor_id is None:
            vendors_created += 1
        existing_product_id = get_product_id_by_vendor_name(conn, vendor_id, product_display)
        product_id, _ = upsert_product(conn, vendor_id, product_display)
        if existing_product_id is None:
            products_created += 1
        link_cve_product(
            conn,
            cve_id,
            product_id,
            source=source,
            evidence={"vendor": vendor_display, "product": product_display},
        )
        links_created += 1
        if versions:
            for version in versions:
                if not version:
                    continue
                _link_cve_product_version(conn, cve_id, product_id, str(version), source)
                versions_created += 1
    return {
        "vendors_created": vendors_created,
        "products_created": products_created,
        "links_created": links_created,
        "versions_created": versions_created,
    }


def _link_cve_product_version(
    conn: Any, cve_id: str, product_id: int, version: str, source: str
) -> None:
    if not _table_exists(conn, "cve_product_versions"):
        return
    conn.execute(
        """
        INSERT INTO cve_product_versions
            (cve_id, product_id, version, source, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (cve_id, product_id, version, source, utc_now_iso()),
    )


def insert_cve_snapshot(
    conn: Any,
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
        INSERT INTO cve_snapshots
            (cve_id, observed_at, nvd_last_modified_at, preferred_cvss_version,
             preferred_base_score, preferred_base_severity, preferred_vector,
             cvss_v40_json, cvss_v31_json, snapshot_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
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


def get_latest_cve_snapshot(conn: Any, cve_id: str) -> dict[str, object] | None:
    cursor = conn.execute(
        """
        SELECT preferred_cvss_version, preferred_base_score, preferred_base_severity,
               preferred_vector, cvss_v40_json, cvss_v31_json, nvd_last_modified_at
        FROM cve_snapshots
        WHERE cve_id = %s
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
    conn: Any,
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def _table_exists(conn: Any, table: str) -> bool:
    cursor = conn.execute("SELECT to_regclass(%s)", (f"public.{table}",))
    row = cursor.fetchone()
    return bool(row and row[0])


def _table_columns(conn: Any, table: str) -> set[str]:
    cursor = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {row[0] for row in cursor.fetchall()}


def column_exists(conn: Any, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    return column in _table_columns(conn, table)


def record_source_run(
    conn: Any,
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    conn: Any, source_id: str, reason: str, pause_minutes: int
) -> None:
    pause_until = utc_now_iso_offset(seconds=pause_minutes * 60)
    conn.execute(
        """
        UPDATE sources
        SET enabled = 0,
            pause_until = %s,
            paused_reason = %s,
            updated_at = %s
        WHERE id = %s
        """,
        (pause_until, reason, utc_now_iso(), source_id),
    )
    conn.commit()


def record_health_alert(conn: Any, source_id: str, alert_type: str, message: str) -> None:
    conn.execute(
        """
        INSERT INTO health_alerts (source_id, alert_type, message, created_at)
        VALUES (%s, %s, %s, %s)
        """,
        (source_id, alert_type, message, utc_now_iso()),
    )
    conn.commit()


def get_source_run_streaks(conn: Any, source_id: str, limit: int = 20) -> dict[str, int]:
    cursor = conn.execute(
        """
        SELECT status, items_accepted
        FROM source_runs
        WHERE source_id = %s
        ORDER BY started_at DESC
        LIMIT %s
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
        SELECT status, items_found
        FROM source_runs
        WHERE source_id = %s
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (source_id, limit),
    )
    for status, items_found in cursor.fetchall():
        if status == "ok" and int(items_found) == 0:
            consecutive_zero += 1
            continue
        break
    return {"consecutive_errors": consecutive_errors, "consecutive_zero": consecutive_zero}


def get_source_zero_days(conn: Any, source_id: str) -> int | None:
    cursor = conn.execute(
        """
        SELECT started_at
        FROM source_runs
        WHERE source_id = %s AND status = 'ok' AND items_accepted > 0
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (source_id,),
    )
    row = cursor.fetchone()
    now = _parse_iso(utc_now_iso())
    if row and row[0]:
        return int((now - _parse_iso(str(row[0]))).days)
    cursor = conn.execute(
        """
        SELECT MIN(started_at)
        FROM source_runs
        WHERE source_id = %s
        """,
        (source_id,),
    )
    row = cursor.fetchone()
    if row and row[0]:
        return int((now - _parse_iso(str(row[0]))).days)
    return None


def enqueue_job(
    conn: Any,
    job_type: str,
    payload: dict[str, object] | None,
    priority: int = 0,
    debounce: bool = False,
    dedupe: bool = False,
) -> str:
    if debounce and _has_pending_job(conn, job_type):
        return _get_latest_job_id(conn, job_type)
    payload_json = json_dumps(payload) if payload else None
    if dedupe and payload_json:
        existing = _get_pending_job_id_with_payload(conn, job_type, payload_json)
        if existing:
            return existing
    job_id = _new_job_id()
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO jobs
            (id, job_type, status, priority, payload_json, result_json, requested_at, started_at,
             finished_at, locked_by, locked_at, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            job_id,
            job_type,
            "queued",
            int(priority),
            payload_json,
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


def list_jobs_filtered(
    conn: Any,
    status: str | None = None,
    job_type: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[Job], int]:
    where: list[str] = []
    params: list[object] = []
    if status:
        where.append("status = %s")
        params.append(status)
    if job_type:
        where.append("job_type = %s")
        params.append(job_type)
    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql
    total = conn.execute(
        f"SELECT COUNT(*) FROM jobs {where_sql}",
        params,
    ).fetchone()[0]
    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        f"""
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error, priority
        FROM jobs
        {where_sql}
        ORDER BY requested_at DESC
        LIMIT %s OFFSET %s
        """,
        [*params, page_size, offset],
    )
    return [_row_to_job(row) for row in cursor.fetchall()], int(total or 0)


def list_jobs(conn: Any, limit: int = 50) -> list[Job]:
    cursor = conn.execute(
        """
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error, priority
        FROM jobs
        ORDER BY requested_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [_row_to_job(row) for row in cursor.fetchall()]


def list_queued_job_stats(conn: Any) -> list[dict[str, object]]:
    if not _table_exists(conn, "jobs"):
        return []
    cursor = conn.execute(
        """
        SELECT job_type, COUNT(*) AS queued_count, MIN(requested_at) AS oldest_requested_at
        FROM jobs
        WHERE status = 'queued'
        GROUP BY job_type
        ORDER BY queued_count DESC, job_type ASC
        """
    )
    rows = []
    for job_type, queued_count, oldest_requested_at in cursor.fetchall():
        rows.append(
            {
                "job_type": job_type,
                "queued": int(queued_count or 0),
                "oldest_requested_at": oldest_requested_at,
            }
        )
    return rows


def get_schema_version(conn: Any) -> str | None:
    if not _table_exists(conn, "schema_migrations"):
        return None
    row = conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def count_table(conn: Any, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] or 0)


def get_dashboard_metrics(conn: Any) -> dict[str, object]:
    metrics: dict[str, object] = {}
    metrics["cves_missing_description_count"] = 0
    metrics["cves_missing_products_count"] = 0
    job_counts: dict[str, dict[str, int]] = {}
    if _table_exists(conn, "jobs"):
        cursor = conn.execute(
            """
            SELECT job_type, status, COUNT(*)
            FROM jobs
            GROUP BY job_type, status
            """
        )
        for job_type, status, count in cursor.fetchall():
            job_counts.setdefault(job_type, {})[status] = int(count or 0)
        inflight = conn.execute(
            """
            SELECT job_type, COUNT(*)
            FROM jobs
            WHERE started_at IS NOT NULL AND finished_at IS NULL
            GROUP BY job_type
            """
        ).fetchall()
        for job_type, count in inflight:
            current = job_counts.setdefault(job_type, {}).get("running", 0)
            job_counts[job_type]["running"] = max(int(count or 0), int(current or 0))
    metrics["job_counts_by_type_status"] = job_counts
    failures_since = get_setting(conn, "dashboard_failures_since", None)
    counts_since = get_setting(conn, "dashboard_job_counts_since", None) or failures_since
    if counts_since and _table_exists(conn, "jobs"):
        for job_type in list(job_counts.keys()):
            if "failed" in job_counts[job_type]:
                job_counts[job_type]["failed"] = 0
            if "succeeded" in job_counts[job_type]:
                job_counts[job_type]["succeeded"] = 0
        cursor = conn.execute(
            """
            SELECT job_type, status, COUNT(*)
            FROM jobs
            WHERE status IN ('failed', 'succeeded')
              AND COALESCE(finished_at, requested_at) >= %s
            GROUP BY job_type, status
            """,
            (counts_since,),
        )
        for job_type, status, count in cursor.fetchall():
            job_counts.setdefault(job_type, {})[status] = int(count or 0)
    metrics["job_failures_since"] = failures_since
    metrics["job_counts_since"] = counts_since
    metrics["articles_pending_fetch"] = (
        job_counts.get("fetch_article_content", {}).get("queued", 0)
        + job_counts.get("fetch_article_content", {}).get("running", 0)
    )
    metrics["articles_pending_summarize"] = (
        job_counts.get("summarize_article_llm", {}).get("queued", 0)
        + job_counts.get("summarize_article_llm", {}).get("running", 0)
    )
    metrics["articles_pending_context"] = (
        job_counts.get("summarize_article_context_llm", {}).get("queued", 0)
        + job_counts.get("summarize_article_context_llm", {}).get("running", 0)
    )
    metrics["articles_pending_publish"] = (
        job_counts.get("write_article_markdown", {}).get("queued", 0)
        + job_counts.get("write_article_markdown", {}).get("running", 0)
    )

    article_columns = _table_columns(conn, "articles") if _table_exists(conn, "articles") else set()
    state_counts = get_article_state_counts_by_source(conn)
    total_new = sum(v.get("new_count", 0) for v in state_counts.values())
    total_gathered = sum(v.get("gathered_count", 0) for v in state_counts.values())
    total_summarized = sum(v.get("summarized_count", 0) for v in state_counts.values())
    missing_content_count = 0
    content_error_count = 0
    content_404_count = 0
    content_stale_count = 0
    missing_summary_count = 0
    missing_context_count = 0
    if article_columns:
        url_clause = "original_url IS NOT NULL AND original_url != ''" if "original_url" in article_columns else None
        error_exclude_clause = None
        error_404_clause = None
        error_stale_clause = None
        if "content_error" in article_columns:
            error_404_clause = (
                "content_error IN ('http_404','http_410') "
                "OR content_error LIKE '%%HTTP Error 404%%' "
                "OR content_error LIKE '%%HTTP Error 410%%'"
            )
            error_stale_clause = "content_error = 'stale_older_than_week'"
            error_exclude_clause = (
                f"(content_error IS NULL OR NOT ({error_404_clause} OR {error_stale_clause}))"
            )
        if "has_full_content" in article_columns and "content_text" in article_columns:
            content_clause = "(has_full_content = 0 AND (content_text IS NULL OR content_text = ''))"
        elif "has_full_content" in article_columns and "extracted_text_path" in article_columns:
            content_clause = "(has_full_content = 0 AND (extracted_text_path IS NULL OR extracted_text_path = ''))"
        elif "has_full_content" in article_columns:
            content_clause = "has_full_content = 0"
        elif "content_text" in article_columns:
            content_clause = "(content_text IS NULL OR content_text = '')"
        elif "extracted_text_path" in article_columns:
            content_clause = "(extracted_text_path IS NULL OR extracted_text_path = '')"
        else:
            content_clause = None
        if content_clause:
            where = content_clause
            if url_clause:
                where = f"{where} AND {url_clause}"
            if error_exclude_clause:
                where = f"{where} AND {error_exclude_clause}"
            row = conn.execute(f"SELECT COUNT(*) FROM articles WHERE {where}").fetchone()
            missing_content_count = int(row[0] or 0)
        if "content_error" in article_columns:
            where = "content_error IS NOT NULL AND content_error != ''"
            if error_exclude_clause:
                where = f"{where} AND {error_exclude_clause}"
            row = conn.execute(
                f"SELECT COUNT(*) FROM articles WHERE {where}"
            ).fetchone()
            content_error_count = int(row[0] or 0)
            if error_404_clause:
                row = conn.execute(
                    f"SELECT COUNT(*) FROM articles WHERE {error_404_clause}"
                ).fetchone()
                content_404_count = int(row[0] or 0)
            if error_stale_clause:
                row = conn.execute(
                    f"SELECT COUNT(*) FROM articles WHERE {error_stale_clause}"
                ).fetchone()
                content_stale_count = int(row[0] or 0)
        if "summary_llm" in article_columns:
            where = "summary_llm IS NULL OR summary_llm = ''"
            if error_exclude_clause:
                where = f"({where}) AND {error_exclude_clause}"
            row = conn.execute(f"SELECT COUNT(*) FROM articles WHERE {where}").fetchone()
            missing_summary_count = int(row[0] or 0)
        if "context_llm" in article_columns:
            clauses = ["(context_llm IS NULL OR context_llm = '')"]
            content_ready = []
            if "has_full_content" in article_columns:
                content_ready.append("has_full_content = 1")
            if "content_text" in article_columns:
                content_ready.append("(content_text IS NOT NULL AND content_text != '')")
            if "extracted_text_path" in article_columns:
                content_ready.append("(extracted_text_path IS NOT NULL AND extracted_text_path != '')")
            if content_ready:
                clauses.append("(" + " OR ".join(content_ready) + ")")
            where = " AND ".join(clauses)
            if error_exclude_clause:
                where = f"({where}) AND {error_exclude_clause}"
            row = conn.execute(f"SELECT COUNT(*) FROM articles WHERE {where}").fetchone()
            missing_context_count = int(row[0] or 0)

    metrics["articles_missing_content_count"] = missing_content_count
    metrics["articles_with_content_error_count"] = content_error_count
    metrics["articles_404_count"] = content_404_count
    metrics["articles_stale_count"] = content_stale_count
    metrics["articles_missing_summary_count"] = missing_summary_count
    metrics["articles_missing_context_count"] = missing_context_count
    metrics["articles_missing_products_count"] = 0
    metrics["articles_missing_threat_actors_count"] = 0
    if _table_exists(conn, "articles"):
        cols = _table_columns(conn, "articles")
        error_exclude_clause = None
        if "content_error" in cols:
            error_404_clause = (
                "content_error IN ('http_404','http_410') "
                "OR content_error LIKE '%%HTTP Error 404%%' "
                "OR content_error LIKE '%%HTTP Error 410%%'"
            )
            error_stale_clause = "content_error = 'stale_older_than_week'"
            error_exclude_clause = (
                f"(content_error IS NULL OR NOT ({error_404_clause} OR {error_stale_clause}))"
            )
        if _table_exists(conn, "article_products"):
            where = "1=1"
            if error_exclude_clause:
                where = f"{where} AND {error_exclude_clause}"
            row = conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT a.id
                    FROM articles a
                    LEFT JOIN article_products ap ON ap.article_id = a.id
                    WHERE {where}
                    GROUP BY a.id
                    HAVING COUNT(ap.article_id) = 0
                ) t
                """
            ).fetchone()
            metrics["articles_missing_products_count"] = int(row[0] or 0)
        if _table_exists(conn, "article_threat_actors"):
            where = "1=1"
            if error_exclude_clause:
                where = f"{where} AND {error_exclude_clause}"
            row = conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT a.id
                    FROM articles a
                    LEFT JOIN article_threat_actors ata ON ata.article_id = a.id
                    WHERE {where}
                    GROUP BY a.id
                    HAVING COUNT(ata.article_id) = 0
                ) t
                """
            ).fetchone()
            metrics["articles_missing_threat_actors_count"] = int(row[0] or 0)


    cve_missing_desc = 0
    if _table_exists(conn, "cves") and "description_text" in _table_columns(conn, "cves"):
        row = conn.execute(
            "SELECT COUNT(*) FROM cves c WHERE c.description_text IS NULL OR c.description_text = ''"
        ).fetchone()
        cve_missing_desc = int(row[0] or 0)
    metrics["cves_missing_description_count"] = cve_missing_desc

    cve_missing_products = 0
    if _table_exists(conn, "cves") and _table_exists(conn, "cve_products"):
        row = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT c.cve_id
                FROM cves c
                LEFT JOIN cve_products cp ON cp.cve_id = c.cve_id
                LEFT JOIN cve_product_versions cpv ON cpv.cve_id = c.cve_id
                GROUP BY c.cve_id
                HAVING COUNT(cp.cve_id) = 0 OR COUNT(cpv.cve_id) = 0
            ) t
            """
        ).fetchone()
        cve_missing_products = int(row[0] or 0)
    metrics["cves_missing_products_count"] = cve_missing_products
    metrics["cves_missing_threat_actors_count"] = count_cves_missing_threat_actors(conn)
    # Keep dashboard "missing" metrics aligned with actionable queue logic.
    metrics["articles_missing_products_count"] = count_articles_missing_products(conn)
    metrics["articles_missing_threat_actors_count"] = count_articles_missing_threat_actors(conn)
    metrics["cves_missing_products_count"] = len(list_cve_ids_missing_products(conn, limit=None))
    metrics["cves_missing_threat_actors_count"] = count_cves_missing_threat_actors(conn)
    daily_missing_days = 0
    if _table_exists(conn, "articles"):
        cols = _table_columns(conn, "articles")
        if "published_at" in cols or "ingested_at" in cols:
            if "published_at" in cols and "ingested_at" in cols:
                date_expr = "DATE(COALESCE(published_at, ingested_at))"
                where_expr = "COALESCE(published_at, ingested_at) IS NOT NULL"
            elif "published_at" in cols:
                date_expr = "DATE(published_at)"
                where_expr = "published_at IS NOT NULL"
            else:
                date_expr = "DATE(ingested_at)"
                where_expr = "ingested_at IS NOT NULL"
            if "content_error" in cols:
                error_404_clause = (
                    "content_error IN ('http_404','http_410') "
                    "OR content_error LIKE '%%HTTP Error 404%%' "
                    "OR content_error LIKE '%%HTTP Error 410%%'"
                )
                error_stale_clause = "content_error = 'stale_older_than_week'"
                where_expr = f"{where_expr} AND NOT ({error_404_clause} OR {error_stale_clause})"
            cursor = conn.execute(
                f"""
                SELECT DISTINCT {date_expr}
                FROM articles
                WHERE {where_expr}
                """
            )
            article_days = {str(row[0]) for row in cursor.fetchall() if row and row[0]}
            brief_days: set[str] = set()
            if _table_exists(conn, "daily_briefs"):
                cursor = conn.execute("SELECT brief_day FROM daily_briefs")
                brief_days = {str(row[0]) for row in cursor.fetchall() if row and row[0]}
            daily_missing_days = len(article_days - brief_days)
    metrics["daily_brief_missing_days_count"] = daily_missing_days
    metrics["events_candidate_count"] = 0
    if _table_exists(conn, "events"):
        event_cols = _table_columns(conn, "events")
        lifecycle_col = "lifecycle" in event_cols
        candidate_col = "candidate" in event_cols
        if lifecycle_col:
            row = conn.execute(
                "SELECT COUNT(*) FROM events WHERE COALESCE(lifecycle, status, '') = 'candidate'"
            ).fetchone()
            metrics["events_candidate_count"] = int(row[0] or 0)
        elif candidate_col:
            row = conn.execute("SELECT COUNT(*) FROM events WHERE candidate = 1").fetchone()
            metrics["events_candidate_count"] = int(row[0] or 0)
    return metrics


def get_last_job_by_type(conn: Any, job_type: str) -> Job | None:
    if not _table_exists(conn, "jobs"):
        return None
    row = conn.execute(
        """
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error, priority
        FROM jobs
        WHERE job_type = %s
        ORDER BY requested_at DESC
        LIMIT 1
        """,
        (job_type,),
    ).fetchone()
    return _row_to_job(row) if row else None


def get_job(conn: Any, job_id: str) -> Job | None:
    if not _table_exists(conn, "jobs"):
        return None
    row = conn.execute(
        """
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error, priority
        FROM jobs
        WHERE id = %s
        """,
        (job_id,),
    ).fetchone()
    return _row_to_job(row) if row else None


def list_jobs_by_types_since(
    conn: Any, *, types: list[str], since: str
) -> list[Job]:
    if not _table_exists(conn, "jobs") or not types:
        return []
    placeholders = ",".join("%s" for _ in types)
    cursor = conn.execute(
        f"""
        SELECT id, job_type, status, payload_json, result_json, requested_at, started_at,
               finished_at, locked_by, locked_at, error, priority
        FROM jobs
        WHERE requested_at >= %s AND job_type IN ({placeholders})
        ORDER BY requested_at ASC
        """,
        (since, *types),
    )
    return [_row_to_job(row) for row in cursor.fetchall()]


def insert_llm_run(
    conn: Any,
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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


def list_llm_runs(conn: Any, limit: int = 10) -> list[dict[str, object]]:
    if not _table_exists(conn, "llm_runs"):
        return []
    provider_join = ""
    model_join = ""
    provider_select = "NULL"
    model_select = "NULL"
    if _table_exists(conn, "llm_providers"):
        provider_join = "LEFT JOIN llm_providers p ON p.id = r.provider_id"
        provider_select = "p.name"
    if _table_exists(conn, "llm_models"):
        model_join = "LEFT JOIN llm_models m ON m.id = r.model_id"
        model_select = "m.model_name"
    cursor = conn.execute(
        f"""
        SELECT r.id, r.ts, r.job_id, r.provider_id, r.model_id, r.prompt_name,
               r.input_chars, r.output_chars, r.latency_ms, r.ok, r.error,
               {provider_select} AS provider_name,
               {model_select} AS model_name
        FROM llm_runs r
        {provider_join}
        {model_join}
        ORDER BY r.ts DESC
        LIMIT %s
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
            provider_name,
            model_name,
        ) = row
        items.append(
            {
                "id": run_id,
                "ts": ts,
                "job_id": job_id,
                "provider_id": provider_id,
                "provider_name": provider_name,
                "model_id": model_id,
                "model_name": model_name,
                "prompt_name": prompt_name,
                "input_chars": input_chars,
                "output_chars": output_chars,
                "latency_ms": latency_ms,
                "ok": bool(ok),
                "error": error,
            }
        )
    return items


def update_job_result(conn: Any, job_id: str, result: dict[str, object]) -> bool:
    cursor = conn.execute(
        """
        UPDATE jobs
        SET result_json = %s
        WHERE id = %s AND status = 'running'
        """,
        (json_dumps(result), job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def touch_job_lock(conn: Any, job_id: str) -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET locked_at = %s
        WHERE id = %s AND status = 'running'
        """,
        (now, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def cancel_job(conn: Any, job_id: str, reason: str = "canceled_by_admin") -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'canceled',
            finished_at = %s,
            error = %s,
            locked_by = NULL,
            locked_at = NULL
        WHERE id = %s AND status IN ('queued', 'running')
        """,
        (now, reason, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def cancel_all_jobs(conn: Any, reason: str = "canceled_by_admin") -> int:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'canceled',
            finished_at = %s,
            error = %s,
            locked_by = NULL,
            locked_at = NULL
        WHERE status IN ('queued', 'running')
        """,
        (now, reason),
    )
    conn.commit()
    return int(cursor.rowcount or 0)


def cancel_jobs_by_type(
    conn: Any,
    job_type: str,
    status: str | None = "queued",
    reason: str = "canceled_by_admin",
) -> int:
    now = utc_now_iso()
    if status is None:
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = 'canceled',
                finished_at = %s,
                error = %s,
                locked_by = NULL,
                locked_at = NULL
            WHERE job_type = %s AND status IN ('queued', 'running')
            """,
            (now, reason, job_type),
        )
    else:
        cursor = conn.execute(
            """
            UPDATE jobs
            SET status = 'canceled',
                finished_at = %s,
                error = %s,
                locked_by = NULL,
                locked_at = NULL
            WHERE job_type = %s AND status = %s
            """,
            (now, reason, job_type, status),
        )
    conn.commit()
    return int(cursor.rowcount or 0)


def release_job(
    conn: Any,
    job_id: str,
    delay_seconds: int = 0,
    reason: str = "released_by_worker",
) -> bool:
    next_run = utc_now_iso_offset(seconds=delay_seconds)
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'queued',
            requested_at = %s,
            started_at = NULL,
            finished_at = NULL,
            locked_by = NULL,
            locked_at = NULL,
            error = %s
        WHERE id = %s AND status = 'running'
        """,
        (next_run, reason, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def is_job_canceled(conn: Any, job_id: str) -> bool:
    row = conn.execute("SELECT status FROM jobs WHERE id = %s", (job_id,)).fetchone()
    return bool(row and row[0] == "canceled")


def has_pending_job(
    conn: Any, job_type: str, exclude_job_id: str | None = None
) -> bool:
    if exclude_job_id:
        cursor = conn.execute(
            """
            SELECT 1 FROM jobs
            WHERE job_type = %s AND status IN ('queued', 'running') AND id != %s
            LIMIT 1
            """,
            (job_type, exclude_job_id),
        )
    else:
        cursor = conn.execute(
            """
            SELECT 1 FROM jobs
            WHERE job_type = %s AND status IN ('queued', 'running')
            LIMIT 1
            """,
            (job_type,),
        )
    return cursor.fetchone() is not None


def enqueue_build_site_if_needed(
    conn: Any, reason: str | None = None, debounce_seconds: int = 60
) -> str | None:
    logger = logging.getLogger("sempervigil.jobs")
    if has_pending_job(conn, "build_site"):
        logger.info("build_site_skip_pending reason=%s", reason or "")
        return None
    now = utc_now_iso()
    last_enqueued = get_setting(conn, "build_site.last_enqueued_at", None)
    if isinstance(last_enqueued, str):
        if _parse_iso(last_enqueued) + timedelta(seconds=debounce_seconds) > _parse_iso(
            now
        ):
            logger.info(
                "build_site_skip_debounce reason=%s last_enqueued=%s debounce_seconds=%s",
                reason or "",
                last_enqueued,
                debounce_seconds,
            )
            return None
    payload = {"reason": reason} if reason else None
    job_id = enqueue_job(conn, "build_site", payload, debounce=True)
    set_setting(conn, "build_site.last_enqueued_at", now)
    logger.info(
        "build_site_enqueued job_id=%s reason=%s debounce_seconds=%s",
        job_id,
        reason or "",
        debounce_seconds,
    )
    return job_id


def has_pending_article_job(
    conn: Any, job_type: str, article_id: int
) -> bool:
    if not _table_exists(conn, "jobs"):
        return False
    pattern = f'%\"article_id\":{article_id}%'
    cursor = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE job_type = %s AND status IN ('queued', 'running') AND payload_json LIKE %s
        LIMIT 1
        """,
        (job_type, pattern),
    )
    return cursor.fetchone() is not None


def count_failed_article_jobs(
    conn: Any, job_type: str, article_id: int
) -> int:
    if not _table_exists(conn, "jobs"):
        return 0
    patterns = [
        f'%\"article_id\":{article_id}%',
        f'%\"article_id\": {article_id}%',
        f'%\"article_id\":\"{article_id}\"%',
        f'%\"article_id\": \"{article_id}\"%',
    ]
    for pattern in patterns:
        cursor = conn.execute(
            """
            SELECT COUNT(*) FROM jobs
            WHERE job_type = %s AND status = 'failed' AND payload_json LIKE %s
            """,
            (job_type, pattern),
        )
        row = cursor.fetchone()
        if row and row[0]:
            return int(row[0] or 0)
    return 0


def get_pending_article_job_id(
    conn: Any, job_type: str, article_id: int
) -> str | None:
    if not _table_exists(conn, "jobs"):
        return None
    patterns = [
        f'%\"article_id\":{article_id}%',
        f'%\"article_id\": {article_id}%',
        f'%\"article_id\":\"{article_id}\"%',
        f'%\"article_id\": \"{article_id}\"%',
    ]
    for pattern in patterns:
        row = conn.execute(
            """
            SELECT id FROM jobs
            WHERE job_type = %s AND status IN ('queued', 'running') AND payload_json LIKE %s
            ORDER BY requested_at ASC
            LIMIT 1
            """,
            (job_type, pattern),
        ).fetchone()
        if row:
            return row[0]
    return None


def get_pending_cve_job_id(conn: Any, cve_id: str) -> str | None:
    if not _table_exists(conn, "jobs"):
        return None
    patterns = [
        f'%\"cve_id\":\"{cve_id}\"%',
        f'%\"cve_id\": \"{cve_id}\"%',
        f'%\"cve_id\":{json.dumps(cve_id)}%',
        f'%\"cve_id\": {json.dumps(cve_id)}%',
    ]
    for pattern in patterns:
        row = conn.execute(
            """
            SELECT id FROM jobs
            WHERE job_type = 'cve_sync' AND status IN ('queued', 'running') AND payload_json LIKE %s
            ORDER BY requested_at ASC
            LIMIT 1
            """,
            (pattern,),
        ).fetchone()
        if row:
            return row[0]
    return None


def get_source_name(conn: Any, source_id: str) -> str | None:
    row = conn.execute("SELECT name FROM sources WHERE id = %s", (source_id,)).fetchone()
    return row[0] if row else None

def get_pending_job_id_for_cve(conn: Any, job_type: str, cve_id: str) -> str | None:
    if not _table_exists(conn, "jobs"):
        return None
    patterns = [
        f'%"cve_id":"{cve_id}"%',
        f'%"cve_id": "{cve_id}"%',
        f'%"cve_id":{json.dumps(cve_id)}%',
        f'%"cve_id": {json.dumps(cve_id)}%',
    ]
    for pattern in patterns:
        row = conn.execute(
            """
            SELECT id FROM jobs
            WHERE job_type = %s AND status IN ('queued', 'running') AND payload_json LIKE %s
            ORDER BY requested_at ASC
            LIMIT 1
            """,
            (job_type, pattern),
        ).fetchone()
        if row:
            return row[0]
    return None


def upsert_cve_kev_entries(
    conn: Any, entries: list[dict[str, object]], *, sync_at: str
) -> int:
    if not entries or not _table_exists(conn, "cve_kev"):
        return 0
    count = 0
    for entry in entries:
        cve_id = str(entry.get("cve_id") or "").strip()
        if not cve_id:
            continue
        cursor = conn.execute(
            """
            INSERT INTO cve_kev (
                cve_id, added_at, due_date, vendor_project, product, vulnerability_name,
                short_description, required_action, ransomware_use, notes, raw_json, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cve_id) DO UPDATE SET
                added_at = EXCLUDED.added_at,
                due_date = EXCLUDED.due_date,
                vendor_project = EXCLUDED.vendor_project,
                product = EXCLUDED.product,
                vulnerability_name = EXCLUDED.vulnerability_name,
                short_description = EXCLUDED.short_description,
                required_action = EXCLUDED.required_action,
                ransomware_use = EXCLUDED.ransomware_use,
                notes = EXCLUDED.notes,
                raw_json = EXCLUDED.raw_json,
                updated_at = EXCLUDED.updated_at
            WHERE cve_kev.added_at IS DISTINCT FROM EXCLUDED.added_at
               OR cve_kev.due_date IS DISTINCT FROM EXCLUDED.due_date
               OR cve_kev.vendor_project IS DISTINCT FROM EXCLUDED.vendor_project
               OR cve_kev.product IS DISTINCT FROM EXCLUDED.product
               OR cve_kev.vulnerability_name IS DISTINCT FROM EXCLUDED.vulnerability_name
               OR cve_kev.short_description IS DISTINCT FROM EXCLUDED.short_description
               OR cve_kev.required_action IS DISTINCT FROM EXCLUDED.required_action
               OR cve_kev.ransomware_use IS DISTINCT FROM EXCLUDED.ransomware_use
               OR cve_kev.notes IS DISTINCT FROM EXCLUDED.notes
               OR cve_kev.raw_json IS DISTINCT FROM EXCLUDED.raw_json
            """,
            (
                cve_id,
                entry.get("added_at"),
                entry.get("due_date"),
                entry.get("vendor_project"),
                entry.get("product"),
                entry.get("vulnerability_name"),
                entry.get("short_description"),
                entry.get("required_action"),
                entry.get("ransomware_use"),
                entry.get("notes"),
                entry.get("raw_json"),
                sync_at,
            ),
        )
        count += int(cursor.rowcount or 0)
    conn.commit()
    return count


def prune_cve_kev_entries(conn: Any, *, sync_at: str) -> int:
    if not _table_exists(conn, "cve_kev"):
        return 0
    cursor = conn.execute(
        "DELETE FROM cve_kev WHERE updated_at < %s",
        (sync_at,),
    )
    conn.commit()
    return int(cursor.rowcount or 0)


def get_cve_kev(conn: Any, cve_id: str) -> dict[str, object] | None:
    if not _table_exists(conn, "cve_kev"):
        return None
    row = conn.execute(
        """
        SELECT cve_id, added_at, due_date, vendor_project, product, vulnerability_name,
               short_description, required_action, ransomware_use, notes, raw_json, updated_at
        FROM cve_kev
        WHERE cve_id = %s
        """,
        (cve_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "cve_id": row[0],
        "added_at": row[1],
        "due_date": row[2],
        "vendor_project": row[3],
        "product": row[4],
        "vulnerability_name": row[5],
        "short_description": row[6],
        "required_action": row[7],
        "ransomware_use": row[8],
        "notes": row[9],
        "raw_json": row[10],
        "updated_at": row[11],
    }


def get_cve_kev_map(conn: Any, cve_ids: list[str]) -> dict[str, dict[str, object]]:
    if not cve_ids or not _table_exists(conn, "cve_kev"):
        return {}
    cursor = conn.execute(
        """
        SELECT cve_id, added_at, due_date, vendor_project, product, vulnerability_name,
               short_description, required_action, ransomware_use, notes, raw_json, updated_at
        FROM cve_kev
        WHERE cve_id = ANY(%s)
        """,
        (cve_ids,),
    )
    mapping: dict[str, dict[str, object]] = {}
    for row in cursor.fetchall():
        mapping[row[0]] = {
            "cve_id": row[0],
            "added_at": row[1],
            "due_date": row[2],
            "vendor_project": row[3],
            "product": row[4],
            "vulnerability_name": row[5],
            "short_description": row[6],
            "required_action": row[7],
            "ransomware_use": row[8],
            "notes": row[9],
            "raw_json": row[10],
            "updated_at": row[11],
        }
    return mapping


def set_cve_kev_link(
    conn: Any, cve_id: str, kev_cve_id: str | None, checked_at: str, *, commit: bool = True
) -> None:
    if not _table_exists(conn, "cves"):
        return
    columns = _table_columns(conn, "cves")
    if "kev_checked_at" not in columns:
        return
    conn.execute(
        """
        UPDATE cves
        SET kev_cve_id = %s,
            kev_checked_at = %s
        WHERE cve_id = %s
        """,
        (kev_cve_id, checked_at, cve_id),
    )
    if commit:
        conn.commit()


def mark_article_products_checked(
    conn: Any, article_id: int, checked_at: str | None = None, *, commit: bool = True
) -> None:
    if not _table_exists(conn, "articles"):
        return
    columns = _table_columns(conn, "articles")
    if "article_products_checked_at" not in columns:
        return
    conn.execute(
        """
        UPDATE articles
        SET article_products_checked_at = %s
        WHERE id = %s
        """,
        (checked_at or utc_now_iso(), int(article_id)),
    )
    if commit:
        conn.commit()


def mark_article_threat_actors_checked(
    conn: Any, article_id: int, checked_at: str | None = None, *, commit: bool = True
) -> None:
    if not _table_exists(conn, "articles"):
        return
    columns = _table_columns(conn, "articles")
    if "article_threat_actors_checked_at" not in columns:
        return
    conn.execute(
        """
        UPDATE articles
        SET article_threat_actors_checked_at = %s
        WHERE id = %s
        """,
        (checked_at or utc_now_iso(), int(article_id)),
    )
    if commit:
        conn.commit()


def mark_cve_products_checked(
    conn: Any, cve_id: str, checked_at: str | None = None, *, commit: bool = True
) -> None:
    if not _table_exists(conn, "cves"):
        return
    columns = _table_columns(conn, "cves")
    if "cve_products_checked_at" not in columns:
        return
    conn.execute(
        """
        UPDATE cves
        SET cve_products_checked_at = %s
        WHERE cve_id = %s
        """,
        (checked_at or utc_now_iso(), cve_id),
    )
    if commit:
        conn.commit()


def mark_cve_threat_actors_checked(
    conn: Any, cve_id: str, checked_at: str | None = None, *, commit: bool = True
) -> None:
    if not _table_exists(conn, "cves"):
        return
    columns = _table_columns(conn, "cves")
    if "cve_threat_actors_checked_at" not in columns:
        return
    conn.execute(
        """
        UPDATE cves
        SET cve_threat_actors_checked_at = %s
        WHERE cve_id = %s
        """,
        (checked_at or utc_now_iso(), cve_id),
    )
    if commit:
        conn.commit()


def list_cve_ids_needing_kev_check(
    conn: Any, *, limit: int | None = None, since: str | None = None
) -> list[str]:
    if not _table_exists(conn, "cves"):
        return []
    columns = _table_columns(conn, "cves")
    if "kev_checked_at" not in columns:
        return []
    where: list[str] = []
    params: list[object] = []
    # "Need" is defined as never-checked CVEs (plus optional recency filter).
    # Re-checks are scheduled explicitly by job payload when needed.
    where.append("c.kev_checked_at IS NULL")
    if since:
        where.append("COALESCE(c.last_modified_at, c.published_at) >= %s")
        params.append(since)
    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql
    sql = f"""
        SELECT c.cve_id
        FROM cves c
        {where_sql}
        ORDER BY COALESCE(c.last_modified_at, c.published_at) DESC
    """
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    cursor = conn.execute(sql, tuple(params))
    return [row[0] for row in cursor.fetchall() if row and row[0]]



def get_batch_job_counts(conn: Any, batch_id: str) -> dict[str, int]:
    pattern = f'%\"batch_id\":\"{batch_id}\"%'
    cursor = conn.execute(
        """
        SELECT status, COUNT(*)
        FROM jobs
        WHERE job_type = 'write_article_markdown' AND payload_json LIKE %s
        GROUP BY status
        """,
        (pattern,),
    )
    counts = {"total": 0, "queued": 0, "running": 0, "succeeded": 0, "failed": 0}
    for status, count in cursor.fetchall():
        counts["total"] += count
        counts[status] = count
    return counts


def count_articles_total(conn: Any, source_id: str) -> int:
    if not _table_exists(conn, "articles"):
        return 0
    cursor = conn.execute("SELECT COUNT(*) FROM articles WHERE source_id = %s", (source_id,))
    return int(cursor.fetchone()[0] or 0)


def get_article_state_counts_by_source(conn: Any) -> dict[str, dict[str, int]]:
    if not _table_exists(conn, "articles"):
        return {}
    columns = _table_columns(conn, "articles")
    if "source_id" not in columns:
        return {}
    if "has_full_content" in columns:
        has_content_expr = "a.has_full_content = 1"
    elif "content_text" in columns:
        has_content_expr = "(a.content_text IS NOT NULL AND a.content_text != '')"
    elif "extracted_text_path" in columns:
        has_content_expr = "(a.extracted_text_path IS NOT NULL AND a.extracted_text_path != '')"
    else:
        has_content_expr = "FALSE"
    has_summary = "summary_llm" in columns
    summary_missing_expr = (
        "(a.summary_llm IS NULL OR a.summary_llm = '')" if has_summary else "FALSE"
    )
    summary_present_expr = (
        "(a.summary_llm IS NOT NULL AND a.summary_llm != '')" if has_summary else "FALSE"
    )
    if has_summary:
        new_expr = f"(NOT ({has_content_expr})) AND {summary_missing_expr}"
    else:
        new_expr = f"(NOT ({has_content_expr}))"
    cursor = conn.execute(
        f"""
        SELECT a.source_id,
               SUM(CASE WHEN {new_expr} THEN 1 ELSE 0 END) AS new_count,
               SUM(CASE WHEN ({has_content_expr}) AND {summary_missing_expr} THEN 1 ELSE 0 END) AS gathered_count,
               SUM(CASE WHEN {summary_present_expr} THEN 1 ELSE 0 END) AS summarized_count
        FROM articles a
        GROUP BY a.source_id
        """
    )
    results: dict[str, dict[str, int]] = {}
    for source_id, new_count, gathered_count, summarized_count in cursor.fetchall():
        results[str(source_id)] = {
            "new_count": int(new_count or 0),
            "gathered_count": int(gathered_count or 0),
            "summarized_count": int(summarized_count or 0),
        }
    return results


def insert_source_health_event(
    conn: Any,
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    conn: Any, source_id: str, limit: int = 50
) -> list[dict[str, object]]:
    cursor = conn.execute(
        """
        SELECT id, source_id, ts, ok, found_count, accepted_count, seen_count,
               filtered_count, error_count, last_error, duration_ms
        FROM source_health_history
        WHERE source_id = %s
        ORDER BY ts DESC
        LIMIT %s
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


def count_articles_since(conn: Any, source_id: str, since_iso: str) -> int:
    cursor = conn.execute(
        """
        SELECT COUNT(*)
        FROM articles
        WHERE source_id = %s AND published_at >= %s
        """,
        (source_id, since_iso),
    )
    return int(cursor.fetchone()[0])


def get_last_source_run(conn: Any, source_id: str) -> dict[str, object] | None:
    cursor = conn.execute(
        """
        SELECT started_at, items_accepted, status, error
        FROM source_runs
        WHERE source_id = %s
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


def list_articles_per_day(conn: Any, since_day: str) -> list[dict[str, object]]:
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
        WHERE {date_expr} >= %s
        GROUP BY day
        ORDER BY day
        """,
        (since_day,),
    )
    return [{"day": row[0], "count": row[1]} for row in cursor.fetchall() if row[0]]


def get_source_stats(
    conn: Any, days: int, runs: int
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
            "SELECT COUNT(*) FROM articles WHERE source_id = %s",
            (source_id,),
        ).fetchone()[0]
        if has_full_content_col:
            full_content = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = %s AND has_full_content = 1",
                (source_id,),
            ).fetchone()[0]
        elif extracted_text_col:
            full_content = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = %s AND extracted_text_path IS NOT NULL",
                (source_id,),
            ).fetchone()[0]
        else:
            full_content = 0
        summaries = (
            conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = %s AND summary_llm IS NOT NULL",
                (source_id,),
            ).fetchone()[0]
            if has_summary_col
            else 0
        )
        if brief_day_col:
            recent_articles = conn.execute(
                "SELECT COUNT(*) FROM articles WHERE source_id = %s AND brief_day >= %s",
                (source_id, since_day),
            ).fetchone()[0]
        else:
            recent_articles = conn.execute(
                """
                SELECT COUNT(*)
                FROM articles
                WHERE source_id = %s AND COALESCE(substr(published_at, 1, 10), substr(ingested_at, 1, 10)) >= %s
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
                    WHERE source_id = %s
                    ORDER BY ts DESC
                    LIMIT %s
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
                "SELECT started_at FROM source_runs WHERE source_id = %s ORDER BY started_at DESC LIMIT 1",
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
                WHERE source_id = %s AND ok = 1
                ORDER BY ts DESC
                LIMIT 1
                """,
                (source_id,),
            ).fetchone()
            last_error_row = conn.execute(
                """
                SELECT last_error
                FROM source_health_history
                WHERE source_id = %s AND ok = 0 AND last_error IS NOT NULL
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
    conn: Any,
    worker_id: str,
    allowed_types: list[str] | None = None,
    lock_timeout_seconds: int | None = None,
) -> Job | None:
    if allowed_types is not None and not allowed_types:
        return None
    for _ in range(20):
        with conn.transaction():
            stale_params: list[object] = []
            stale_type_clause = ""
            if allowed_types is not None:
                stale_placeholders = ",".join(["%s"] * len(allowed_types))
                stale_type_clause = f" AND job_type IN ({stale_placeholders})"
                stale_params.extend(allowed_types)
            if lock_timeout_seconds is not None:
                cutoff = utc_now_iso_offset(seconds=-lock_timeout_seconds)
                conn.execute(
                    f"""
                    UPDATE jobs
                    SET status = 'queued',
                        locked_by = NULL,
                        locked_at = NULL,
                        started_at = NULL,
                        error = 'stale_lock_requeued'
                    WHERE status = 'running'
                      AND locked_at IS NOT NULL
                      AND locked_at < %s
                      {stale_type_clause}
                    """,
                    tuple([cutoff] + stale_params),
                )
            params: list[object] = []
            type_clause = ""
            if allowed_types is not None:
                placeholders = ",".join(["%s"] * len(allowed_types))
                type_clause = f" AND job_type IN ({placeholders})"
                params.extend(allowed_types)
            now = utc_now_iso()
            cursor = conn.execute(
                f"""
                WITH next_job AS (
                    SELECT id
                    FROM jobs
                    WHERE status = 'queued' AND locked_by IS NULL {type_clause}
                    ORDER BY priority DESC, requested_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE jobs
                SET status = 'running', started_at = %s, locked_by = %s, locked_at = %s
                WHERE id IN (SELECT id FROM next_job)
                RETURNING id, job_type, status, payload_json, result_json, requested_at, started_at,
                          finished_at, locked_by, locked_at, error, priority
                """,
                tuple(params + [now, worker_id, now]),
            )
            row = cursor.fetchone()
            if not row:
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
                    SET status = 'queued',
                        requested_at = %s,
                        started_at = NULL,
                        locked_by = NULL,
                        locked_at = NULL
                    WHERE id = %s AND status = 'running'
                    """,
                    (not_before, row[0]),
                )
                continue
            job = _row_to_job(row)
            return Job(
                id=job.id,
                job_type=job.job_type,
                status="running",
                priority=job.priority,
                payload=job.payload,
                result=job.result,
                requested_at=job.requested_at,
                started_at=now,
                finished_at=job.finished_at,
                locked_by=worker_id,
                locked_at=now,
                error=job.error,
            )
    return None


def try_acquire_lease(
    conn: Any,
    lease_name: str,
    holder: str,
    ttl_seconds: int,
) -> bool:
    key = _lease_key(lease_name)
    cursor = conn.execute("SELECT pg_try_advisory_lock(%s)", (key,))
    row = cursor.fetchone()
    return bool(row and row[0])


def release_lease(conn: Any, lease_name: str, holder: str) -> bool:
    key = _lease_key(lease_name)
    cursor = conn.execute("SELECT pg_advisory_unlock(%s)", (key,))
    row = cursor.fetchone()
    return bool(row and row[0])


def _lease_key(lease_name: str) -> int:
    digest = hashlib.sha256(lease_name.encode("utf-8")).digest()
    return struct.unpack(">q", digest[:8])[0]



def complete_job(
    conn: Any, job_id: str, result: dict[str, object] | None = None
) -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'succeeded', finished_at = %s, error = NULL, result_json = %s
        WHERE id = %s AND status = 'running'
        """,
        (now, json_dumps(result) if result else None, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def fail_job(conn: Any, job_id: str, error: str) -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'failed', finished_at = %s, error = %s
        WHERE id = %s AND status = 'running'
        """,
        (now, error, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def fail_job_force(conn: Any, job_id: str, error: str) -> bool:
    now = utc_now_iso()
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'failed', finished_at = %s, error = %s
        WHERE id = %s AND status != 'succeeded'
        """,
        (now, error, job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def requeue_job(
    conn: Any,
    job_id: str,
    payload: dict[str, object],
    requested_at: str,
) -> bool:
    cursor = conn.execute(
        """
        UPDATE jobs
        SET status = 'queued',
            requested_at = %s,
            payload_json = %s,
            result_json = NULL,
            started_at = NULL,
            finished_at = NULL,
            locked_by = NULL,
            locked_at = NULL,
            error = NULL
        WHERE id = %s AND status = 'running'
        """,
        (requested_at, json_dumps(payload), job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def _row_to_source(row: tuple) -> Source:
    if len(row) == 12:
        (
            source_id,
            name,
            enabled,
            base_url,
            topic_key,
            default_frequency_minutes,
            kind,
            url,
            pause_until,
            paused_reason,
            robots_notes,
            overrides_raw,
        ) = row
    else:
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
            overrides_raw,
        ) = row
        kind = None
        url = None
    overrides = None
    if overrides_raw is not None:
        if isinstance(overrides_raw, dict):
            overrides = overrides_raw
        else:
            try:
                overrides = json.loads(overrides_raw)
            except (TypeError, json.JSONDecodeError):
                overrides = None
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
        overrides=overrides,
        kind=kind,
        url=url,
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
        priority,
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
        priority=int(priority or 0),
        payload=payload,
        result=result,
        requested_at=requested_at,
        started_at=started_at,
        finished_at=finished_at,
        locked_by=locked_by,
        locked_at=locked_at,
        error=error,
    )


def _has_pending_job(conn: Any, job_type: str) -> bool:
    cursor = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE job_type = %s AND status IN ('queued', 'running')
        LIMIT 1
        """,
        (job_type,),
    )
    return cursor.fetchone() is not None


def _get_pending_job_id_with_payload(
    conn: Any, job_type: str, payload_json: str
) -> str | None:
    cursor = conn.execute(
        """
        SELECT id FROM jobs
        WHERE job_type = %s AND status IN ('queued', 'running') AND payload_json = %s
        ORDER BY requested_at DESC
        LIMIT 1
        """,
        (job_type, payload_json),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _get_latest_job_id(conn: Any, job_type: str) -> str:
    cursor = conn.execute(
        """
        SELECT id FROM jobs
        WHERE job_type = %s
        ORDER BY requested_at DESC
        LIMIT 1
        """,
        (job_type,),
    )
    row = cursor.fetchone()
    return row[0] if row else _new_job_id()


def _new_job_id() -> str:
    return f"job_{uuid.uuid4().hex}"


def _get_article_id(conn: Any, source_id: str, stable_id: str) -> int | None:
    cursor = conn.execute(
        "SELECT id FROM articles WHERE source_id = %s AND stable_id = %s",
        (source_id, stable_id),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _brief_day_from(value: str) -> str:
    tz_name = os.environ.get("SV_APP_TIMEZONE") or "America/New_York"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(tz).date().isoformat()
    except ValueError:
        return datetime.now(tz).date().isoformat()


def get_article_by_id(conn: Any, article_id: int) -> dict[str, object] | None:
    if not _table_exists(conn, "articles"):
        return None
    columns = _table_columns(conn, "articles")
    wanted = [
        "id",
        "source_id",
        "stable_id",
        "original_url",
        "normalized_url",
        "title",
        "published_at",
        "published_at_source",
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
        "context_llm",
        "context_model",
        "context_generated_at",
        "context_error",
        "brief_day",
        "has_full_content",
        "extracted_text_path",
        "raw_html_path",
        "meta_json",
        "created_at",
        "c.updated_at",
    ]
    selected = [name for name in wanted if name in columns]
    if "id" not in selected:
        return None
    cursor = conn.execute(
        f"SELECT {', '.join(selected)} FROM articles WHERE id = %s",
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
    suppressed = _meta_is_suppressed(article.get("meta_json"))
    return {
        "id": article.get("id"),
        "source_id": article.get("source_id"),
        "stable_id": article.get("stable_id"),
        "original_url": article.get("original_url"),
        "normalized_url": article.get("normalized_url"),
        "title": article.get("title"),
        "published_at": article.get("published_at"),
        "published_at_source": article.get("published_at_source"),
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
        "context_llm": article.get("context_llm"),
        "context_model": article.get("context_model"),
        "context_generated_at": article.get("context_generated_at"),
        "context_error": article.get("context_error"),
        "brief_day": article.get("brief_day"),
        "has_full_content": has_full_content,
        "meta_json": article.get("meta_json"),
        "suppressed": suppressed,
        "created_at": article.get("created_at"),
        "updated_at": article.get("updated_at"),
    }


def get_article_tags(conn: Any, article_id: int) -> list[str]:
    if not _table_exists(conn, "article_tags"):
        return []
    cursor = conn.execute(
        "SELECT tag FROM article_tags WHERE article_id = %s ORDER BY tag",
        (article_id,),
    )
    return [row[0] for row in cursor.fetchall() if row and row[0]]


def list_article_ids_missing_content(conn: Any, source_id: str) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    clauses: list[str] = ["source_id = %s"]
    params: list[object] = [source_id]
    url_parts: list[str] = []
    if "original_url" in columns:
        url_parts.append("(original_url IS NOT NULL AND original_url != '')")
    if "normalized_url" in columns:
        url_parts.append("(normalized_url IS NOT NULL AND normalized_url != '')")
    if url_parts:
        clauses.append("(" + " OR ".join(url_parts) + ")")
    if "has_full_content" in columns and "content_text" in columns:
        clauses.append("(has_full_content = 0 AND (content_text IS NULL OR content_text = ''))")
    elif "has_full_content" in columns and "extracted_text_path" in columns:
        clauses.append("(has_full_content = 0 AND (extracted_text_path IS NULL OR extracted_text_path = ''))")
    elif "has_full_content" in columns:
        clauses.append("has_full_content = 0")
    elif "content_text" in columns:
        clauses.append("(content_text IS NULL OR content_text = '')")
    elif "extracted_text_path" in columns:
        clauses.append("(extracted_text_path IS NULL OR extracted_text_path = '')")
    if "content_error" in columns:
        error_terminal_clause = (
            "content_error IN ('http_404','http_410','stale_older_than_week','max_retries_exceeded') "
            "OR content_error LIKE '%%HTTP Error 404%%' "
            "OR content_error LIKE '%%HTTP Error 410%%' "
            "OR content_error LIKE 'fetch_failed:HTTP Error 30%%' "
            "OR content_error LIKE 'fetch_failed:HTTP Error 401%%' "
            "OR content_error LIKE 'fetch_failed:HTTP Error 403%%'"
        )
        clauses.append(f"(content_error IS NULL OR NOT ({error_terminal_clause}))")
    where_sql = " AND ".join(clauses)
    cursor = conn.execute(
        f"SELECT id FROM articles WHERE {where_sql} ORDER BY ingested_at DESC",
        params,
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_missing_content_all(conn: Any, limit: int | None = None) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    clauses: list[str] = []
    params: list[object] = []
    url_parts: list[str] = []
    if "original_url" in columns:
        url_parts.append("(original_url IS NOT NULL AND original_url != '')")
    if "normalized_url" in columns:
        url_parts.append("(normalized_url IS NOT NULL AND normalized_url != '')")
    if url_parts:
        clauses.append("(" + " OR ".join(url_parts) + ")")
    if "has_full_content" in columns and "content_text" in columns:
        clauses.append("(has_full_content = 0 AND (content_text IS NULL OR content_text = ''))")
    elif "has_full_content" in columns and "extracted_text_path" in columns:
        clauses.append("(has_full_content = 0 AND (extracted_text_path IS NULL OR extracted_text_path = ''))")
    elif "has_full_content" in columns:
        clauses.append("has_full_content = 0")
    elif "content_text" in columns:
        clauses.append("(content_text IS NULL OR content_text = '')")
    elif "extracted_text_path" in columns:
        clauses.append("(extracted_text_path IS NULL OR extracted_text_path = '')")
    if "content_error" in columns:
        error_terminal_clause = (
            "content_error IN ('http_404','http_410','stale_older_than_week','max_retries_exceeded') "
            "OR content_error LIKE '%%HTTP Error 404%%' "
            "OR content_error LIKE '%%HTTP Error 410%%'"
            "OR content_error LIKE 'fetch_failed:HTTP Error 30%%' "
            "OR content_error LIKE 'fetch_failed:HTTP Error 401%%' "
            "OR content_error LIKE 'fetch_failed:HTTP Error 403%%'"
        )
        clauses.append(f"(content_error IS NULL OR NOT ({error_terminal_clause}))")
    where_sql = " AND ".join(clauses) if clauses else "1=1"
    limit_sql = ""
    if limit is not None:
        limit_sql = " LIMIT %s"
        params.append(int(limit))
    cursor = conn.execute(
        f"SELECT id FROM articles WHERE {where_sql} ORDER BY ingested_at DESC{limit_sql}",
        params,
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_ready_for_summary_all(conn: Any) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "summary_llm" not in columns:
        return []
    clauses: list[str] = ["(summary_llm IS NULL OR summary_llm = '')"]
    if "has_full_content" in columns:
        clauses.append("has_full_content = 1")
    elif "content_text" in columns:
        clauses.append("(content_text IS NOT NULL AND content_text != '')")
    elif "extracted_text_path" in columns:
        clauses.append("(extracted_text_path IS NOT NULL AND extracted_text_path != '')")
    where_sql = " AND ".join(clauses)
    cursor = conn.execute(
        f"SELECT id FROM articles WHERE {where_sql} ORDER BY ingested_at DESC"
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_ready_for_context_all(conn: Any, limit: int = 200) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "context_llm" not in columns:
        return []
    clauses: list[str] = ["(context_llm IS NULL OR context_llm = '')"]
    content_ready = []
    if "has_full_content" in columns:
        content_ready.append("has_full_content = 1")
    if "content_text" in columns:
        content_ready.append("(content_text IS NOT NULL AND content_text != '')")
    if "extracted_text_path" in columns:
        content_ready.append("(extracted_text_path IS NOT NULL AND extracted_text_path != '')")
    if not content_ready:
        return []
    clauses.append("(" + " OR ".join(content_ready) + ")")
    if "content_error" in columns:
        error_404_clause = (
            "content_error IN ('http_404','http_410') "
            "OR content_error LIKE '%%HTTP Error 404%%' "
            "OR content_error LIKE '%%HTTP Error 410%%'"
        )
        error_stale_clause = "content_error = 'stale_older_than_week'"
        clauses.append(f"(content_error IS NULL OR NOT ({error_404_clause} OR {error_stale_clause}))")
    where_sql = " AND ".join(clauses)
    cursor = conn.execute(
        f"SELECT id FROM articles WHERE {where_sql} ORDER BY ingested_at DESC LIMIT %s",
        (int(limit),),
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_with_content_error_all(conn: Any) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "content_error" not in columns:
        return []
    url_parts: list[str] = []
    if "original_url" in columns:
        url_parts.append("(original_url IS NOT NULL AND original_url != '')")
    if "normalized_url" in columns:
        url_parts.append("(normalized_url IS NOT NULL AND normalized_url != '')")
    where_sql = "(content_error IS NOT NULL AND content_error != '')"
    if url_parts:
        where_sql = where_sql + " AND (" + " OR ".join(url_parts) + ")"
    cursor = conn.execute(
        f"SELECT id FROM articles WHERE {where_sql} ORDER BY ingested_at DESC"
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_missing_summary(conn: Any, source_id: str) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "summary_llm" not in columns:
        return []
    cursor = conn.execute(
        """
        SELECT id FROM articles
        WHERE source_id = %s AND (summary_llm IS NULL OR summary_llm = '')
        ORDER BY ingested_at DESC
        """,
        (source_id,),
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_missing_context_pack(conn: Any, limit: int | None = 200) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "context_llm" not in columns:
        return []
    clauses: list[str] = ["(context_llm IS NULL OR context_llm = '')"]
    content_ready = []
    if "has_full_content" in columns:
        content_ready.append("has_full_content = 1")
    if "content_text" in columns:
        content_ready.append("(content_text IS NOT NULL AND content_text != '')")
    if "extracted_text_path" in columns:
        content_ready.append("(extracted_text_path IS NOT NULL AND extracted_text_path != '')")
    if content_ready:
        clauses.append("(" + " OR ".join(content_ready) + ")")
    elif "summary_llm" in columns:
        clauses.append("(summary_llm IS NOT NULL AND summary_llm != '')")
    if "content_error" in columns:
        error_404_clause = (
            "content_error IN ('http_404','http_410') "
            "OR content_error LIKE '%%HTTP Error 404%%' "
            "OR content_error LIKE '%%HTTP Error 410%%'"
        )
        error_stale_clause = "content_error = 'stale_older_than_week'"
        clauses.append(f"(content_error IS NULL OR NOT ({error_404_clause} OR {error_stale_clause}))")
    where_sql = " AND ".join(clauses)
    limit_sql = ""
    params: list[object] = []
    if limit is not None:
        limit_sql = " LIMIT %s"
        params.append(int(limit))
    cursor = conn.execute(
        f"""
        SELECT id FROM articles
        WHERE {where_sql}
        ORDER BY ingested_at DESC{limit_sql}
        """,
        params,
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_ready_for_summary(conn: Any, source_id: str) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "summary_llm" not in columns:
        return []
    clauses: list[str] = ["source_id = %s", "(summary_llm IS NULL OR summary_llm = '')"]
    if "has_full_content" in columns:
        clauses.append("has_full_content = 1")
    elif "content_text" in columns:
        clauses.append("(content_text IS NOT NULL AND content_text != '')")
    elif "extracted_text_path" in columns:
        clauses.append("(extracted_text_path IS NOT NULL AND extracted_text_path != '')")
    where_sql = " AND ".join(clauses)
    cursor = conn.execute(
        f"""
        SELECT id FROM articles
        WHERE {where_sql}
        ORDER BY ingested_at DESC
        """,
        (source_id,),
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_ready_for_summary(conn: Any, source_id: str) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    columns = _table_columns(conn, "articles")
    if "summary_llm" not in columns:
        return []
    clauses: list[str] = ["source_id = %s", "(summary_llm IS NULL OR summary_llm = '')"]
    if "has_full_content" in columns:
        clauses.append("has_full_content = 1")
    elif "content_text" in columns:
        clauses.append("(content_text IS NOT NULL AND content_text != '')")
    elif "extracted_text_path" in columns:
        clauses.append("(extracted_text_path IS NOT NULL AND extracted_text_path != '')")
    where_sql = " AND ".join(clauses)
    cursor = conn.execute(
        f"""
        SELECT id FROM articles
        WHERE {where_sql}
        ORDER BY ingested_at DESC
        """,
        (source_id,),
    )
    return [int(row[0]) for row in cursor.fetchall()]


def list_article_ids_for_source_since(
    conn: Any, source_id: str, since_iso: str
) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    cursor = conn.execute(
        """
        SELECT id FROM articles
        WHERE source_id = %s AND ingested_at >= %s
        ORDER BY ingested_at DESC
        """,
        (source_id, since_iso),
    )
    return [int(row[0]) for row in cursor.fetchall()]


def _load_text_file(path: str, limit: int = 250_000) -> str | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = handle.read(limit + 1)
            return data[:limit]
    except OSError:
        return None


def list_article_tags(conn: Any) -> list[dict[str, object]]:
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


def upsert_vendor(conn: Any, vendor_display: str) -> int:
    vendor_norm = normalize_name(vendor_display)
    if not vendor_norm or str(vendor_display or "").strip().lower() in {"unknown", "n/a", "none", "null"}:
        raise ValueError("vendor_unknown")
    display = vendor_display.strip() or vendor_norm.replace("_", " ").title()
    now = utc_now_iso()
    try:
        conn.execute(
            """
            INSERT INTO vendors (name_norm, display_name, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT(name_norm) DO UPDATE SET display_name = excluded.display_name
            """,
            (vendor_norm, display, now),
        )
    except pg_errors.UniqueViolation:
        conn.rollback()
        _reset_serial_sequence(conn, "vendors", "id")
        conn.execute(
            """
            INSERT INTO vendors (name_norm, display_name, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT(name_norm) DO UPDATE SET display_name = excluded.display_name
            """,
            (vendor_norm, display, now),
        )
    row = conn.execute(
        "SELECT id FROM vendors WHERE name_norm = %s",
        (vendor_norm,),
    ).fetchone()
    conn.commit()
    return int(row[0])


def _normalize_vendor_display(value: object) -> str | None:
    vendor = str(value or "").strip()
    if not vendor:
        return None
    if vendor.lower() in {"unknown", "n/a", "none", "null"}:
        return None
    return vendor


def get_vendor_id_by_name(conn: Any, display_name: str) -> int | None:
    if not _table_exists(conn, "vendors"):
        return None
    norm = normalize_name(display_name)
    if not norm:
        return None
    row = conn.execute(
        "SELECT id FROM vendors WHERE name_norm = %s",
        (norm,),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def get_product_id_by_vendor_name(conn: Any, vendor_id: int, display_name: str) -> int | None:
    if not _table_exists(conn, "products"):
        return None
    norm = normalize_name(display_name)
    if not norm:
        return None
    row = conn.execute(
        "SELECT id FROM products WHERE vendor_id = %s AND name_norm = %s",
        (vendor_id, norm),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def get_threat_actor_id_by_key(conn: Any, actor_key: str) -> int | None:
    if not _table_exists(conn, "threat_actors"):
        return None
    key = (actor_key or "").strip()
    if not key:
        return None
    row = conn.execute(
        "SELECT id FROM threat_actors WHERE actor_key = %s",
        (key,),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def upsert_threat_actor(
    conn: Any,
    actor_key: str | None,
    display_name: str,
    actor_type: str,
    country: str | None = None,
    confidence: int | None = None,
) -> int:
    display = (display_name or "").strip()
    if not display or display.lower() in {"unknown", "n/a", "none", "null"}:
        raise ValueError("actor_unknown")
    key = (actor_key or "").strip() or slugify(display)
    actor_kind = (actor_type or "").strip() or "unknown"
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO threat_actors (
            actor_key, display_name, actor_type, country, confidence,
            first_seen, last_seen, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(actor_key) DO UPDATE SET
            display_name = excluded.display_name,
            actor_type = excluded.actor_type,
            country = excluded.country,
            confidence = excluded.confidence,
            last_seen = excluded.last_seen,
            updated_at = excluded.updated_at,
            first_seen = COALESCE(threat_actors.first_seen, excluded.first_seen)
        """,
        (key, display, actor_kind, country, confidence, now, now, now, now),
    )
    row = conn.execute(
        "SELECT id FROM threat_actors WHERE actor_key = %s",
        (key,),
    ).fetchone()
    conn.commit()
    return int(row[0])


def add_threat_actor_alias(conn: Any, actor_id: int, alias: str) -> None:
    alias_clean = (alias or "").strip()
    if not alias_clean:
        return
    conn.execute(
        """
        INSERT INTO threat_actor_aliases (actor_id, alias)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (actor_id, alias_clean),
    )
    conn.commit()


def link_article_threat_actor(conn: Any, article_id: int, actor_id: int) -> None:
    conn.execute(
        """
        INSERT INTO article_threat_actors (article_id, actor_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (article_id, actor_id),
    )
    conn.commit()


def link_cve_threat_actor(conn: Any, cve_id: str, actor_id: int) -> None:
    conn.execute(
        """
        INSERT INTO cve_threat_actors (cve_id, actor_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (cve_id, actor_id),
    )
    conn.commit()


def _fetch_threat_actor_aliases(conn: Any, actor_ids: list[int]) -> dict[int, list[str]]:
    if not actor_ids or not _table_exists(conn, "threat_actor_aliases"):
        return {}
    placeholders = ",".join(["%s"] * len(actor_ids))
    cursor = conn.execute(
        f"""
        SELECT actor_id, alias
        FROM threat_actor_aliases
        WHERE actor_id IN ({placeholders})
        ORDER BY alias
        """,
        actor_ids,
    )
    mapping: dict[int, list[str]] = {}
    for actor_id, alias in cursor.fetchall():
        mapping.setdefault(int(actor_id), []).append(alias)
    return mapping


def get_article_threat_actors(conn: Any, article_id: int) -> list[dict[str, object]]:
    if not _table_exists(conn, "article_threat_actors"):
        return []
    cursor = conn.execute(
        """
        SELECT ta.id, ta.actor_key, ta.display_name, ta.actor_type, ta.country, ta.confidence
        FROM article_threat_actors ata
        JOIN threat_actors ta ON ta.id = ata.actor_id
        WHERE ata.article_id = %s
        ORDER BY ta.display_name
        """,
        (article_id,),
    )
    rows = cursor.fetchall()
    actor_ids = [int(row[0]) for row in rows]
    aliases = _fetch_threat_actor_aliases(conn, actor_ids)
    return [
        {
            "actor_id": int(row[0]),
            "actor_key": row[1],
            "display_name": row[2],
            "actor_type": row[3],
            "country": row[4],
            "confidence": row[5],
            "aliases": aliases.get(int(row[0]), []),
        }
        for row in rows
    ]


def get_cve_threat_actors(conn: Any, cve_id: str) -> list[dict[str, object]]:
    if not _table_exists(conn, "cve_threat_actors"):
        return []
    cursor = conn.execute(
        """
        SELECT ta.id, ta.actor_key, ta.display_name, ta.actor_type, ta.country, ta.confidence
        FROM cve_threat_actors cta
        JOIN threat_actors ta ON ta.id = cta.actor_id
        WHERE cta.cve_id = %s
        ORDER BY ta.display_name
        """,
        (cve_id,),
    )
    rows = cursor.fetchall()
    actor_ids = [int(row[0]) for row in rows]
    aliases = _fetch_threat_actor_aliases(conn, actor_ids)
    return [
        {
            "actor_id": int(row[0]),
            "actor_key": row[1],
            "display_name": row[2],
            "actor_type": row[3],
            "country": row[4],
            "confidence": row[5],
            "aliases": aliases.get(int(row[0]), []),
        }
        for row in rows
    ]

def list_cve_tags(conn: Any, cve_id: str) -> list[str]:
    return []


def list_article_cve_tags(conn: Any, article_ids: list[int]) -> dict[int, list[str]]:
    if not article_ids:
        return {}
    return {}


def delete_vendor_product_tags(conn: Any) -> int:
    removed = 0
    if _table_exists(conn, "article_tags"):
        cursor = conn.execute(
            """
            DELETE FROM article_tags
            WHERE tag LIKE 'vendor:%%' OR tag LIKE 'product:%%'
            """,
        )
        removed += int(cursor.rowcount or 0)
    if _table_exists(conn, "cve_tags"):
        cursor = conn.execute(
            """
            DELETE FROM cve_tags
            WHERE tag LIKE 'vendor:%%' OR tag LIKE 'product:%%'
            """,
        )
        removed += int(cursor.rowcount or 0)
    conn.commit()
    return removed



def upsert_product(
    conn: Any, vendor_id: int, product_display: str
) -> tuple[int, str]:
    product_norm = normalize_name(product_display)
    if not product_norm or str(product_display or "").strip().lower() in {"unknown", "n/a", "none", "null"}:
        raise ValueError("product_unknown")
    vendor_row = conn.execute(
        "SELECT name_norm FROM vendors WHERE id = %s",
        (vendor_id,),
    ).fetchone()
    if not vendor_row or not vendor_row[0]:
        raise ValueError("vendor_missing")
    vendor_norm = vendor_row[0]
    product_key = f"{vendor_norm}:{product_norm}"
    display = product_display.strip() or product_norm.replace("_", " ").title()
    now = utc_now_iso()
    try:
        conn.execute(
            """
            INSERT INTO products (vendor_id, name_norm, display_name, product_key, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(vendor_id, name_norm) DO UPDATE SET display_name = excluded.display_name
            """,
            (vendor_id, product_norm, display, product_key, now),
        )
    except pg_errors.UniqueViolation:
        conn.rollback()
        _reset_serial_sequence(conn, "products", "id")
        conn.execute(
            """
            INSERT INTO products (vendor_id, name_norm, display_name, product_key, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(vendor_id, name_norm) DO UPDATE SET display_name = excluded.display_name
            """,
            (vendor_id, product_norm, display, product_key, now),
        )
    row = conn.execute(
        "SELECT id, product_key FROM products WHERE vendor_id = %s AND name_norm = %s",
        (vendor_id, product_norm),
    ).fetchone()
    conn.commit()
    return int(row[0]), str(row[1])


def link_cve_product(
    conn: Any,
    cve_id: str,
    product_id: int,
    source: str = "nvd",
    evidence: dict[str, object] | None = None,
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO cve_products (cve_id, product_id, source, evidence_json, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (cve_id, product_id, source, json_dumps(evidence) if evidence else None, now),
    )
    conn.commit()


def backfill_products_from_cves(
    conn: Any, limit: int | None = None
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
        + (" LIMIT %s" if limit else ""),
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
        for vendor_display, product_display in pairs:
            vendor_norm = _normalize_vendor_display(vendor_display)
            if not vendor_norm:
                continue
            if not product_display:
                continue
            vendor_id = upsert_vendor(conn, vendor_norm)
            product_id, _ = upsert_product(conn, vendor_id, product_display)
            link_cve_product(
                conn,
                cve_id,
                product_id,
                evidence={"cpes": cpes[:25]},
            )
            stats["links_created"] += 1
    return stats




def list_products_for_article(conn: Any, article_id: int) -> list[dict[str, str]]:
    if not _table_exists(conn, "article_products") or not _table_exists(conn, "products"):
        return []
    cursor = conn.execute(
        """
        SELECT p.id,
               p.product_key,
               v.name_norm AS vendor_norm,
               p.name_norm AS product_norm,
               p.display_name,
               v.display_name AS vendor_display
        FROM article_products ap
        JOIN products p ON p.id = ap.product_id
        JOIN vendors v ON v.id = p.vendor_id
        WHERE ap.article_id = %s
        ORDER BY v.name_norm, p.name_norm
        """,
        (article_id,),
    )
    items = []
    for product_id, product_key, vendor_norm, product_norm, display_name, vendor_display in cursor.fetchall():
        vendor_label = vendor_display or vendor_norm or ""
        product_label = display_name or product_norm or product_key or ""
        label = f"{vendor_label} — {product_label}" if vendor_label and product_label else (product_label or vendor_label)
        items.append(
            {
                "product_id": product_id,
                "product_key": product_key,
                "vendor": vendor_norm or "",
                "product": product_norm or "",
                "display_name": label or product_key or "",
                "vendor_display": vendor_display or vendor_norm or "",
                "product_display": display_name or product_norm or product_key or "",
            }
        )
    return items


def list_products_with_article_counts(conn: Any, limit: int = 200) -> list[dict[str, object]]:
    if not _table_exists(conn, "products"):
        return []
    article_join = ""
    if _table_exists(conn, "article_products"):
        article_join = "LEFT JOIN article_products ap ON ap.product_id = p.id"
    cursor = conn.execute(
        f"""
        SELECT p.id, p.product_key, p.display_name, v.display_name,
               COUNT(ap.article_id) as article_count
        FROM products p
        LEFT JOIN vendors v ON v.id = p.vendor_id
        {article_join}
        GROUP BY p.id, v.display_name
        ORDER BY article_count DESC, v.display_name, p.display_name
        LIMIT %s
        """,
        (limit,),
    )
    items = []
    for row in cursor.fetchall():
        items.append(
            {
                "product_id": row[0],
                "product_key": row[1],
                "product_name": row[2],
                "vendor_name": row[3],
                "article_count": int(row[4] or 0),
            }
        )
    return items
def query_products(
    conn: Any,
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
        where.append("(LOWER(p.display_name) LIKE %s OR LOWER(p.name_norm) LIKE %s)")
        params.extend([like, like])
    if vendor:
        like = f"%{vendor.lower()}%"
        where.append("(LOWER(v.display_name) LIKE %s OR LOWER(v.name_norm) LIKE %s)")
        params.extend([like, like])
    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql

    count_cursor = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM products p
        LEFT JOIN vendors v ON v.id = p.vendor_id
        {where_sql}
        """,
        params,
    )
    total = count_cursor.fetchone()[0]

    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        f"""
        SELECT
            p.id,
            p.product_key,
            p.display_name,
            v.display_name,
            (
                COALESCE((SELECT COUNT(*) FROM article_products ap WHERE ap.product_id = p.id), 0)
                + COALESCE((SELECT COUNT(*) FROM cve_products cp WHERE cp.product_id = p.id), 0)
            ) AS link_count
        FROM products p
        LEFT JOIN vendors v ON v.id = p.vendor_id
        {where_sql}
        ORDER BY v.display_name, p.display_name
        LIMIT %s OFFSET %s
        """,
        [*params, page_size, offset],
    )
    items = [
        {
            "product_id": row[0],
            "product_key": row[1],
            "product_name": row[2],
            "vendor_name": row[3],
            "link_count": int(row[4] or 0),
        }
        for row in cursor.fetchall()
    ]
    return items, total


def get_product(conn: Any, product_key: str) -> dict[str, object] | None:
    if not _table_exists(conn, "products"):
        return None
    row = conn.execute(
        """
        SELECT p.id, p.product_key, p.display_name, v.display_name, v.name_norm
        FROM products p
        LEFT JOIN vendors v ON v.id = p.vendor_id
        WHERE p.product_key = %s
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
    conn: Any,
    product_id: int,
    severity_min: float | None,
    severities: list[str] | None,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "cve_products") or not _table_exists(conn, "cves"):
        return [], 0
    columns = _table_columns(conn, "cves")
    has_kev = _table_exists(conn, "cve_kev") and "kev_cve_id" in columns
    where: list[str] = ["cp.product_id = %s"]
    params: list[object] = [product_id]
    if severity_min is not None:
        where.append("c.preferred_base_score >= %s")
        params.append(severity_min)
    if severities:
        normalized = [value.upper() for value in severities]
        placeholders = ",".join("%s" for _ in normalized)
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
    kev_cve_expr = "c.kev_cve_id" if "kev_cve_id" in columns else "NULL"
    kev_due_expr = "k.due_date" if has_kev else "NULL"
    cursor = conn.execute(
        f"""
        SELECT c.cve_id, c.published_at, c.last_modified_at, preferred_base_score,
               c.preferred_base_severity, c.description_text,
               {kev_cve_expr}, {kev_due_expr}
        FROM cve_products cp
        JOIN cves c ON c.cve_id = cp.cve_id
        { "LEFT JOIN cve_kev k ON k.cve_id = c.kev_cve_id" if has_kev else "" }
        WHERE {where_sql}
        ORDER BY c.last_modified_at DESC
        LIMIT %s OFFSET %s
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
            "kev_cve_id": row[6],
            "kev_due_date": row[7],
            "kev_known_exploited": bool(row[6]),
        }
        for row in cursor.fetchall()
    ]
    return items, total


def get_product_facets(conn: Any, product_id: int) -> dict[str, int]:
    if not _table_exists(conn, "cve_products") or not _table_exists(conn, "cves"):
        return {}
    cursor = conn.execute(
        """
        SELECT COALESCE(c.preferred_base_severity, 'UNKNOWN') as severity, COUNT(*)
        FROM cve_products cp
        JOIN cves c ON c.cve_id = cp.cve_id
        WHERE cp.product_id = %s
        GROUP BY severity
        """,
        (product_id,),
    )
    return {row[0]: int(row[1]) for row in cursor.fetchall()}




def link_article_product(
    conn: Any,
    article_id: int,
    product_id: int,
    source: str = "llm",
    evidence: dict[str, object] | None = None,
) -> None:
    if not _table_exists(conn, "article_products"):
        return
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO article_products (article_id, product_id, source, evidence_json, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(article_id, product_id) DO UPDATE SET
            source=excluded.source,
            evidence_json=COALESCE(excluded.evidence_json, article_products.evidence_json)
        """,
        (
            article_id,
            product_id,
            source,
            json_dumps(evidence) if evidence else None,
            now,
        ),
    )
    conn.commit()


def count_articles_for_product(conn: Any, product_id: int) -> int:
    if not _table_exists(conn, "article_products"):
        return 0
    row = conn.execute(
        "SELECT COUNT(*) FROM article_products WHERE product_id = %s",
        (product_id,),
    ).fetchone()
    return int(row[0] or 0)


def count_products_for_article(conn: Any, article_id: int) -> int:
    if not _table_exists(conn, "article_products"):
        return 0
    row = conn.execute(
        "SELECT COUNT(*) FROM article_products WHERE article_id = %s",
        (article_id,),
    ).fetchone()
    return int(row[0] or 0)


def list_articles_for_product(
    conn: Any,
    product_id: int,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "article_products") or not _table_exists(conn, "articles"):
        return [], 0
    tag_join = ""
    tag_select = "'' AS tags"
    if _table_exists(conn, "article_tags"):
        tag_join = "LEFT JOIN article_tags t ON t.article_id = a.id"
        tag_select = "COALESCE(string_agg(t.tag, ',' ORDER BY t.tag), '') AS tags"
    count_cursor = conn.execute(
        """
        SELECT COUNT(*)
        FROM article_products ap
        JOIN articles a ON a.id = ap.article_id
        WHERE ap.product_id = %s
        """,
        (product_id,),
    )
    total = count_cursor.fetchone()[0]
    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        f"""
        SELECT a.id, a.title, a.original_url, a.published_at, a.ingested_at,
               a.source_id, s.name AS source_name, a.summary_llm, a.meta_json,
               {tag_select}
        FROM article_products ap
        JOIN articles a ON a.id = ap.article_id
        LEFT JOIN sources s ON s.id = a.source_id
        {tag_join}
        WHERE ap.product_id = %s
        GROUP BY a.id, s.name
        ORDER BY COALESCE(a.published_at, a.ingested_at) DESC
        LIMIT %s OFFSET %s
        """,
        (product_id, page_size, offset),
    )
    items = []
    for row in cursor.fetchall():
        items.append(
            {
                "id": row[0],
                "title": row[1],
                "original_url": row[2],
                "published_at": row[3],
                "ingested_at": row[4],
                "source_id": row[5],
                "source_name": row[6] or "",
                "summary_llm": row[7],
                "meta_json": row[8],
                "tags": row[9] or "",
            }
        )
    return items, int(total or 0)


def count_articles_missing_products(conn: Any) -> int:
    if not _table_exists(conn, "article_products") or not _table_exists(conn, "articles"):
        return 0
    article_columns = _table_columns(conn, "articles")
    checked_clause = (
        "AND a.article_products_checked_at IS NULL"
        if "article_products_checked_at" in article_columns
        else ""
    )
    cursor = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT a.id
            FROM articles a
            LEFT JOIN article_products ap ON ap.article_id = a.id
            WHERE (a.content_text IS NOT NULL AND a.content_text != '')
            {checked_clause}
            GROUP BY a.id
            HAVING COUNT(ap.article_id) = 0
        ) t
        """
    )
    row = cursor.fetchone()
    return int(row[0] or 0)

def count_articles_missing_threat_actors(conn: Any) -> int:
    if not (_table_exists(conn, "articles") and _table_exists(conn, "article_threat_actors")):
        return 0
    article_columns = _table_columns(conn, "articles")
    checked_clause = (
        "AND a.article_threat_actors_checked_at IS NULL"
        if "article_threat_actors_checked_at" in article_columns
        else ""
    )
    cursor = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT a.id
            FROM articles a
            LEFT JOIN article_threat_actors ata ON ata.article_id = a.id
            WHERE (a.content_text IS NOT NULL AND a.content_text != '')
            {checked_clause}
            GROUP BY a.id
            HAVING COUNT(ata.article_id) = 0
        ) t
        """
    )
    row = cursor.fetchone()
    return int(row[0] or 0)

def count_cves_missing_threat_actors(conn: Any) -> int:
    if not (_table_exists(conn, "cves") and _table_exists(conn, "cve_threat_actors")):
        return 0
    cve_columns = _table_columns(conn, "cves")
    checked_clause = (
        "AND c.cve_threat_actors_checked_at IS NULL"
        if "cve_threat_actors_checked_at" in cve_columns
        else ""
    )
    cursor = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT c.cve_id
            FROM cves c
            LEFT JOIN cve_threat_actors cta ON cta.cve_id = c.cve_id
            WHERE (
                (c.description_text IS NOT NULL AND c.description_text != '')
                OR (
                    c.reference_domains_json IS NOT NULL
                    AND btrim(c.reference_domains_json) NOT IN ('', '[]', 'null')
                )
            )
            {checked_clause}
            GROUP BY c.cve_id
            HAVING COUNT(cta.cve_id) = 0
        ) t
        """
    )
    row = cursor.fetchone()
    return int(row[0] or 0)


def list_article_ids_missing_products(conn: Any, limit: int | None = 500) -> list[int]:
    if not _table_exists(conn, "article_products") or not _table_exists(conn, "articles"):
        return []
    article_columns = _table_columns(conn, "articles")
    checked_clause = (
        "AND a.article_products_checked_at IS NULL"
        if "article_products_checked_at" in article_columns
        else ""
    )
    sql = f"""
        SELECT a.id
        FROM articles a
        LEFT JOIN article_products ap ON ap.article_id = a.id
        WHERE (a.content_text IS NOT NULL AND a.content_text != '')
        {checked_clause}
        GROUP BY a.id
        HAVING COUNT(ap.article_id) = 0
        ORDER BY COALESCE(a.published_at, a.ingested_at) DESC
    """
    params: list[object] = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cursor = conn.execute(sql, tuple(params))
    return [int(row[0]) for row in cursor.fetchall()]


def infer_article_products_from_cves(
    conn: Any,
    article_id: int,
    cve_ids: list[str],
) -> dict[str, int]:
    if not cve_ids:
        return {"links": 0}
    if not _table_exists(conn, "article_products") or not _table_exists(conn, "cve_products"):
        return {"links": 0}
    placeholders = ",".join("%s" for _ in cve_ids)
    cursor = conn.execute(
        f"""
        SELECT cp.cve_id, cp.product_id
        FROM cve_products cp
        WHERE cp.cve_id IN ({placeholders})
        """,
        cve_ids,
    )
    links = 0
    seen: set[tuple[int, str]] = set()
    for cve_id, product_id in cursor.fetchall():
        if not product_id:
            continue
        key = (int(product_id), str(cve_id))
        if key in seen:
            continue
        seen.add(key)
        link_article_product(
            conn,
            article_id=article_id,
            product_id=int(product_id),
            source="cve_inferred",
            evidence={"cve_id": str(cve_id)},
        )
        links += 1
    return {"links": links}
def list_product_keys_for_cve(conn: Any, cve_id: str) -> list[str]:
    if not _table_exists(conn, "cve_products") or not _table_exists(conn, "products"):
        return []
    cursor = conn.execute(
        """
        SELECT p.product_key
        FROM cve_products cp
        JOIN products p ON p.id = cp.product_id
        WHERE cp.cve_id = %s
        ORDER BY p.product_key
        """,
        (cve_id,),
    )
    return [row[0] for row in cursor.fetchall()]


def get_product_display_by_key(conn: Any, product_key: str) -> dict[str, str] | None:
    if not _table_exists(conn, "products") or not _table_exists(conn, "vendors"):
        return None
    row = conn.execute(
        """
        SELECT p.display_name, v.display_name
        FROM products p
        LEFT JOIN vendors v ON v.id = p.vendor_id
        WHERE p.product_key = %s
        """,
        (product_key,),
    ).fetchone()
    if not row:
        return None
    return {"product": row[0], "vendor": row[1]}


def create_event(
    conn: Any,
    kind: str,
    title: str,
    severity: str | None,
    first_seen_at: str,
    last_seen_at: str,
    summary: str | None = None,
    meta: dict[str, object] | None = None,
    event_key: str | None = None,
    status: str = "open",
    occurred_at: str | None = None,
    confidence: float | None = None,
    manual: bool = False,
    visibility: str = "active",
    confidence_tier: str = "watch",
    reasons: list[str] | None = None,
    candidate: bool = False,
    lifecycle: str | None = None,
    entity: str | None = None,
    incident_date: str | None = None,
    evidence: list[str] | None = None,
    publish_state: str | None = None,
    published_at: str | None = None,
    site_slug: str | None = None,
) -> str:
    event_id = f"evt_{uuid.uuid4().hex[:12]}"
    now = utc_now_iso()
    event_columns = _table_columns(conn, "events")
    cols = [
        "id",
        "kind",
        "title",
        "summary",
        "severity",
        "created_at",
        "updated_at",
        "first_seen_at",
        "last_seen_at",
        "status",
        "meta_json",
        "event_key",
        "occurred_at",
        "summary_updated_at",
        "confidence",
        "manual",
        "is_manual",
        "visibility",
        "confidence_tier",
        "reasons",
    ]
    vals = [
        event_id,
        kind,
        title,
        summary,
        severity,
        now,
        now,
        first_seen_at,
        last_seen_at,
        status,
        json_dumps(meta) if meta else None,
        event_key,
        occurred_at,
        now if summary else None,
        confidence,
        1 if manual else 0,
        1 if manual else 0,
        visibility,
        confidence_tier,
        json_dumps(reasons or []),
    ]
    if "candidate" in event_columns:
        cols.append("candidate")
        vals.append(bool(candidate))
    if "lifecycle" in event_columns:
        cols.append("lifecycle")
        vals.append((lifecycle or "").strip() or "candidate")
    if "entity" in event_columns:
        cols.append("entity")
        vals.append(entity)
    if "incident_date" in event_columns:
        cols.append("incident_date")
        vals.append(incident_date)
    if "evidence" in event_columns:
        cols.append("evidence")
        vals.append(json_dumps(evidence or []))
    if "publish_state" in event_columns:
        cols.append("publish_state")
        vals.append((publish_state or "").strip() or "draft")
    if "published_at" in event_columns:
        cols.append("published_at")
        vals.append(published_at)
    if "site_slug" in event_columns:
        cols.append("site_slug")
        vals.append(site_slug)
    placeholders = ", ".join(["%s"] * len(cols))
    conn.execute(
        f"INSERT INTO events ({', '.join(cols)}) VALUES ({placeholders})",
        tuple(vals),
    )
    conn.commit()
    return event_id


def upsert_event_by_key(
    conn: Any,
    event_key: str,
    kind: str,
    title: str,
    severity: str | None,
    first_seen_at: str,
    last_seen_at: str,
    summary: str | None = None,
    meta: dict[str, object] | None = None,
    status: str = "open",
    occurred_at: str | None = None,
    confidence: float | None = None,
    manual: bool = False,
    visibility: str = "active",
    confidence_tier: str = "watch",
    reasons: list[str] | None = None,
    candidate: bool = False,
    lifecycle: str | None = None,
    entity: str | None = None,
    incident_date: str | None = None,
    evidence: list[str] | None = None,
    publish_state: str | None = None,
    published_at: str | None = None,
    site_slug: str | None = None,
) -> tuple[str, bool]:
    row = conn.execute(
        "SELECT id, visibility FROM events WHERE event_key = %s",
        (event_key,),
    ).fetchone()
    if row:
        event_id, current_visibility = row
        new_visibility = visibility
        if current_visibility == "suppressed" and visibility == "active":
            new_visibility = "suppressed"
        conn.execute(
            """
            UPDATE events
            SET title = %s,
                kind = %s,
                severity = %s,
                updated_at = %s,
                last_seen_at = %s,
                summary = COALESCE(%s, summary),
                summary_updated_at = CASE WHEN %s THEN %s ELSE summary_updated_at END,
                occurred_at = COALESCE(%s, occurred_at),
                confidence = COALESCE(%s, confidence),
                status = %s,
                manual = CASE WHEN %s THEN 1 ELSE manual END,
                is_manual = CASE WHEN %s THEN 1 ELSE is_manual END,
                visibility = %s,
                confidence_tier = %s,
                reasons = COALESCE(%s, reasons)
            WHERE id = %s
            """,
            (
                title,
                kind,
                severity,
                utc_now_iso(),
                last_seen_at,
                summary,
                summary is not None,
                utc_now_iso(),
                occurred_at,
                confidence,
                status,
                bool(manual),
                bool(manual),
                new_visibility,
                confidence_tier,
                json_dumps(reasons) if reasons is not None else None,
                event_id,
            ),
        )
        if candidate is not None and "candidate" in _table_columns(conn, "events"):
            conn.execute("UPDATE events SET candidate = COALESCE(%s, candidate) WHERE id = %s", (bool(candidate), event_id))
        if lifecycle is not None and "lifecycle" in _table_columns(conn, "events"):
            conn.execute(
                "UPDATE events SET lifecycle = COALESCE(%s, lifecycle) WHERE id = %s",
                ((lifecycle or "").strip() or None, event_id),
            )
        if entity is not None and "entity" in _table_columns(conn, "events"):
            conn.execute("UPDATE events SET entity = COALESCE(%s, entity) WHERE id = %s", (entity, event_id))
        if incident_date is not None and "incident_date" in _table_columns(conn, "events"):
            conn.execute("UPDATE events SET incident_date = COALESCE(%s, incident_date) WHERE id = %s", (incident_date, event_id))
        if evidence is not None and "evidence" in _table_columns(conn, "events"):
            conn.execute("UPDATE events SET evidence = COALESCE(%s, evidence) WHERE id = %s", (json_dumps(evidence), event_id))
        if publish_state is not None and "publish_state" in _table_columns(conn, "events"):
            conn.execute(
                "UPDATE events SET publish_state = COALESCE(%s, publish_state) WHERE id = %s",
                ((publish_state or "").strip() or None, event_id),
            )
        if published_at is not None and "published_at" in _table_columns(conn, "events"):
            conn.execute(
                "UPDATE events SET published_at = COALESCE(%s, published_at) WHERE id = %s",
                (published_at, event_id),
            )
        if site_slug is not None and "site_slug" in _table_columns(conn, "events"):
            conn.execute(
                "UPDATE events SET site_slug = COALESCE(%s, site_slug) WHERE id = %s",
                (site_slug, event_id),
            )
        conn.commit()
        return event_id, False
    event_id = create_event(
        conn,
        kind=kind,
        title=title,
        severity=severity,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        summary=summary,
        meta=meta,
        event_key=event_key,
        status=status,
        occurred_at=occurred_at,
        confidence=confidence,
        manual=manual,
        visibility=visibility,
        confidence_tier=confidence_tier,
        reasons=reasons,
        candidate=candidate,
        lifecycle=lifecycle,
        entity=entity,
        incident_date=incident_date,
        evidence=evidence,
        publish_state=publish_state,
        published_at=published_at,
        site_slug=site_slug,
    )
    return event_id, True


def update_event(
    conn: Any,
    event_id: str,
    *,
    title: str | None = None,
    summary: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    kind: str | None = None,
    visibility: str | None = None,
    confidence: float | None = None,
    confidence_tier: str | None = None,
    candidate: bool | None = None,
    lifecycle: str | None = None,
    entity: str | None = None,
    incident_date: str | None = None,
    reasons: list[str] | None = None,
    tags: list[str] | None = None,
    is_event: bool | None = None,
    publish_state: str | None = None,
    published_at: str | None = None,
    site_slug: str | None = None,
) -> bool:
    event_columns = _table_columns(conn, "events")
    updates: list[str] = []
    params: list[object] = []
    now = utc_now_iso()
    updates.append("updated_at = %s")
    params.append(now)
    if title is not None:
        updates.append("title = %s")
        params.append(title)
    if summary is not None:
        updates.append("summary = %s")
        params.append(summary)
        updates.append("summary_updated_at = %s")
        params.append(now)
    if severity is not None:
        updates.append("severity = %s")
        params.append(severity)
    if status is not None:
        updates.append("status = %s")
        params.append(status)
    if kind is not None:
        updates.append("kind = %s")
        params.append(kind)
    if visibility is not None and "visibility" in event_columns:
        updates.append("visibility = %s")
        params.append(visibility)
    if confidence is not None and "confidence" in event_columns:
        updates.append("confidence = %s")
        params.append(confidence)
    if confidence_tier is not None and "confidence_tier" in event_columns:
        updates.append("confidence_tier = %s")
        params.append(confidence_tier)
    if candidate is not None and "candidate" in event_columns:
        updates.append("candidate = %s")
        params.append(bool(candidate))
    if lifecycle is not None and "lifecycle" in event_columns:
        updates.append("lifecycle = %s")
        params.append((lifecycle or "").strip() or "candidate")
    if entity is not None and "entity" in event_columns:
        updates.append("entity = %s")
        params.append(entity)
    if incident_date is not None and "incident_date" in event_columns:
        updates.append("incident_date = %s")
        params.append(incident_date)
    if reasons is not None and "reasons" in event_columns:
        updates.append("reasons = %s")
        params.append(json_dumps(reasons))
    if publish_state is not None and "publish_state" in event_columns:
        updates.append("publish_state = %s")
        params.append((publish_state or "").strip() or "draft")
    if published_at is not None and "published_at" in event_columns:
        updates.append("published_at = %s")
        params.append(published_at)
    if site_slug is not None and "site_slug" in event_columns:
        updates.append("site_slug = %s")
        params.append(site_slug)

    if tags is not None or is_event is not None:
        existing = conn.execute(
            "SELECT meta_json FROM events WHERE id = %s",
            (event_id,),
        ).fetchone()
        meta = {}
        if existing and existing[0]:
            try:
                meta = json.loads(existing[0])
            except Exception:
                meta = {}
        if tags is not None:
            meta["tags"] = tags
        if is_event is not None:
            meta["is_event"] = bool(is_event)
        updates.append("meta_json = %s")
        params.append(json_dumps(meta))

    if not updates:
        return False
    params.append(event_id)
    cursor = conn.execute(
        f"UPDATE events SET {', '.join(updates)} WHERE id = %s",
        tuple(params),
    )
    conn.commit()
    return cursor.rowcount == 1


def delete_event(conn: Any, event_id: str) -> bool:
    if _table_exists(conn, "event_articles"):
        conn.execute("DELETE FROM event_articles WHERE event_id = %s", (event_id,))
    if _table_exists(conn, "event_items"):
        conn.execute("DELETE FROM event_items WHERE event_id = %s", (event_id,))
    if _table_exists(conn, "event_web_sources"):
        conn.execute("DELETE FROM event_web_sources WHERE event_id = %s", (event_id,))
    cursor = conn.execute("DELETE FROM events WHERE id = %s", (event_id,))
    conn.commit()
    return cursor.rowcount == 1


def upsert_event_item(
    conn: Any, event_id: str, item_type: str, item_key: str
) -> None:
    conn.execute(
        """
        INSERT INTO event_items (event_id, item_type, item_key, created_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (event_id, item_type, item_key, utc_now_iso()),
    )
    conn.commit()


def link_event_article(conn: Any, event_id: str, article_id: int, added_by: str) -> None:
    now = utc_now_iso()
    if _table_exists(conn, "event_articles"):
        conn.execute(
            """
            INSERT INTO event_articles (event_id, article_id, added_by, created_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (event_id, article_id, added_by, now),
        )
    else:
        upsert_event_item(conn, event_id, "article", str(article_id))
    columns = _table_columns(conn, "events")
    candidate_select = "candidate" if "candidate" in columns else "NULL AS candidate"
    entity_select = "entity" if "entity" in columns else "NULL AS entity"
    incident_select = "incident_date" if "incident_date" in columns else "NULL AS incident_date"
    evidence_select = "evidence" if "evidence" in columns else "NULL AS evidence"
    candidate_select = "e.candidate" if "candidate" in columns else "NULL AS candidate"
    entity_select = "e.entity" if "entity" in columns else "NULL AS entity"
    incident_select = "e.incident_date" if "incident_date" in columns else "NULL AS incident_date"
    evidence_select = "e.evidence" if "evidence" in columns else "NULL AS evidence"
    candidate_select = "e.candidate" if "candidate" in columns else "NULL AS candidate"
    entity_select = "e.entity" if "entity" in columns else "NULL AS entity"
    incident_select = "e.incident_date" if "incident_date" in columns else "NULL AS incident_date"
    evidence_select = "e.evidence" if "evidence" in columns else "NULL AS evidence"
    if "visibility" in columns:
        conn.execute(
            """
            UPDATE events
            SET updated_at = %s,
                last_seen_at = %s,
                visibility = 'active'
            WHERE id = %s
            """,
            (now, now, event_id),
        )
    else:
        conn.execute(
            """
            UPDATE events
            SET updated_at = %s,
                last_seen_at = %s
            WHERE id = %s
            """,
            (now, now, event_id),
        )
    conn.commit()
    rebuild_event_timeline_from_articles(conn, event_id)


def event_publish_readiness(conn: Any, event_id: str) -> dict[str, object]:
    event = get_event(conn, event_id)
    if not event:
        return {"ready": False, "reasons": ["event_not_found"], "article_count": 0, "timeline_count": 0}
    settings = get_setting(conn, "events.settings", {}) if _table_exists(conn, "settings") else {}
    if not isinstance(settings, dict):
        settings = {}
    min_articles = int(settings.get("publish_min_articles", 2) or 2)
    min_timeline = int(settings.get("publish_min_timeline_entries", 2) or 2)
    min_bullets = int(settings.get("publish_min_narrative_bullets", 3) or 3)
    min_sections = int(settings.get("publish_min_narrative_sections", 2) or 2)
    min_promoted_sources = int(settings.get("publish_min_promoted_sources", 0) or 0)
    articles = (event.get("items") or {}).get("articles") or []
    timeline = event.get("timeline") or []
    narrative = event.get("narrative") or {}
    bullets = narrative.get("bullets") if isinstance(narrative, dict) else []
    sections = narrative.get("sections") if isinstance(narrative, dict) else {}
    reasons: list[str] = []
    if len(articles) < min_articles:
        reasons.append(f"need_at_least_{min_articles}_articles")
    if len(timeline) < min_timeline:
        reasons.append(f"need_at_least_{min_timeline}_timeline_entries")
    if not isinstance(bullets, list) or len(bullets) < min_bullets:
        reasons.append(f"need_at_least_{min_bullets}_narrative_bullets")
    section_count = 0
    if isinstance(sections, dict):
        for value in sections.values():
            if not isinstance(value, dict):
                continue
            points = value.get("points")
            if isinstance(points, list) and points:
                section_count += 1
    if section_count < min_sections:
        reasons.append(f"need_at_least_{min_sections}_narrative_sections")
    promoted_sources = 0
    if min_promoted_sources > 0 and _table_exists(conn, "event_web_sources"):
        row = conn.execute(
            "SELECT COUNT(*) FROM event_web_sources WHERE event_id = %s AND status = 'promoted'",
            (event_id,),
        ).fetchone()
        promoted_sources = int(row[0] or 0) if row else 0
        if promoted_sources < min_promoted_sources:
            reasons.append(f"need_at_least_{min_promoted_sources}_promoted_web_sources")
    if str(event.get("lifecycle") or "").lower() not in {"confirmed", "active", "open"}:
        reasons.append("lifecycle_not_publishable")
    return {
        "ready": len(reasons) == 0,
        "reasons": reasons,
        "article_count": len(articles),
        "timeline_count": len(timeline),
        "narrative_bullet_count": len(bullets) if isinstance(bullets, list) else 0,
        "narrative_section_count": section_count,
        "promoted_source_count": promoted_sources,
    }


def set_event_publish_state(
    conn: Any,
    event_id: str,
    state: str,
    *,
    site_slug: str | None = None,
) -> bool:
    event_columns = _table_columns(conn, "events")
    if "publish_state" not in event_columns:
        return False
    now = utc_now_iso()
    normalized = (state or "").strip().lower() or "draft"
    updates = ["publish_state = %s", "updated_at = %s"]
    params: list[object] = [normalized, now]
    if "published_at" in event_columns:
        if normalized == "published":
            updates.append("published_at = COALESCE(published_at, %s)")
            params.append(now)
        else:
            updates.append("published_at = NULL")
    if "site_slug" in event_columns:
        slug_value = (site_slug or "").strip() or None
        updates.append("site_slug = COALESCE(%s, site_slug)")
        params.append(slug_value)
    params.append(event_id)
    cursor = conn.execute(
        f"UPDATE events SET {', '.join(updates)} WHERE id = %s",
        tuple(params),
    )
    conn.commit()
    return cursor.rowcount == 1


def list_event_articles(conn: Any, event_id: str) -> list[dict[str, object]]:
    if _table_exists(conn, "event_articles"):
        cursor = conn.execute(
            """
            SELECT a.id, a.title, a.original_url, a.published_at, a.source_id, s.name
            FROM event_articles ea
            JOIN articles a ON a.id = ea.article_id
            LEFT JOIN sources s ON s.id = a.source_id
            WHERE ea.event_id = %s
            ORDER BY a.published_at DESC NULLS LAST
            """,
            (event_id,),
        )
    elif _table_exists(conn, "event_items"):
        cursor = conn.execute(
            """
            SELECT a.id, a.title, a.original_url, a.published_at, a.source_id, s.name
            FROM event_items ei
            JOIN articles a ON a.id = CAST(ei.item_key AS INTEGER)
            LEFT JOIN sources s ON s.id = a.source_id
            WHERE ei.event_id = %s AND ei.item_type = 'article'
            ORDER BY a.published_at DESC NULLS LAST
            """,
            (event_id,),
        )
    else:
        return []
    rows = []
    for row in cursor.fetchall():
        rows.append(
            {
                "article_id": row[0],
                "title": row[1],
                "url": row[2],
                "published_at": row[3],
                "source_id": row[4],
                "source_name": row[5],
            }
        )
    return rows


def _event_summary_payload(raw: object) -> tuple[str, list[str]]:
    text = str(raw or "").strip()
    if not text:
        return "", []
    try:
        parsed = json.loads(text)
    except Exception:
        return text, []
    if not isinstance(parsed, dict):
        return text, []
    summary = str(parsed.get("summary") or "").strip()
    bullets_raw = parsed.get("bullets") or parsed.get("key_points") or parsed.get("tldr") or []
    bullets: list[str] = []
    if isinstance(bullets_raw, list):
        bullets = [str(item).strip() for item in bullets_raw if str(item).strip()]
    return summary or text, bullets


def _event_context_payload(raw: object) -> tuple[list[str], list[str]]:
    text = str(raw or "").strip()
    if not text:
        return [], []
    try:
        parsed = json.loads(text)
    except Exception:
        return [], []
    if not isinstance(parsed, dict):
        return [], []
    facts_raw = parsed.get("facts") or []
    timeline_raw = parsed.get("timeline") or []
    facts = [str(item).strip() for item in facts_raw if str(item).strip()] if isinstance(facts_raw, list) else []
    timeline = [str(item).strip() for item in timeline_raw if str(item).strip()] if isinstance(timeline_raw, list) else []
    return facts[:8], timeline[:8]


def _first_sentence(text: str, max_len: int = 260) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", clean)
    first = str(parts[0] if parts else clean).strip()
    if len(first) > max_len:
        return first[: max_len - 3].rstrip() + "..."
    return first


def _event_classify_point(line: str) -> str:
    lower = line.lower()
    if any(
        token in lower
        for token in (
            "breach",
            "compromis",
            "intrusion",
            "initial access",
            "attack vector",
            "unauthorized access",
            "exfiltrat",
            "malware",
            "ransomware",
        )
    ):
        return "breach_compromise"
    if any(
        token in lower
        for token in (
            "impact",
            "affected",
            "records",
            "customers",
            "users",
            "downtime",
            "disruption",
            "loss",
            "stolen",
        )
    ):
        return "impact"
    if any(
        token in lower
        for token in (
            "contain",
            "patch",
            "mitigat",
            "recover",
            "restor",
            "response",
            "investigat",
            "notified",
            "rotated",
            "disabled",
        )
    ):
        return "response_recovery"
    if any(
        token in lower
        for token in (
            "lesson",
            "recommend",
            "future",
            "improve",
            "control",
            "governance",
            "postmortem",
            "after action",
            "root cause",
        )
    ):
        return "lessons_learned"
    return "impact"


def _append_unique(items: list[str], value: str, *, limit: int) -> None:
    clean = str(value or "").strip()
    if not clean:
        return
    if len(clean) > 280:
        clean = clean[:277].rstrip() + "..."
    key = re.sub(r"\s+", " ", clean.lower())
    for existing in items:
        existing_key = re.sub(r"\s+", " ", str(existing).strip().lower())
        if existing_key == key:
            return
    items.append(clean)
    if len(items) > limit:
        del items[limit:]


def _build_event_narrative_from_articles(
    article_rows: list[dict[str, object]],
) -> dict[str, object]:
    bullets: list[str] = []
    sections: dict[str, list[str]] = {
        "breach_compromise": [],
        "impact": [],
        "response_recovery": [],
        "lessons_learned": [],
    }
    latest_summary = ""
    for idx, row in enumerate(article_rows):
        summary_text = str(row.get("summary_text") or "").strip()
        summary_bullets = row.get("summary_bullets") or []
        summary_facts = row.get("summary_facts") or []
        if idx == 0 and summary_text:
            latest_summary = _first_sentence(summary_text)
        if idx == 0 and isinstance(summary_facts, list):
            first_fact = next((str(item).strip() for item in summary_facts if str(item).strip()), "")
            if first_fact:
                latest_summary = first_fact
        if isinstance(summary_bullets, list):
            for bullet in summary_bullets:
                point = str(bullet).strip()
                if not point:
                    continue
                _append_unique(bullets, point, limit=12)
                bucket = _event_classify_point(point)
                _append_unique(sections[bucket], point, limit=8)
        if isinstance(summary_facts, list):
            for fact in summary_facts:
                point = str(fact).strip()
                if not point:
                    continue
                _append_unique(bullets, point, limit=12)
                bucket = _event_classify_point(point)
                _append_unique(sections[bucket], point, limit=8)
        if summary_text:
            # Split on sentence boundaries to get fallback points from prose summaries.
            parts = re.split(r"(?<=[.!?])\s+", summary_text)
            for part in parts[:4]:
                point = str(part).strip().strip('"')
                if not point:
                    continue
                if len(point) > 280:
                    point = point[:277].rstrip() + "..."
                _append_unique(bullets, point, limit=12)
                bucket = _event_classify_point(point)
                _append_unique(sections[bucket], point, limit=8)
    if not latest_summary and bullets:
        latest_summary = bullets[0]
    narrative_sections = {
        "breach_compromise": {
            "title": "Breach and Compromise",
            "points": sections["breach_compromise"][:4],
        },
        "impact": {
            "title": "Impact",
            "points": sections["impact"][:4],
        },
        "response_recovery": {
            "title": "Response and Recovery",
            "points": sections["response_recovery"][:4],
        },
        "lessons_learned": {
            "title": "Lessons Learned",
            "points": sections["lessons_learned"][:4],
        },
    }
    return {
        "summary": latest_summary,
        "bullets": bullets[:10],
        "sections": narrative_sections,
    }


def update_event_summary_from_articles(conn: Any, event_id: str) -> str | None:
    event_row = conn.execute(
        """
        SELECT summary,
               COALESCE(is_manual, manual) AS is_manual,
               meta_json
        FROM events
        WHERE id = %s
        """,
        (event_id,),
    ).fetchone()
    if not event_row:
        return None
    existing_summary = str(event_row[0] or "").strip()
    event_is_manual = bool(event_row[1])
    current_meta_raw = event_row[2]
    article_cols = _table_columns(conn, "articles")
    summary_llm_col = "a.summary_llm" if "summary_llm" in article_cols else "NULL AS summary_llm"
    summary_col = "a.summary" if "summary" in article_cols else "NULL AS summary"
    context_col = "a.context_llm" if "context_llm" in article_cols else "NULL AS context_llm"
    content_col = "a.content_text" if "content_text" in article_cols else "NULL AS content_text"
    select_cols = f"a.id, a.title, a.published_at, a.original_url, {summary_llm_col}, {summary_col}, {context_col}, {content_col}"
    if _table_exists(conn, "event_articles"):
        cursor = conn.execute(
            f"""
            SELECT {select_cols}
            FROM event_articles ea
            JOIN articles a ON a.id = ea.article_id
            WHERE ea.event_id = %s
            ORDER BY a.published_at DESC NULLS LAST
            LIMIT 20
            """,
            (event_id,),
        )
    else:
        cursor = conn.execute(
            f"""
            SELECT {select_cols}
            FROM event_items ei
            JOIN articles a ON a.id = CAST(ei.item_key AS INTEGER)
            WHERE ei.event_id = %s AND ei.item_type = 'article'
            ORDER BY a.published_at DESC NULLS LAST
            LIMIT 20
            """,
            (event_id,),
        )
    rows: list[dict[str, object]] = []
    for article_id, title, published_at, url, summary_llm, summary, context_llm, content_text in cursor.fetchall():
        summary_text, summary_bullets = _event_summary_payload(summary_llm)
        context_facts, context_timeline = _event_context_payload(context_llm)
        if not summary_text:
            # Avoid building event narratives from raw article bodies (often boilerplate/noisy).
            # Prefer curated article summary fields; if absent, let pipeline fill later.
            summary_text = str(summary or "").strip()
        summary_facts = [*context_facts, *context_timeline]
        rows.append(
            {
                "article_id": article_id,
                "title": title,
                "published_at": published_at,
                "url": url,
                "summary_text": summary_text,
                "summary_bullets": summary_bullets,
                "summary_facts": summary_facts,
            }
        )
    timeline = rebuild_event_timeline_from_articles(conn, event_id)
    narrative = _build_event_narrative_from_articles(rows)
    summary_text = str(narrative.get("summary") or "").strip()
    if not summary_text:
        return None
    meta: dict[str, object] = {}
    if current_meta_raw:
        try:
            parsed_meta = json.loads(current_meta_raw)
            if isinstance(parsed_meta, dict):
                meta = parsed_meta
        except Exception:
            meta = {}
    # Keep an event-authored summary intact; derived text is tracked separately.
    if event_is_manual and existing_summary:
        canonical_summary = existing_summary
        meta["derived_summary"] = summary_text
    else:
        canonical_summary = summary_text
        if existing_summary:
            meta["previous_summary"] = existing_summary
    meta["timeline"] = timeline
    meta["narrative"] = narrative
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE events
        SET summary = %s,
            meta_json = %s,
            summary_updated_at = %s,
            updated_at = %s
        WHERE id = %s
        """,
        (canonical_summary, json_dumps(meta), now, now, event_id),
    )
    conn.commit()
    return canonical_summary


def update_event_report(
    conn: Any,
    event_id: str,
    report: dict[str, object],
    *,
    profile_id: str | None = None,
    profile_name: str | None = None,
    model_id: str | None = None,
    model_name: str | None = None,
) -> bool:
    if not _table_exists(conn, "events"):
        return False
    row = conn.execute(
        "SELECT meta_json FROM events WHERE id = %s",
        (event_id,),
    ).fetchone()
    if not row:
        return False
    meta: dict[str, object] = {}
    if row[0]:
        try:
            parsed = json.loads(row[0])
            if isinstance(parsed, dict):
                meta = parsed
        except Exception:
            meta = {}
    now = utc_now_iso()
    meta["report"] = report if isinstance(report, dict) else {}
    meta["report_generated_at"] = now
    if profile_id:
        meta["report_profile_id"] = str(profile_id)
    if profile_name:
        meta["report_profile_name"] = str(profile_name)
    if model_id:
        meta["report_model_id"] = str(model_id)
    if model_name:
        meta["report_model_name"] = str(model_name)
    conn.execute(
        """
        UPDATE events
        SET meta_json = %s,
            updated_at = %s
        WHERE id = %s
        """,
        (json_dumps(meta), now, event_id),
    )
    conn.commit()
    return True


def touch_event(conn: Any, event_id: str, seen_at: str) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        UPDATE events
        SET last_seen_at = CASE WHEN last_seen_at > %s THEN last_seen_at ELSE %s END,
            updated_at = %s
        WHERE id = %s
        """,
        (seen_at, seen_at, now, event_id),
    )
    conn.commit()


def _severity_rank(severity: str | None) -> int:
    order = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "UNKNOWN": 0}
    if not severity:
        return -1
    return order.get(severity.upper(), 0)


def _event_title_for(conn: Any, event_id: str) -> str | None:
    cursor = conn.execute(
        """
        SELECT p.product_key
        FROM event_items ei
        JOIN products p ON p.product_key = ei.item_key
        WHERE ei.event_id = %s AND ei.item_type = 'product'
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
        WHERE event_id = %s AND item_type = 'cve'
        ORDER BY item_key
        LIMIT 1
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if row:
        return f"CVE activity: {row[0]}"
    return None


def update_event_rollups(conn: Any, event_id: str) -> None:
    if not _table_exists(conn, "events"):
        return
    cursor = conn.execute(
        """
        SELECT c.preferred_base_severity
        FROM event_items ei
        JOIN cves c ON c.cve_id = ei.item_key
        WHERE ei.event_id = %s AND ei.item_type = 'cve'
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
        "SELECT COUNT(*) FROM event_items WHERE event_id = %s AND item_type = 'cve'",
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
        SET severity = %s, title = %s, updated_at = %s
        WHERE id = %s
        """,
        (best or "UNKNOWN", title, now, event_id),
    )
    conn.commit()


def _ensure_json_list(value: object) -> list[object]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
        return []
    return []


def _find_event_for_cve(conn: Any, cve_id: str) -> str | None:
    if _table_exists(conn, "events"):
        columns = _table_columns(conn, "events")
        if "event_key" in columns:
            row = conn.execute(
                "SELECT id FROM events WHERE event_key = %s LIMIT 1",
                (f"cve:{cve_id}",),
            ).fetchone()
            if row:
                return row[0]
    if not _table_exists(conn, "event_items"):
        return None
    row = conn.execute(
        """
        SELECT event_id
        FROM event_items
        WHERE item_type = 'cve' AND item_key = %s
        LIMIT 1
        """,
        (cve_id,),
    ).fetchone()
    return row[0] if row else None


def _cve_has_article(conn: Any, cve_id: str) -> bool:
    if not _table_exists(conn, "article_cves"):
        return False
    row = conn.execute(
        "SELECT 1 FROM article_cves WHERE cve_id = %s LIMIT 1",
        (cve_id,),
    ).fetchone()
    return row is not None


def find_merge_candidate_event(
    conn: Any,
    product_keys: list[str],
    window_days: int,
    min_shared_products: int,
) -> str | None:
    if not product_keys or not _table_exists(conn, "event_items"):
        return None
    placeholders = ",".join("%s" for _ in product_keys)
    cutoff = utc_now_iso_offset(seconds=-(window_days * 86400))
    cursor = conn.execute(
        f"""
        SELECT e.id, COUNT(*) as matches, e.last_seen_at
        FROM events e
        JOIN event_items ei ON ei.event_id = e.id
        WHERE e.status = 'open'
          AND e.kind = 'cve_cluster'
          AND e.last_seen_at >= %s
          AND ei.item_type = 'product'
          AND ei.item_key IN ({placeholders})
        GROUP BY e.id
        HAVING COUNT(*) >= %s
        ORDER BY matches DESC, e.last_seen_at DESC
        LIMIT 1
        """,
        [cutoff, *product_keys, min_shared_products],
    )
    row = cursor.fetchone()
    return row[0] if row else None


def upsert_event_for_cve(
    conn: Any,
    cve_id: str,
    published_at: str | None,
    window_days: int,
    min_shared_products: int,
) -> tuple[str | None, str]:
    if not _table_exists(conn, "events") or not _table_exists(conn, "event_items"):
        raise ValueError("events tables not initialized")
    event_id = _find_event_for_cve(conn, cve_id)
    if not event_id and not _cve_has_article(conn, cve_id):
        return None, "skipped_no_articles"
    product_keys = list_product_keys_for_cve(conn, cve_id)
    now = utc_now_iso()
    event_key = f"cve:{cve_id}"
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
        event_key=event_key,
    )
    upsert_event_item(conn, event_id, "cve", cve_id)
    for product_key in product_keys:
        upsert_event_item(conn, event_id, "product", product_key)
    update_event_rollups(conn, event_id)
    return event_id, "created"


def link_article_to_events(
    conn: Any,
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
        link_event_article(conn, event_id, article_id, "auto")
        touch_event(conn, event_id, published_at or now)
        attached += 1
    return attached


def list_events(
    conn: Any,
    status: str | None,
    kind: str | None,
    severity: str | None,
    query: str | None,
    after: str | None,
    before: str | None,
    page: int,
    page_size: int,
    include_suppressed: bool = False,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "events"):
        return [], 0
    where: list[str] = []
    params: list[object] = []
    columns = _table_columns(conn, "events")
    candidate_select = "candidate" if "candidate" in columns else "NULL AS candidate"
    lifecycle_select = "lifecycle" if "lifecycle" in columns else "NULL AS lifecycle"
    entity_select = "entity" if "entity" in columns else "NULL AS entity"
    incident_select = "incident_date" if "incident_date" in columns else "NULL AS incident_date"
    evidence_select = "evidence" if "evidence" in columns else "NULL AS evidence"
    publish_state_select = "publish_state" if "publish_state" in columns else "NULL AS publish_state"
    published_at_select = "published_at" if "published_at" in columns else "NULL AS published_at"
    site_slug_select = "site_slug" if "site_slug" in columns else "NULL AS site_slug"
    if not include_suppressed and "visibility" in columns:
        where.append("visibility = 'active'")
    if status:
        where.append("status = %s")
        params.append(status)
    if kind:
        where.append("kind = %s")
        params.append(kind)
    if severity:
        where.append("severity = %s")
        params.append(severity)
    if query:
        like = f"%{query.lower()}%"
        where.append("(LOWER(title) LIKE %s OR LOWER(summary) LIKE %s)")
        params.extend([like, like])
    if after:
        where.append("last_seen_at >= %s")
        params.append(after)
    if before:
        where.append("last_seen_at <= %s")
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
               first_seen_at, last_seen_at, status, event_key, occurred_at, summary_updated_at, confidence, manual,
               visibility, confidence_tier, reasons, is_manual, {candidate_select}, {lifecycle_select}, {entity_select}, {incident_select}, {evidence_select},
               {publish_state_select}, {published_at_select}, {site_slug_select}
        FROM events
        {where_sql}
        ORDER BY last_seen_at DESC
        LIMIT %s OFFSET %s
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
            "event_key": row[10],
            "occurred_at": row[11],
            "summary_updated_at": row[12],
            "confidence": row[13],
            "manual": bool(row[14]),
            "visibility": row[15],
            "confidence_tier": row[16],
            "reasons": _ensure_json_list(row[17]),
            "is_manual": bool(row[18]),
            "candidate": bool(row[19]) if row[19] is not None else False,
            "lifecycle": row[20],
            "entity": row[21],
            "incident_date": row[22],
            "evidence": _ensure_json_list(row[23]),
            "publish_state": row[24],
            "published_at": row[25],
            "site_slug": row[26],
        }
        for row in cursor.fetchall()
    ]
    return items, total


def list_events_with_counts(
    conn: Any,
    status: str | None,
    kind: str | None,
    severity: str | None,
    query: str | None,
    after: str | None,
    before: str | None,
    page: int,
    page_size: int,
    include_legacy: bool = False,
    include_suppressed: bool = False,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "events"):
        return [], 0
    where: list[str] = []
    params: list[object] = []
    columns = _table_columns(conn, "events")
    candidate_select = "e.candidate" if "candidate" in columns else "NULL AS candidate"
    lifecycle_select = "e.lifecycle" if "lifecycle" in columns else "NULL AS lifecycle"
    entity_select = "e.entity" if "entity" in columns else "NULL AS entity"
    incident_select = "e.incident_date" if "incident_date" in columns else "NULL AS incident_date"
    evidence_select = "e.evidence" if "evidence" in columns else "NULL AS evidence"
    publish_state_select = "e.publish_state" if "publish_state" in columns else "NULL AS publish_state"
    published_at_select = "e.published_at" if "published_at" in columns else "NULL AS published_at"
    site_slug_select = "e.site_slug" if "site_slug" in columns else "NULL AS site_slug"
    if not include_suppressed and "visibility" in columns:
        where.append("e.visibility = 'active'")
    if status:
        where.append("e.status = %s")
        params.append(status)
    if kind:
        where.append("e.kind = %s")
        params.append(kind)
    if severity:
        where.append("e.severity = %s")
        params.append(severity)
    if query:
        like = f"%{query.lower()}%"
        where.append("(LOWER(e.title) LIKE %s OR LOWER(e.summary) LIKE %s)")
        params.extend([like, like])
    if after:
        where.append("e.last_seen_at >= %s")
        params.append(after)
    if before:
        where.append("e.last_seen_at <= %s")
        params.append(before)
    where_sql = " AND ".join(where)
    if where_sql:
        where_sql = "WHERE " + where_sql
    count_cursor = conn.execute(
        f"SELECT COUNT(*) FROM events e {where_sql}",
        params,
    )
    total = count_cursor.fetchone()[0]
    offset = max(page - 1, 0) * page_size
    article_counts_join = ""
    if _table_exists(conn, "event_articles"):
        article_counts_join = """
        LEFT JOIN (
            SELECT ea.event_id,
                   COUNT(*) AS article_count,
                   MAX(a.published_at) AS last_article_at
            FROM event_articles ea
            JOIN articles a ON a.id = ea.article_id
            GROUP BY ea.event_id
        ) ac ON ac.event_id = e.id
        """
    elif _table_exists(conn, "event_items"):
        article_counts_join = """
        LEFT JOIN (
            SELECT ei.event_id,
                   COUNT(*) AS article_count,
                   MAX(a.published_at) AS last_article_at
            FROM event_items ei
            JOIN articles a ON a.id = CAST(ei.item_key AS INTEGER)
            WHERE ei.item_type = 'article'
            GROUP BY ei.event_id
        ) ac ON ac.event_id = e.id
        """
    cve_join = (
        """
        LEFT JOIN (
            SELECT event_id, string_agg(item_key, ',' ORDER BY item_key) AS cve_ids
            FROM event_items
            WHERE item_type = 'cve'
            GROUP BY event_id
        ) ec ON ec.event_id = e.id
        """
        if _table_exists(conn, "event_items")
        else ""
    )
    product_join = (
        """
        LEFT JOIN (
            SELECT event_id, string_agg(item_key, ',' ORDER BY item_key) AS product_keys
            FROM event_items
            WHERE item_type = 'product'
            GROUP BY event_id
        ) ep ON ep.event_id = e.id
        """
        if _table_exists(conn, "event_items")
        else ""
    )
    cursor = conn.execute(
        f"""
        SELECT e.id, e.kind, e.title, e.summary, e.severity, e.created_at, e.updated_at,
               e.first_seen_at, e.last_seen_at, e.status, e.event_key, e.occurred_at,
               e.summary_updated_at, e.confidence, e.manual, e.visibility, e.confidence_tier, e.reasons, e.is_manual,
               {candidate_select}, {lifecycle_select}, {entity_select}, {incident_select}, {evidence_select},
               {publish_state_select}, {published_at_select}, {site_slug_select},
               COALESCE(ac.article_count, 0) AS article_count,
               ac.last_article_at,
               ec.cve_ids,
               ep.product_keys
        FROM events e
        {article_counts_join}
        {cve_join}
        {product_join}
        {where_sql}
        ORDER BY e.last_seen_at DESC
        LIMIT %s OFFSET %s
        """,
        [*params, page_size, offset],
    )
    items = []
    for row in cursor.fetchall():
        items.append(
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
                "event_key": row[10],
                "occurred_at": row[11],
                "summary_updated_at": row[12],
                "confidence": row[13],
                "manual": bool(row[14]),
                "visibility": row[15],
                "confidence_tier": row[16],
                "reasons": _ensure_json_list(row[17]),
                "is_manual": bool(row[18]),
                "candidate": bool(row[19]) if row[19] is not None else False,
                "lifecycle": row[20],
                "entity": row[21],
                "incident_date": row[22],
                "evidence": _ensure_json_list(row[23]),
                "publish_state": row[24],
                "published_at": row[25],
                "site_slug": row[26],
                "article_count": int(row[27] or 0),
                "last_article_at": row[28],
                "cve_ids": row[29].split(",") if row[29] else [],
                "product_keys": row[30].split(",") if row[30] else [],
                "source": "events",
            }
        )
    if include_legacy and _table_exists(conn, "event_items"):
        legacy_rows = conn.execute(
            """
            SELECT ei.event_id,
                   COUNT(*) FILTER (WHERE ei.item_type = 'article') AS article_count,
                   MAX(a.published_at) AS last_article_at
            FROM event_items ei
            LEFT JOIN events e ON e.id = ei.event_id
            LEFT JOIN articles a ON a.id = CAST(ei.item_key AS INTEGER)
            WHERE e.id IS NULL
            GROUP BY ei.event_id
            ORDER BY MAX(a.published_at) DESC NULLS LAST
            """,
        ).fetchall()
        for row in legacy_rows:
            items.append(
                {
                    "id": row[0],
                    "kind": "legacy",
                    "title": "Legacy event",
                    "summary": "",
                    "severity": "",
                    "created_at": None,
                    "updated_at": None,
                    "first_seen_at": None,
                    "last_seen_at": None,
                    "status": "legacy",
                    "event_key": None,
                    "occurred_at": None,
                    "summary_updated_at": None,
                    "confidence": None,
                    "manual": False,
                    "article_count": int(row[1] or 0),
                    "last_article_at": row[2],
                    "cve_ids": [],
                    "product_keys": [],
                    "source": "legacy",
                }
            )
    return items, total


def get_event(conn: Any, event_id: str) -> dict[str, object] | None:
    if not _table_exists(conn, "events"):
        return None
    columns = _table_columns(conn, "events")
    candidate_select = "candidate" if "candidate" in columns else "NULL AS candidate"
    lifecycle_select = "lifecycle" if "lifecycle" in columns else "NULL AS lifecycle"
    entity_select = "entity" if "entity" in columns else "NULL AS entity"
    incident_select = "incident_date" if "incident_date" in columns else "NULL AS incident_date"
    evidence_select = "evidence" if "evidence" in columns else "NULL AS evidence"
    publish_state_select = "publish_state" if "publish_state" in columns else "NULL AS publish_state"
    published_at_select = "published_at" if "published_at" in columns else "NULL AS published_at"
    site_slug_select = "site_slug" if "site_slug" in columns else "NULL AS site_slug"
    row = conn.execute(
        f"""
        SELECT id, kind, title, summary, severity, created_at, updated_at,
               first_seen_at, last_seen_at, status, meta_json,
               event_key, occurred_at, summary_updated_at, confidence, manual,
               visibility, confidence_tier, reasons, is_manual,
               {candidate_select}, {lifecycle_select}, {entity_select}, {incident_select}, {evidence_select},
               {publish_state_select}, {published_at_select}, {site_slug_select}
        FROM events
        WHERE id = %s
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
        "event_key": row[11],
        "occurred_at": row[12],
        "summary_updated_at": row[13],
        "confidence": row[14],
        "manual": bool(row[15]),
        "visibility": row[16],
        "confidence_tier": row[17],
        "reasons": _ensure_json_list(row[18]),
        "is_manual": bool(row[19]),
        "candidate": bool(row[20]) if row[20] is not None else False,
        "lifecycle": row[21],
        "entity": row[22],
        "incident_date": row[23],
        "evidence": _ensure_json_list(row[24]),
        "publish_state": row[25],
        "published_at": row[26],
        "site_slug": row[27],
    }
    timeline = event["meta"].get("timeline")
    event["timeline"] = timeline if isinstance(timeline, list) else []
    narrative = event["meta"].get("narrative")
    event["narrative"] = narrative if isinstance(narrative, dict) else {}
    report = event["meta"].get("report")
    event["report"] = report if isinstance(report, dict) else {}
    event["report_generated_at"] = event["meta"].get("report_generated_at")
    cves_cursor = conn.execute(
        """
        SELECT c.cve_id, c.published_at, preferred_base_score,
               c.preferred_base_severity, c.description_text
        FROM event_items ei
        JOIN cves c ON c.cve_id = ei.item_key
        WHERE ei.event_id = %s AND ei.item_type = 'cve'
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
        WHERE ei.event_id = %s AND ei.item_type = 'product'
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
        if _table_exists(conn, "event_articles"):
            article_cursor = conn.execute(
                """
                SELECT a.id, a.title, a.published_at, a.original_url
                FROM event_articles ea
                JOIN articles a ON a.id = ea.article_id
                WHERE ea.event_id = %s
                ORDER BY a.published_at DESC
                """,
                (event_id,),
            )
        else:
            article_cursor = conn.execute(
                """
                SELECT a.id, a.title, a.published_at, a.original_url
                FROM event_items ei
                JOIN articles a ON a.id = CAST(ei.item_key AS INTEGER)
                WHERE ei.event_id = %s AND ei.item_type = 'article'
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
    conn: Any,
    product_key: str,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "event_items"):
        return [], 0
    columns = _table_columns(conn, "events")
    visibility_filter = " AND e.visibility = 'active'" if "visibility" in columns else ""
    count_cursor = conn.execute(
        """
        SELECT COUNT(DISTINCT e.id)
        FROM event_items ei
        JOIN events e ON e.id = ei.event_id
        WHERE ei.item_type = 'product' AND ei.item_key = %s
        """
        + visibility_filter,
        (product_key,),
    )
    total = count_cursor.fetchone()[0]
    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        """
        SELECT e.id, e.kind, e.title, e.severity, e.last_seen_at, e.status
        FROM event_items ei
        JOIN events e ON e.id = ei.event_id
        WHERE ei.item_type = 'product' AND ei.item_key = %s
        """
        + visibility_filter
        + """
        ORDER BY e.last_seen_at DESC
        LIMIT %s OFFSET %s
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


def list_event_web_sources(
    conn: Any,
    event_id: str,
    include_discarded: bool = False,
) -> list[dict[str, object]]:
    if not _table_exists(conn, "event_web_sources"):
        return []
    status_filter = "" if include_discarded else " AND status != 'discarded'"
    cursor = conn.execute(
        """
        SELECT id, url, title, snippet, domain, published_at, engine, category,
               score, score_reasons, status, discovered_at, promoted_article_id, metadata
        FROM event_web_sources
        WHERE event_id = %s
        """
        + status_filter
        + " ORDER BY discovered_at DESC",
        (event_id,),
    )
    rows = []
    for row in cursor.fetchall():
        score_reasons = row[9]
        if isinstance(score_reasons, str):
            score_reasons = json.loads(score_reasons) if score_reasons else {}
        elif score_reasons is None:
            score_reasons = {}
        metadata = row[13]
        if isinstance(metadata, str):
            metadata = json.loads(metadata) if metadata else {}
        elif metadata is None:
            metadata = {}
        rows.append(
            {
                "id": row[0],
                "url": row[1],
                "title": row[2],
                "snippet": row[3],
                "domain": row[4],
                "published_at": row[5],
                "engine": row[6],
                "category": row[7],
                "score": row[8],
                "score_reasons": score_reasons,
                "status": row[10],
                "discovered_at": row[11],
                "promoted_article_id": row[12],
                "metadata": metadata,
            }
        )
    return rows


def get_event_web_source(conn: Any, source_id: str) -> dict[str, object] | None:
    if not _table_exists(conn, "event_web_sources"):
        return None
    row = conn.execute(
        """
        SELECT id, event_id, url, title, snippet, domain, published_at, engine, category,
               score, score_reasons, status, discovered_at, promoted_article_id, metadata
        FROM event_web_sources
        WHERE id = %s
        """,
        (source_id,),
    ).fetchone()
    if not row:
        return None
    score_reasons = row[10]
    if isinstance(score_reasons, str):
        score_reasons = json.loads(score_reasons) if score_reasons else {}
    elif score_reasons is None:
        score_reasons = {}
    metadata = row[14]
    if isinstance(metadata, str):
        metadata = json.loads(metadata) if metadata else {}
    elif metadata is None:
        metadata = {}
    return {
        "id": row[0],
        "event_id": row[1],
        "url": row[2],
        "title": row[3],
        "snippet": row[4],
        "domain": row[5],
        "published_at": row[6],
        "engine": row[7],
        "category": row[8],
        "score": row[9],
        "score_reasons": score_reasons,
        "status": row[11],
        "discovered_at": row[12],
        "promoted_article_id": row[13],
        "metadata": metadata,
    }


def clear_event_web_sources(
    conn: Any,
    event_id: str,
    *,
    keep_promoted: bool = True,
) -> int:
    if not _table_exists(conn, "event_web_sources"):
        return 0
    if keep_promoted:
        cursor = conn.execute(
            """
            DELETE FROM event_web_sources
            WHERE event_id = %s AND promoted_article_id IS NULL
            RETURNING id
            """,
            (event_id,),
        )
    else:
        cursor = conn.execute(
            """
            DELETE FROM event_web_sources
            WHERE event_id = %s
            RETURNING id
            """,
            (event_id,),
        )
    deleted = len(cursor.fetchall())
    conn.commit()
    return deleted


def update_event_web_source_status(
    conn: Any,
    source_id: str,
    status: str,
    *,
    metadata_patch: dict[str, object] | None = None,
) -> None:
    if not _table_exists(conn, "event_web_sources"):
        return
    if metadata_patch:
        row = conn.execute(
            "SELECT metadata FROM event_web_sources WHERE id = %s",
            (source_id,),
        ).fetchone()
        metadata = {}
        if row:
            existing = row[0]
            if isinstance(existing, str):
                metadata = json.loads(existing) if existing else {}
            elif isinstance(existing, dict):
                metadata = dict(existing)
        metadata.update(metadata_patch)
        conn.execute(
            "UPDATE event_web_sources SET status = %s, metadata = %s WHERE id = %s",
            (status, json_dumps(metadata), source_id),
        )
    else:
        conn.execute(
            "UPDATE event_web_sources SET status = %s WHERE id = %s",
            (status, source_id),
        )
    conn.commit()


def upsert_event_web_source(
    conn: Any,
    event_id: str,
    result: dict[str, object],
    score: int,
    reasons: dict[str, int],
) -> str | None:
    if not _table_exists(conn, "event_web_sources"):
        return None
    raw_url = str(result.get("url") or "").strip()
    if not raw_url:
        return None
    normalized = normalize_url(raw_url)
    if not normalized:
        return None
    hash_value = url_hash(normalized)
    domain = (urlparse(normalized).netloc or "").lower()
    source_id = f"evtws_{uuid.uuid4().hex}"
    conn.execute(
        """
        INSERT INTO event_web_sources
            (id, event_id, url, url_hash, title, snippet, domain, published_at,
             engine, category, score, score_reasons, status, discovered_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'new', %s, %s)
        ON CONFLICT(event_id, url_hash) DO UPDATE SET
            title=excluded.title,
            snippet=excluded.snippet,
            domain=excluded.domain,
            published_at=excluded.published_at,
            engine=excluded.engine,
            category=excluded.category,
            score=excluded.score,
            score_reasons=excluded.score_reasons,
            discovered_at=excluded.discovered_at
        RETURNING id
        """,
        (
            source_id,
            event_id,
            normalized,
            hash_value,
            result.get("title"),
            result.get("snippet"),
            domain,
            result.get("published_at"),
            result.get("engine"),
            result.get("category"),
            score,
            json_dumps(reasons or {}),
            utc_now_iso(),
            json_dumps(result.get("metadata") or {}),
        ),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM event_web_sources WHERE event_id = %s AND url_hash = %s",
        (event_id, hash_value),
    ).fetchone()
    return row[0] if row else None


def mark_event_web_source_status(conn: Any, source_id: str, status: str) -> None:
    update_event_web_source_status(conn, source_id, status)


def _normalize_event_web_published_at(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000.0
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
        except Exception:  # noqa: BLE001
            return None
    text = str(value).strip()
    if not text:
        return None
    if not re.search(r"\b\d{4}\b", text):
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:  # noqa: BLE001
        return None


def update_event_web_source_published_at(conn: Any, source_id: str, published_at: str | None) -> None:
    if not _table_exists(conn, "event_web_sources"):
        return
    normalized = _normalize_event_web_published_at(published_at)
    conn.execute(
        "UPDATE event_web_sources SET published_at = %s WHERE id = %s",
        (normalized, source_id),
    )
    conn.commit()


def promote_event_web_source_to_article(conn: Any, source_id: str) -> int | None:
    if not _table_exists(conn, "event_web_sources"):
        return None
    row = conn.execute(
        """
        SELECT event_id, url, title, snippet, domain, published_at, promoted_article_id
        FROM event_web_sources
        WHERE id = %s
        """,
        (source_id,),
    ).fetchone()
    if not row:
        return None
    event_id, url, title, snippet, domain, published_at, promoted_article_id = row
    if promoted_article_id:
        return promoted_article_id
    source_id_value = "web_enrich"
    upsert_source(
        conn,
        {
            "id": source_id_value,
            "name": "Web Enrichment",
            "enabled": True,
            "base_url": None,
            "topic_key": None,
            "default_frequency_minutes": 1440,
            "pause_until": None,
            "paused_reason": None,
            "robots_notes": None,
        },
    )
    stable_id = url_hash(normalize_url(url))
    article = Article(
        id=None,
        stable_id=stable_id,
        original_url=url,
        normalized_url=normalize_url(url),
        title=title or url,
        source_id=source_id_value,
        published_at=_normalize_event_web_published_at(published_at),
        published_at_source="web_search",
        ingested_at=utc_now_iso(),
        summary=snippet or None,
        tags=[],
    )
    insert_articles(conn, [article])
    article_id = get_article_id(conn, source_id_value, stable_id)
    if not article_id:
        return None
    link_event_article(conn, event_id, article_id, "enrich")
    conn.execute(
        """
        UPDATE event_web_sources
        SET status = 'promoted', promoted_article_id = %s
        WHERE id = %s
        """,
        (article_id, source_id),
    )
    conn.commit()
    return article_id


def rebuild_event_timeline_from_articles(conn: Any, event_id: str) -> list[dict[str, object]]:
    if not _table_exists(conn, "events"):
        return []
    articles = list_event_articles(conn, event_id)
    if not articles:
        row = conn.execute("SELECT meta_json FROM events WHERE id = %s", (event_id,)).fetchone()
        meta = {}
        if row and row[0]:
            try:
                meta = json.loads(row[0])
            except Exception:
                meta = {}
        if isinstance(meta, dict):
            meta["timeline"] = []
            conn.execute(
                "UPDATE events SET meta_json = %s, updated_at = %s WHERE id = %s",
                (json_dumps(meta), utc_now_iso(), event_id),
            )
            conn.commit()
        return []
    article_ids = [int(a["article_id"]) for a in articles if a.get("article_id") is not None]
    snippet_by_id: dict[int, str] = {}
    facts_by_id: dict[int, list[str]] = {}
    if article_ids:
        placeholders = ", ".join(["%s"] * len(article_ids))
        article_cols = _table_columns(conn, "articles")
        summary_llm_col = "summary_llm" if "summary_llm" in article_cols else "NULL AS summary_llm"
        summary_col = "summary" if "summary" in article_cols else "NULL AS summary"
        context_col = "context_llm" if "context_llm" in article_cols else "NULL AS context_llm"
        cursor = conn.execute(
            f"SELECT id, {summary_llm_col}, {summary_col}, {context_col} FROM articles WHERE id IN ({placeholders})",
            tuple(article_ids),
        )
        for article_id, summary_llm, summary, context_llm in cursor.fetchall():
            parsed_summary, parsed_bullets = _event_summary_payload(summary_llm)
            context_facts, context_timeline = _event_context_payload(context_llm)
            text = parsed_summary or str(summary or "").strip()
            if parsed_bullets:
                text = f"{text} " + " ".join(parsed_bullets[:2])
                text = text.strip()
            if text:
                snippet_by_id[int(article_id)] = text[:320]
            facts: list[str] = []
            for point in [*parsed_bullets[:4], *context_facts[:4], *context_timeline[:3]]:
                clean = str(point).strip()
                if not clean or clean in facts:
                    continue
                facts.append(clean)
            if facts:
                facts_by_id[int(article_id)] = facts[:8]
    entries: list[dict[str, object]] = []
    for article in sorted(articles, key=lambda x: str(x.get("published_at") or "")):
        article_id = int(article.get("article_id") or 0)
        entries.append(
            {
                "date": article.get("published_at"),
                "article_id": article_id,
                "title": article.get("title"),
                "source_name": article.get("source_name"),
                "url": article.get("url"),
                "summary": snippet_by_id.get(article_id, ""),
                "facts": facts_by_id.get(article_id, []),
            }
        )
    row = conn.execute("SELECT meta_json FROM events WHERE id = %s", (event_id,)).fetchone()
    meta = {}
    if row and row[0]:
        try:
            meta = json.loads(row[0])
        except Exception:
            meta = {}
    if not isinstance(meta, dict):
        meta = {}
    meta["timeline"] = entries
    conn.execute(
        "UPDATE events SET meta_json = %s, updated_at = %s WHERE id = %s",
        (json_dumps(meta), utc_now_iso(), event_id),
    )
    conn.commit()
    return entries


def rebuild_events_from_cves(
    conn: Any,
    window_days: int,
    min_shared_products: int,
    limit: int | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, object]:
    stats = {
        "events_created": 0,
        "events_merged": 0,
        "events_existing": 0,
        "cves_processed": 0,
        "articles_linked": 0,
    }
    if not _table_exists(conn, "cves"):
        return stats
    cursor = conn.execute(
        "SELECT cve_id, published_at FROM cves ORDER BY published_at"
        + (" LIMIT %s" if limit else ""),
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


def delete_all_articles(conn: Any, *, delete_files: bool = False) -> dict[str, object]:
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

    with conn.transaction():
        if _table_exists(conn, "article_tags"):
            cursor = conn.execute("DELETE FROM article_tags")
            stats["tables"]["article_tags"] = cursor.rowcount
        if _table_exists(conn, "article_cves"):
            cursor = conn.execute("DELETE FROM article_cves")
            stats["tables"]["article_cves"] = cursor.rowcount
        if _table_exists(conn, "articles"):
            cursor = conn.execute("DELETE FROM articles")
            stats["tables"]["articles"] = cursor.rowcount

    if delete_files:
        _delete_content_files(conn, file_paths, stats)
    return stats


def delete_all_cves(conn: Any) -> dict[str, object]:
    stats: dict[str, object] = {"tables": {}}
    with conn.transaction():
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
    return stats


def delete_all_events(conn: Any) -> dict[str, object]:
    stats: dict[str, object] = {"tables": {}}
    with conn.transaction():
        if _table_exists(conn, "event_signals"):
            cursor = conn.execute("DELETE FROM event_signals")
            stats["tables"]["event_signals"] = cursor.rowcount
        if _table_exists(conn, "event_articles"):
            cursor = conn.execute("DELETE FROM event_articles")
            stats["tables"]["event_articles"] = cursor.rowcount
        if _table_exists(conn, "event_items"):
            cursor = conn.execute("DELETE FROM event_items")
            stats["tables"]["event_items"] = cursor.rowcount
        if _table_exists(conn, "events"):
            cursor = conn.execute("DELETE FROM events")
            stats["tables"]["events"] = cursor.rowcount
    return stats


def purge_weak_events(
    conn: Any,
    *,
    dry_run: bool = True,
    mode: str = "suppress",
    older_than_days: int | None = None,
    kinds: list[str] | None = None,
    require_no_victims: bool = False,
    require_no_cves: bool = False,
    require_no_sources: bool = False,
    require_research: bool = False,
    confidence_below: float | None = None,
    only_empty_cve_clusters: bool = False,
    exclude_manual: bool = True,
) -> dict[str, object]:
    if not _table_exists(conn, "events"):
        return {"candidates": 0, "deleted": 0, "kept": 0, "by_reason": {}}
    columns = _table_columns(conn, "events")
    manual_expr = "COALESCE(is_manual, manual)" if "is_manual" in columns else "manual"
    candidate_expr = "candidate" if "candidate" in columns else "false"
    visibility_supported = "visibility" in columns
    kinds = kinds or []
    stats = {
        "dry_run": dry_run,
        "mode": mode,
        "candidates": 0,
        "matched": 0,
        "deleted": 0,
        "kept": 0,
        "by_reason": {},
        "sample_deleted": [],
    }
    now = datetime.now(tz=timezone.utc)
    rows = conn.execute(
        f"""
        SELECT id, kind, event_key, title, summary, created_at, updated_at, last_seen_at,
               confidence, entity, evidence, {manual_expr} AS manual, {candidate_expr} AS candidate
        FROM events
        """
    ).fetchall()

    def note(reason: str) -> None:
        stats["by_reason"][reason] = stats["by_reason"].get(reason, 0) + 1

    def _article_count(event_id: str) -> int:
        if _table_exists(conn, "event_articles"):
            cursor = conn.execute(
                "SELECT COUNT(*) FROM event_articles WHERE event_id = %s",
                (event_id,),
            )
        elif _table_exists(conn, "event_items"):
            cursor = conn.execute(
                "SELECT COUNT(*) FROM event_items WHERE event_id = %s AND item_type = 'article'",
                (event_id,),
            )
        else:
            return 0
        return int(cursor.fetchone()[0])

    def _cve_count(event_id: str) -> int:
        if not _table_exists(conn, "event_items"):
            return 0
        cursor = conn.execute(
            "SELECT COUNT(*) FROM event_items WHERE event_id = %s AND item_type = 'cve'",
            (event_id,),
        )
        return int(cursor.fetchone()[0])

    research_cues = (
        "survey",
        "report",
        "analysis",
        "research",
        "study",
        "trend",
        "outlook",
        "forecast",
        "guidance",
        "whitepaper",
    )

    purge_ids: list[str] = []
    for (
        event_id,
        kind,
        event_key,
        title,
        summary,
        created_at,
        updated_at,
        last_seen_at,
        confidence,
        entity,
        evidence,
        manual,
        candidate,
    ) in rows:
        stats["candidates"] += 1
        if exclude_manual and manual:
            stats["kept"] += 1
            note("manual")
            continue
        if exclude_manual and kind == "manual":
            stats["kept"] += 1
            note("manual_kind")
            continue
        if exclude_manual and event_key and str(event_key).startswith("manual:"):
            stats["kept"] += 1
            note("manual_key")
            continue
        if kinds and kind not in kinds:
            stats["kept"] += 1
            note("kind_skipped")
            continue

        article_count = _article_count(event_id)
        cve_count = _cve_count(event_id)
        has_victim = bool(entity)
        conf_value = float(confidence or 0)
        combined = " ".join(str(value or "") for value in (title, summary))
        if evidence:
            combined = combined + " " + " ".join(evidence if isinstance(evidence, list) else [str(evidence)])
        combined = combined.lower()
        is_research = any(cue in combined for cue in research_cues)

        matches = True
        if require_no_victims and has_victim:
            matches = False
            note("has_victim")
        if require_no_cves and cve_count > 0:
            matches = False
            note("has_cves")
        if require_no_sources and article_count > 0:
            matches = False
            note("has_sources")
        if require_research and not is_research:
            matches = False
            note("not_research")
        if confidence_below is not None and conf_value >= float(confidence_below):
            matches = False
            note("confidence_high")

        if only_empty_cve_clusters and kind == "cve_cluster" and article_count == 0:
            matches = True
            note("cve_cluster_empty")

        if not matches:
            stats["kept"] += 1
            continue

        if older_than_days is not None:
            too_old = False
            for raw_dt in (last_seen_at, updated_at, created_at):
                if not raw_dt:
                    continue
                try:
                    last_dt = datetime.fromisoformat(str(raw_dt))
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    if last_dt + timedelta(days=older_than_days) < now:
                        too_old = True
                        break
                except ValueError:
                    continue
            if not too_old:
                stats["kept"] += 1
                note("not_old_enough")
                continue

        stats["matched"] += 1
        purge_ids.append(event_id)
        note("matched")

    if not purge_ids or dry_run:
        stats["deleted"] = 0
        stats["sample_deleted"] = [
            {"event_id": event_id, "reason": "dry_run"} for event_id in purge_ids[:25]
        ]
        return stats

    placeholders = ",".join("%s" for _ in purge_ids)
    with conn.transaction():
        if mode == "suppress" and visibility_supported:
            conn.execute(
                f"UPDATE events SET visibility = 'suppressed' WHERE id IN ({placeholders})",
                purge_ids,
            )
        else:
            if _table_exists(conn, "event_articles"):
                conn.execute(
                    f"DELETE FROM event_articles WHERE event_id IN ({placeholders})",
                    purge_ids,
                )
            if _table_exists(conn, "event_items"):
                conn.execute(
                    f"DELETE FROM event_items WHERE event_id IN ({placeholders})",
                    purge_ids,
                )
            if _table_exists(conn, "event_signals"):
                conn.execute(
                    f"DELETE FROM event_signals WHERE event_id IN ({placeholders})",
                    purge_ids,
                )
            if _table_exists(conn, "event_web_sources"):
                conn.execute(
                    f"DELETE FROM event_web_sources WHERE event_id IN ({placeholders})",
                    purge_ids,
                )
            conn.execute(f"DELETE FROM events WHERE id IN ({placeholders})", purge_ids)
    stats["deleted"] = len(purge_ids)
    stats["sample_deleted"] = [
        {"event_id": event_id, "reason": "purged"} for event_id in purge_ids[:25]
    ]
    return stats


def normalize_cve_cluster_event_keys(conn: Any, limit: int = 200) -> dict[str, object]:
    if not _table_exists(conn, "events"):
        return {"updated": 0}
    cursor = conn.execute(
        """
        SELECT id, title
        FROM events
        WHERE (event_key IS NULL OR event_key = '')
          AND kind = 'cve_cluster'
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    updated = 0
    sample = []
    for event_id, title in cursor.fetchall():
        if not title:
            continue
        match = _CVE_RE.search(title)
        if not match:
            continue
        event_key = f"cve:{match.group(0).upper()}"
        conn.execute(
            "UPDATE events SET event_key = %s WHERE id = %s",
            (event_key, event_id),
        )
        updated += 1
        if len(sample) < 25:
            sample.append({"event_id": event_id, "event_key": event_key})
    if updated:
        conn.commit()
    return {"updated": updated, "sample": sample}


def normalize_cve_event_keys(conn: Any, limit: int = 200) -> dict[str, object]:
    return normalize_cve_cluster_event_keys(conn, limit=limit)


def delete_all_content(conn: Any, *, delete_files: bool = False) -> dict[str, object]:
    articles = delete_all_articles(conn, delete_files=delete_files)
    cves = delete_all_cves(conn)
    events = delete_all_events(conn)
    return {"articles": articles, "cves": cves, "events": events}


def _delete_content_files(
    conn: Any, file_paths: list[str], stats: dict[str, object]
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
    conn: Any,
    query: str | None,
    source_id: str | None,
    has_summary: bool | None,
    has_context: bool | None,
    missing: str | None,
    content_state: str | None,
    content_error: bool | None,
    content_error_kind: str | None,
    summary_error: bool | None,
    needs: str | None,
    after: str | None,
    before: str | None,
    tags: list[str] | None,
    watchlist_enabled: bool,
    watchlist_hit: bool | None,
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
        parts = ["a.title LIKE %s"]
        params.append(like)
        if "content_text" in columns:
            parts.append("a.content_text LIKE %s")
            params.append(like)
        if "summary_llm" in columns:
            parts.append("a.summary_llm LIKE %s")
            params.append(like)
        where.append("(" + " OR ".join(parts) + ")")
    if source_id:
        where.append("a.source_id = %s")
        params.append(source_id)
    content_missing_clause = None
    if "content_text" in columns:
        content_missing_clause = "(a.content_text IS NULL OR a.content_text = '')"
    elif "has_full_content" in columns:
        content_missing_clause = "a.has_full_content = 0"
    elif "extracted_text_path" in columns:
        content_missing_clause = "(a.extracted_text_path IS NULL OR a.extracted_text_path = '')"
    summary_missing_clause = None
    if "summary_llm" in columns:
        summary_missing_clause = "(a.summary_llm IS NULL OR a.summary_llm = '')"
    context_missing_clause = None
    if "context_llm" in columns:
        context_missing_clause = "(a.context_llm IS NULL OR a.context_llm = '')"
    if has_summary is True:
        if "summary_llm" in columns:
            where.append("a.summary_llm IS NOT NULL")
        else:
            return [], 0
    if has_summary is False:
        if "summary_llm" in columns:
            where.append("a.summary_llm IS NULL")
    if has_context is True:
        if "context_llm" in columns:
            where.append("a.context_llm IS NOT NULL")
        else:
            return [], 0
    if has_context is False:
        if "context_llm" in columns:
            where.append("a.context_llm IS NULL")
    if missing == "content":
        if content_missing_clause:
            where.append(content_missing_clause)
        else:
            return [], 0
    if content_state:
        if content_state == "full":
            if "has_full_content" in columns:
                where.append("a.has_full_content = 1")
            elif "content_text" in columns:
                where.append("(a.content_text IS NOT NULL AND a.content_text != '')")
        elif content_state == "partial":
            if "has_full_content" in columns and "content_text" in columns:
                where.append("a.has_full_content = 0 AND a.content_text IS NOT NULL AND a.content_text != ''")
        elif content_state == "missing":
            if "content_text" in columns:
                where.append("(a.content_text IS NULL OR a.content_text = '')")
            elif "has_full_content" in columns:
                where.append("a.has_full_content = 0")
    if missing == "summary":
        if summary_missing_clause:
            where.append(summary_missing_clause)
        else:
            return [], 0
    if missing == "context":
        if context_missing_clause:
            where.append(context_missing_clause)
        else:
            return [], 0
    if missing == "products":
        if not _table_exists(conn, "article_products"):
            return [], 0
        where.append("NOT EXISTS (SELECT 1 FROM article_products ap WHERE ap.article_id = a.id)")
    if missing == "threat_actors":
        if not _table_exists(conn, "article_threat_actors"):
            return [], 0
        where.append("NOT EXISTS (SELECT 1 FROM article_threat_actors at WHERE at.article_id = a.id)")
    if content_error:
        if "content_error" in columns:
            where.append("(a.content_error IS NOT NULL AND a.content_error != '')")
        else:
            return [], 0
    if content_error_kind:
        if "content_error" not in columns:
            return [], 0
        kind = content_error_kind.strip().lower()
        error_404_clause = (
            "a.content_error IN ('http_404','http_410') "
            "OR a.content_error LIKE '%%HTTP Error 404%%' "
            "OR a.content_error LIKE '%%HTTP Error 410%%'"
        )
        error_stale_clause = "a.content_error = 'stale_older_than_week'"
        if kind in ("404", "410", "404/410"):
            where.append(f"({error_404_clause})")
        elif kind in ("stale", "stale_older_than_week"):
            where.append(f"({error_stale_clause})")
        elif kind == "other":
            where.append(
                f"(a.content_error IS NOT NULL AND a.content_error != '' AND NOT ({error_404_clause} OR {error_stale_clause}))"
            )
    if summary_error:
        if "summary_error" in columns:
            where.append("(a.summary_error IS NOT NULL AND a.summary_error != '')")
        else:
            return [], 0
    if needs:
        url_clause = None
        if "original_url" in columns:
            url_clause = "(a.original_url IS NOT NULL AND a.original_url != '')"
        if needs == "fetch":
            if content_missing_clause:
                clause = content_missing_clause
                if url_clause:
                    clause = f"{clause} AND {url_clause}"
                where.append(clause)
            else:
                return [], 0
        elif needs == "summarize":
            if summary_missing_clause and content_missing_clause:
                where.append(f"({summary_missing_clause}) AND NOT ({content_missing_clause})")
            elif summary_missing_clause:
                where.append(summary_missing_clause)
            else:
                return [], 0
        elif needs == "context":
            if context_missing_clause:
                if "content_text" in columns:
                    where.append(
                        f"({context_missing_clause}) AND (a.content_text IS NOT NULL AND a.content_text != '')"
                    )
                else:
                    return [], 0
            else:
                return [], 0
        elif needs == "publish":
            if summary_missing_clause and content_missing_clause:
                where.append(f"(NOT {summary_missing_clause} OR NOT {content_missing_clause})")
            elif summary_missing_clause:
                where.append(f"NOT {summary_missing_clause}")
            elif content_missing_clause:
                where.append(f"NOT {content_missing_clause}")
            else:
                return [], 0
        elif needs == "attention":
            attention_parts = []
            if "content_error" in columns:
                attention_parts.append("(a.content_error IS NOT NULL AND a.content_error != '')")
            if "summary_error" in columns:
                attention_parts.append("(a.summary_error IS NOT NULL AND a.summary_error != '')")
            if attention_parts:
                where.append("(" + " OR ".join(attention_parts) + ")")
            else:
                return [], 0
    if after:
        if "published_at" in columns:
            where.append("a.published_at >= %s")
            params.append(after)
    if before:
        if "published_at" in columns:
            where.append("a.published_at <= %s")
            params.append(before)
    if tags:
        if not _table_exists(conn, "article_tags"):
            return [], 0
        where.append(
            "EXISTS (SELECT 1 FROM article_tags t WHERE t.article_id = a.id AND t.tag IN ({}))".format(
                ",".join("%s" for _ in tags)
            )
        )
        params.extend(tags)

    watchlist_available = (
        watchlist_enabled
        and _table_exists(conn, "article_cves")
        and _table_exists(conn, "cve_scope")
    )
    if watchlist_available and watchlist_hit is not None:
        where.append(
            """
            EXISTS (
                SELECT 1 FROM article_cves ac
                JOIN cve_scope cs ON cs.cve_id = ac.cve_id
                WHERE ac.article_id = a.id AND cs.in_scope = %s
            )
            """.strip()
        )
        params.append(1 if watchlist_hit else 0)

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
    watchlist_select = (
        """
        EXISTS (
            SELECT 1 FROM article_cves ac
            JOIN cve_scope cs ON cs.cve_id = ac.cve_id
            WHERE ac.article_id = a.id AND cs.in_scope = 1
        ) AS watchlist_hit
        """
        if watchlist_available
        else "NULL AS watchlist_hit"
    )
    has_content_select = "NULL AS has_content"
    content_len_select = "NULL AS content_len"
    if "content_text" in columns:
        content_len_select = "MAX(length(a.content_text)) AS content_len"
    if "has_full_content" in columns:
        has_content_select = "CASE WHEN a.has_full_content = 1 THEN 1 ELSE 0 END AS has_content"
    elif "content_text" in columns:
        has_content_select = (
            "CASE WHEN a.content_text IS NOT NULL AND a.content_text != '' THEN 1 ELSE 0 END AS has_content"
        )
    elif "extracted_text_path" in columns:
        has_content_select = (
            "CASE WHEN a.extracted_text_path IS NOT NULL AND a.extracted_text_path != '' THEN 1 ELSE 0 END AS has_content"
        )
    content_error_select = (
        "a.content_error" if "content_error" in columns else "NULL AS content_error"
    )
    summary_error_select = (
        "a.summary_error" if "summary_error" in columns else "NULL AS summary_error"
    )
    cursor = conn.execute(
        f"""
        SELECT a.id,
               MAX(a.title) as title,
               MAX(a.original_url) as original_url,
               MAX(a.published_at) as published_at,
               MAX(a.ingested_at) as ingested_at,
               { 'MAX(a.summary_llm)' if 'summary_llm' in columns else 'NULL' } as summary_llm,
               MAX(a.source_id) as source_id,
               MAX(s.name) as source_name,
               string_agg(t.tag, ',') as tags,
               {watchlist_select},
               {has_content_select},
               {content_len_select},
               {content_error_select},
               {summary_error_select},
               MAX(a.meta_json) as meta_json
        FROM articles a
        LEFT JOIN sources s ON s.id = a.source_id
        LEFT JOIN article_tags t ON t.article_id = a.id
        {where_sql}
        GROUP BY a.id
        ORDER BY {order_col} DESC
        LIMIT %s OFFSET %s
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
        watchlist_hit_value,
        has_content_value,
        content_len_value,
        content_error_value,
        summary_error_value,
        meta_json_value,
    ) in cursor.fetchall():
        suppressed_value = _meta_is_suppressed(meta_json_value)
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
                "watchlist_hit": bool(watchlist_hit_value) if watchlist_available else None,
                "has_content": bool(has_content_value) if has_content_value is not None else None,
                "content_len": int(content_len_value or 0) if content_len_value is not None else 0,
                "content_error": content_error_value,
                "summary_error": summary_error_value,
                "suppressed": suppressed_value,
            }
        )
    return items, total


def get_cve(conn: Any, cve_id: str) -> dict[str, object] | None:
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
        "kev_cve_id",
        "kev_checked_at",
        "c.updated_at",
    ]
    selected = [col for col in selected if col in columns]
    if not selected:
        return None
    cursor = conn.execute(
        f"""
        SELECT {", ".join(selected)}
        FROM cves
        WHERE cve_id = %s
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
    vendor_products = list_cve_vendor_products(conn, cve_id)
    threat_actors = get_cve_threat_actors(conn, cve_id)
    scope = None
    if _table_exists(conn, "cve_scope"):
        scope = conn.execute(
            "SELECT in_scope, reasons_json FROM cve_scope WHERE cve_id = %s",
            (cve_id,),
        ).fetchone()
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
        "tags": list_cve_tags(conn, cve_id),
        "vendor_products": vendor_products,
        "threat_actors": threat_actors,
        "in_scope": bool(scope[0]) if scope else None,
        "scope_reasons": json.loads(scope[1] or "[]") if scope else [],
        "kev_cve_id": data.get("kev_cve_id"),
        "kev_checked_at": data.get("kev_checked_at"),
        "kev": get_cve_kev(conn, cve_id)
        if _table_exists(conn, "cve_kev") and data.get("kev_cve_id")
        else None,
        "updated_at": data.get("updated_at"),
    }


def get_cve_last_seen(conn: Any, cve_id: str) -> str | None:
    cursor = conn.execute(
        "SELECT MAX(observed_at) FROM cve_snapshots WHERE cve_id = %s",
        (cve_id,),
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] else None


def search_cves(
    conn: Any,
    query: str | None,
    severities: list[str] | None,
    min_cvss: float | None,
    missing_description: bool | None,
    missing_products: bool | None,
    kev: bool | None,
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
    has_scope = _table_exists(conn, "cve_scope")
    has_kev = _table_exists(conn, "cve_kev") and "kev_cve_id" in columns
    where: list[str] = []
    params: list[object] = []
    if query:
        like = f"%{query}%"
        where.append(
            "(cve_id LIKE %s OR description_text LIKE %s OR LOWER(affected_products_json) LIKE %s OR LOWER(affected_cpes_json) LIKE %s)"
        )
        params.extend([like, like, like.lower(), like.lower()])
    if severities:
        normalized = [severity.upper() for severity in severities]
        include_unknown = "UNKNOWN" in normalized
        normalized = [value for value in normalized if value != "UNKNOWN"]
        condition_parts = []
        if normalized:
            condition_parts.append(
                "preferred_base_severity IN ({})".format(",".join("%s" for _ in normalized))
            )
            params.extend(normalized)
        if include_unknown:
            condition_parts.append("preferred_base_severity IS NULL")
        if condition_parts:
            where.append("(" + " OR ".join(condition_parts) + ")")
    if min_cvss is not None:
        where.append("preferred_base_score >= %s")
        params.append(min_cvss)
    if missing_description:
        if "description_text" in columns:
            where.append("(description_text IS NULL OR description_text = '')")
        else:
            return [], 0
    if missing_products:
        if _table_exists(conn, "cve_products"):
            where.append("NOT EXISTS (SELECT 1 FROM cve_products cp WHERE cp.cve_id = cve_id)")
        elif "affected_products_json" in columns:
            where.append("(affected_products_json IS NULL OR affected_products_json = '' OR affected_products_json = '[]')")
        else:
            return [], 0
    if kev is not None:
        if not has_kev:
            return [], 0
        where.append("kev_cve_id IS NOT NULL" if kev else "kev_cve_id IS NULL")
    if after:
        where.append("published_at >= %s")
        params.append(after)
    if before:
        where.append("published_at <= %s")
        params.append(before)
    if vendor_keywords:
        for keyword in vendor_keywords:
            like = f"%{keyword.lower()}%"
            where.append(
                "(LOWER(description_text) LIKE %s OR LOWER(affected_products_json) LIKE %s OR LOWER(affected_cpes_json) LIKE %s OR LOWER(reference_domains_json) LIKE %s)"
            )
            params.extend([like, like, like, like])
    if product_keywords:
        for keyword in product_keywords:
            like = f"%{keyword.lower()}%"
            where.append(
                "(LOWER(description_text) LIKE %s OR LOWER(affected_products_json) LIKE %s OR LOWER(affected_cpes_json) LIKE %s OR LOWER(reference_domains_json) LIKE %s)"
            )
            params.extend([like, like, like, like])
    if in_scope and has_scope:
        where.append("cve_id IN (SELECT cve_id FROM cve_scope WHERE in_scope = 1)")
    elif in_scope and settings:
        filters = settings.get("filters") or {}
        scope_sevs = filters.get("severities") or []
        if scope_sevs:
            where.append(
                "preferred_base_severity IN ({})".format(",".join("%s" for _ in scope_sevs))
            )
            params.extend([severity.upper() for severity in scope_sevs])
        min_score = filters.get("min_cvss")
        if min_score is not None:
            where.append("preferred_base_score >= %s")
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
                keyword_where.append("LOWER(description_text) LIKE %s")
                params.append(like)
                keyword_where.append("LOWER(affected_products_json) LIKE %s")
                params.append(like)
                keyword_where.append("LOWER(affected_cpes_json) LIKE %s")
                params.append(like)
                keyword_where.append("LOWER(reference_domains_json) LIKE %s")
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
        "kev_cve_id",
        "kev_checked_at",
    ]
    selected = [col for col in selected if col in columns]
    cursor = conn.execute(
        f"""
        SELECT {", ".join(selected)}
        FROM cves
        {where_sql}
        ORDER BY last_modified_at DESC
        LIMIT %s OFFSET %s
        """,
        [*params, page_size, offset],
    )
    items = []
    rows = cursor.fetchall()
    data_rows = [dict(zip(selected, row)) for row in rows]
    cve_ids = [row.get("cve_id") for row in data_rows if row.get("cve_id")]
    scope_map: dict[str, dict[str, object]] = {}
    if has_scope and cve_ids:
        scope_cursor = conn.execute(
            """
            SELECT cve_id, in_scope, reasons_json
            FROM cve_scope
            WHERE cve_id = ANY(%s)
            """,
            (cve_ids,),
        )
        for cve_id_val, in_scope_val, reasons_json in scope_cursor.fetchall():
            scope_map[str(cve_id_val)] = {
                "in_scope": bool(in_scope_val),
                "reasons_json": reasons_json,
            }
    kev_map = get_cve_kev_map(conn, cve_ids) if has_kev and cve_ids else {}
    for data in data_rows:
        cve_id = data.get("cve_id")
        scope_info = scope_map.get(str(cve_id)) if cve_id else None
        kev_info = kev_map.get(str(cve_id)) if cve_id else None
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
                "in_scope": scope_info.get("in_scope") if scope_info else None,
                "scope_reasons": json.loads(scope_info.get("reasons_json") or "[]") if scope_info else [],
                "kev_cve_id": data.get("kev_cve_id"),
                "kev_checked_at": data.get("kev_checked_at"),
                "kev_due_date": kev_info.get("due_date") if kev_info else None,
                "kev_added_at": kev_info.get("added_at") if kev_info else None,
                "kev_ransomware": kev_info.get("ransomware_use") if kev_info else None,
            }
        )
    return items, total


def _list_cve_product_versions(conn: Any, cve_id: str | None) -> list[str]:
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
        WHERE cpv.cve_id = %s
        ORDER BY v.display_name, p.display_name, cpv.version
        """,
        (cve_id,),
    )
    return [
        f"{vendor}:{product}:{version}"
        for vendor, product, version in cursor.fetchall()
        if vendor and product and version
    ]


def list_watchlist_vendors(conn: Any) -> list[dict[str, object]]:
    if not _table_exists(conn, "watched_vendors"):
        return []
    cursor = conn.execute(
        """
        SELECT id, vendor_norm, display_name, enabled, created_at
        FROM watched_vendors
        ORDER BY display_name
        """
    )
    return [
        {
            "id": row[0],
            "vendor_norm": row[1],
            "display_name": row[2],
            "enabled": bool(row[3]),
            "created_at": row[4],
        }
        for row in cursor.fetchall()
    ]


def list_watchlist_products(conn: Any) -> list[dict[str, object]]:
    if not _table_exists(conn, "watched_products"):
        return []
    cursor = conn.execute(
        """
        SELECT id, vendor_norm, product_norm, display_name, match_mode, enabled, created_at
        FROM watched_products
        ORDER BY display_name
        """
    )
    return [
        {
            "id": row[0],
            "vendor_norm": row[1],
            "product_norm": row[2],
            "display_name": row[3],
            "match_mode": row[4],
            "enabled": bool(row[5]),
            "created_at": row[6],
        }
        for row in cursor.fetchall()
    ]


def add_watchlist_vendor(conn: Any, display_name: str) -> dict[str, object]:
    vendor_norm = normalize_name(display_name)
    record_id = f"wv_{uuid.uuid4().hex}"
    conn.execute(
        """
        INSERT INTO watched_vendors
            (id, vendor_norm, display_name, enabled, created_at)
        VALUES (%s, %s, %s, 1, %s)
        ON CONFLICT DO NOTHING
        """,
        (record_id, vendor_norm, display_name, utc_now_iso()),
    )
    conn.execute(
        """
        UPDATE watched_vendors
        SET display_name = %s, enabled = 1
        WHERE vendor_norm = %s
        """,
        (display_name, vendor_norm),
    )
    conn.commit()
    return {"id": record_id, "vendor_norm": vendor_norm, "display_name": display_name, "enabled": True}


def add_watchlist_product(
    conn: Any,
    display_name: str,
    vendor_norm: str | None,
    match_mode: str,
) -> dict[str, object]:
    product_norm = normalize_name(display_name)
    vendor_norm_val = normalize_name(vendor_norm) if vendor_norm else None
    record_id = f"wp_{uuid.uuid4().hex}"
    conn.execute(
        """
        INSERT INTO watched_products
            (id, vendor_norm, product_norm, display_name, match_mode, enabled, created_at)
        VALUES (%s, %s, %s, %s, %s, 1, %s)
        ON CONFLICT DO NOTHING
        """,
        (record_id, vendor_norm_val, product_norm, display_name, match_mode, utc_now_iso()),
    )
    conn.commit()
    return {
        "id": record_id,
        "vendor_norm": vendor_norm_val,
        "product_norm": product_norm,
        "display_name": display_name,
        "match_mode": match_mode,
        "enabled": True,
    }


def update_watchlist_vendor(conn: Any, vendor_id: str, enabled: bool) -> None:
    conn.execute(
        "UPDATE watched_vendors SET enabled = %s WHERE id = %s",
        (1 if enabled else 0, vendor_id),
    )
    conn.commit()


def update_watchlist_product(
    conn: Any, product_id: str, enabled: bool, match_mode: str | None = None
) -> None:
    if match_mode:
        conn.execute(
            "UPDATE watched_products SET enabled = %s, match_mode = %s WHERE id = %s",
            (1 if enabled else 0, match_mode, product_id),
        )
    else:
        conn.execute(
            "UPDATE watched_products SET enabled = %s WHERE id = %s",
            (1 if enabled else 0, product_id),
        )
    conn.commit()


def delete_watchlist_vendor(conn: Any, vendor_id: str) -> None:
    conn.execute("DELETE FROM watched_vendors WHERE id = %s", (vendor_id,))
    conn.commit()


def delete_watchlist_product(conn: Any, product_id: str) -> None:
    conn.execute("DELETE FROM watched_products WHERE id = %s", (product_id,))
    conn.commit()


def list_watchlist_suggestions(conn: Any, limit: int = 20) -> dict[str, list[dict[str, object]]]:
    vendors: list[dict[str, object]] = []
    products: list[dict[str, object]] = []
    if _table_exists(conn, "cve_products") and _table_exists(conn, "products") and _table_exists(conn, "vendors"):
        cursor = conn.execute(
            """
            SELECT v.display_name, v.name_norm, COUNT(DISTINCT cp.cve_id) AS cnt
            FROM cve_products cp
            JOIN products p ON p.id = cp.product_id
            JOIN vendors v ON v.id = p.vendor_id
            GROUP BY v.id
            ORDER BY cnt DESC
            LIMIT %s
            """,
            (limit,),
        )
        vendors = [
            {"display_name": row[0], "vendor_norm": row[1], "count": int(row[2] or 0)}
            for row in cursor.fetchall()
        ]
        cursor = conn.execute(
            """
            SELECT p.display_name, p.name_norm, v.name_norm, COUNT(DISTINCT cp.cve_id) AS cnt
            FROM cve_products cp
            JOIN products p ON p.id = cp.product_id
            JOIN vendors v ON v.id = p.vendor_id
            GROUP BY p.id
            ORDER BY cnt DESC
            LIMIT %s
            """,
            (limit,),
        )
        products = [
            {
                "display_name": row[0],
                "product_norm": row[1],
                "vendor_norm": row[2],
                "count": int(row[3] or 0),
            }
            for row in cursor.fetchall()
        ]
    return {"vendors": vendors, "products": products}


def list_cve_ids(conn: Any) -> list[str]:
    if not _table_exists(conn, "cves"):
        return []
    cursor = conn.execute("SELECT cve_id FROM cves")
    return [row[0] for row in cursor.fetchall() if row and row[0]]


def list_cve_ids_missing_description(conn: Any, limit: int | None = None) -> list[str]:
    if not _table_exists(conn, "cves"):
        return []
    sql = "SELECT cve_id FROM cves WHERE description_text IS NULL OR description_text = '' ORDER BY published_at DESC"
    params: list[object] = []
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    cursor = conn.execute(sql, tuple(params))
    return [row[0] for row in cursor.fetchall() if row and row[0]]


def _cve_vendor_product_norms(conn: Any, cve_id: str) -> list[tuple[str, str]]:
    if not (_table_exists(conn, "cve_products") and _table_exists(conn, "products") and _table_exists(conn, "vendors")):
        return []
    cursor = conn.execute(
        """
        SELECT v.name_norm, p.name_norm
        FROM cve_products cp
        JOIN products p ON p.id = cp.product_id
        JOIN vendors v ON v.id = p.vendor_id
        WHERE cp.cve_id = %s
        """,
        (cve_id,),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]

def list_cve_ids_missing_products(conn: Any, limit: int | None = None) -> list[str]:
    if not _table_exists(conn, "cves"):
        return []
    cve_columns = _table_columns(conn, "cves")
    checked_clause = (
        "AND c.cve_products_checked_at IS NULL"
        if "cve_products_checked_at" in cve_columns
        else ""
    )
    sql = """
    SELECT c.cve_id
    FROM cves c
    LEFT JOIN cve_products cp ON cp.cve_id = c.cve_id
    LEFT JOIN cve_product_versions cpv ON cpv.cve_id = c.cve_id
    WHERE (
        (c.description_text IS NOT NULL AND c.description_text != '')
        OR (
            c.reference_domains_json IS NOT NULL
            AND btrim(c.reference_domains_json) NOT IN ('', '[]', 'null')
        )
    )
    """ + checked_clause + """
    GROUP BY c.cve_id
    HAVING COUNT(cp.cve_id) = 0 OR COUNT(cpv.cve_id) = 0
    ORDER BY MAX(c.published_at) DESC
    """
    params: list[object] = []
    if limit:
        sql += " LIMIT %s"
        params.append(limit)
    cursor = conn.execute(sql, tuple(params))
    return [row[0] for row in cursor.fetchall() if row and row[0]]



def evaluate_cve_scope(
    conn: Any, cve_id: str, min_cvss: float | None = None
) -> dict[str, object]:
    reasons: list[str] = []
    in_scope = False
    if not _table_exists(conn, "cves"):
        return {"in_scope": False, "reasons": []}
    row = conn.execute(
        "SELECT preferred_base_score FROM cves WHERE cve_id = %s",
        (cve_id,),
    ).fetchone()
    preferred_score = row[0] if row else None
    if min_cvss is not None and preferred_score is not None:
        if float(preferred_score) >= float(min_cvss):
            in_scope = True
            reasons.append(f"severity>={min_cvss}")

    vendor_matches = []
    if _table_exists(conn, "watched_vendors"):
        cursor = conn.execute(
            "SELECT vendor_norm FROM watched_vendors WHERE enabled = 1"
        )
        vendor_matches = [row[0] for row in cursor.fetchall()]
    product_matches = []
    if _table_exists(conn, "watched_products"):
        cursor = conn.execute(
            "SELECT vendor_norm, product_norm, match_mode FROM watched_products WHERE enabled = 1"
        )
        product_matches = [(row[0], row[1], row[2]) for row in cursor.fetchall()]

    pairs = _cve_vendor_product_norms(conn, cve_id)
    for vendor_norm, product_norm in pairs:
        if vendor_norm in vendor_matches:
            in_scope = True
            reasons.append(f"matched_vendor:{vendor_norm}")
        for watch_vendor, watch_product, match_mode in product_matches:
            if watch_vendor and watch_vendor != vendor_norm:
                continue
            if match_mode == "contains":
                if watch_product and watch_product in product_norm:
                    in_scope = True
                    reasons.append(f"matched_product:{vendor_norm}:{watch_product}")
            else:
                if watch_product == product_norm:
                    in_scope = True
                    reasons.append(f"matched_product:{vendor_norm}:{product_norm}")
    return {"in_scope": in_scope, "reasons": reasons}


def upsert_cve_scope(conn: Any, cve_id: str, in_scope: bool, reasons: list[str]) -> None:
    if not _table_exists(conn, "cve_scope"):
        return
    conn.execute(
        """
        INSERT INTO cve_scope (id, cve_id, in_scope, reasons_json, computed_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(cve_id) DO UPDATE SET
            in_scope=excluded.in_scope,
            reasons_json=excluded.reasons_json,
            computed_at=excluded.computed_at
        """,
        (
            cve_id,
            cve_id,
            1 if in_scope else 0,
            json_dumps(reasons),
            utc_now_iso(),
        ),
    )
    conn.commit()


def compute_scope_for_cves(
    conn: Any, cve_ids: list[str], min_cvss: float | None = None
) -> dict[str, int]:
    updated = 0
    for cve_id in cve_ids:
        result = evaluate_cve_scope(conn, cve_id, min_cvss=min_cvss)
        upsert_cve_scope(conn, cve_id, bool(result["in_scope"]), list(result["reasons"]))
        updated += 1
    return {"updated": updated}


def list_cve_vendor_products(conn: Any, cve_id: str) -> list[dict[str, object]]:
    if not (_table_exists(conn, "cve_products") and _table_exists(conn, "products") and _table_exists(conn, "vendors")):
        return []
    cursor = conn.execute(
        """
        SELECT v.display_name, v.name_norm, p.display_name, p.name_norm, p.product_key
        FROM cve_products cp
        JOIN products p ON p.id = cp.product_id
        JOIN vendors v ON v.id = p.vendor_id
        WHERE cp.cve_id = %s
        ORDER BY v.display_name, p.display_name
        """,
        (cve_id,),
    )
    return [
        {
            "vendor_display": row[0],
            "vendor_norm": row[1],
            "product_display": row[2],
            "product_norm": row[3],
            "product_key": row[4],
        }
        for row in cursor.fetchall()
    ]


def list_threat_actors(
    conn: Any, query: str | None, page: int, page_size: int
) -> tuple[list[dict[str, object]], int]:
    if not _table_exists(conn, "threat_actors"):
        return [], 0
    where = ""
    params: list[object] = []
    if query:
        like = f"%{query}%"
        where = "WHERE ta.actor_key ILIKE %s OR ta.display_name ILIKE %s"
        params.extend([like, like])
    count_row = conn.execute(
        f"SELECT COUNT(*) FROM threat_actors ta {where}",
        params,
    ).fetchone()
    total = int(count_row[0] or 0) if count_row else 0
    offset = max(page - 1, 0) * page_size
    cursor = conn.execute(
        f"""
        SELECT ta.id,
               ta.actor_key,
               ta.display_name,
               ta.actor_type,
               COUNT(DISTINCT taa.alias) AS alias_count,
               COUNT(DISTINCT ata.article_id) AS article_count,
               COUNT(DISTINCT cta.cve_id) AS cve_count
        FROM threat_actors ta
        LEFT JOIN threat_actor_aliases taa ON taa.actor_id = ta.id
        LEFT JOIN article_threat_actors ata ON ata.actor_id = ta.id
        LEFT JOIN cve_threat_actors cta ON cta.actor_id = ta.id
        {where}
        GROUP BY ta.id
        ORDER BY (COUNT(DISTINCT ata.article_id) + COUNT(DISTINCT cta.cve_id)) DESC, ta.display_name
        LIMIT %s OFFSET %s
        """,
        [*params, page_size, offset],
    )
    items = []
    for row in cursor.fetchall():
        items.append(
            {
                "actor_id": row[0],
                "actor_key": row[1],
                "display_name": row[2],
                "actor_type": row[3],
                "alias_count": int(row[4] or 0),
                "article_count": int(row[5] or 0),
                "cve_count": int(row[6] or 0),
            }
        )
    return items, total


def get_threat_actor_detail(conn: Any, actor_key: str) -> dict[str, object] | None:
    if not _table_exists(conn, "threat_actors"):
        return None
    row = conn.execute(
        """
        SELECT id, actor_key, display_name, actor_type, country, confidence, first_seen, last_seen
        FROM threat_actors
        WHERE actor_key = %s
        """,
        (actor_key,),
    ).fetchone()
    if not row:
        return None
    actor_id = int(row[0])
    aliases = []
    if _table_exists(conn, "threat_actor_aliases"):
        aliases = [
            r[0]
            for r in conn.execute(
                "SELECT alias FROM threat_actor_aliases WHERE actor_id = %s ORDER BY alias",
                (actor_id,),
            ).fetchall()
        ]
    articles = []
    if _table_exists(conn, "article_threat_actors"):
        cursor = conn.execute(
            """
            SELECT a.id, a.title, a.published_at, a.ingested_at, s.name
            FROM article_threat_actors ata
            JOIN articles a ON a.id = ata.article_id
            LEFT JOIN sources s ON s.id = a.source_id
            WHERE ata.actor_id = %s
            ORDER BY COALESCE(a.published_at, a.ingested_at) DESC
            LIMIT 200
            """,
            (actor_id,),
        )
        for article_id, title, published_at, ingested_at, source_name in cursor.fetchall():
            articles.append(
                {
                    "id": article_id,
                    "title": title or "",
                    "published_at": published_at or ingested_at or "",
                    "source": source_name or "",
                }
            )
    cves = []
    if _table_exists(conn, "cve_threat_actors"):
        cursor = conn.execute(
            """
            SELECT c.cve_id, c.published_at, c.last_modified_at, c.preferred_base_severity
            FROM cve_threat_actors cta
            JOIN cves c ON c.cve_id = cta.cve_id
            WHERE cta.actor_id = %s
            ORDER BY COALESCE(c.published_at, c.last_modified_at) DESC
            LIMIT 200
            """,
            (actor_id,),
        )
        for cve_id, published_at, last_modified_at, severity in cursor.fetchall():
            cves.append(
                {
                    "cve_id": cve_id,
                    "published_at": published_at or last_modified_at or "",
                    "severity": severity or "",
                }
            )
    return {
        "actor_id": actor_id,
        "actor_key": row[1],
        "display_name": row[2],
        "actor_type": row[3],
        "country": row[4],
        "confidence": row[5],
        "first_seen": row[6],
        "last_seen": row[7],
        "aliases": aliases,
        "articles": articles,
        "cves": cves,
    }


def list_article_ids_missing_threat_actors(conn: Any, limit: int | None = 200) -> list[int]:
    if not (_table_exists(conn, "articles") and _table_exists(conn, "article_threat_actors")):
        return []
    article_columns = _table_columns(conn, "articles")
    checked_clause = (
        "AND a.article_threat_actors_checked_at IS NULL"
        if "article_threat_actors_checked_at" in article_columns
        else ""
    )
    sql = f"""
        SELECT a.id
        FROM articles a
        LEFT JOIN article_threat_actors ata ON ata.article_id = a.id
        WHERE (a.content_text IS NOT NULL AND a.content_text != '')
        {checked_clause}
        GROUP BY a.id
        HAVING COUNT(ata.article_id) = 0
        ORDER BY COALESCE(a.published_at, a.ingested_at) DESC
    """
    params: list[object] = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cursor = conn.execute(sql, tuple(params))
    return [int(row[0]) for row in cursor.fetchall() if row and row[0] is not None]


def list_cve_ids_missing_threat_actors(conn: Any, limit: int | None = 200) -> list[str]:
    if not (_table_exists(conn, "cves") and _table_exists(conn, "cve_threat_actors")):
        return []
    cve_columns = _table_columns(conn, "cves")
    checked_clause = (
        "AND c.cve_threat_actors_checked_at IS NULL"
        if "cve_threat_actors_checked_at" in cve_columns
        else ""
    )
    sql = f"""
        SELECT c.cve_id
        FROM cves c
        LEFT JOIN cve_threat_actors cta ON cta.cve_id = c.cve_id
        WHERE (
            (c.description_text IS NOT NULL AND c.description_text != '')
            OR (
                c.reference_domains_json IS NOT NULL
                AND btrim(c.reference_domains_json) NOT IN ('', '[]', 'null')
            )
        )
        {checked_clause}
        GROUP BY c.cve_id
        HAVING COUNT(cta.cve_id) = 0
        ORDER BY COALESCE(c.published_at, c.last_modified_at) DESC
    """
    params: list[object] = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cursor = conn.execute(sql, tuple(params))
    return [str(row[0]) for row in cursor.fetchall() if row and row[0]]


def _list_article_cve_ids(conn: Any, article_id: int) -> list[str]:
    if not _table_exists(conn, "article_cves"):
        return []
    cursor = conn.execute(
        "SELECT cve_id FROM article_cves WHERE article_id = %s",
        (article_id,),
    )
    return [row[0] for row in cursor.fetchall() if row and row[0]]


def list_article_cve_ids(conn: Any, article_id: int) -> list[str]:
    return _list_article_cve_ids(conn, article_id)


def list_event_ids_for_article(conn: Any, article_id: int) -> list[str]:
    if _table_exists(conn, "event_articles"):
        cursor = conn.execute(
            "SELECT event_id FROM event_articles WHERE article_id = %s",
            (article_id,),
        )
        return [row[0] for row in cursor.fetchall() if row and row[0]]
    if _table_exists(conn, "event_items"):
        cursor = conn.execute(
            """
            SELECT event_id
            FROM event_items
            WHERE item_type = 'article' AND item_key = %s
            """,
            (str(article_id),),
        )
        return [row[0] for row in cursor.fetchall() if row and row[0]]
    return []


def list_article_ids_without_event(conn: Any, limit: int | None = 200) -> list[int]:
    if not _table_exists(conn, "articles"):
        return []
    if _table_exists(conn, "event_articles"):
        sql = """
            SELECT a.id
            FROM articles a
            LEFT JOIN event_articles ea ON ea.article_id = a.id
            WHERE ea.article_id IS NULL
              AND COALESCE(BTRIM(a.content_text), '') <> ''
            ORDER BY a.published_at DESC NULLS LAST
        """
        params: list[object] = []
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
        cursor = conn.execute(sql, tuple(params))
    elif _table_exists(conn, "event_items"):
        sql = """
            SELECT a.id
            FROM articles a
            LEFT JOIN event_items ei ON ei.item_type = 'article' AND ei.item_key = CAST(a.id AS TEXT)
            WHERE ei.event_id IS NULL
              AND COALESCE(BTRIM(a.content_text), '') <> ''
            ORDER BY a.published_at DESC NULLS LAST
        """
        params = []
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
        cursor = conn.execute(sql, tuple(params))
    else:
        sql = """
            SELECT id
            FROM articles
            WHERE COALESCE(BTRIM(content_text), '') <> ''
            ORDER BY published_at DESC NULLS LAST
        """
        params = []
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
        cursor = conn.execute(sql, tuple(params))
    return [row[0] for row in cursor.fetchall() if row and row[0] is not None]


def compute_watchlist_hits(
    conn: Any,
    *,
    item_type: str,
    item_key: str | int,
    min_cvss: float | None = None,
) -> dict[str, object]:
    if item_type == "cve":
        result = evaluate_cve_scope(conn, str(item_key), min_cvss=min_cvss)
        return {"hit": bool(result["in_scope"]), "reasons": list(result["reasons"])}
    if item_type == "article":
        reasons: list[str] = []
        hit = False
        try:
            article_id = int(item_key)
        except (TypeError, ValueError):
            return {"hit": False, "reasons": []}
        for cve_id in _list_article_cve_ids(conn, article_id):
            result = evaluate_cve_scope(conn, str(cve_id), min_cvss=min_cvss)
            if result["in_scope"]:
                hit = True
                reasons.append(f"cve:{cve_id}")
                reasons.extend(list(result["reasons"]))
        return {"hit": hit, "reasons": reasons}
    return {"hit": False, "reasons": []}


def cve_data_completeness(conn: Any, limit: int = 20) -> dict[str, object]:
    if not _table_exists(conn, "cves"):
        return {"counts": {}, "missing": []}
    columns = _table_columns(conn, "cves")
    total = count_table(conn, "cves")
    def _count_where(clause: str) -> int:
        row = conn.execute(f"SELECT COUNT(*) FROM cves c WHERE {clause}").fetchone()
        return int(row[0] or 0)

    counts = {"total": total}
    if "description_text" in columns:
        counts["with_description"] = _count_where("c.description_text IS NOT NULL AND c.description_text != ''")
        counts["good_description"] = _count_where("length(c.description_text) >= 80")
    if "reference_domains_json" in columns:
        counts["with_domains"] = _count_where("c.reference_domains_json IS NOT NULL AND c.reference_domains_json != '[]'")
    if "affected_products_json" in columns:
        counts["with_products"] = _count_where("c.affected_products_json IS NOT NULL AND c.affected_products_json != '[]'")
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
        cvss_any.append("c.preferred_base_score IS NOT NULL")
    counts["has_any_cvss"] = _count_where(" OR ".join(cvss_any)) if cvss_any else 0

    where_missing = []
    if "description_text" in columns:
        where_missing.append("(c.description_text IS NULL OR c.description_text = '')")
    if "reference_domains_json" in columns:
        where_missing.append("(c.reference_domains_json IS NULL OR c.reference_domains_json = '[]')")
    if "affected_products_json" in columns:
        where_missing.append("(c.affected_products_json IS NULL OR c.affected_products_json = '[]')")
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
            parts.append("c.preferred_base_score IS NULL")
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
            SELECT c.cve_id, c.description_text, c.affected_products_json, c.reference_domains_json,
                   c.cvss_v31_json, c.cvss_v40_json, c.cvss_v31_list_json, c.cvss_v40_list_json,
                   c.preferred_base_score
            FROM cves c
            WHERE {" OR ".join(where_missing)}
            ORDER BY c.published_at DESC
            LIMIT %s
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




def update_article_suppressed(
    conn: Any, article_id: int, suppressed: bool, reason: str | None = None
) -> dict[str, object]:
    if not _table_exists(conn, "articles"):
        raise ValueError("articles table not found")
    row = conn.execute(
        "SELECT meta_json FROM articles WHERE id = %s",
        (article_id,),
    ).fetchone()
    meta: dict[str, object] = {}
    if row and row[0]:
        try:
            meta = json.loads(row[0])
            if not isinstance(meta, dict):
                meta = {}
        except Exception:
            meta = {}
    meta["suppressed"] = bool(suppressed)
    if suppressed:
        meta["suppressed_at"] = utc_now_iso()
        if reason:
            meta["suppressed_reason"] = reason
    else:
        meta.pop("suppressed_at", None)
        meta.pop("suppressed_reason", None)
    conn.execute(
        "UPDATE articles SET meta_json = %s, updated_at = %s WHERE id = %s",
        (json.dumps(meta), utc_now_iso(), article_id),
    )
    conn.commit()
    return {"id": article_id, "suppressed": bool(suppressed)}
def update_article_content(
    conn: Any,
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
        SET content_text = %s, content_html = %s, content_fetched_at = %s,
            content_error = %s, has_full_content = %s
        WHERE id = %s
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
    conn: Any,
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
        SET summary_llm = %s, summary_model = %s, summary_generated_at = %s,
            summary_error = %s
        WHERE id = %s
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


def update_article_context_pack(
    conn: Any,
    article_id: int,
    *,
    context_llm: str | None,
    context_model: str | None,
    context_generated_at: str | None,
    context_error: str | None,
) -> None:
    conn.execute(
        """
        UPDATE articles
        SET context_llm = %s, context_model = %s, context_generated_at = %s,
            context_error = %s, updated_at = %s
        WHERE id = %s
        """,
        (
            context_llm,
            context_model,
            context_generated_at,
            context_error,
            utc_now_iso(),
            article_id,
        ),
    )
    conn.commit()


def _insert_article_tags(conn: Any, article_id: int, tags: list[str]) -> None:
    if not tags:
        return
    rows = [(article_id, tag, None) for tag in tags]
    conn.executemany(
        """
        INSERT INTO article_tags (article_id, tag, tag_type)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.commit()


def _source_from_dict(source_dict: dict[str, object]) -> Source:
    source_id = source_dict.get("id")
    name = source_dict.get("name") or source_id
    enabled = source_dict.get("enabled", True)
    base_url = source_dict.get("base_url") or source_dict.get("url")
    kind = source_dict.get("kind")
    url = source_dict.get("url") or base_url
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
        kind=str(kind) if kind else None,
        url=str(url) if url else None,
    )


def _last_run_map(conn: Any) -> dict[str, str]:
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
