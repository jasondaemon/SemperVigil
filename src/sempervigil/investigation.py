"""Bounded read-only investigation services. Not wired to API, MCP, or workers.

The host supplies authenticated scopes, never the request body. Discovery
metadata is not trusted evidence or permission to publish a record.
"""
from contextlib import contextmanager
from datetime import date
import hashlib
import json
from typing import Callable
from urllib.parse import urlsplit

import psycopg
from psycopg.rows import dict_row

REQUEST_BYTES = 4096
RESPONSE_BYTES = 65536
SCAN_ROWS = 200
READ_SCOPE = "investigation:read"
EVIDENCE_SCOPE = "investigation:evidence:read"
DOCUMENT_CHARS = 131072
PASSAGE_CHARS = 2048


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_field")
        result[key] = value
    return result


def _bad_constant(value):
    raise ValueError("invalid_json_constant")


def parse_request(raw: bytes, *, fields: set[str]) -> dict:
    if type(raw) is not bytes or not 0 < len(raw) <= REQUEST_BYTES:
        raise ValueError("request_size")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                           parse_constant=_bad_constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("invalid_json") from exc
    if not isinstance(value, dict) or value.keys() - fields:
        raise ValueError("unexpected_fields")
    return value


def _text(value, maximum: int) -> bool:
    return isinstance(value, str) and 0 < len(value) <= maximum and not any(
        ord(char) < 32 or 0xD800 <= ord(char) <= 0xDFFF for char in value)


def _positive(value, maximum: int) -> bool:
    return type(value) is int and 1 <= value <= maximum


def _day(value) -> date:
    if not isinstance(value, str) or len(value) != 10:
        raise ValueError("invalid_day")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("invalid_day")
    return parsed


def _visible_metadata(raw) -> bool:
    # Match the existing truthy suppression policy, but fail closed on bad data.
    if raw is None or raw == "":
        return True
    if not isinstance(raw, str) or len(raw) > 8192:
        return False
    try:
        meta = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_bad_constant)
        return isinstance(meta, dict) and not bool(meta.get("suppressed"))
    except (ValueError, RecursionError):
        return False


def _safe_url(value) -> bool:
    if not _text(value, 2048):
        return False
    try:
        url = urlsplit(value)
        return url.scheme in {"https", "http"} and bool(url.hostname) and not url.username and not url.password
    except ValueError:
        return False


def _version(record: dict) -> str:
    return hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=True,
                                     separators=(",", ":")).encode()).hexdigest()


@contextmanager
def postgres_reader(dsn: str):
    """Fresh, bounded read-only transaction; never initialize or migrate the DB.

    Deployment must additionally provision a least-privilege database role.
    """
    with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row,
                        options="-c default_transaction_read_only=on -c statement_timeout=2000 "
                                "-c lock_timeout=250 -c idle_in_transaction_session_timeout=5000") as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        yield conn


class InvestigationReader:
    def __init__(self, session: Callable):
        """Accept a trusted read-only session factory, not client connection data."""
        self._session = session

    @staticmethod
    def _authorize(scopes: frozenset[str]) -> None:
        if READ_SCOPE not in scopes:
            raise PermissionError("read_scope_required")

    def search_articles(self, raw: bytes, *, scopes: frozenset[str]) -> dict:
        self._authorize(scopes)
        request = parse_request(raw, fields={"start_day", "end_day", "query", "source_id", "limit", "before_id"})
        start, end = _day(request.get("start_day")), _day(request.get("end_day"))
        if not 0 <= (end - start).days < 31:
            raise ValueError("day_window_exceeds_31_days")
        limit = request.get("limit", 20)
        before = request.get("before_id", 9223372036854775807)
        if not _positive(limit, 50) or not _positive(before, 9223372036854775807):
            raise ValueError("invalid_pagination")
        clauses = ["brief_day >= %s", "brief_day <= %s", "id < %s", "length(brief_day) = 10"]
        params = [start.isoformat(), end.isoformat(), before]
        for name, maximum in (("query", 200), ("source_id", 128)):
            if name in request:
                value = request[name]
                if not _text(value, maximum) or not value.strip():
                    raise ValueError("invalid_" + name)
                if name == "query":
                    escaped = value.replace("!", "!!").replace("%", "!%").replace("_", "!_")
                    clauses.append("LOWER(title) LIKE LOWER(%s) ESCAPE '!'")
                    params.append("%" + escaped + "%")
                else:
                    clauses.append("source_id = %s")
                    params.append(value)
        # Do not load HTML, extracted paths, summaries, or arbitrary metadata.
        sql = """SELECT id,
          CASE WHEN length(source_id) <= 128 THEN source_id END AS source_id,
          CASE WHEN length(title) <= 512 THEN title END AS title,
          CASE WHEN length(original_url) <= 2048 THEN original_url END AS url,
          brief_day,
          CASE WHEN length(meta_json) <= 8192 THEN meta_json
               WHEN meta_json IS NULL THEN NULL ELSE 'invalid' END AS policy_meta
          FROM articles WHERE """ + " AND ".join(clauses) + " ORDER BY id DESC LIMIT %s"
        with self._session() as conn:
            rows = conn.execute(sql, (*params, SCAN_ROWS + 1)).fetchall()
        result = {"items": [], "next_before_id": None, "has_more": False,
                  "coverage": "stored_brief_day_and_title_only", "snapshot": "per_request",
                  "omissions_possible": True, "stop_reason": "window_exhausted"}
        last = None
        for index, row in enumerate(rows[:SCAN_ROWS]):
            if (_visible_metadata(row["policy_meta"]) and _text(row["title"], 512)
                    and _text(row["source_id"], 128) and _safe_url(row["url"])):
                item = {key: row[key] for key in ("id", "source_id", "title", "url", "brief_day")}
                item["metadata_version"] = _version(item)
                result["items"].append(item)
                # Reserve space for pagination metadata before admitting another row.
                if len(json.dumps(result, ensure_ascii=True).encode()) > RESPONSE_BYTES - 1024:
                    result["items"].pop()
                    result.update(next_before_id=last or before, has_more=True, stop_reason="response_limit")
                    return result
            last = row["id"]
            if len(result["items"]) == limit:
                more = index + 1 < len(rows)
                result.update(next_before_id=last if more else None, has_more=more,
                              stop_reason="page_limit" if more else "window_exhausted")
                return result
        if len(rows) > SCAN_ROWS:
            result.update(next_before_id=last, has_more=True, stop_reason="scan_limit")
        return result

    def get_event_record(self, raw: bytes, *, scopes: frozenset[str]) -> dict:
        self._authorize(scopes)
        request = parse_request(raw, fields={"event_id"})
        event_id = request.get("event_id")
        if not _text(event_id, 128) or not event_id.strip():
            raise ValueError("invalid_event_id")
        # Legacy report prose and linkage collections are deliberately not exposed.
        with self._session() as conn:
            row = conn.execute("""SELECT id, kind, title, severity, status, lifecycle, updated_at
                FROM events WHERE id = %s AND visibility = 'active'
                AND length(title) <= 512 AND length(kind) <= 128
                AND length(status) <= 128 AND length(lifecycle) <= 128
                AND (severity IS NULL OR length(severity) <= 128)
                AND length(updated_at) <= 64""", (event_id,)).fetchone()
        record = dict(row) if row else None
        if record is not None:
            record["metadata_version"] = _version(record)
        return {"record": record, "record_kind": "legacy_event_metadata",
                "validated_revision": False, "linked_evidence_included": False}

    def get_article_evidence(self, raw: bytes, *, scopes: frozenset[str]) -> dict:
        """Read exact stored text, not a model summary or an incident assignment.

        Subsequent slices require the first response's version. This checks the
        current DB snapshot but does not persist old text or approve citations.
        """
        self._authorize(scopes)
        if EVIDENCE_SCOPE not in scopes:
            raise PermissionError("evidence_scope_required")
        request = parse_request(raw, fields={"article_id", "start", "max_chars", "expected_version"})
        article_id = request.get("article_id")
        start, maximum = request.get("start", 0), request.get("max_chars", PASSAGE_CHARS)
        expected = request.get("expected_version")
        if not _positive(article_id, 9223372036854775807):
            raise ValueError("invalid_article_id")
        if type(start) is not int or not 0 <= start < DOCUMENT_CHARS:
            raise ValueError("invalid_start")
        if not _positive(maximum, PASSAGE_CHARS):
            raise ValueError("invalid_max_chars")
        if "expected_version" in request and (not isinstance(expected, str)
                or len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected)):
            raise ValueError("invalid_expected_version")
        if start and expected is None:
            raise ValueError("expected_version_required")
        with self._session() as conn:
            row = conn.execute("""SELECT id,
              CASE WHEN length(source_id) <= 128 THEN source_id END AS source_id,
              CASE WHEN length(title) <= 512 THEN title END AS title,
              CASE WHEN length(original_url) <= 2048 THEN original_url END AS url,
              CASE WHEN length(meta_json) <= 8192 THEN meta_json
                   WHEN meta_json IS NULL THEN NULL ELSE 'invalid' END AS policy_meta,
              length(content_text) AS text_length,
              CASE WHEN length(content_text) <= %s THEN content_text END AS content_text
              FROM articles WHERE id = %s""", (DOCUMENT_CHARS, article_id)).fetchone()
        if (row is None or not _visible_metadata(row["policy_meta"])
                or not _text(row["source_id"], 128) or not _text(row["title"], 512)
                or not _safe_url(row["url"])):
            return {"status": "unavailable"}
        if row["text_length"] is not None and row["text_length"] > DOCUMENT_CHARS:
            return {"status": "content_too_large"}
        text = row["content_text"]
        if text is None or not text.strip():
            return {"status": "content_missing"}
        snapshot = {key: row[key] for key in ("id", "source_id", "title", "url", "content_text")}
        snapshot["version_schema"] = "article_text_v1"
        version = _version(snapshot)
        if expected is not None and version != expected:
            return {"status": "stale_snapshot"}
        if start >= len(text):
            raise ValueError("start_past_content")
        end = min(start + maximum, len(text))
        result = {"status": "available", "article_id": article_id,
                  "source_id": row["source_id"], "url": row["url"], "title": row["title"],
                  "document_version": version, "version_schema": "article_text_v1",
                  "text_basis": "stored_content_text", "offset_unit": "unicode_code_point",
                  "start": start, "end": end, "text": text[start:end], "total_chars": len(text),
                  "next_start": end if end < len(text) else None,
                  "origin_id": None, "incident_id": None,
                  "scope_status": "unassigned", "validated_evidence": False}
        if len(json.dumps(result, ensure_ascii=True).encode()) > RESPONSE_BYTES:
            raise ValueError("evidence_response_size")
        return result
