from __future__ import annotations

import os
import sqlite3
from typing import Any

from .migrations import apply_migrations
from .migrations_pg import apply_migrations_pg

_MIGRATIONS_APPLIED = {"sqlite": False, "postgres": False}


def get_db_url() -> str | None:
    url = os.environ.get("SV_DB_URL", "").strip()
    return url or None


def is_postgres_url(url: str | None) -> bool:
    if not url:
        return False
    return url.startswith("postgres://") or url.startswith("postgresql://")


class DBConn:
    def __init__(self, conn: Any, backend: str) -> None:
        self._conn = conn
        self.backend = backend

    def execute(self, sql: str, params: tuple | list | None = None):
        sql = _normalize_sql(sql, self.backend)
        params = params or ()
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return cursor

    def executemany(self, sql: str, seq_of_params):
        sql = _normalize_sql(sql, self.backend)
        cursor = self._conn.cursor()
        cursor.executemany(sql, seq_of_params)
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


def connect_db(path: str) -> DBConn:
    url = get_db_url()
    if url and is_postgres_url(url):
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - depends on env
            raise RuntimeError("psycopg is required for PostgreSQL support") from exc
        raw = psycopg.connect(url)
        conn = DBConn(raw, "postgres")
        if not _MIGRATIONS_APPLIED["postgres"]:
            apply_migrations_pg(conn)
            _MIGRATIONS_APPLIED["postgres"] = True
        return conn

    os.makedirs(os.path.dirname(path), exist_ok=True)
    raw = sqlite3.connect(path)
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA synchronous=NORMAL")
    raw.execute("PRAGMA busy_timeout=5000")
    if not _MIGRATIONS_APPLIED["sqlite"]:
        apply_migrations(raw)
        _MIGRATIONS_APPLIED["sqlite"] = True
    return DBConn(raw, "sqlite")


def _normalize_sql(sql: str, backend: str) -> str:
    if backend != "postgres":
        return sql
    normalized = _replace_insert_or_ignore(sql)
    normalized = normalized.replace("BEGIN IMMEDIATE", "BEGIN")
    normalized = _convert_qmark_to_percent(normalized)
    return normalized


def _replace_insert_or_ignore(sql: str) -> str:
    upper = sql.upper()
    if "INSERT OR IGNORE" not in upper:
        return sql
    replaced = _replace_first_case_insensitive(sql, "INSERT OR IGNORE", "INSERT")
    if "ON CONFLICT" in replaced.upper():
        return replaced
    stripped = replaced.rstrip().rstrip(";")
    return stripped + " ON CONFLICT DO NOTHING"


def _replace_first_case_insensitive(text: str, needle: str, replacement: str) -> str:
    idx = text.upper().find(needle.upper())
    if idx == -1:
        return text
    return text[:idx] + replacement + text[idx + len(needle) :]


def _convert_qmark_to_percent(sql: str) -> str:
    out = []
    in_single = False
    in_double = False
    escape = False
    for ch in sql:
        if ch == "\\" and not escape:
            escape = True
            out.append(ch)
            continue
        if ch == "'" and not in_double and not escape:
            in_single = not in_single
        elif ch == '"' and not in_single and not escape:
            in_double = not in_double
        if ch == "?" and not in_single and not in_double:
            out.append("%s")
        else:
            out.append(ch)
        escape = False
    return "".join(out)
