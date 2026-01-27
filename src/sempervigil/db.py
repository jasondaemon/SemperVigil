from __future__ import annotations

import os
from typing import Any

from .migrations_pg import apply_migrations_pg

_MIGRATIONS_APPLIED = {"postgres": False}


def get_db_url() -> str:
    url = os.environ.get("SV_DB_URL", "").strip()
    if not url:
        raise RuntimeError("SV_DB_URL is required for Postgres")
    return url


def is_postgres_url(url: str | None) -> bool:
    if not url:
        return False
    return url.startswith("postgres://") or url.startswith("postgresql://")


class DBConn:
    def __init__(self, conn: Any, backend: str) -> None:
        self._conn = conn
        self.backend = backend

    def execute(self, sql: str, params: tuple | list | None = None):
        params = params or ()
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return cursor

    def executemany(self, sql: str, seq_of_params):
        cursor = self._conn.cursor()
        cursor.executemany(sql, seq_of_params)
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __getattr__(self, name: str):
        return getattr(self._conn, name)


def connect_db() -> DBConn:
    url = get_db_url()
    if not is_postgres_url(url):
        raise RuntimeError("SV_DB_URL must be a PostgreSQL URL")
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
