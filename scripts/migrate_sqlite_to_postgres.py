from __future__ import annotations

import argparse
import os
import sqlite3
from typing import Iterable


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default=os.environ.get("SV_SQLITE_PATH", "/data/state.sqlite3"))
    parser.add_argument("--pg-url", default=os.environ.get("SV_DB_URL", ""))
    return parser.parse_args()


def _list_tables(conn: sqlite3.Connection) -> list[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return [row[0] for row in cursor.fetchall()]


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _chunked(rows: Iterable[tuple], size: int = 500) -> Iterable[list[tuple]]:
    batch: list[tuple] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def main() -> int:
    args = _parse_args()
    if not args.pg_url:
        raise SystemExit("SV_DB_URL is required for Postgres migration")

    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit("psycopg is required for Postgres migration") from exc

    sqlite_conn = sqlite3.connect(args.sqlite)
    pg_conn = psycopg.connect(args.pg_url)
    pg_conn.autocommit = False

    tables = _list_tables(sqlite_conn)
    skip = {"schema_migrations"}
    for table in tables:
        if table in skip:
            continue
        columns = _table_columns(sqlite_conn, table)
        if not columns:
            continue
        cols_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = (
            f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) "
            "ON CONFLICT DO NOTHING"
        )
        cursor = sqlite_conn.execute(f"SELECT {cols_sql} FROM {table}")
        for batch in _chunked(cursor.fetchall(), 500):
            pg_conn.execute("BEGIN")
            with pg_conn.cursor() as pg_cursor:
                pg_cursor.executemany(insert_sql, batch)
            pg_conn.commit()

    sqlite_conn.close()
    pg_conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
