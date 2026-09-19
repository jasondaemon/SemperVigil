"""Explicit disposable-PostgreSQL gate; never collected by the offline suite."""
from contextlib import contextmanager
import json
import os

import psycopg
from psycopg.rows import dict_row
import pytest

from sempervigil.investigation import InvestigationReader, READ_SCOPE, postgres_reader


def test_readonly_retrieval_on_disposable_postgres():
    # Temporary tables shadow application tables; no schema initialization/migration.
    with psycopg.connect(os.environ["SV_TEST_DB_URL"], autocommit=True, row_factory=dict_row) as conn:
        conn.execute("""CREATE TEMP TABLE articles(id BIGINT PRIMARY KEY, source_id TEXT,
            title TEXT, original_url TEXT, brief_day TEXT, meta_json TEXT)""")
        conn.execute("""CREATE TEMP TABLE events(id TEXT PRIMARY KEY, kind TEXT, title TEXT,
            severity TEXT, status TEXT, lifecycle TEXT, updated_at TEXT, visibility TEXT)""")
        for id, meta in ((1, None), (2, '{"suppressed":true}'), (3, '{')):
            conn.execute("INSERT INTO articles VALUES (%s, 'source', '100%%_ incident', 'https://example.org', '2026-05-01', %s)", (id, meta))
        conn.execute("""INSERT INTO events VALUES ('one','breach','Acme','high',
            'open','active','2026-05-01','active')""")

        @contextmanager
        def session():
            with conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                conn.execute("SET LOCAL statement_timeout = '2s'")
                yield conn

        reader = InvestigationReader(session)
        scopes = frozenset({READ_SCOPE})
        result = reader.search_articles(json.dumps({"start_day": "2026-05-01",
            "end_day": "2026-05-31", "query": "%_", "limit": 1}).encode(), scopes=scopes)
        assert [item["id"] for item in result["items"]] == [1]
        assert not result["has_more"]
        event = reader.get_event_record(b'{"event_id":"one"}', scopes=scopes)
        assert event["record"]["title"] == "Acme"
        assert not event["validated_revision"]

    with postgres_reader(os.environ["SV_TEST_DB_URL"]) as conn:
        assert conn.execute("SHOW transaction_read_only").fetchone()["transaction_read_only"] == "on"
        assert conn.execute("SHOW statement_timeout").fetchone()["statement_timeout"] == "2s"
        # A schema write must be rejected before any persistent relation is created.
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("CREATE TABLE sempervigil_investigation_must_not_write (id INTEGER)")
        conn.rollback()
