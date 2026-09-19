"""Explicit disposable-PostgreSQL gate; never collected by the offline suite."""
from contextlib import contextmanager
import json
import os
import secrets
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from sempervigil.investigation import EVIDENCE_SCOPE, InvestigationReader, READ_SCOPE, postgres_reader


@pytest.fixture(scope="module", autouse=True)
def require_named_disposable_database():
    with postgres_reader(os.environ["SV_TEST_DB_URL"]) as conn:
        name = conn.execute("SELECT current_database() AS name").fetchone()["name"]
    if name != "sempervigil_test" and not name.startswith("sempervigil_test_"):
        pytest.fail("Use an explicitly disposable database named sempervigil_test or sempervigil_test_*")


def test_readonly_retrieval_on_disposable_postgres():
    # Temporary tables shadow application tables; no schema initialization/migration.
    with psycopg.connect(os.environ["SV_TEST_DB_URL"], autocommit=True, row_factory=dict_row) as conn:
        conn.execute("""CREATE TEMP TABLE articles(id BIGINT PRIMARY KEY, source_id TEXT,
            title TEXT, original_url TEXT, brief_day TEXT, meta_json TEXT, content_text TEXT)""")
        conn.execute("""CREATE TEMP TABLE events(id TEXT PRIMARY KEY, kind TEXT, title TEXT,
            severity TEXT, status TEXT, lifecycle TEXT, updated_at TEXT, visibility TEXT)""")
        for id, meta in ((1, None), (2, '{"suppressed":true}'), (3, '{')):
            conn.execute("INSERT INTO articles VALUES (%s, 'source', '100%%_ incident', 'https://example.org', '2026-05-01', %s, NULL)", (id, meta))
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

        text = "Caf\u00e9 \U0001f512\nIncident details."
        conn.execute("UPDATE articles SET content_text=%s WHERE id=1", (text,))
        evidence_scopes = scopes | {EVIDENCE_SCOPE}
        first = reader.get_article_evidence(b'{"article_id":1,"max_chars":6}', scopes=evidence_scopes)
        assert first["text"] == text[:6]
        continuation = json.dumps({"article_id": 1, "start": 6,
                                   "expected_version": first["document_version"]}).encode()
        second = reader.get_article_evidence(continuation, scopes=evidence_scopes)
        assert first["text"] + second["text"] == text
        conn.execute("UPDATE articles SET content_text=%s WHERE id=1", (text + " updated",))
        assert reader.get_article_evidence(continuation, scopes=evidence_scopes) == {"status": "stale_snapshot"}
        conn.execute("UPDATE articles SET meta_json=%s WHERE id=1", ('{"suppressed":true}',))
        assert reader.get_article_evidence(continuation, scopes=evidence_scopes) == {"status": "unavailable"}

    with postgres_reader(os.environ["SV_TEST_DB_URL"]) as conn:
        assert conn.execute("SHOW transaction_read_only").fetchone()["transaction_read_only"] == "on"
        assert conn.execute("SHOW statement_timeout").fetchone()["statement_timeout"] == "2s"
        # A schema write must be rejected before any persistent relation is created.
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("CREATE TABLE sempervigil_investigation_must_not_write (id INTEGER)")
        conn.rollback()


def test_statement_timeout_is_enforced():
    with postgres_reader(os.environ["SV_TEST_DB_URL"]) as conn:
        with pytest.raises(psycopg.errors.QueryCanceled):
            conn.execute("SELECT pg_sleep(3)")
        conn.rollback()


def test_least_privilege_role_can_read_but_cannot_write():
    """Disposable-only role/schema; no application migration or existing data."""
    dsn = os.environ["SV_TEST_DB_URL"]
    name = "sv_investigation_test_" + uuid4().hex[:16]
    password = secrets.token_urlsafe(32)
    identifier = sql.Identifier(name)
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD {}").format(identifier, sql.Literal(password)))
        try:
            admin.execute(sql.SQL("CREATE SCHEMA {}").format(identifier))
            admin.execute(sql.SQL("SET search_path TO {}").format(identifier))
            admin.execute("""CREATE TABLE articles(id BIGINT PRIMARY KEY, source_id TEXT,
                title TEXT, original_url TEXT, brief_day TEXT, meta_json TEXT, content_text TEXT)""")
            admin.execute("""CREATE TABLE events(id TEXT PRIMARY KEY, kind TEXT, title TEXT,
                severity TEXT, status TEXT, lifecycle TEXT, updated_at TEXT, visibility TEXT)""")
            admin.execute("""INSERT INTO articles VALUES (1, 'source', 'Incident',
                'https://example.org', '2026-05-01', NULL, 'Exact original text.')""")
            admin.execute("""INSERT INTO events VALUES ('one', 'breach', 'Incident',
                'high', 'open', 'active', '2026-05-01', 'active')""")
            admin.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(identifier, identifier))
            admin.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(identifier, identifier))
            admin.execute(sql.SQL("ALTER ROLE {} SET search_path TO {}").format(identifier, identifier))
            reader_dsn = make_conninfo(dsn, user=name, password=password)
            reader = InvestigationReader(lambda: postgres_reader(reader_dsn))
            scopes = frozenset({READ_SCOPE, EVIDENCE_SCOPE})
            result = reader.search_articles(b'{"start_day":"2026-05-01","end_day":"2026-05-31"}', scopes=scopes)
            assert [item["id"] for item in result["items"]] == [1]
            assert reader.get_event_record(b'{"event_id":"one"}', scopes=scopes)["record"]["id"] == "one"
            assert reader.get_article_evidence(b'{"article_id":1}', scopes=scopes)["text"] == "Exact original text."
            # Test permissions without the factory's read-only transaction guard.
            with psycopg.connect(reader_dsn, autocommit=True) as restricted:
                assert not restricted.execute("SELECT rolsuper FROM pg_roles WHERE rolname=current_user").fetchone()[0]
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    restricted.execute("UPDATE articles SET title='must not change' WHERE id=1")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    restricted.execute("CREATE TABLE must_not_create(id INTEGER)")
            assert admin.execute("SELECT title FROM articles WHERE id=1").fetchone()[0] == "Incident"
        finally:
            admin.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(identifier))
            admin.execute(sql.SQL("DROP OWNED BY {}").format(identifier))
            admin.execute(sql.SQL("DROP ROLE {}").format(identifier))
