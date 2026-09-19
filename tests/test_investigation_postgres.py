"""Explicit disposable-PostgreSQL gate; never collected by the offline suite."""
from contextlib import contextmanager
import json
import os
import secrets
import asyncio
from pathlib import Path
import sys
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from sempervigil.investigation import EVIDENCE_SCOPE, InvestigationReader, READ_SCOPE, postgres_reader
from sempervigil.investigation_mcp import verify_database_role


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


@pytest.fixture
def restricted_database():
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
            admin.execute("CREATE TABLE event_articles(event_id TEXT, article_id BIGINT)")
            admin.execute("""CREATE TABLE jobs(id TEXT PRIMARY KEY, job_type TEXT, status TEXT,
                priority INTEGER, payload_json TEXT, result_json TEXT, requested_at TEXT,
                started_at TEXT, finished_at TEXT, locked_by TEXT, locked_at TEXT, error TEXT,
                queue_name TEXT, attempt_count INTEGER, max_attempts INTEGER, available_at TEXT,
                heartbeat_at TEXT, lease_expires_at TEXT, parent_job_id TEXT, dedupe_key TEXT)""")
            admin.execute("""INSERT INTO articles VALUES (1, 'source', 'Incident',
                'https://example.org', '2026-05-01', NULL, 'Exact original text.')""")
            admin.execute("""INSERT INTO events VALUES ('one', 'breach', 'Incident',
                'high', 'open', 'active', '2026-05-01', 'active')""")
            admin.execute("INSERT INTO event_articles VALUES ('one', 1)")
            admin.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(identifier, identifier))
            admin.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(identifier, identifier))
            admin.execute(sql.SQL("ALTER ROLE {} SET search_path TO {}").format(identifier, identifier))
            reader_dsn = make_conninfo(dsn, user=name, password=password)
            yield admin, reader_dsn, identifier
        finally:
            admin.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(identifier))
            admin.execute(sql.SQL("DROP OWNED BY {}").format(identifier))
            admin.execute(sql.SQL("DROP ROLE {}").format(identifier))


def test_least_privilege_role_can_read_but_cannot_write(restricted_database):
    admin, reader_dsn, identifier = restricted_database
    session = lambda: postgres_reader(reader_dsn)
    verify_database_role(session)
    with pytest.raises(PermissionError):
        verify_database_role(lambda: postgres_reader(os.environ["SV_TEST_DB_URL"]))
    reader = InvestigationReader(session)
    scopes = frozenset({READ_SCOPE, EVIDENCE_SCOPE})
    result = reader.search_articles(b'{"start_day":"2026-05-01","end_day":"2026-05-31"}', scopes=scopes)
    assert [item["id"] for item in result["items"]] == [1]
    assert reader.get_event_record(b'{"event_id":"one"}', scopes=scopes)["record"]["id"] == "one"
    assert reader.get_article_evidence(b'{"article_id":1}', scopes=scopes)["text"] == "Exact original text."
    with psycopg.connect(reader_dsn, autocommit=True) as restricted:
        assert not restricted.execute("SELECT rolsuper FROM pg_roles WHERE rolname=current_user").fetchone()[0]
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            restricted.execute("UPDATE articles SET title='must not change' WHERE id=1")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            restricted.execute("CREATE TABLE must_not_create(id INTEGER)")
    assert admin.execute("SELECT title FROM articles WHERE id=1").fetchone()[0] == "Incident"
    admin.execute(sql.SQL("GRANT UPDATE(title) ON articles TO {}").format(identifier))
    with pytest.raises(PermissionError):
        verify_database_role(session)


def test_stdio_client_reads_disposable_database(restricted_database):
    pytest.importorskip("mcp")
    from mcp import Client
    from mcp.client.stdio import StdioServerParameters

    _, reader_dsn, _ = restricted_database
    server = StdioServerParameters(command=sys.executable,
        args=["-m", "sempervigil.investigation_mcp"], env={
            "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
            "SV_INVESTIGATION_MCP_ENABLED": "1", "SV_INVESTIGATION_DB_URL": reader_dsn,
            "SV_INVESTIGATION_EVIDENCE_ENABLED": "1"})
    async def check():
        async with Client(server, read_timeout_seconds=10) as client:
            assert {tool.name for tool in (await client.list_tools()).tools} == {
                "search_articles", "get_event_record", "get_article_evidence"}
            result = await client.call_tool("get_article_evidence", {"article_id": 1})
            assert not result.is_error
            assert result.structured_content["text"] == "Exact original text."
            denied = await client.call_tool("execute_sql", {"query": "DELETE FROM articles"})
            assert denied.is_error
    asyncio.run(check())


def test_private_event_review_on_restricted_postgres(restricted_database, tmp_path):
    from sempervigil.event_review import snapshot, save, draft, validate_review
    admin, reader_dsn, _ = restricted_database
    admin.execute("UPDATE articles SET content_text='Incident responders reported an investigation is still ongoing.' WHERE id=1")
    session = lambda: postgres_reader(reader_dsn)
    scopes = frozenset({READ_SCOPE, EVIDENCE_SCOPE})
    packet = snapshot(session, event_id="one", aliases=["Incident"], scopes=scopes)
    assert len(packet["documents"]) == 1
    proposed = draft(packet)
    assert len(proposed["passages"]) == 1
    decisions = {"workflow": packet["workflow"], "packet_version": packet["packet_version"],
                 "decisions": {proposed["passages"][0]["id"]: "include"}, "note": "Check attribution."}
    page = save(packet, tmp_path, decisions)
    assert page.is_file() and not packet["public_eligible"]
    assert snapshot(session, event_id="one", aliases=["Incident"], scopes=scopes) == packet
    admin.execute("UPDATE articles SET meta_json='{\"suppressed\":true}' WHERE id=1")
    changed = snapshot(session, event_id="one", aliases=["Incident"], scopes=scopes)
    assert changed["documents"] == []
    with pytest.raises(ValueError, match="stale_or_invalid_review"):
        validate_review(json.dumps(decisions).encode(), changed)


def test_private_review_concurrent_admission(restricted_database, monkeypatch, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from sempervigil import event_review_jobs
    admin, reader_dsn, identifier = restricted_database
    admin.execute(sql.SQL("GRANT INSERT ON jobs TO {}").format(identifier))
    admin.execute("UPDATE articles SET content_text='Incident responders reported an investigation is still ongoing.' WHERE id=1")
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_DB_URL", reader_dsn)
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    def submit():
        return event_review_jobs.submit(lambda: psycopg.connect(reader_dsn), event_id="one", aliases=["Incident"])
    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(lambda _: submit(), range(2)))
    assert first == second
    row = admin.execute("SELECT job_type,queue_name,priority,max_attempts,payload_json FROM jobs").fetchone()
    assert row[:4] == ("event_review_private", "llm_local", -10, 1)
    assert admin.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
    result = event_review_jobs.run(json.loads(row[4]))
    assert result["status"] == "review_ready" and result["public_eligible"] is False
    assert (tmp_path / "private" / result["artifact"]).is_file()
    assert admin.execute("SELECT title FROM events WHERE id='one'").fetchone()[0] == "Incident"
