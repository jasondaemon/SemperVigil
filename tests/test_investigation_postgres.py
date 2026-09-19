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


def test_event_report_compare_and_swap_on_postgres():
    from sempervigil.storage import update_event_report
    # Legacy storage explicitly checks public.events, unlike the retrieval service.
    # The named-disposable guard above applies; never initialize an application DB.
    with psycopg.connect(os.environ["SV_TEST_DB_URL"], autocommit=True) as admin:
        admin.execute("CREATE TABLE public.events(id TEXT PRIMARY KEY, meta_json TEXT, updated_at TEXT)")
        try:
            admin.execute("INSERT INTO public.events VALUES ('one',%s,'2026-05-01')",
                          (json.dumps({"report": {"overview": "Previous"}}),))
            assert not update_event_report(admin, "one", {"overview": "Wrong"}, expected_updated_at="stale")
            assert update_event_report(admin, "one", {"overview": "Current"}, expected_updated_at="2026-05-01")
            meta, stamp = admin.execute("SELECT meta_json,updated_at FROM events WHERE id='one'").fetchone()
            assert json.loads(meta)["report"] == {"overview": "Current"}
            with psycopg.connect(os.environ["SV_TEST_DB_URL"], autocommit=True) as other:
                class ConcurrentWriter:
                    def execute(self, query, params=()):
                        if query.lstrip().startswith("UPDATE events"):
                            other.execute("UPDATE events SET meta_json=%s WHERE id='one'",
                                          (json.dumps({"report": {"overview": "Current"}, "editor_note": "keep"}),))
                        return admin.execute(query, params)
                    def commit(self): admin.commit()
                assert not update_event_report(ConcurrentWriter(), "one", {"overview": "Obsolete"}, expected_updated_at=stamp)
            final = json.loads(admin.execute("SELECT meta_json FROM events WHERE id='one'").fetchone()[0])
            assert final == {"report": {"overview": "Current"}, "editor_note": "keep"}
        finally:
            admin.execute("DROP TABLE public.events")


def test_private_revision_store_is_immutable_and_concurrent(restricted_database, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from sempervigil.event_review import snapshot, save
    from sempervigil.event_assessment import assess, request_for
    from sempervigil.event_revision import save_revision
    from sempervigil.event_revision_store import SCHEMA, persist
    admin, reader_dsn, identifier = restricted_database
    admin.execute(SCHEMA)
    admin.execute(sql.SQL("GRANT SELECT,INSERT ON event_private_revisions TO {}").format(identifier))
    admin.execute("UPDATE articles SET content_text='Incident responders reported an investigation is still ongoing.' WHERE id=1")
    packet = snapshot(lambda: postgres_reader(reader_dsn), event_id="one", aliases=["Incident"],
                      scopes=frozenset({READ_SCOPE,EVIDENCE_SCOPE}))
    request = request_for(packet)
    result = assess(packet, lambda _: {"decisions": [
        {"id": key, "decision": "hold", "reason": "insufficient_context"} for key in request["mapping"]]})
    page = save(packet, tmp_path, assessment=result)
    descriptor = save_revision(packet, result, "a" * 64, page)
    raw = (page.parent / ("revision-" + descriptor["version"] + ".json")).read_bytes()
    def write():
        return persist(lambda: psycopg.connect(reader_dsn), packet, raw, descriptor,
                       artifact=page.name, html=page.read_bytes())
    with ThreadPoolExecutor(max_workers=2) as pool:
        outputs = list(pool.map(lambda _: write(), range(2)))
    assert sorted(r["status"] for r in outputs) == ["reused", "stored"]
    assert all(r["public_eligible"] is False for r in outputs)
    assert admin.execute("SELECT count(*) FROM event_private_revisions").fetchone()[0] == 1
    assert admin.execute("SELECT title FROM events WHERE id='one'").fetchone()[0] == "Incident"
    stamp = admin.execute("SELECT recorded_at FROM event_private_revisions").fetchone()[0]
    assert write()["status"] == "reused"
    assert admin.execute("SELECT recorded_at FROM event_private_revisions").fetchone()[0] == stamp
    admin.execute("UPDATE event_private_revisions SET packet_json='{}'")
    with pytest.raises(ValueError, match="private_revision_conflict"): write()
    assert admin.execute("SELECT packet_json FROM event_private_revisions").fetchone()[0] == '{}'


def test_revision_window_locks_sources_and_membership(restricted_database):
    from sempervigil.event_review import snapshot
    from sempervigil.event_revision_store import locked_current_snapshot
    admin, reader_dsn, identifier = restricted_database
    packet = snapshot(lambda: postgres_reader(reader_dsn), event_id="one", aliases=["Incident"],
                      scopes=frozenset({READ_SCOPE,EVIDENCE_SCOPE}))
    with pytest.raises(ValueError, match="revision_membership_constraint_required"):
        with locked_current_snapshot(lambda: psycopg.connect(reader_dsn), packet):
            pytest.fail("missing FK cannot guard membership inserts")
    # Match the existing production FK contract, absent in the minimal fixture.
    admin.execute("ALTER TABLE event_articles ADD FOREIGN KEY(event_id) REFERENCES events(id)")
    admin.execute("ALTER TABLE event_articles ADD FOREIGN KEY(article_id) REFERENCES articles(id)")
    admin.execute(sql.SQL("GRANT UPDATE ON events,articles,event_articles TO {}").format(identifier))
    admin.execute(sql.SQL("GRANT INSERT,DELETE ON event_articles TO {}").format(identifier))
    admin.execute("UPDATE articles SET content_text='Incident responders reported an investigation is still ongoing.' WHERE id=1")
    admin.execute("INSERT INTO articles SELECT 2,source_id,title,original_url,brief_day,meta_json,content_text FROM articles WHERE id=1")
    packet = snapshot(lambda: postgres_reader(reader_dsn), event_id="one", aliases=["Incident"],
                      scopes=frozenset({READ_SCOPE,EVIDENCE_SCOPE}))
    with pytest.raises(ValueError, match="dedicated_revision_transaction_required"):
        with locked_current_snapshot(lambda: psycopg.connect(reader_dsn, autocommit=True), packet):
            pytest.fail("unsafe autocommit window")
    # Report bookkeeping alone is not a changed evidence dataset.
    admin.execute("UPDATE events SET updated_at='2026-05-02' WHERE id='one'")
    with locked_current_snapshot(lambda: psycopg.connect(reader_dsn), packet):
        for statement in (
            "UPDATE articles SET content_text='changed' WHERE id=1",
            "INSERT INTO event_articles VALUES ('one',2)",
            "DELETE FROM event_articles WHERE event_id='one' AND article_id=1",
        ):
            with psycopg.connect(reader_dsn) as writer:
                writer.execute("SET LOCAL lock_timeout = '100ms'")
                with pytest.raises(psycopg.errors.LockNotAvailable): writer.execute(statement)
                writer.rollback()
    admin.execute("UPDATE articles SET content_text='Changed source after collection.' WHERE id=1")
    with pytest.raises(ValueError, match="stale_revision_snapshot"):
        with locked_current_snapshot(lambda: psycopg.connect(reader_dsn), packet):
            pytest.fail("stale evidence accepted")


def test_qualified_publication_transaction(restricted_database):
    from concurrent.futures import ThreadPoolExecutor
    from sempervigil.event_review import snapshot
    from sempervigil.event_scope import propose
    from sempervigil.event_revision_store import source_version, locked_current_snapshot
    from sempervigil.event_publication_store import SCHEMA, promote, load_export
    from sempervigil.investigation import _version
    admin, dsn, role = restricted_database
    admin.execute(SCHEMA)
    admin.execute("ALTER TABLE event_articles ADD FOREIGN KEY(event_id) REFERENCES events(id)")
    admin.execute("ALTER TABLE event_articles ADD FOREIGN KEY(article_id) REFERENCES articles(id)")
    admin.execute(sql.SQL("GRANT SELECT ON event_quote_qualifications,event_public_revisions,event_public_pointers TO {}").format(role))
    admin.execute(sql.SQL("GRANT INSERT ON event_public_revisions,event_public_pointers TO {}").format(role))
    admin.execute(sql.SQL("GRANT UPDATE ON events,articles,event_articles,event_public_pointers TO {}").format(role))
    text = "Incident responders reported compromise of the contact system."
    admin.execute("UPDATE articles SET content_text=%s WHERE id=1", (text,))
    packet = snapshot(lambda: postgres_reader(dsn), event_id="one", aliases=["Incident"],
                      scopes=frozenset({READ_SCOPE,EVIDENCE_SCOPE}))
    start = text.index("contact system")
    scope = propose(packet, article_id=1, start=0, end=len(text), focus=[
        {"role": "entity", "start": 0, "end": 8},
        {"role": "affected_system", "start": start, "end": start+14}])
    qualification = {"workflow": "event-quote-qualification-v1", "event_id": "one",
        "source_version": source_version(packet), "scope_version": scope["scope_version"],
        "reviewer": {"kind": "policy", "id": "test-only", "version": "a"*64},
        "quotes": [{"article_id": 1, "start": 0, "end": len(text), "quote": text}]}
    qid = _version(qualification)
    factory = lambda: psycopg.connect(dsn)
    def run(q=qid, predecessor=None):
        return promote(factory, packet, scope, qualification_id=q, expected_predecessor=predecessor)
    with pytest.raises(ValueError, match="unavailable"): run()
    admin.execute("INSERT INTO event_quote_qualifications VALUES ('one',%s,%s,'test',NULL)",
                  (qid,json.dumps(qualification)))
    with factory() as restricted:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            restricted.execute("UPDATE event_quote_qualifications SET revoked_at='test'")
        restricted.rollback()
    def concurrent_attempt(_):
        try:
            return run()["status"]
        except psycopg.errors.LockNotAvailable:
            return "deferred"
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(concurrent_attempt, range(2)))
    assert outcomes.count("promoted") == 1 and set(outcomes) <= {"promoted", "reused", "deferred"}
    first = run()
    assert first["status"] == "reused"
    from sempervigil.event_activation import authorize_and_activate
    first_manifest = {"workflow": "event-release-authorization-v1",
                      "revisions": {"one": first["revision_id"]}, "withdrawn": {}}
    with factory() as blocker:
        blocker.execute("LOCK TABLE event_public_pointers IN ROW EXCLUSIVE MODE")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            authorize_and_activate(factory, first_manifest, lambda: pytest.fail("contended activation"))
    with factory() as blocker:
        blocker.execute("SELECT id FROM events WHERE id='one' FOR UPDATE")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            authorize_and_activate(factory, first_manifest, lambda: pytest.fail("contended event activation"))
    switched = []
    def switch_under_locks():
        admin.execute("SET lock_timeout='100ms'")
        try:
            with pytest.raises(psycopg.errors.LockNotAvailable):
                admin.execute("UPDATE event_quote_qualifications SET revoked_at='test' WHERE qualification_id=%s", (qid,))
            with pytest.raises(psycopg.errors.LockNotAvailable):
                admin.execute("UPDATE event_public_pointers SET updated_at='changed' WHERE event_id='one'")
        finally:
            admin.execute("SET lock_timeout=0")
        switched.append(True)
    authorize_and_activate(factory, first_manifest, switch_under_locks)
    assert switched == [True]
    exported = load_export(factory, ["one", "no-pointer"])
    assert exported["managed_event_ids"] == ["one"]
    assert exported["promoted_revision_ids"] == {"one": first["revision_id"]}
    assert not exported["withdrawn"] and not exported["withheld"]
    with pytest.raises(ValueError, match="dedicated_export_transaction"):
        load_export(lambda: psycopg.connect(dsn, autocommit=True), ["one"])
    assert admin.execute("SELECT count(*) FROM event_public_revisions").fetchone()[0] == 1
    original = admin.execute("SELECT bundle_json,recorded_at FROM event_public_revisions").fetchone()
    admin.execute("UPDATE events SET updated_at='2026-05-02' WHERE id='one'")
    packet = snapshot(lambda: postgres_reader(dsn), event_id="one", aliases=["Incident"],
                      scopes=frozenset({READ_SCOPE,EVIDENCE_SCOPE}))
    assert run()["status"] == "reused"
    assert admin.execute("SELECT bundle_json,recorded_at FROM event_public_revisions").fetchone() == original
    assert load_export(factory, ["one"])["promoted_revision_ids"] == {"one": first["revision_id"]}
    admin.execute("UPDATE articles SET content_text=%s WHERE id=1", (text + " More reporting.",))
    changed = load_export(factory, ["one"])
    assert changed["withheld"] == {"one": "evidence_changed"} and not changed["qualified_revisions"]
    admin.execute("UPDATE articles SET content_text=%s,meta_json=%s WHERE id=1", (text, '{"suppressed":true}'))
    suppressed = load_export(factory, ["one"])
    assert suppressed["withdrawn"] == {"one": "evidence_unavailable"} and not suppressed["promoted_revision_ids"]
    admin.execute("UPDATE articles SET meta_json=NULL WHERE id=1")
    admin.execute("DELETE FROM event_articles WHERE event_id='one' AND article_id=1")
    assert load_export(factory, ["one"])["withdrawn"] == {"one": "evidence_unavailable"}
    admin.execute("INSERT INTO event_articles VALUES ('one',1)")
    admin.execute("UPDATE events SET visibility='hidden' WHERE id='one'")
    assert load_export(factory, ["one"])["withdrawn"] == {"one": "event_unavailable"}
    admin.execute("UPDATE events SET visibility='active' WHERE id='one'")
    # A second independently recorded qualification can advance the predecessor.
    qualification["reviewer"]["version"] = "b"*64
    second_q = _version(qualification)
    admin.execute("INSERT INTO event_quote_qualifications VALUES ('one',%s,%s,'test',NULL)",
                  (second_q,json.dumps(qualification)))
    with pytest.raises(ValueError, match="predecessor_conflict"): run(second_q)
    second = run(second_q, first["revision_id"])
    assert second["revision_id"] != first["revision_id"]
    with pytest.raises(ValueError, match="inventory_changed"):
        authorize_and_activate(factory, first_manifest, lambda: pytest.fail("superseded activation"))
    with pytest.raises(ValueError, match="predecessor_conflict"): run()
    assert admin.execute("SELECT revision_id FROM event_public_pointers").fetchone()[0] == second["revision_id"]
    assert admin.execute("SELECT count(*) FROM event_public_revisions").fetchone()[0] == 2
    # Event lock serializes a one-way revocation with pointer promotion.
    with locked_current_snapshot(factory, packet):
        admin.execute("SET lock_timeout='100ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            admin.execute("UPDATE event_quote_qualifications SET revoked_at='test' WHERE qualification_id=%s", (second_q,))
        admin.execute("SET lock_timeout=0")
    admin.execute("UPDATE event_quote_qualifications SET revoked_at='test' WHERE qualification_id=%s", (second_q,))
    second_manifest = {**first_manifest, "revisions": {"one": second["revision_id"]}}
    with pytest.raises(ValueError, match="qualification_revoked"):
        authorize_and_activate(factory, second_manifest, lambda: pytest.fail("revoked activation"))
    authorize_and_activate(factory, {**second_manifest, "revisions": {},
        "withdrawn": {"one": second["revision_id"]}}, lambda: switched.append(True))
    assert switched == [True, True]
    with pytest.raises(ValueError, match="unavailable"): run(second_q, first["revision_id"])
    revoked = load_export(factory, ["one"])
    assert revoked["managed_event_ids"] == ["one"]
    assert revoked["withdrawn"] == {"one": "qualification_revoked"}
    assert not revoked["qualified_revisions"] and not revoked["promoted_revision_ids"]
    with pytest.raises(psycopg.errors.RaiseException):
        admin.execute("UPDATE event_quote_qualifications SET revoked_at=NULL WHERE qualification_id=%s", (second_q,))
    with pytest.raises(psycopg.errors.RaiseException):
        admin.execute("UPDATE event_quote_qualifications SET qualification_json='{}' WHERE qualification_id=%s", (qid,))
    with pytest.raises(psycopg.errors.RaiseException): admin.execute("DELETE FROM event_quote_qualifications")
    admin.execute(sql.SQL("GRANT INSERT ON event_quote_qualifications TO {}").format(role))
    with pytest.raises(PermissionError, match="read_only_role"): run()
    with pytest.raises(PermissionError, match="read_only_role"):
        authorize_and_activate(factory, second_manifest, lambda: pytest.fail("overprivileged activation"))
    admin.execute(sql.SQL("REVOKE INSERT ON event_quote_qualifications FROM {}").format(role))
    admin.execute("ALTER TABLE event_quote_qualifications DISABLE TRIGGER event_qualification_guard")
    with pytest.raises(ValueError, match="revocation_guard"): run()
    with pytest.raises(ValueError, match="revocation_guard"):
        authorize_and_activate(factory, second_manifest, lambda: pytest.fail("unguarded activation"))
    admin.execute("ALTER TABLE event_quote_qualifications ENABLE TRIGGER event_qualification_guard")
    admin.execute("UPDATE articles SET content_text='Changed evidence after qualification' WHERE id=1")
    with pytest.raises(ValueError, match="stale_revision_snapshot"): run()
    assert admin.execute("SELECT revision_id FROM event_public_pointers").fetchone()[0] == second["revision_id"]
