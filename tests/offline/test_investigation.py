"""Bounded retrieval contracts using an in-memory SQL fixture, not PostgreSQL QA."""
from contextlib import contextmanager
import json
import sqlite3

import pytest

from sempervigil.investigation import (
    InvestigationReader, READ_SCOPE, RESPONSE_BYTES, postgres_reader,
)

pytestmark = pytest.mark.offline
SCOPES = frozenset({READ_SCOPE})


def request(**updates):
    return json.dumps({"start_day": "2026-05-01", "end_day": "2026-05-31", **updates}).encode()


@pytest.fixture
def setup_reader():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.executescript("""
      CREATE TABLE articles(id INTEGER PRIMARY KEY, source_id TEXT, title TEXT,
        original_url TEXT, brief_day TEXT, meta_json TEXT);
      CREATE TABLE events(id TEXT PRIMARY KEY, kind TEXT, title TEXT, severity TEXT,
        status TEXT, lifecycle TEXT, updated_at TEXT, visibility TEXT);
    """)
    queries = []

    class Connection:
        def execute(self, sql, params=()):
            queries.append((sql, params))
            assert sql.lstrip().startswith("SELECT")
            return db.execute(sql.replace("%s", "?"), params)

    @contextmanager
    def session():
        db.execute("PRAGMA query_only=ON")
        try:
            yield Connection()
        finally:
            db.execute("PRAGMA query_only=OFF")

    def article(id=1, *, title="Acme incident", source="source", day="2026-05-01",
                meta=None, url="https://example.org/news"):
        db.execute("INSERT INTO articles VALUES (?, ?, ?, ?, ?, ?)",
                   (id, source, title, url, day, meta))

    yield InvestigationReader(session), article, db, queries
    db.close()


def test_search_is_parameterized_literal_and_scoped(setup_reader):
    reader, article, _, queries = setup_reader
    article(1, title="100%_! ' OR 1=1 --")
    article(2, title="100percent")
    article(3, title="100%_! ' OR 1=1 --", day="2026-04-30")
    article(4, title="100%_! ' OR 1=1 --", source="other")
    result = reader.search_articles(request(query="%_! ' OR 1=1 --", source_id="source"), scopes=SCOPES)
    assert [item["id"] for item in result["items"]] == [1]
    assert "OR 1=1" not in queries[0][0]
    assert result["coverage"] == "stored_brief_day_and_title_only"
    assert not result["has_more"]


@pytest.mark.parametrize("updates", [
    {"start_day": "2026-02-30"}, {"start_day": "20260501"},
    {"start_day": None}, {"end_day": "2026-06-01"}, {"end_day": "2026-04-30"},
    {"limit": True}, {"limit": 0}, {"limit": 51}, {"limit": 1.5},
    {"before_id": False}, {"before_id": -1}, {"before_id": 2**63},
    {"query": ""}, {"query": " "}, {"query": "x" * 201}, {"query": "\x00"},
    {"query": "\ud800"}, {"source_id": 1}, {"include_suppressed": True},
])
def test_invalid_requests_do_not_reach_database(setup_reader, updates):
    reader, _, _, queries = setup_reader
    with pytest.raises(ValueError):
        reader.search_articles(request(**updates), scopes=SCOPES)
    assert not queries


@pytest.mark.parametrize("raw", [
    b"{}" * 3000, b"[]", b"\xff", b"{", b'{"limit":1,"limit":2}',
    b'{"limit":NaN}', b'{"limit":Infinity}', b"[" * 1500 + b"]" * 1500,
])
def test_malformed_raw_input(setup_reader, raw):
    reader, _, _, queries = setup_reader
    with pytest.raises(ValueError):
        reader.search_articles(raw, scopes=SCOPES)
    assert not queries


@pytest.mark.parametrize("operation,raw", [
    ("search_articles", request()), ("get_event_record", b'{"event_id":"one"}'),
])
def test_scope_required_before_any_query(setup_reader, operation, raw):
    reader, _, _, queries = setup_reader
    with pytest.raises(PermissionError):
        getattr(reader, operation)(raw, scopes=frozenset())
    assert not queries


@pytest.mark.parametrize("meta", [
    '{"suppressed":true}', '{"suppressed":"false"}', '{"suppressed":1}',
    '{"suppressed":false,"suppressed":true}', '[]', '{', 'null', 'x' * 8193,
])
def test_suppression_and_malformed_policy_fail_closed(setup_reader, meta):
    reader, article, _, _ = setup_reader
    article(meta=meta)
    assert reader.search_articles(request(), scopes=SCOPES)["items"] == []


def test_pagination_progresses_across_hidden_rows(setup_reader):
    reader, article, _, queries = setup_reader
    article(1)
    for id in range(2, 204):
        article(id, meta='{"suppressed":true}')
    first = reader.search_articles(request(), scopes=SCOPES)
    assert first["items"] == []
    assert first["has_more"] and first["next_before_id"] == 4
    assert first["stop_reason"] == "scan_limit"
    second = reader.search_articles(request(before_id=first["next_before_id"]), scopes=SCOPES)
    assert [item["id"] for item in second["items"]] == [1]
    assert not second["has_more"]
    assert all(params[-1] == 201 for _, params in queries)


def test_pages_have_no_duplicates_and_respect_output_limit(setup_reader):
    reader, article, _, _ = setup_reader
    for id in range(1, 51):
        article(id, title="\U0001f512" * 512, url="https://example.org/" + "x" * 1900)
    ids, before = [], 2**63 - 1
    for _ in range(51):
        result = reader.search_articles(request(before_id=before, limit=50), scopes=SCOPES)
        assert len(json.dumps(result, ensure_ascii=True).encode()) <= RESPONSE_BYTES
        ids.extend(item["id"] for item in result["items"])
        if not result["has_more"]:
            break
        assert result["next_before_id"] < before
        before = result["next_before_id"]
    assert ids == list(range(50, 0, -1))


def test_page_limit_and_metadata_version(setup_reader):
    reader, article, db, _ = setup_reader
    article(1)
    article(2)
    first = reader.search_articles(request(limit=1), scopes=SCOPES)
    assert first["stop_reason"] == "page_limit"
    assert first["next_before_id"] == 2
    second = reader.search_articles(request(limit=1, before_id=2), scopes=SCOPES)
    assert [item["id"] for item in second["items"]] == [1]
    assert not second["has_more"]
    db.execute("UPDATE articles SET title='Changed' WHERE id=1")
    changed = reader.search_articles(request(before_id=2), scopes=SCOPES)
    assert changed["items"][0]["metadata_version"] != second["items"][0]["metadata_version"]


@pytest.mark.parametrize("url", ["javascript:alert(1)", "https://user:secret@example.org", "https://[", "x" * 2049])
def test_unsafe_links_not_returned(setup_reader, url):
    reader, article, _, _ = setup_reader
    article(url=url)
    assert not reader.search_articles(request(), scopes=SCOPES)["items"]


def test_event_record_is_metadata_only_and_hidden_equals_missing(setup_reader):
    reader, _, db, _ = setup_reader
    db.execute("INSERT INTO events VALUES ('one','breach','Acme','high','open','active','2026-05-01','active')")
    raw = b'{"event_id":"one"}'
    result = reader.get_event_record(raw, scopes=SCOPES)
    assert result["record"]["title"] == "Acme"
    assert not result["validated_revision"]
    assert not result["linked_evidence_included"]
    assert "summary" not in result["record"]
    db.execute("UPDATE events SET visibility='suppressed'")
    assert reader.get_event_record(raw, scopes=SCOPES) == reader.get_event_record(b'{"event_id":"missing"}', scopes=SCOPES)


@pytest.mark.parametrize("raw", [b'{}', b'{"event_id":true}', b'{"event_id":""}', b'{"event_id":"one","scopes":["investigation:read"]}'])
def test_invalid_event_request(setup_reader, raw):
    reader, _, _, queries = setup_reader
    with pytest.raises(ValueError):
        reader.get_event_record(raw, scopes=SCOPES)
    assert not queries


def test_database_errors_are_not_empty_success():
    @contextmanager
    def failing_session():
        raise TimeoutError("statement timeout")
        yield
    with pytest.raises(TimeoutError):
        InvestigationReader(failing_session).search_articles(request(), scopes=SCOPES)


def test_postgres_factory_sets_read_only_and_timeouts(monkeypatch):
    calls = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            calls.append("closed")

        def execute(self, sql):
            calls.append(sql)

    def connect(dsn, **kwargs):
        calls.append(kwargs)
        return Connection()

    monkeypatch.setattr("sempervigil.investigation.psycopg.connect", connect)
    with postgres_reader("unused-test-dsn"):
        pass
    assert "default_transaction_read_only=on" in calls[0]["options"]
    assert "statement_timeout=2000" in calls[0]["options"]
    assert "lock_timeout=250" in calls[0]["options"]
    assert "idle_in_transaction_session_timeout=5000" in calls[0]["options"]
    assert calls[0]["connect_timeout"] == 3
    assert calls[1] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    assert calls[2] == "closed"
