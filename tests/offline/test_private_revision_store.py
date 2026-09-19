from unittest.mock import Mock

import pytest

from sempervigil import event_revision_store as store
from test_private_revision import revision
from test_event_review import database

pytestmark = pytest.mark.offline


def test_disabled_store_never_connects_or_parses(monkeypatch):
    monkeypatch.delenv("SV_EVENT_PRIVATE_REVISION_STORE", raising=False)
    connect = Mock(side_effect=AssertionError("must not connect"))
    monkeypatch.setattr(store.psycopg, "connect", connect)
    assert store.persist_if_enabled("unused", {}, b"bad", {}, artifact="bad", html=b"") is None
    connect.assert_not_called()


@pytest.mark.parametrize("value", ["true", "", "2"])
def test_invalid_enablement_rejected(monkeypatch, value):
    monkeypatch.setenv("SV_EVENT_PRIVATE_REVISION_STORE", value)
    with pytest.raises(ValueError): store.enabled()


def test_schema_is_additive_and_has_no_public_pointer():
    assert "REFERENCES events(id)" in store.SCHEMA
    assert "PRIMARY KEY (event_id, revision_id)" in store.SCHEMA
    assert "octet_length" in store.SCHEMA
    assert "UPDATE " not in store.SCHEMA and "ALTER " not in store.SCHEMA


def test_worker_uses_store_only_after_valid_receipt(revision, monkeypatch):
    from sempervigil import event_review_jobs as jobs
    job, _, complete, payload = revision
    monkeypatch.setenv("SV_EVENT_PRIVATE_REVISION_STORE", "1")
    stored = {"status": "stored", "version": job.result["private_revision"]["version"], "public_eligible": False}
    persist = Mock(return_value=stored)
    monkeypatch.setattr(store, "persist_if_enabled", persist)
    result = jobs.run(payload, complete=complete)
    assert result["revision_storage"] == stored and result["public_eligible"] is False
    assert result["model_cache_hit"] is True
    assert persist.call_args.args[3] == job.result["private_revision"]
    complete.assert_called_once()


@pytest.mark.parametrize("field,value", [("publication_gates", []), ("generation_version", "bad"),
                                         ("generation_version", 1)])
def test_rehashed_invalid_receipt_rejected_before_database(revision, field, value):
    import json
    from sempervigil.investigation import _version
    job, path, _, _ = revision
    receipt = json.loads(path.read_bytes())
    receipt[field] = value
    descriptor = {**job.result["private_revision"], "version": _version(receipt)}
    packet = json.loads((path.parent / "packet.json").read_bytes())
    connect = Mock(side_effect=AssertionError("must not connect"))
    html_path = path.parent / receipt["artifact"]
    with pytest.raises(ValueError):
        store.persist(connect, packet, json.dumps(receipt).encode(), descriptor,
                      artifact=html_path.name, html=html_path.read_bytes())
    connect.assert_not_called()
