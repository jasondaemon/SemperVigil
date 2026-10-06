import json
import pytest
from test_event_source_reports_postgres import database
from sempervigil import event_source_reports_v2 as reports
from sempervigil import event_report_contract_v2 as contract
@pytest.fixture
def legacy_duplicate_publication(database, monkeypatch):
    conn, _, _ = database
    from sempervigil.article_evidence import source_for
    from sempervigil import event_render
    monkeypatch.setattr(event_render, "resolve", lambda b, **kw: ({}, {}))
    text = reports.snapshot(conn, "evt_test")["sources"][0]["text"]
    conn.execute("DELETE FROM event_articles WHERE event_id='evt_test'")
    for aid in (2, 3):
        conn.execute("INSERT INTO articles VALUES(%s,%s,%s,%s,'2026-10-01','2026-10-02','{}')",
                     (aid, "Original " + str(aid), "https://example.org/" + str(aid), text))
        conn.execute("INSERT INTO event_articles VALUES('evt_test',%s)", (aid,))
    original = reports.snapshot(conn, "evt_test")
    conn.execute("CREATE TABLE article_evidence_revisions(revision_id TEXT,article_id INTEGER,source_version TEXT)")
    bindings = []
    for aid in (2, 3):
        version = source_for({"id": aid, "title": "Original " + str(aid), "content_text": text})["source_version"]
        conn.execute("INSERT INTO article_evidence_revisions VALUES(%s,%s,%s)", ("aer_" + str(aid), aid, version))
        bindings.append({"article_id": aid, "evidence_revision_id": "aer_" + str(aid),
                         "title": "Original " + str(aid), "url": "https://example.org/" + str(aid),
                         "candidate_id": "ic_" + str(aid)})
    bundle = {"workflow": "legacy", "sources": bindings}
    revision = reports._version(bundle)
    qid = reports._version({})
    conn.execute("INSERT INTO event_quote_qualifications VALUES('evt_test',%s,'{}','now',NULL)", (qid,))
    conn.execute("INSERT INTO event_public_revisions VALUES('evt_test',%s,%s,NULL,%s,'now')",
                 (revision, qid, contract.encode(bundle)))
    conn.execute("INSERT INTO event_public_pointers VALUES('evt_test',%s,'now')", (revision,))
    conn.commit()
    return conn, original, bundle, revision


def test_legacy_duplicate_baseline_preserves_every_original_identity(legacy_duplicate_publication):
    conn, original, _, _ = legacy_duplicate_publication
    baseline, generation = reports.published_baseline(conn, "evt_test", original)
    assert generation is None
    assert {k: v for k, v in baseline.items() if k != "legacy_evidence"} == original
    assert len(baseline["sources"]) == 1 and len(baseline["membership"]) == 2
    assert [b["article_id"] for b in baseline["legacy_evidence"]] == [2, 3]
    assert len({b["source_version"] for b in baseline["legacy_evidence"]}) == 2
    assert [b["candidate_id"] for b in baseline["legacy_evidence"]] == ["ic_2", "ic_3"]
    assert reports.meaningful_change(baseline, original) is False


def test_legacy_baseline_excludes_new_alias_and_reconstructs_original_canonical(legacy_duplicate_publication):
    conn, original, _, _ = legacy_duplicate_publication
    conn.execute("INSERT INTO event_articles VALUES('evt_test',1)")
    current = reports.snapshot(conn, "evt_test")
    assert current["sources"][0]["article_id"] == 1
    baseline, _ = reports.published_baseline(conn, "evt_test", current)
    assert {k: v for k, v in baseline.items() if k != "legacy_evidence"} == original
    assert baseline["sources"][0]["article_id"] == 2
    assert [m["article_id"] for m in baseline["membership"]] == [2, 3]
    assert reports.meaningful_change(baseline, current) is False


@pytest.mark.parametrize("aid", [2, 3])
@pytest.mark.parametrize("mutation", ["title", "body", "suppressed", "unlinked", "evidence_missing", "evidence_wrong_member", "invalid_url", "changed_url", "empty_body"])
def test_legacy_duplicate_baseline_rejects_changed_or_unavailable_member(legacy_duplicate_publication, aid, mutation):
    conn, _, _, _ = legacy_duplicate_publication
    statements = {
        "title": "UPDATE articles SET title='Corrected title' WHERE id=%s",
        "body": "UPDATE articles SET content_text=content_text || ' Correction.' WHERE id=%s",
        "suppressed": "UPDATE articles SET meta_json='{\"suppressed\":true}' WHERE id=%s",
        "unlinked": "DELETE FROM event_articles WHERE article_id=%s",
        "evidence_missing": "DELETE FROM article_evidence_revisions WHERE article_id=%s",
        "evidence_wrong_member": "UPDATE article_evidence_revisions SET article_id=100 WHERE article_id=%s",
        "invalid_url": "UPDATE articles SET original_url='file:///tmp/evidence' WHERE id=%s",
        "empty_body": "UPDATE articles SET content_text='' WHERE id=%s",
        "changed_url": "UPDATE articles SET original_url='https://example.org/corrected' WHERE id=%s",
    }
    conn.execute(statements[mutation], (aid,))
    if mutation == "invalid_url":
        # Use the last admissible current snapshot to exercise the independent
        # reconstruction check; fresh admission itself also rejects this URL.
        current = reports._source_material("evt_test", "Acme incident", [
            (2,"Original 2","https://example.org/2","valid",None,None,{}),
            (3,"Original 3","https://example.org/3","valid",None,None,{})], [2,3])
        with pytest.raises(ValueError, match="url_invalid"):
            reports.snapshot(conn, "evt_test")
    else:
        current = reports.snapshot(conn, "evt_test")
    assert reports.published_baseline(conn, "evt_test", current) == (None, None)


@pytest.mark.parametrize("sources", [None, {}, [None], [], [{"article_id": 2, "evidence_revision_id": "aer_2"}]*2,
    [{"article_id": True}], [{"article_id": "2"}], [{"article_id": -2}], [{"article_id": 999}]])
def test_legacy_baseline_rejects_invalid_source_bindings(legacy_duplicate_publication, sources):
    conn, original, bundle, revision = legacy_duplicate_publication
    bundle["sources"] = sources
    conn.execute("UPDATE event_public_revisions SET bundle_json=%s WHERE revision_id=%s",
                 (contract.encode(bundle), revision))
    assert reports.published_baseline(conn, "evt_test", original) == (None, None)


def test_legacy_baseline_stable_for_public_binding_order(legacy_duplicate_publication):
    conn, original, bundle, revision = legacy_duplicate_publication
    baseline, _ = reports.published_baseline(conn, "evt_test", original)
    bundle["sources"].reverse()
    conn.execute("UPDATE event_public_revisions SET bundle_json=%s WHERE revision_id=%s",
                 (contract.encode(bundle), revision))
    assert reports.published_baseline(conn, "evt_test", original) == (baseline, None)
