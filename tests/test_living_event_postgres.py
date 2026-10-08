"""Living reports retain immutable evidence while successor admission stays fresh."""
import copy, json
import pytest
from test_event_report_v2_policy_postgres import setup
from test_event_source_reports_postgres import database
from sempervigil import event_source_reports_v2 as reports
from sempervigil import event_source_report_publication_v2 as publication
from sempervigil.event_publication_store import load_export


def bundle(s, revision=None):
    return json.loads(s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',
                                    (revision or s.old,)).fetchone()[0])


def export(s):
    def dedicated():
        c = s.factory(s.promotion); c.commit(); return c
    return load_export(dedicated, ['evt_test'])


@pytest.mark.parametrize('change', ['body', 'metadata', 'membership'])
def test_ordinary_source_updates_keep_original_qualified_evidence(setup, change):
    s = setup; old = bundle(s); material = publication.published_material(s.conn, old)
    original = copy.deepcopy(material['snapshot'])
    if change == 'body': s.conn.execute("UPDATE articles SET content_text='New reporting supersedes the live body.' WHERE id=1")
    if change == 'metadata': s.conn.execute("UPDATE articles SET title='Revised title',original_url='https://example.org/revised' WHERE id=1")
    if change == 'membership': s.conn.execute("DELETE FROM event_articles WHERE event_id='evt_test' AND article_id=1")
    s.conn.execute("UPDATE events SET title='Updated event title' WHERE id='evt_test'"); s.conn.commit()
    retained = publication.published_material(s.conn, old)
    assert retained['snapshot'] == original and retained['report'] == material['report']
    assert export(s)['qualified_revisions']['evt_test'] == old
    assert export(s)['promoted_revision_ids']['evt_test'] == s.old
    with pytest.raises(ValueError, match='sources_changed'):
        publication.current_material(s.conn, old['run_id'])
    assert not s.calls


@pytest.mark.parametrize('change', ['suppressed', 'deleted', 'withdrawn', 'revoked', 'snapshot', 'response'])
def test_explicit_invalidation_or_tampering_still_blocks_retained_export(setup, change):
    s = setup; old = bundle(s)
    if change == 'suppressed': s.conn.execute("UPDATE articles SET meta_json='{\"suppressed\":true}' WHERE id=1")
    if change == 'deleted':
        s.conn.execute('DELETE FROM event_articles WHERE article_id=1'); s.conn.execute('DELETE FROM articles WHERE id=1')
    if change == 'withdrawn': s.conn.execute("UPDATE events SET visibility='withdrawn' WHERE id='evt_test'")
    if change == 'revoked':
        s.conn.execute("UPDATE event_quote_qualifications SET revoked_at='2026-10-08' WHERE event_id='evt_test'")
    if change == 'snapshot':
        record = reports._load(s.conn, old['run_id']); packet = record['snapshot']
        packet['sources'][0]['text'] += ' Corrupted saved body.'
        s.conn.execute('UPDATE event_source_report_runs SET snapshot_json=%s WHERE run_id=%s',
                       (json.dumps(packet), old['run_id']))
    if change == 'response':
        # Disposable admin deliberately simulates corrupt storage beyond the
        # production completed-call mutation guard, which otherwise rejects it.
        s.conn.execute('ALTER TABLE event_source_report_calls DISABLE TRIGGER USER')
        s.conn.execute("UPDATE event_source_report_calls SET response_json='{}' WHERE run_id=%s AND phase='writer'",
                       (old['run_id'],))
    s.conn.commit()
    with pytest.raises((ValueError, KeyError)):
        publication.published_material(s.conn, old)
    exported = export(s)
    assert not exported['qualified_revisions']
    assert 'evt_test' in (set(exported['withheld']) | set(exported['withdrawn']))
    assert not s.calls


@pytest.mark.parametrize('boundary', ['queued', 'accepted', 'promotion'])
def test_successor_freshness_holds_do_not_withdraw_the_last_report(setup, boundary):
    s = setup; old = bundle(s); rid = reports.submit(s.conn, 'evt_test')['run_id']
    if boundary != 'queued': assert s.execute(rid)['status'] == 'accepted'
    if boundary == 'promotion':
        queued = publication.submit(s.conn, rid, automatic=True, factory=lambda: s.factory(s.admission))
    count = len(s.calls)
    s.conn.execute("UPDATE articles SET content_text=content_text||' Later correction.' WHERE id=1"); s.conn.commit()
    if boundary == 'queued': assert s.execute(rid)['status'] == 'held'
    elif boundary == 'accepted':
        with pytest.raises(ValueError, match='sources_changed'): s.publish(rid, automatic=True)
    else:
        from sempervigil.event_approval import run
        with pytest.raises(ValueError, match='sources_changed'):
            run({'approval_id': queued['approval_id']}, factory=lambda: s.factory(s.promotion))
    assert len(s.calls) == count
    assert export(s)['qualified_revisions']['evt_test'] == old


def test_qualified_successor_replaces_content_at_one_canonical_permalink(setup, tmp_path):
    from sempervigil.publish import write_events_exports
    s = setup
    def write(slug, title):
        exported = export(s)
        return write_events_exports([{'id': 'evt_test', 'site_slug': slug, 'title': title}],
            str(tmp_path/'content'), str(tmp_path/'static'),
            qualified_revisions=exported['qualified_revisions'],
            promoted_revision_ids=exported['promoted_revision_ids'])
    before = write('old-title-alias', 'Original title')
    page = tmp_path/'content/events/evt_test.md'; old_content = page.read_text()
    assert s.old in old_content
    rid = reports.submit(s.conn, 'evt_test')['run_id']
    assert s.execute(rid)['status'] == 'accepted'
    _, promoted = s.publish(rid, automatic=True)
    after = write('new-title-alias', 'Changed title')
    assert before == after and promoted['revision_id'] in page.read_text()
    assert page.read_text() != old_content
    assert [p.name for p in (tmp_path/'content/events').glob('*.md')] == ['evt_test.md']
    data = json.loads((tmp_path/'static/index/events.json').read_text())
    assert len(data) == 1 and data[0]['url'] == '/events/evt_test/'
    assert data[0]['event_revision'] == promoted['revision_id']
    assert s.conn.execute('SELECT count(*) FROM events').fetchone()[0] == 1
    assert s.conn.execute('SELECT count(*) FROM event_public_revisions').fetchone()[0] == 2


def test_legacy_retained_report_binds_original_evidence_and_preserves_derivative_metadata(database):
    from test_event_source_reports_postgres import execute, generated
    from sempervigil import event_source_report_publication as legacy_publication
    conn, _, _ = database
    submitted, result, calls, _ = execute(conn, [generated(), {'ready': True, 'issues': []}])
    assert result['status'] == 'accepted'
    rid = submitted['run_id']; original = legacy_publication.current_material(conn, rid)
    conn.execute("UPDATE articles SET content_text='A replacement live publisher body.' WHERE id=1"); conn.commit()
    assert legacy_publication.current_material(conn, rid, published=True)['snapshot'] == original['snapshot']
    assert len(calls) == 2
    # A recomputed local packet hash cannot substitute evidence that the immutable
    # writer request did not contain, even if all original quotes remain present.
    packet = copy.deepcopy(original['snapshot'])
    packet['sources'][0]['title'] = 'Fabricated evidence title'
    packet['membership'][0]['title'] = 'Fabricated evidence title'
    from sempervigil.investigation import _version
    packet['source_version'] = _version({k:packet[k] for k in
        ('event_id', 'title', 'sources', 'membership', 'excluded_article_ids')})
    conn.execute('UPDATE event_source_report_runs SET snapshot_json=%s,source_version=%s WHERE run_id=%s',
                 (json.dumps(packet), packet['source_version'], rid)); conn.commit()
    with pytest.raises(ValueError, match='input_integrity'):
        legacy_publication.current_material(conn, rid, published=True)
