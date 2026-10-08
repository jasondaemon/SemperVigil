"""First publication uses real separate-role gates and synthetic model replies."""
import copy, json
from types import SimpleNamespace
import pytest
from test_event_report_v2_policy_postgres import setup
from test_event_source_reports_postgres import database, response
from sempervigil import event_source_reports_v2 as reports, event_report_final_editor as editor
from sempervigil import event_source_report_publication_v2 as publication, event_report_v2_policy as policy


@pytest.fixture
def initial(setup, monkeypatch):
    s = setup
    s.conn.execute("ALTER TABLE events ADD COLUMN entity TEXT; ALTER TABLE events ADD COLUMN publish_state TEXT")
    s.conn.execute("INSERT INTO events VALUES('evt_initial','Acme legacy EHR breach','active','confirmed','Acme','draft')")
    s.conn.execute("""INSERT INTO articles VALUES(3,'Acme EHR disclosure','https://example.org/initial',
        'Acme said certain patient records may be affected. The company rotated credentials. The legacy EHR breach occurred in January 2025.',
        '2026-10-01','2026-10-01','{}'); INSERT INTO event_articles VALUES('evt_initial',3)""")
    s.conn.execute('ALTER TABLE articles ADD COLUMN has_full_content INTEGER DEFAULT 1')
    s.conn.execute("INSERT INTO llm_models VALUES('e','p','fixture-editor',1)"); s.conn.commit()
    from sempervigil.services import ai_service
    monkeypatch.setattr(ai_service, 'get_model', lambda *_: {'id': 'e', 'model_name': 'fixture-editor', 'max_context': 128000})
    monkeypatch.setattr(reports, 'configuration', lambda _: ({'id': 'w', 'model_name': 'fixture-writer', 'max_context': 128000}, {'id': 'p'}, 'b'*64))
    monkeypatch.setenv(editor.CONFIG_ENV, json.dumps({'workflow': editor.WORKFLOW, 'model': 'fixture-editor',
        'reasoning_effort': 'high', 'max_completion_tokens': 12000, 'context_overrides': {}}))
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_PHASE_CONFIG', json.dumps({'writer': {'reasoning_effort': 'none', 'max_completion_tokens': 6000}}))
    monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS', 'evt_initial')
    monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_ID', 'fixture-first-report')
    monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_TOKENS', '70000')
    p = {**s.p, 'max_runs': 1, 'run_tokens': 70000, 'admission_kind': 'initial_report',
         'incident_identity': {'event_id': 'evt_initial', 'entity': 'Acme', 'system': 'legacy EHR',
            'incident_window': 'January 2025', 'source_anchors': [{'source_id': 'S3',
                'quote': 'The legacy EHR breach occurred in January 2025.'}],
            'query_terms': ['legacy', 'EHR'], 'incident_year': 2025}}
    monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY', json.dumps(p))
    value = editor.narrative(copy.deepcopy(s.value))
    for item in value['items']:
        for cite in item['citations']: cite['source_id'] = 'S3'
    return SimpleNamespace(s=s, p=p, value=value, calls=[])


def execute(t, *, change=None):
    rid = reports.submit(t.s.conn, 'evt_initial')['run_id']
    def complete(payload):
        t.calls.append(payload)
        if len(t.calls) == 1:
            if change == 'entity': t.s.conn.execute("UPDATE events SET entity='Other' WHERE id='evt_initial'"); t.s.conn.commit()
            if change == 'source': t.s.conn.execute("UPDATE articles SET content_text=content_text||' A later correction.' WHERE id=3"); t.s.conn.commit()
            if change == 'unknown': raise ValueError('synthetic unknown transport')
            return response(t.value)
        if change == 'empty':
            return {'id': 'synthetic-empty', 'choices': [{'finish_reason': 'length', 'message': {'content': ''}}],
                    'usage': {'prompt_tokens': 9, 'completion_tokens': 12000, 'total_tokens': 12009}}
        return response({'report': t.value, 'review': {'ready': True, 'issues': [], 'locator_warnings': [], 'editorial_warnings': []}})
    job = SimpleNamespace(job_type=reports.JOB_TYPE, queue_name='openai', status='running', max_attempts=1, payload={'run_id': rid})
    return rid, reports.run(t.s.conn, job, complete=complete)


def test_initial_report_two_calls_and_separate_role_first_publication(initial, monkeypatch, tmp_path):
    t = initial; rid, result = execute(t)
    assert result['status'] == 'accepted' and len(t.calls) == 2, result
    snap = reports._load(t.s.conn, rid)['snapshot']
    assert snap['previous_report'] is None and snap['evidence_delta']['baseline'] == 'initial'
    assert snap['incident_identity']['incident_window'] == 'January 2025'
    assert json.loads(t.calls[0]['messages'][1]['content'])['coverage']['omitted_source_ids'] == []
    branches = t.calls[0]['response_format']['json_schema']['schema']['properties']['items']['items']['anyOf']
    assert all('what_changed' not in b['properties']['section']['enum'] for b in branches)
    packet = json.loads(t.calls[0]['messages'][1]['content'])
    edited_packet = json.loads(t.calls[1]['messages'][1]['content'])['evidence']
    assert packet == edited_packet and edited_packet['coverage']['mode'] == 'complete'
    assert edited_packet['incident_identity'] == t.p['incident_identity']
    assert not t.s.conn.execute("SELECT 1 FROM event_public_pointers WHERE event_id='evt_initial'").fetchone()
    _, promoted = t.s.publish(rid, automatic=True)
    raw = t.s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE event_id=%s AND revision_id=%s',
                          ('evt_initial', promoted['revision_id'])).fetchone()[0]
    bundle = json.loads(raw)
    assert bundle['predecessor'] is None and bundle['revision_provenance']['update_reason'] == 'initial_report'
    assert bundle['attack']['catalog'] is None and not any(bundle['attack']['mappings'].values())
    assert publication.published_material(t.s.conn, bundle)['report']['items'][0]['text'] == t.value['items'][0]['text']
    from sempervigil.event_render import render
    _, html = render(bundle, event_id='evt_initial', expected_revision=promoted['revision_id'])
    assert 'Initial source-backed incident report' in html
    assert t.s.conn.execute('SELECT count(*) FROM event_source_report_calls WHERE run_id=%s', (rid,)).fetchone()[0] == 2
    assert reports.submit(t.s.conn, 'evt_initial')['status'] == 'unchanged'

    # First reports use the same native export and final activation authorization.
    from sempervigil.event_publication_store import load_export
    from sempervigil.event_release import INDEX_PATH, fragment_identity, publication_history
    from sempervigil.event_activation import authorize_and_activate
    from sempervigil.event_source_report_render_v2 import index_entry
    import hashlib
    def dedicated():
        c = t.s.factory(t.s.promotion); c.commit(); return c
    exported = load_export(dedicated, ['evt_initial', 'evt_test'])
    assert exported['qualified_revisions']['evt_initial'] == bundle
    assert exported['promoted_revision_ids']['evt_test'] == t.s.old
    revisions = exported['promoted_revision_ids']
    updates = dict(t.s.conn.execute('SELECT event_id,updated_at FROM event_public_pointers').fetchall())
    histories = publication_history(t.s.conn, sorted(revisions), revisions, updates)
    entries, fragments, pages = [], {}, {}
    for eid in sorted(revisions):
        public = exported['qualified_revisions'][eid]
        _, page_html = render(public, event_id=eid, expected_revision=revisions[eid])
        entries.append({**index_entry(public, event_id=eid, expected_revision=revisions[eid]),
                        'url': '/events/'+eid+'/', 'revision_published_at': updates[eid],
                        'publication_history': histories[eid]})
        page = tmp_path/'events'/eid/'index.html'; page.parent.mkdir(parents=True); page.write_text(page_html)
        fragments[eid] = fragment_identity(page_html); pages[eid] = eid
    index = tmp_path/INDEX_PATH; index.parent.mkdir(parents=True); index.write_text(json.dumps(entries))
    manifest = {'workflow': 'event-release-authorization-v2', 'revisions': revisions,
                'withdrawn': {}, 'pages': pages, 'index_sha256': hashlib.sha256(index.read_bytes()).hexdigest(),
                'fragments': fragments}
    switched = []
    authorize_and_activate(dedicated, manifest, lambda: switched.append(True), release=tmp_path)
    assert switched == [True]
    assert t.s.conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0] == t.s.old
    t.s.conn.execute("UPDATE articles SET content_text=content_text||' New reporting.' WHERE id=3")
    t.s.conn.commit()
    with pytest.raises(ValueError, match='initial_draft_required'): reports.submit(t.s.conn, 'evt_initial')
    t.s.conn.rollback()
    assert len(t.calls) == 2
    # Ordinary updates preserve the last qualified report at the same event URL.
    assert load_export(dedicated, ['evt_initial'])['qualified_revisions']['evt_initial'] == bundle
    authorize_and_activate(dedicated, manifest, lambda: switched.append(True), release=tmp_path)
    assert switched == [True, True]
    # Research identity comes from qualified original evidence after the bounded
    # initial cohort closes, even if the live publisher body has been refreshed.
    from sempervigil.event_report_initial import research_identity
    monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED', '0')
    monkeypatch.delenv('SV_EVENT_REPORT_V2_POLICY')
    assert research_identity(t.s.conn, 'evt_initial') == t.p['incident_identity']


@pytest.mark.parametrize('change, count', [('entity', 1), ('source', 1), ('unknown', 1), ('empty', 2)])
def test_initial_changed_or_failed_run_holds_without_retry_or_publication(initial, change, count):
    t = initial; rid, result = execute(t, change=change)
    assert result['status'] == 'held' and len(t.calls) == count
    record = reports._load(t.s.conn, rid)
    assert record['budget_tokens'] == 70000
    assert not t.s.conn.execute("SELECT 1 FROM event_public_pointers WHERE event_id='evt_initial'").fetchone()
    reports.tick(t.s.conn)
    assert len(t.calls) == count
    assert t.s.conn.execute("SELECT count(*) FROM event_source_report_runs WHERE event_id='evt_initial'").fetchone()[0] == 1
    if change == 'unknown': assert record['reserved_tokens'] > 0
    if change == 'empty': assert record['charged_tokens'] == 12309 and record['reserved_tokens'] == 0


def test_first_report_duplicate_publisher_url_counts_and_packs_one_source(initial):
    t = initial
    t.s.conn.execute('''INSERT INTO articles(id,title,original_url,content_text,published_at,ingested_at,meta_json,has_full_content)
        SELECT 4,'Older feed capture','https://example.org/initial/',content_text||' Old page footer.',
        '2026-10-01','2026-09-30','{}',1 FROM articles WHERE id=3''')
    t.s.conn.execute("INSERT INTO event_articles VALUES('evt_initial',4)"); t.s.conn.commit()
    rid, result = execute(t)
    assert result['status'] == 'accepted' and len(t.calls) == 2, result
    packet = reports._load(t.s.conn, rid)['snapshot']
    assert len(packet['sources']) == 1 and len(packet['membership']) == 2
    assert len(packet['capture_history'][0]['captures']) == 2
    for request in t.calls:
        model = json.loads(request['messages'][1]['content'])
        if 'evidence' in model: model = model['evidence']
        assert len(model['sources']) == 1 and 'capture_history' not in model
    _, promoted = t.s.publish(rid, automatic=True)
    public = json.loads(t.s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',
                                         (promoted['revision_id'],)).fetchone()[0])
    from sempervigil.event_source_report_render_v2 import index_entry
    assert index_entry(public, event_id='evt_initial', expected_revision=promoted['revision_id'])['counts']['articles'] == 1
    assert t.s.conn.execute("SELECT count(*) FROM event_articles WHERE event_id='evt_initial'").fetchone()[0] == 2


def test_successor_research_identity_is_verified_from_qualified_predecessor(initial, monkeypatch):
    t = initial; rid, result = execute(t)
    assert result['status'] == 'accepted'; t.s.publish(rid, automatic=True)
    t.s.conn.execute("UPDATE articles SET content_text=content_text||' Additional reporting.' WHERE id=3"); t.s.conn.commit()
    monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY', json.dumps(t.s.p))
    monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_ID', 'fixture-successor-identity')
    monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_TOKENS', '64000')
    t.calls.clear(); successor, result = execute(t)
    assert result['status'] == 'accepted', result
    assert 'incident_identity' not in reports._load(t.s.conn, successor)['snapshot']
    t.s.publish(successor, automatic=True)
    from sempervigil.event_report_initial import research_identity
    monkeypatch.delenv('SV_EVENT_REPORT_V2_POLICY')
    assert research_identity(t.s.conn, 'evt_initial') == t.p['incident_identity']
    assert len(t.calls) == 2


@pytest.mark.parametrize('change', ['expired', 'disabled', 'entity', 'state', 'anchor', 'withdrawn_history'])
def test_initial_invalid_authority_or_identity_fails_before_admission(initial, monkeypatch, change):
    t = initial
    if change == 'expired':
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        p = {**t.p, 'starts_at': (now-timedelta(hours=2)).isoformat(),
             'expires_at': (now-timedelta(seconds=1)).isoformat()}
        monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY', json.dumps(p))
    if change == 'disabled': monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED', '0')
    if change == 'entity': t.s.conn.execute("UPDATE events SET entity='Other' WHERE id='evt_initial'")
    if change == 'state': t.s.conn.execute("UPDATE events SET publish_state='published' WHERE id='evt_initial'")
    if change == 'anchor': t.s.conn.execute("UPDATE articles SET content_text='A different incident.' WHERE id=3")
    if change == 'withdrawn_history':
        t.s.conn.execute("INSERT INTO event_quote_qualifications VALUES('evt_initial',%s,'{}','2026-01-01',NULL)", ('c'*64,))
        t.s.conn.execute("INSERT INTO event_public_revisions VALUES('evt_initial',%s,%s,NULL,'{}','2026-01-01')", ('c'*64, 'c'*64))
    t.s.conn.commit()
    with pytest.raises((ValueError, PermissionError)): reports.submit(t.s.conn, 'evt_initial')
    t.s.conn.rollback()
    assert not t.calls
    assert t.s.conn.execute("SELECT count(*) FROM event_source_report_runs WHERE event_id='evt_initial'").fetchone()[0] == 0
    assert t.s.conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0] == t.s.old


def test_initial_held_run_consumes_lifetime_slot_and_other_cases_are_excluded(initial):
    t = initial
    rid, result = execute(t, change='empty')
    assert result['status'] == 'held' and len(t.calls) == 2
    assert reports.submit(t.s.conn, 'evt_initial')['reused']
    t.s.conn.execute("UPDATE articles SET content_text=content_text||' Changed evidence.' WHERE id=3"); t.s.conn.commit()
    with pytest.raises(ValueError, match='run_limit'): reports.submit(t.s.conn, 'evt_initial')
    t.s.conn.rollback()
    with pytest.raises(PermissionError): reports.submit(t.s.conn, 'evt_test')
    assert len(t.calls) == 2
    assert t.s.conn.execute("SELECT count(*) FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'='fixture-first-report'").fetchone()[0] == 1
