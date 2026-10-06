"""A genuine new source follows final editing through normal successor gates."""
import copy,json
import pytest
from types import SimpleNamespace
from test_event_report_v2_policy_postgres import setup
from test_event_source_reports_postgres import database,response
from sempervigil import event_report_final_editor as editor,event_source_reports_v2 as reports
from sempervigil import event_source_report_publication_v2 as publication


@pytest.mark.parametrize("same_model",[False,True])
def test_genuine_successor_final_editor_uses_fresh_qualification_and_same_event(setup,monkeypatch,same_model):
    s=setup
    profile={'workflow':editor.WORKFLOW,'model':'fixture-editor','reasoning_effort':'high','max_completion_tokens':12000,'context_overrides':{}}
    monkeypatch.setenv(editor.CONFIG_ENV,json.dumps(profile))
    s.conn.execute("INSERT INTO llm_models VALUES('e','p','fixture-editor',1)");s.conn.commit()
    from sempervigil.services import ai_service
    monkeypatch.setattr(ai_service,'get_model',lambda *a:{'id':'e','model_name':'fixture-editor','max_context':128000})
    writer_model='fixture-editor' if same_model else 'fixture-writer'
    if same_model:
        monkeypatch.setenv(editor.MODEL_POLICY_ENV,editor.SOURCE_CHECKING_PASSES)
        monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL',writer_model)
    monkeypatch.setattr(reports,'configuration',lambda _:({'id':'w','model_name':writer_model,'max_context':128000},{'id':'p'},'b'*64))
    rid=next(x['run_id'] for x in reports.tick(s.conn) if x['status']=='queued')
    final=copy.deepcopy(s.value)
    final['items'][0]['text']='Acme reported possible patient-record exposure. The investigation continued in the newly supplied reporting.'
    final['items'][0]['citations'].append({'source_id':'S2','quote':'The investigation continued.'})
    calls=[]
    def complete(payload):
        calls.append(payload)
        return response(editor.narrative(s.value) if len(calls)==1 else {'report':editor.narrative(final),'review':{'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[]}})
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':rid})
    result=reports.run(s.conn,job,complete=complete)
    assert result['status']=='accepted' and len(calls)==2,result
    _,promoted=s.publish(rid,automatic=True)
    assert promoted['revision_id']!=s.old
    bundle=json.loads(s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(promoted['revision_id'],)).fetchone()[0])
    material=publication.published_material(s.conn,bundle)
    assert material['report']==final and material['derivation']['workflow']==editor.WORKFLOW
    assert material['event_id']=='evt_test'
    assert s.conn.execute('SELECT count(*) FROM events').fetchone()[0]==1
    qualification=publication.qualification(material,rid)
    assert qualification['derivation']['editor_response_version']
    assert reports._load(s.conn,rid)['charged_tokens']==600


def test_new_same_model_policy_does_not_relax_legacy_autonomous_guard(setup,monkeypatch):
    s=setup
    monkeypatch.delenv(editor.CONFIG_ENV,raising=False)
    monkeypatch.setenv(editor.MODEL_POLICY_ENV,editor.SOURCE_CHECKING_PASSES)
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL',reports.MODEL)
    monkeypatch.setattr(reports,'configuration',lambda _:({'id':'r','model_name':reports.MODEL,'max_context':128000},{'id':'p'},'b'*64))
    with pytest.raises(ValueError,match='independent_models_required'):reports.submit(s.conn,'evt_test',trigger='evidence_change')
    assert not s.calls


@pytest.mark.parametrize('change', ['expiry', 'generation_disabled', 'autonomous_disabled'])
@pytest.mark.parametrize('boundary', [1, 2])
def test_policy_change_during_overall_deadline_query_never_transports(setup, monkeypatch, change, boundary):
    from datetime import datetime, timedelta
    from sempervigil import event_report_v2_policy as policy, event_report_transport as transport
    s = setup
    profile = {'workflow': editor.WORKFLOW, 'model': 'fixture-editor', 'reasoning_effort': 'high',
               'max_completion_tokens': 12000, 'context_overrides': {}}
    monkeypatch.setenv(editor.CONFIG_ENV, json.dumps(profile))
    s.conn.execute("INSERT INTO llm_models VALUES('e','p','fixture-editor',1)")
    s.conn.commit()
    from sempervigil.services import ai_service
    monkeypatch.setattr(ai_service, 'get_model', lambda *a: {'id': 'e', 'model_name': 'fixture-editor', 'max_context': 128000})
    monkeypatch.setattr(reports, 'configuration', lambda _: ({'id': 'w', 'model_name': 'fixture-writer', 'max_context': 128000}, {'id': 'p'}, 'b'*64))
    rid = reports.submit(s.conn, 'evt_test')['run_id']
    queries = []
    class Conn:
        def __getattr__(self, name): return getattr(s.conn, name)
        def execute(self, sql, *args, **kwargs):
            cursor = s.conn.execute(sql, *args, **kwargs)
            if str(sql).startswith('SELECT min(created_at) FROM event_source_report_calls'):
                queries.append(sql)
                if len(queries) == boundary:
                    if change == 'expiry':
                        # Keep the pinned policy unchanged; only its window ends
                        # while the new query runs, after full freshness passed.
                        class ExpiredClock(datetime):
                            @classmethod
                            def now(cls, tz=None): return datetime.now(tz) + timedelta(hours=2)
                        monkeypatch.setattr(policy, 'datetime', ExpiredClock)
                    elif change == 'generation_disabled':
                        monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED', '0')
                    else:
                        monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS', '0')
            return cursor
    # Exercise both parent guards: after the durable journal, and after client
    # credential preparation immediately before the real transport boundary.
    monkeypatch.setattr(reports, 'ready_client', lambda _: ({'base_url': 'https://example.invalid/v1'}, {}))
    monkeypatch.setattr(transport, 'complete', lambda *a, **k: pytest.fail('HTTP after expired/disabled query'))
    job = SimpleNamespace(job_type=reports.JOB_TYPE, queue_name='openai', status='running',
                          max_attempts=1, payload={'run_id': rid})
    result = reports.run(Conn(), job)
    assert result['status'] == 'held'
    assert len(queries) == boundary
    assert s.conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0] == s.old
    row = s.conn.execute('SELECT status,error,reservation,response_json FROM event_source_report_calls WHERE run_id=%s', (rid,)).fetchone()
    assert row[0] == 'failed' and row[2] > 0 and row[3] is None
    proof = json.loads(row[1])['proof']
    assert proof['kind'] == 'instrumented_authority' and proof['failure_stage'] == 'authority_recheck'
    record = reports._load(s.conn, rid)
    assert record['reserved_tokens'] == 0 and record['charged_tokens'] == 0
    assert not s.calls
    # The attempted journal and lifetime cohort admission survive; no paid retry.
    assert record['budget_tokens'] == 32000
    assert reports.run(Conn(), job)['reused']
    assert len(queries) == boundary



def test_delayed_child_start_holds_durable_journal_as_proven_zero_http(setup, monkeypatch):
    import time
    from sempervigil import event_report_transport as transport
    s = setup
    profile = {'workflow': editor.WORKFLOW, 'model': 'fixture-editor', 'reasoning_effort': 'high',
               'max_completion_tokens': 12000, 'context_overrides': {}}
    monkeypatch.setenv(editor.CONFIG_ENV, json.dumps(profile))
    s.conn.execute("INSERT INTO llm_models VALUES('e','p','fixture-editor',1)")
    s.conn.commit()
    from sempervigil.services import ai_service
    monkeypatch.setattr(ai_service, 'get_model', lambda *a: {'id': 'e', 'model_name': 'fixture-editor', 'max_context': 128000})
    monkeypatch.setattr(reports, 'configuration', lambda _: ({'id': 'w', 'model_name': 'fixture-writer', 'max_context': 128000}, {'id': 'p'}, 'b'*64))
    rid = reports.submit(s.conn, 'evt_test')['run_id']
    monkeypatch.setattr(reports, 'ready_client', lambda _: ({'type': 'fixture', 'base_url': 'http://127.0.0.1:1/v1'}, {}))
    original = transport.complete
    def delayed(*args, **kwargs):
        kwargs['authorized_until'] = time.time() + 0.05
        return original(*args, **kwargs)
    monkeypatch.setattr(transport, 'complete', delayed)
    monkeypatch.setattr(transport, '_CHILD', 'import time; time.sleep(0.2)\n' + transport._CHILD)
    job = SimpleNamespace(job_type=reports.JOB_TYPE, queue_name='openai', status='running',
                          max_attempts=1, payload={'run_id': rid})
    assert reports.run(s.conn, job)['status'] == 'held'
    row = s.conn.execute('SELECT status,error,reservation,response_json FROM event_source_report_calls WHERE run_id=%s', (rid,)).fetchone()
    assert row[0] == 'failed' and row[2] > 0 and row[3] is None
    proof = json.loads(row[1])['proof']
    assert proof['kind'] == 'instrumented_authority' and proof['failure_stage'] == 'child_authorization_deadline'
    record = reports._load(s.conn, rid)
    assert record['reserved_tokens'] == 0 and record['charged_tokens'] == 0
    assert s.conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0] == s.old
    assert reports.run(s.conn, job)['reused']
