"""Real lifecycle/provenance checks with synthetic replies; no paid requests."""
import copy
import json
from types import SimpleNamespace

import pytest
from test_event_source_reports_postgres import database, generated, response
from sempervigil import event_source_reports_v2 as reports
from sempervigil import event_source_report_publication_v2 as publication
from sempervigil import event_report_final_editor as editor
from sempervigil import event_report_contract_v2 as contract
from sempervigil.investigation import _version


@pytest.fixture
def setup(database, monkeypatch):
    conn, factory, namespace = database
    for key in ('SV_EVENT_REPORT_V2_ENABLED', 'SV_EVENT_REPORT_V2_GENERATION_ENABLED'):
        monkeypatch.setenv(key, '1')
    monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS', 'evt_test')
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL', 'fixture-writer')
    profile = {'workflow': editor.WORKFLOW, 'model': 'fixture-editor', 'reasoning_effort': 'high',
               'max_completion_tokens': 12000, 'context_overrides': {}}
    monkeypatch.setenv(editor.CONFIG_ENV, json.dumps(profile))
    monkeypatch.setattr(reports, 'configuration', lambda _: ({'id': 'w', 'model_name': 'fixture-writer', 'max_context': 128000}, {'id': 'p'}, 'b'*64))
    conn.execute("CREATE TABLE llm_providers(id TEXT,name TEXT,type TEXT,is_enabled INTEGER); CREATE TABLE llm_models(id TEXT,provider_id TEXT,model_name TEXT,is_enabled INTEGER)")
    conn.execute("INSERT INTO llm_providers VALUES('p','openai','openai_compatible',1); INSERT INTO llm_models VALUES('e','p','fixture-editor',1)")
    from sempervigil.services import ai_service
    monkeypatch.setattr(ai_service, 'get_model', lambda *a: {'id':'e','model_name':'fixture-editor','max_context':128000})
    conn.commit()
    draft = generated()
    for item in draft['items']:
        item['attack_mappings'] = []
    final = copy.deepcopy(draft)
    final['items'][0]['text'] = 'Acme said certain patient records may be affected. The available statement does not establish complete exposure.'
    calls = []
    def execute(result=None, side_effect=None):
        admitted = reports.submit(conn, 'evt_test', debounce_seconds=0, budget_tokens=64000)
        rid = admitted['run_id']
        output = result or {'report': editor.narrative(final), 'review': {'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[]}}
        def complete(payload):
            calls.append(payload)
            if side_effect:
                side_effect(len(calls))
            return response(editor.narrative(draft) if len(calls)==1 else output)
        job = SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':rid})
        return rid, reports.run(conn, job, complete=complete), job
    return SimpleNamespace(conn=conn,factory=factory,namespace=namespace,draft=draft,final=final,calls=calls,execute=execute,profile=profile)


def test_new_final_artifact_promotes_and_exports_under_separated_roles(setup):
    s=setup;rid,result,job=s.execute()
    assert result['status']=='accepted' and len(s.calls)==2, result
    material=publication.current_material(s.conn,rid)
    assert material['report']==s.final and material['report']!=s.draft
    lineage=material['derivation']
    assert lineage['report_version']==_version(s.final)!=lineage['draft_version']
    assert lineage['artifact_version']!=lineage['report_version']
    packet=json.loads(s.calls[1]['messages'][1]['content'])
    assert packet['evidence']['sources']==reports._load(s.conn,rid)['snapshot']['sources']
    assert 'attack_reference' not in packet['evidence']
    assert all('attack_mappings' not in item for item in packet['report']['items'])
    assert 'attack_mappings' not in s.calls[0]['response_format']['json_schema']['schema']['properties']['items']['items']['anyOf'][0]['properties']
    assert reports.run(s.conn,job,complete=lambda _:pytest.fail('paid replay'))['reused']
    admission='adm_'+s.namespace;promotion='pro_'+s.namespace
    for role in (admission,promotion):
        s.conn.execute(f'CREATE ROLE "{role}"');s.conn.execute(f'GRANT USAGE ON SCHEMA "{s.namespace}" TO "{role}"')
        s.conn.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{s.namespace}" TO "{role}"')
        s.conn.execute(f'GRANT UPDATE ON events,articles,event_articles,event_source_report_runs TO "{role}"')
        s.conn.execute(f'REVOKE SELECT ON llm_models,llm_providers FROM "{role}"')
    s.conn.execute(f'GRANT INSERT ON jobs,event_quote_qualifications,event_review_approvals TO "{admission}"')
    s.conn.execute(f'GRANT INSERT,UPDATE ON event_public_revisions,event_public_pointers TO "{promotion}"');s.conn.commit()
    try:
        queued=publication.submit(s.conn,rid,factory=lambda:s.factory(admission))
        from sempervigil.event_approval import run
        promoted=run({'approval_id':queued['approval_id']},factory=lambda:s.factory(promotion))
        bundle=json.loads(s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(promoted['revision_id'],)).fetchone()[0])
        assert publication.published_material(s.conn,bundle)['report']==s.final
        from sempervigil.event_render import render
        _,html=render(bundle,event_id='evt_test',expected_revision=promoted['revision_id'])
        assert s.final['items'][0]['text'] in html
        assert publication.qualification(material,rid)['derivation']==lineage
    finally:
        for role in (admission,promotion):
            s.conn.execute(f'DROP OWNED BY "{role}"');s.conn.execute(f'DROP ROLE "{role}"')
        s.conn.commit()


@pytest.mark.parametrize('tamper',['old_report','old_ready','editor_receipt','lineage','source','manual_derivative'])
def test_final_editor_cannot_reuse_old_approval_or_changed_material(setup,tamper):
    s=setup;rid,result,_=s.execute();assert result['status']=='accepted'
    if tamper=='old_report':s.conn.execute('UPDATE event_source_report_runs SET report_json=%s WHERE run_id=%s',(contract.encode(s.draft),rid))
    elif tamper in {'old_ready','editor_receipt','lineage'}:
        import psycopg
        with pytest.raises(psycopg.errors.CheckViolation,match='immutable'):
            if tamper=='lineage':s.conn.execute("UPDATE event_source_report_derivatives SET projection_json='{}' WHERE run_id=%s",(rid,))
            else:s.conn.execute("UPDATE event_source_report_calls SET response_json='{}' WHERE run_id=%s AND phase='review'",(rid,))
        s.conn.rollback()
        assert publication.current_material(s.conn,rid)['report']==s.final
        return
    elif tamper=='source':s.conn.execute("UPDATE articles SET content_text=content_text||' New detail.' WHERE id=1")
    s.conn.commit()
    with pytest.raises(Exception):publication.current_material(s.conn,rid,**({'derivative':{}} if tamper=='manual_derivative' else {}))


@pytest.mark.parametrize('failure',['unresolved','invalid','issue_binding','context','stale','transport'])
def test_failures_hold_without_third_call(setup,monkeypatch,failure):
    s=setup
    output={'report':editor.narrative(s.final),'review':{'ready':False,'issues':[{'item_id':'P01','reason':'A material premise cannot be resolved from the complete source.','source_ids':['S1']}],'locator_warnings':[],'editorial_warnings':[]}}
    if failure=='invalid':output={'ready':True,'issues':[]}
    if failure=='issue_binding':output['review']['issues'][0]['item_id']='P99'
    if failure=='context':monkeypatch.setattr(reports,'configuration',lambda _:({'id':'w','model_name':'fixture-writer','max_context':1},{'id':'p'},'b'*64))
    def side_effect(n):
        if failure=='stale' and n==2:s.conn.execute("UPDATE articles SET content_text=content_text||' Changed source.' WHERE id=1");s.conn.commit()
        if failure=='transport' and n==2:raise TimeoutError('unknown HTTP outcome')
    rid,result,job=s.execute(output if failure in {'unresolved','invalid','issue_binding'} else None,side_effect)
    assert result['status']=='held',result
    before=len(s.calls);assert before<=2
    reports.run(s.conn,job,complete=lambda _:pytest.fail('third rescue call'))
    assert len(s.calls)==before
    with pytest.raises(ValueError):publication.current_material(s.conn,rid)
    if failure=='context':assert not s.calls
    if failure=='transport':assert reports._load(s.conn,rid)['reserved_tokens']>0


def test_same_model_guard_not_weakened(setup,monkeypatch):
    s=setup;monkeypatch.setattr(reports,'configuration',lambda _:({'id':'w','model_name':'fixture-editor','max_context':128000},{'id':'p'},'b'*64))
    with pytest.raises(ValueError,match='independent_models_required'):reports.submit(s.conn,'evt_test',budget_tokens=64000)
    assert not s.calls


def test_unchanged_content_keeps_content_hash_but_new_lineage(setup):
    s=setup;rid,result,_=s.execute({'report':editor.narrative(s.draft),'review':{'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[]}})
    assert result['status']=='accepted'
    lineage=publication.current_material(s.conn,rid)['derivation']
    assert lineage['draft_version']==lineage['report_version']==_version(s.draft)
    assert lineage['artifact_version']!=lineage['report_version']


def test_active_editor_rejects_any_taxonomy_output(setup):
    s=setup
    final=editor.narrative(s.final);final['items'][0]['attack_mappings']=[]
    rid,result,_=s.execute({'report':final,'review':{'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[]}})
    assert result['status']=='held' and len(s.calls)==2
    with pytest.raises(ValueError):publication.current_material(s.conn,rid)


def test_unresolved_issue_binds_to_new_final_item_not_input_ids(setup):
    s=setup;final=editor.narrative(s.final)
    final['items'].append({**final['items'][1],'id':'P03','text':'Credential rotation was reported by the company.'})
    rid,result,_=s.execute({'report':final,'review':{'ready':False,'issues':[{'item_id':'P03','reason':'An unresolved material issue is attached to this newly returned paragraph.','source_ids':['S1']}],'locator_warnings':[],'editorial_warnings':[]}})
    assert result['status']=='held' and result.get('reason') is None
    assert reports._load(s.conn,rid)['review']['issues'][0]['item_id']=='P03'


def test_authorized_same_model_separate_invocations_reconstruct_with_honest_lineage(setup,monkeypatch):
    s=setup
    monkeypatch.setenv(editor.MODEL_POLICY_ENV,editor.SOURCE_CHECKING_PASSES)
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL','fixture-editor')
    monkeypatch.setattr(reports,'configuration',lambda _:({'id':'e','model_name':'fixture-editor','max_context':128000},{'id':'p'},'b'*64))
    rid,result,_=s.execute()
    assert result['status']=='accepted' and len(s.calls)==2,result
    assert s.calls[0]['model']==s.calls[1]['model']=='fixture-editor'
    material=publication.current_material(s.conn,rid)
    assert material['derivation']['independence']=='none; same-model separate source-checking invocations'
    assert material['derivation']['invocation_policy']==editor.SOURCE_CHECKING_PASSES
    assert material['derivation']['writer_response_version']!=material['derivation']['editor_response_version']


def test_missing_scoped_authority_cannot_reconstruct_same_model_material(setup,monkeypatch):
    s=setup;monkeypatch.setenv(editor.MODEL_POLICY_ENV,editor.SOURCE_CHECKING_PASSES)
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL','fixture-editor')
    monkeypatch.setattr(reports,'configuration',lambda _:({'id':'e','model_name':'fixture-editor','max_context':128000},{'id':'p'},'b'*64))
    rid,result,_=s.execute();assert result['status']=='accepted'
    record=reports._load(s.conn,rid);record['snapshot'].pop('final_editor_model_policy')
    rows=s.conn.execute('SELECT request_json,response_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(rid,)).fetchall()
    with pytest.raises(ValueError):editor.derive(*(json.loads(v) for row in rows for v in row),record['snapshot'])


def test_expired_overall_window_is_proven_no_http(setup, monkeypatch):
    from datetime import datetime, timezone, timedelta
    s = setup
    admitted = reports.submit(s.conn, 'evt_test', debounce_seconds=0, budget_tokens=64000)
    rid = admitted['run_id']
    monkeypatch.setattr(reports, 'utc_now_iso', lambda: (datetime.now(timezone.utc) - timedelta(seconds=601)).isoformat())
    job = SimpleNamespace(job_type=reports.JOB_TYPE, queue_name='openai', status='running',
                          max_attempts=1, payload={'run_id': rid})
    result = reports.run(s.conn, job, complete=lambda _: pytest.fail('HTTP after overall deadline'))
    assert result['status'] == 'held'
    row = s.conn.execute('SELECT status,error,reservation FROM event_source_report_calls WHERE run_id=%s', (rid,)).fetchone()
    assert row[0] == 'failed' and row[2] > 0
    assert json.loads(row[1])['proof']['kind'] == 'instrumented_authority'
    assert reports._load(s.conn, rid)['reserved_tokens'] == 0
    assert len(s.calls) == 0


def test_short_editor_profile_rejected_before_journal_or_http(setup, monkeypatch):
    from sempervigil import event_report_transport as transport
    s = setup
    admitted = reports.submit(s.conn, 'evt_test', debounce_seconds=0, budget_tokens=64000)
    rid = admitted['run_id']
    monkeypatch.setenv(transport.POLICY_ENV, json.dumps({**transport.DEFAULT, 'editor_seconds': 60}))
    job = SimpleNamespace(job_type=reports.JOB_TYPE, queue_name='openai', status='running',
                          max_attempts=1, payload={'run_id': rid})
    result = reports.run(s.conn, job, complete=lambda _: pytest.fail('HTTP under short profile'))
    assert result['status'] == 'held'
    assert s.conn.execute('SELECT count(*) FROM event_source_report_calls WHERE run_id=%s', (rid,)).fetchone()[0] == 0
