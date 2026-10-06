"""A genuine new source follows final editing through normal successor gates."""
import copy,json
from types import SimpleNamespace
from test_event_report_v2_policy_postgres import setup
from test_event_source_reports_postgres import database,response
from sempervigil import event_report_final_editor as editor,event_source_reports_v2 as reports
from sempervigil import event_source_report_publication_v2 as publication


def test_genuine_successor_final_editor_uses_fresh_qualification_and_same_event(setup,monkeypatch):
    s=setup
    profile={'workflow':editor.WORKFLOW,'model':'fixture-editor','reasoning_effort':'high','max_completion_tokens':12000,'context_overrides':{}}
    monkeypatch.setenv(editor.CONFIG_ENV,json.dumps(profile))
    s.conn.execute("INSERT INTO llm_models VALUES('e','p','fixture-editor',1)");s.conn.commit()
    from sempervigil.services import ai_service
    monkeypatch.setattr(ai_service,'get_model',lambda *a:{'id':'e','model_name':'fixture-editor','max_context':128000})
    monkeypatch.setattr(reports,'configuration',lambda _:({'id':'w','model_name':'fixture-writer','max_context':128000},{'id':'p'},'b'*64))
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
