"""Ordinary old-generation publication survives upgrades; no continuation bypass."""
import copy,json,pathlib
from types import SimpleNamespace
import pytest
from test_event_source_reports_postgres import database,generated,response
from sempervigil import event_source_reports_v2 as reports,event_report_contract_v2 as contract
from sempervigil import event_source_report_publication_v2 as publication
from sempervigil import event_report_generation_identity as generation


@pytest.mark.parametrize('original_pin',[False,True])
def test_ordinary_old_generation_publication_reads_and_exports_with_prompt_tamper_rejected(database,monkeypatch,original_pin):
    conn,factory,namespace=database
    for key in ('SV_EVENT_REPORT_V2_ENABLED','SV_EVENT_REPORT_V2_GENERATION_ENABLED'):monkeypatch.setenv(key,'1')
    monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test')
    monkeypatch.setattr(reports,'configuration',lambda _:({'id':'m','model_name':'test-model'},{'id':'p'},'b'*64))
    old=json.loads((pathlib.Path(__file__).parent/'fixtures/historical_v2_prompts_c91.json').read_text())
    value=generated()
    for item in value['items']:item['attack_mappings']=[]
    original_schema=contract.review_schema;calls=[]
    with monkeypatch.context() as historical:
        historical.setattr(contract,'WRITER',old['writer']);historical.setattr(contract,'ATTACK_WRITER','')
        historical.setattr(contract,'REVIEWER',old['reviewer']);historical.setattr(contract,'ATTACK_REVIEWER','')
        historical.setattr(contract,'REVIEW_CONTRACT',old['review_contract'])
        historical.setattr(contract,'review_schema',lambda report,ids,**kwargs:original_schema(report,ids,**{**kwargs,'editorial':False}))
        rid=reports.submit(conn,'evt_test',debounce_seconds=0,budget_tokens=32000)['run_id']
        if not original_pin:
            snap=reports._load(conn,rid)['snapshot'];snap.pop('generation_prompt_identity')
            conn.execute('UPDATE event_source_report_runs SET snapshot_json=%s WHERE run_id=%s',(contract.encode(snap),rid));conn.commit()
        def complete(payload):
            calls.append(payload);return response(value if len(calls)==1 else {'ready':True,'issues':[],'locator_warnings':[]})
        job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':rid})
        assert reports.run(conn,job,complete=complete)['status']=='accepted'
        admission='adm_'+namespace;promotion='pro_'+namespace
        for role in (admission,promotion):
            conn.execute(f'CREATE ROLE "{role}"');conn.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT UPDATE ON events,articles,event_articles,event_source_report_runs TO "{role}"')
        conn.execute(f'GRANT INSERT ON jobs,event_quote_qualifications,event_review_approvals TO "{admission}"');conn.execute(f'GRANT INSERT,UPDATE ON event_public_revisions,event_public_pointers TO "{promotion}"');conn.commit()
        q=publication.submit(conn,rid,factory=lambda:factory(admission))
        from sempervigil.event_approval import run
        promoted=run({'approval_id':q['approval_id']},factory=lambda:factory(promotion))
        bundle=json.loads(conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(promoted['revision_id'],)).fetchone()[0])
    try:
        material=publication.published_material(conn,bundle)
        assert material['report']==value
        assert material['review']=={'ready':True,'issues':[],'locator_warnings':[]}
        assert publication.qualification(material,rid)==bundle['qualification']
        audit=json.loads(conn.execute('SELECT projection_json FROM event_source_report_derivatives WHERE run_id=%s',(rid,)).fetchone()[0])
        assert not audit.get('continuation')
        from sempervigil.event_publication_store import load_export
        def dedicated():
            c=factory(promotion);c.commit();return c
        exported=load_export(dedicated,['evt_test'])
        assert exported['withdrawn']=={} and exported['qualified_revisions']['evt_test']==bundle
        with pytest.raises(ValueError,match='prompt_integrity'):publication.current_material(conn,rid)
        altered=copy.deepcopy(calls[0]);altered['messages'][0]['content']='Unknown substituted generation prompt.'
        with pytest.raises(ValueError,match='prompt'):
            generation.validate(conn,material,altered,calls[1],published=True,current_writer=contract.WRITER+contract.ATTACK_WRITER,current_reviewer=contract.REVIEWER+contract.ATTACK_REVIEWER)
        import psycopg
        with pytest.raises(psycopg.errors.CheckViolation,match='immutable'):
            conn.execute("UPDATE event_source_report_calls SET request_json=%s WHERE run_id=%s AND phase='writer'",(contract.encode(altered),rid))
        conn.rollback()
    finally:
        for role in (admission,promotion):conn.execute(f'DROP OWNED BY "{role}"');conn.execute(f'DROP ROLE "{role}"')
        conn.commit()
