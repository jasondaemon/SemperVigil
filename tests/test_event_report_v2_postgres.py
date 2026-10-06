import copy,json
from types import SimpleNamespace
import pytest
from test_event_source_reports_postgres import database,generated,response
from sempervigil import event_source_reports as legacy
from sempervigil import event_source_reports_v2 as reports
from sempervigil import event_source_report_publication_v2 as publication
from sempervigil import event_report_contract_v2 as contract

@pytest.mark.parametrize('mapping_case',['valid','binding_invalid','semantic_issue'])
def test_v2_two_call_mapping_and_restricted_publication(database,monkeypatch,mapping_case):
 conn,factory,namespace=database
 monkeypatch.setenv('SV_EVENT_REPORT_V2_ENABLED','1');monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test');monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','1')
 monkeypatch.setattr(reports,'configuration',lambda _:({'id':'m','model_name':'test-model'},{'id':'p'},'b'*64))
 text='Acme said certain patient records may be affected. The company rotated credentials. Acme reported password spraying against several accounts.'
 if mapping_case=='semantic_issue':text+=' Acme explicitly denied successful privilege escalation.'
 conn.execute('UPDATE articles SET content_text=%s WHERE id=1',(text,));conn.execute("INSERT INTO articles VALUES(2,'Additional source','https://example.org/other',%s,'2026-10-01','2026-10-01','{}')",(text+' A second source supplied further context.',));conn.execute("INSERT INTO event_articles VALUES('evt_test',2)");conn.commit()
 value=generated()
 for i in value['items']:i['attack_mappings']=[]
 item=value['items'][1];item.update(section='attack_path',text='Acme reported password spraying against several accounts.',citations=[{'source_id':'S1','quote':'Acme reported password spraying against several accounts.'}])
 item['attack_mappings']=[{'technique_id':'T1110.003','origin':'analyst_applied','behavior_status':'reported','rationale':'The supplied synthetic account names password spraying across multiple accounts.','limitations':'Successful authentication is not established.','source_ids':['S2' if mapping_case=='binding_invalid' else 'S1']}]
 review={'ready':mapping_case!='semantic_issue','issues':[],'locator_warnings':[]}
 if mapping_case=='semantic_issue':
  item['text']='Acme confirmed the attacker escalated to root by exploiting software.'
  item['citations']=[{'source_id':'S1','quote':'Acme explicitly denied successful privilege escalation.'}]
  item['attack_mappings'][0].update(technique_id='T1068',rationale='The synthetic writer incorrectly asserts successful exploitation for root escalation.')
  review['issues']=[{'item_id':'P02','reason':'Complete cited source explicitly denies the root-escalation premise despite valid source ID and exact quote locator.','source_ids':['S1']}]
 submitted=legacy.submit(conn,'evt_test',debounce_seconds=0,budget_tokens=32000);calls=[]
 def complete(payload):calls.append(payload);return response(value if len(calls)==1 else review)
 job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':submitted['run_id']})
 result=legacy.run(conn,job,complete=complete);assert len(calls)==2,result
 rid=submitted['run_id'];record=reports._load(conn,rid)
 if mapping_case=='semantic_issue':
  assert result['status']=='held'
  with pytest.raises(ValueError,match='not_accepted'):publication.current_material(conn,rid)
  return
 assert result['status']=='accepted',result
 material=publication.current_material(conn,rid)
 if mapping_case=='binding_invalid':assert material['report']['items'][1]['attack_mappings']==[]
 else:assert material['resolved_mappings']['P02'][0]['name']=='Password Spraying'
 admission='adm_'+namespace;promotion='pro_'+namespace
 conn.execute(f'CREATE ROLE "{admission}"');conn.execute(f'CREATE ROLE "{promotion}"')
 for role in (admission,promotion):
  conn.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT UPDATE ON events,articles,event_articles,event_source_report_runs TO "{role}"')
 conn.execute(f'GRANT INSERT ON jobs,event_quote_qualifications,event_review_approvals TO "{admission}"');conn.execute(f'GRANT INSERT,UPDATE ON event_public_revisions,event_public_pointers TO "{promotion}"');conn.commit()
 try:
  queued=publication.submit(conn,rid,factory=lambda:factory(admission))
  from sempervigil.event_approval import run
  promoted=run({'approval_id':queued['approval_id']},factory=lambda:factory(promotion))
  bundle=json.loads(conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(promoted['revision_id'],)).fetchone()[0])
  from sempervigil.event_render import render
  metadata,html=render(bundle,event_id='evt_test',expected_revision=promoted['revision_id'])
  assert metadata['event_report_format']==contract.PUBLIC_WORKFLOW
  assert ('techniques/T1110/003/' in html)==(mapping_case=='valid')
  assert publication.current_material(conn,rid,published=True)
  # Same body, changed metadata or suppression must fail the published evidence guard.
  conn.execute("UPDATE articles SET meta_json='{"+ '"suppressed":true' +"}' WHERE id=1");conn.commit()
  with pytest.raises(ValueError,match='evidence_changed'):publication.current_material(conn,rid,published=True)
 finally:
  for role in (admission,promotion):conn.execute(f'DROP OWNED BY "{role}"');conn.execute(f'DROP ROLE "{role}"')
  conn.commit()
