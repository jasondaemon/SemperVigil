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
 review={'ready':mapping_case!='semantic_issue','issues':[],'locator_warnings':[],'editorial_warnings':[]}
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


@pytest.mark.parametrize('decision',['accepted','self_review','old_hash','not_ready','automatic','stale'])
def test_editorial_derivative_keeps_model_review_original_and_normal_gates(database,monkeypatch,decision):
 conn,factory,namespace=database
 monkeypatch.setenv('SV_EVENT_REPORT_V2_ENABLED','1');monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test');monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','1')
 monkeypatch.setattr(reports,'configuration',lambda _:({'id':'m','model_name':'fixture-only'},{'id':'p'},'b'*64))
 value=generated()
 for item in value['items']:item['attack_mappings']=[]
 value['items'][0]['text']='Acme reported possible patient-record exposure. This does not establish that all patient records were exposed.'
 ready={'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[]};calls=[]
 admitted=reports.submit(conn,'evt_test',debounce_seconds=0)
 def complete(payload):calls.append(payload);return response(value if len(calls)==1 else ready)
 job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':admitted['run_id']})
 assert reports.run(conn,job,complete=complete)['status']=='accepted'
 rid=admitted['run_id'];base=publication.current_material(conn,rid)
 evidence=json.loads(conn.execute('SELECT projection_json FROM event_source_report_derivatives WHERE run_id=%s',(rid,)).fetchone()[0])['projection']['evidence']
 from sempervigil.event_report_editorial import prepare,CHECKS
 from sempervigil.attack_catalog_runtime import catalog_for
 first=copy.deepcopy(value['items'][0]);first['text']='Acme reported possible patient-record exposure.'
 last=copy.deepcopy(first);last.update(id='P03',text='This does not establish that all patient records were exposed.',section='analyst_assessment',claim_type='assessment',confidence='moderate',rationale='The source reports possible exposure only. Scope has not been established.',date_label='Undated analyst assessment',date_sort=None)
 proposal=prepare(base['report'],evidence,base['review'],[{'item_id':'P01','parts':[first,last]}],editor='synthetic editor',catalog=catalog_for(evidence['attack_reference']['catalog']))
 review={'workflow':'event-report-independent-editorial-review-v1','ready':True,'reviewer':'synthetic independent reviewer',
         'source_review':'DISPOSABLE TEST ONLY: complete fixture source supports qualified scope; not live approval.',
         **{k:proposal[k] for k in ['report_version','evidence_version','original_report_version','original_review_version']},'checks':{k:True for k in CHECKS}}
 editorial={'proposal':proposal,'review':review,'confirmation':publication.EDITORIAL_CONFIRMATION}
 frozen=conn.execute('SELECT report_json,review_json,spans_json FROM event_source_report_runs WHERE run_id=%s',(rid,)).fetchone()
 frozen_calls=conn.execute('SELECT request_json,response_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(rid,)).fetchall()
 monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','0')
 if decision=='self_review':review['reviewer']=proposal['editor']
 if decision=='old_hash':review['report_version']=proposal['original_report_version']
 if decision=='not_ready':review['ready']=False
 if decision=='stale':conn.execute("UPDATE articles SET content_text=content_text||' Material source update.' WHERE id=1");conn.commit()
 if decision!='accepted':
  with pytest.raises((ValueError,PermissionError)):
   publication.submit(conn,rid,editorial=editorial,automatic=decision=='automatic')
  assert conn.execute('SELECT count(*) FROM event_review_approvals').fetchone()[0]==0
  assert len(calls)==2
  return
 admission='adm_'+namespace;promotion='pro_'+namespace
 for role in (admission,promotion):
  conn.execute(f'CREATE ROLE "{role}"');conn.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT UPDATE ON events,articles,event_articles,event_source_report_runs TO "{role}"')
 conn.execute(f'GRANT INSERT ON jobs,event_quote_qualifications,event_review_approvals TO "{admission}"');conn.execute(f'GRANT INSERT,UPDATE ON event_public_revisions,event_public_pointers TO "{promotion}"');conn.commit()
 try:
  with pytest.raises(PermissionError,match='admission_role_required'):
   publication.submit(conn,rid,editorial=editorial,factory=factory)
  queued=publication.submit(conn,rid,editorial=editorial,factory=lambda:factory(admission))
  from sempervigil.event_approval import run
  promoted=run({'approval_id':queued['approval_id']},factory=lambda:factory(promotion))
  bundle=json.loads(conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(promoted['revision_id'],)).fetchone()[0])
  assert bundle['report']==proposal['report']
  assert publication.current_material(conn,rid)['report']==base['report']
  assert publication.published_material(conn,bundle)['report']==proposal['report']
  lineage=bundle['qualification']['derivation']
  assert lineage['automated_review_scope']=='original_report_only'
  assert lineage['original_review_version']==proposal['original_review_version']
  assert lineage['independent_review_version']==reports._version(review)
  assert 'original_report' not in lineage and 'reviewer' not in lineage
  changed_review=copy.deepcopy(editorial)
  changed_review['review']['source_review']+=' A different synthetic approval must recheck predecessor.'
  with pytest.raises(ValueError,match='predecessor_conflict'):
   publication.submit(conn,rid,editorial=changed_review,factory=lambda:factory(admission))
  from sempervigil.event_render import render
  _,html=render(bundle,event_id='evt_test',expected_revision=promoted['revision_id'])
  assert 'Analyst assessment' in html and 'moderate' not in html and 'Confidence' not in html and 'Rationale' not in html
  from sempervigil.event_publication_store import load_export
  from sempervigil.event_release import check_current
  def export_factory():
   session=factory();session.commit();return session
  assert load_export(export_factory,['evt_test'])['qualified_revisions']['evt_test']==bundle
  check_current(conn,bundle);conn.rollback()
  assert conn.execute('SELECT report_json,review_json,spans_json FROM event_source_report_runs WHERE run_id=%s',(rid,)).fetchone()==frozen
  assert conn.execute('SELECT request_json,response_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(rid,)).fetchall()==frozen_calls
  assert len(calls)==2
  conn.execute('UPDATE event_quote_qualifications SET revoked_at=%s WHERE event_id=%s AND qualification_id=%s',('2026-10-06','evt_test',reports._version(bundle['qualification'])));conn.commit()
  with pytest.raises(ValueError,match='qualification_unavailable'):publication.published_material(conn,bundle)
  conn.rollback()
  assert load_export(export_factory,['evt_test'])['withdrawn']['evt_test']=='qualification_revoked'
 finally:
  conn.rollback()
  for role in (admission,promotion):conn.execute(f'DROP OWNED BY "{role}"');conn.execute(f'DROP ROLE "{role}"')
  conn.commit()
