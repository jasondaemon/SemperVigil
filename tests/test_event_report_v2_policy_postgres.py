"""Real transactions and separated roles, synthetic provider replies only."""
import copy,json
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
import pytest
from test_event_source_reports_postgres import database,generated,response
from sempervigil import event_source_reports as legacy,event_source_reports_v2 as reports
from sempervigil import event_report_contract_v2 as contract
from sempervigil import event_source_report_publication_v2 as publication,event_report_v2_policy as policy


@pytest.fixture
def setup(database,monkeypatch):
 conn,factory,namespace=database
 from sempervigil.event_report_v2_integrity import SCHEMA
 conn.execute(SCHEMA)
 monkeypatch.setenv('SV_EVENT_REPORT_V2_ENABLED','1');monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test');monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','1')
 monkeypatch.setattr(reports,'configuration',lambda _:({'id':'m','model_name':'fixture-writer'},{'id':'p'},'b'*64))
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL','fixture-writer')
 conn.execute("CREATE TABLE llm_providers(id TEXT,name TEXT,type TEXT,is_enabled INTEGER); CREATE TABLE llm_models(id TEXT,provider_id TEXT,model_name TEXT,is_enabled INTEGER)")
 conn.execute("INSERT INTO llm_providers VALUES('p','openai','openai_compatible',1); INSERT INTO llm_models VALUES('r','p','gpt-5.6-luna',1)")
 from sempervigil.services import ai_service
 monkeypatch.setattr(ai_service,'get_model',lambda *a:{'id':'r','model_name':reports.MODEL})
 # Genuine admission/promotion separation even for simulated model decisions.
 admission='adm_'+namespace;promotion='pro_'+namespace
 for role in (admission,promotion):
  conn.execute(f'CREATE ROLE "{role}"');conn.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{role}"');conn.execute(f'GRANT UPDATE ON events,articles,event_articles,event_source_report_runs TO "{role}"')
 conn.execute(f'GRANT DELETE ON event_source_report_runs TO "{admission}"') # Disposable fixture matches existing scheduler DELETE.
 conn.execute(f'GRANT INSERT ON jobs,event_quote_qualifications,event_review_approvals TO "{admission}"');conn.execute(f'GRANT INSERT,UPDATE ON event_public_revisions,event_public_pointers TO "{promotion}"')
 for role in (admission,promotion):conn.execute(f'REVOKE SELECT ON llm_models,llm_providers FROM "{role}"')
 conn.commit()
 value=generated()
 for item in value['items']:item['attack_mappings']=[]
 calls=[]
 def execute(rid,review=None,transport=None):
  replies=[value,review or {'ready':True,'issues':[],'locator_warnings':[]}];local=[]
  def complete(payload):
   local.append(payload);calls.append(payload)
   if transport:raise transport
   value=replies[len(local)-1]
   return response({**value,'editorial_warnings':value.get('editorial_warnings',[])} if 'ready' in value else value)
  job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':rid})
  return reports.run(conn,job,complete=complete)
 def publish(rid,automatic=False):
  from sempervigil.event_approval import run
  queued=publication.submit(conn,rid,automatic=automatic,factory=lambda:factory(admission))
  assert publication.submit(conn,rid,automatic=automatic,factory=lambda:factory(admission))['status']=='reused'
  result=run({'approval_id':queued['approval_id']},factory=lambda:factory(promotion))
  return queued,result
 rid=reports.submit(conn,'evt_test',debounce_seconds=0)['run_id'];assert execute(rid)['status']=='accepted'
 _,base=publish(rid);old=base['revision_id']
 calls.clear()
 now=datetime.now(timezone.utc)
 p={'starts_at':(now-timedelta(minutes=1)).isoformat(),'expires_at':(now+timedelta(hours=1)).isoformat(),'max_runs':2,'max_concurrent':1,'run_tokens':32000,'debounce_seconds':300,'generator_version':'b'*64}
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_ID','v2-fixture');monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_TOKENS','64000');monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p));monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','1')
 conn.execute("INSERT INTO articles SELECT 2,'New incident reporting','https://example.org/new-report',content_text||' The investigation continued.','2026-10-02','2026-10-02','{}' FROM articles WHERE id=1; INSERT INTO event_articles VALUES('evt_test',2)");conn.commit()
 yield SimpleNamespace(conn=conn,factory=factory,namespace=namespace,execute=execute,publish=publish,calls=calls,value=value,old=old,p=p,admission=admission,promotion=promotion)
 conn.rollback()
 for role in (admission,promotion):conn.execute(f'DROP OWNED BY "{role}"');conn.execute(f'DROP ROLE "{role}"')
 conn.commit()


def pointer(s):return s.conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0]


def test_bounded_whole_report_lifecycle_and_abstention(setup,monkeypatch,tmp_path):
 s=setup;events=reports.tick(s.conn);rid=next(e['run_id'] for e in events if e['status']=='queued')
 snap=reports._load(s.conn,rid)['snapshot'];assert snap['autonomous_policy']==policy.policy()
 assert snap['cohort']=={'id':'v2-fixture','limit':64000}
 delay=s.conn.execute('SELECT available_at,requested_at FROM jobs WHERE payload_json::jsonb->>\'run_id\'=%s',(rid,)).fetchone()
 assert (datetime.fromisoformat(delay[0])-datetime.fromisoformat(delay[1])).total_seconds()>=299
 assert s.execute(rid)['status']=='accepted' and len(s.calls)==2
 writer_branch=s.calls[0]['response_format']['json_schema']['schema']['properties']['items']['items']['anyOf'][0]
 assert writer_branch['properties']['text']['description']==contract.schema(['S1'])['properties']['items']['items']['anyOf'][0]['properties']['text']['description']
 monkeypatch.setattr(publication,'connection_factory',lambda _:lambda:s.factory(s.admission))
 receipts=reports.tick(s.conn);q=next(x for x in receipts if x.get('approval_id'))
 assert publication.submit(s.conn,rid,automatic=True,factory=lambda:s.factory(s.admission))['status']=='reused'
 from sempervigil.event_approval import run
 result=run({'approval_id':q['approval_id']},factory=lambda:s.factory(s.promotion))
 assert pointer(s)==result['revision_id']!=s.old
 bundle=json.loads(s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(result['revision_id'],)).fetchone()[0])
 assert not any(bundle['attack']['mappings'].values())
 from sempervigil.event_render import render
 metadata,html=render(bundle,event_id='evt_test',expected_revision=result['revision_id']);assert 'techniques/' not in html
 assert publication.published_material(s.conn,bundle)['report']==s.value
 # Disposable export and activation checks use exact native publication history.
 from sempervigil.event_publication_store import load_export
 from sempervigil.event_release import INDEX_PATH,fragment_identity,verify_release
 from sempervigil.event_activation import authorize_and_activate
 from sempervigil.event_source_report_render_v2 import index_entry
 import hashlib
 def dedicated():
  c=s.factory(s.promotion);c.commit();return c
 exported=load_export(dedicated,['evt_test'])
 assert exported['qualified_revisions']['evt_test']==bundle
 from sempervigil.event_release import publication_history
 updated=s.conn.execute("SELECT updated_at FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0]
 history=publication_history(s.conn,['evt_test'],{'evt_test':result['revision_id']},{'evt_test':updated})['evt_test']
 entry={**index_entry(bundle,event_id='evt_test',expected_revision=result['revision_id']),
        'url':'/events/evt_test/','revision_published_at':updated,'publication_history':history}
 index=tmp_path/INDEX_PATH;index.parent.mkdir(parents=True);index.write_text(json.dumps([entry]))
 page=tmp_path/'events/evt_test/index.html';page.parent.mkdir(parents=True);page.write_text(html)
 manifest={'workflow':'event-release-authorization-v2','revisions':{'evt_test':result['revision_id']},
           'withdrawn':{},'pages':{'evt_test':'evt_test'},'index_sha256':hashlib.sha256(index.read_bytes()).hexdigest(),
           'fragments':{'evt_test':fragment_identity(html)}}
 switched=[]
 authorize_and_activate(dedicated,manifest,lambda:switched.append(True),release=tmp_path)
 assert switched==[True]
 page.write_text(html.replace('possible patient-record','unchecked patient-record'))
 with pytest.raises(ValueError,match='page_projection_mismatch'):
  authorize_and_activate(dedicated,manifest,lambda:switched.append(True),release=tmp_path)
 assert switched==[True]
 with pytest.raises(ValueError,match='predecessor_changed'):
  publication.submit(s.conn,rid,automatic=True,factory=lambda:s.factory(s.admission))
 assert reports.submit(s.conn,'evt_test')['status']=='unchanged'
 assert len(s.calls)==2


@pytest.mark.parametrize('change',['sources','predecessor','expiry','rollback','generation','catalog'])
def test_queued_staleness_never_calls_or_changes_publication(setup,monkeypatch,change):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 if change=='sources':s.conn.execute("UPDATE articles SET content_text=content_text||' A correction.' WHERE id=2");s.conn.commit()
 if change=='predecessor':s.conn.execute("DELETE FROM event_public_pointers WHERE event_id='evt_test'");s.conn.commit()
 if change=='expiry':p=copy.deepcopy(s.p);p['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat();monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 if change=='rollback':monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
 if change=='generation':monkeypatch.setattr(reports,'configuration',lambda _:({'id':'m','model_name':'fixture-writer'},{'id':'p'},'c'*64))
 if change=='catalog':
  from sempervigil import attack_catalog_runtime
  original=attack_catalog_runtime.settings
  monkeypatch.setattr(attack_catalog_runtime,'settings',lambda:{**original(),'catalog':{'sha256':'0'*64}})
 expected=pointer(s) if change!='predecessor' else None
 assert s.execute(rid)['status']=='held' and not s.calls
 assert (pointer(s) if change!='predecessor' else s.conn.execute('SELECT count(*) FROM event_public_pointers').fetchone()[0])==(expected if change!='predecessor' else 0)


@pytest.mark.parametrize('failure',['review','transport','budget'])
def test_failure_old_publication_and_no_retry_spend(setup,monkeypatch,failure):
 s=setup
 if failure=='budget':p=copy.deepcopy(s.p);p['run_tokens']=1;monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 rid=reports.submit(s.conn,'evt_test')['run_id']
 review={'ready':False,'issues':[{'item_id':'P02','reason':'SYNTHETIC review flags an unsupported generalization.','source_ids':['S1']}],'locator_warnings':[]}
 result=s.execute(rid,review=review if failure=='review' else None,transport=RuntimeError('fixture transport failure') if failure=='transport' else None)
 assert result['status']=='held' and pointer(s)==s.old
 count=len(s.calls)
 assert reports.submit(s.conn,'evt_test')['reused'] and s.execute(rid)['reused'] and len(s.calls)==count
 assert count==(2 if failure=='review' else 1 if failure=='transport' else 0)
 with pytest.raises(ValueError):s.publish(rid,automatic=True)
 assert pointer(s)==s.old


@pytest.mark.parametrize('limit',['runs','capacity','busy','policy_conflict'])
def test_lifetime_capacity_and_concurrency(setup,monkeypatch,limit):
 s=setup
 if limit=='runs':p=copy.deepcopy(s.p);p['max_runs']=1;monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 if limit=='capacity':monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_TOKENS','32000')
 first=reports.submit(s.conn,'evt_test')['run_id']
 if limit!='busy':s.conn.execute("UPDATE event_source_report_runs SET status='held' WHERE run_id=%s",(first,));s.conn.commit()
 if limit=='policy_conflict':p=copy.deepcopy(s.p);p['debounce_seconds']=301;monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 s.conn.execute("UPDATE articles SET content_text=content_text||' Further changed evidence.' WHERE id=2");s.conn.commit()
 with pytest.raises(ValueError,match={'runs':'run_limit','capacity':'capacity_exhausted','busy':'busy','policy_conflict':'policy_conflict'}[limit]):reports.submit(s.conn,'evt_test')
 s.conn.rollback();assert not s.calls and pointer(s)==s.old


def test_promotion_rechecks_freshness_and_policy(setup,monkeypatch):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 queued=publication.submit(s.conn,rid,automatic=True,factory=lambda:s.factory(s.admission))
 monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','0')
 from sempervigil.event_approval import run
 with pytest.raises(ValueError):run({'approval_id':queued['approval_id']},factory=lambda:s.factory(s.promotion))
 assert pointer(s)==s.old and len(s.calls)==2


@pytest.mark.parametrize('identifier',['P110','P113','P115'])
def test_real_negative_examples_hold_when_reviewer_identifies_issue(setup,identifier):
 from pathlib import Path
 s=setup
 case=next(c for c in json.loads((Path(__file__).parent/'fixtures/zammad_semantic_negatives.json').read_text()) if c['item']['id']==identifier)
 item=copy.deepcopy(case['item'])
 # Preserve exact reported prose and citation strings; combine cited fixture
 # passages only for persistence/gate testing. Later real evaluation uses full bodies.
 for citation in item['citations']:citation['source_id']='S2'
 extra=' '.join(c['quote'] for c in item['citations'])
 s.conn.execute('UPDATE articles SET content_text=content_text||%s WHERE id=2',(' '+extra,));s.conn.commit()
 s.value['items'].append(item)
 rid=reports.submit(s.conn,'evt_test')['run_id']
 review={'ready':False,'issues':[{'item_id':identifier,'reason':case['expected_issue'],'source_ids':['S2']}],'locator_warnings':[]}
 assert s.execute(rid,review=review)['status']=='held'
 assert len(s.calls)==2 and pointer(s)==s.old
 with pytest.raises(ValueError):s.publish(rid,automatic=True)
 # Simulated negative decisions verify gate behavior; not model accuracy.


def test_parallel_event_admission_serializes_cohort_capacity(setup,monkeypatch):
 from concurrent.futures import ThreadPoolExecutor
 from threading import Barrier
 s=setup
 monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test,evt_other')
 s.conn.execute("INSERT INTO events VALUES('evt_other','Acme incident','active','confirmed'); INSERT INTO event_articles VALUES('evt_other',1)");s.conn.commit()
 other=reports.submit(s.conn,'evt_other')['run_id'];assert s.execute(other)['status']=='accepted';s.publish(other)
 s.calls.clear();monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','1')
 s.conn.execute("INSERT INTO articles SELECT 3,'Concurrent new reporting','https://example.org/concurrent',content_text||' New evidence for concurrent admission.','2026-10-03','2026-10-03','{}' FROM articles WHERE id=1; INSERT INTO event_articles VALUES('evt_test',3),('evt_other',3)");s.conn.commit()
 barrier=Barrier(2)
 def request(event):
  with s.factory() as conn:
   barrier.wait(timeout=5)
   try:return reports.submit(conn,event)['status']
   except ValueError as e:conn.rollback();return str(e)
 with ThreadPoolExecutor(max_workers=2) as pool:
  futures=[pool.submit(request,event) for event in ['evt_test','evt_other']]
  states=[f.result(timeout=10) for f in futures]
 assert sorted(states)==['event_report_v2_busy','queued']
 assert s.conn.execute("SELECT count(*) FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'='v2-fixture'").fetchone()[0]==1
 assert not s.calls


def test_scheduler_failure_isolated_from_legacy_and_ingestion(setup,monkeypatch):
 s=setup
 monkeypatch.setattr(legacy,'_v1_tick',lambda c:[{'status':'legacy_unaffected'}])
 monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY','{}')
 result=legacy.tick(s.conn)
 assert result[0]=={'status':'legacy_unaffected'}
 assert result[1]['status']=='held' and result[1]['reason']=='event_report_v2_policy_required'
 assert not s.calls and pointer(s)==s.old


def test_expired_or_disabled_policy_preserves_published_report(setup,monkeypatch):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 _,result=s.publish(rid,automatic=True)
 bundle=json.loads(s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(result['revision_id'],)).fetchone()[0])
 monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0');monkeypatch.setenv('SV_EVENT_REPORT_V2_ENABLED','0')
 assert publication.published_material(s.conn,bundle)['report']==s.value
 assert pointer(s)==result['revision_id']


@pytest.mark.parametrize('misconfigured',['same_model','reviewer_missing','writer_not_explicit'])
def test_model_readiness_rejects_before_any_transport(setup,monkeypatch,misconfigured):
 s=setup
 if misconfigured=='same_model':monkeypatch.setattr(reports,'configuration',lambda _:({'id':'m','model_name':reports.MODEL},{'id':'p'},'b'*64))
 if misconfigured=='reviewer_missing':s.conn.execute('DELETE FROM llm_models');s.conn.commit()
 if misconfigured=='writer_not_explicit':monkeypatch.delenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL')
 with pytest.raises(ValueError):reports.submit(s.conn,'evt_test')
 s.conn.rollback();assert not s.calls and pointer(s)==s.old


def test_provider_overrun_closes_remaining_cohort(setup,monkeypatch):
 import sys
 s=setup;real=response
 def overrun(value):
  reply=real(value);reply['usage']['total_tokens']=1000000;return reply
 monkeypatch.setattr(sys.modules[__name__],'response',overrun)
 rid=reports.submit(s.conn,'evt_test')['run_id']
 assert s.execute(rid)['status']=='held' and len(s.calls)==1 and pointer(s)==s.old
 s.conn.execute("UPDATE articles SET content_text=content_text||' Additional evidence.' WHERE id=2");s.conn.commit()
 with pytest.raises(ValueError,match='provider_budget_overrun'):reports.submit(s.conn,'evt_test')
 s.conn.rollback();assert pointer(s)==s.old and len(s.calls)==1


def test_promotion_rejects_runtime_options_changed_after_approval(setup,monkeypatch):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 queued=publication.submit(s.conn,rid,automatic=True,factory=lambda:s.factory(s.admission))
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_PHASE_CONFIG',json.dumps({'writer':{'reasoning_effort':'none','max_completion_tokens':6000}}))
 from sempervigil.event_approval import run
 with pytest.raises(ValueError,match='runtime_changed'):run({'approval_id':queued['approval_id']},factory=lambda:s.factory(s.promotion))
 assert pointer(s)==s.old and len(s.calls)==2


@pytest.mark.parametrize('change',['source','expiry','rollback'])
def test_midrun_change_stops_before_reviewer_transport(setup,monkeypatch,change):
 import sys
 s=setup;real=response;seen=[]
 def changed(value):
  seen.append(True)
  if len(seen)==1:
   if change=='source':s.conn.execute("UPDATE articles SET content_text=content_text||' Midrun correction.' WHERE id=2");s.conn.commit()
   if change=='expiry':p=copy.deepcopy(s.p);p['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat();monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
   if change=='rollback':monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','0')
  return real(value)
 monkeypatch.setattr(sys.modules[__name__],'response',changed)
 rid=reports.submit(s.conn,'evt_test')['run_id']
 assert s.execute(rid)['status']=='held' and len(s.calls)==1 and pointer(s)==s.old
 assert s.conn.execute('SELECT phase,status FROM event_source_report_calls WHERE run_id=%s',(rid,)).fetchall()==[('writer','completed')]


def test_false_ready_semantic_fixture_is_not_claimed_as_detected(setup):
 s=setup
 from pathlib import Path
 case=next(c for c in json.loads((Path(__file__).parent/'fixtures/zammad_semantic_negatives.json').read_text()) if c['item']['id']=='P113')
 item=copy.deepcopy(case['item'])
 for citation in item['citations']:citation['source_id']='S2'
 s.conn.execute('UPDATE articles SET content_text=content_text||%s WHERE id=2',(' '+' '.join(c['quote'] for c in item['citations']),));s.conn.commit()
 s.value['items'].append(item);rid=reports.submit(s.conn,'evt_test')['run_id']
 assert s.execute(rid)['status']=='accepted'
 # A simulated false-ready review demonstrates the unresolved semantic gap.
 # Do not promote it or describe deterministic validation as detecting meaning.
 assert pointer(s)==s.old and len(s.calls)==2


@pytest.mark.parametrize('phase',['review','correction','verification'])
def test_autonomous_call_order_and_extra_phases_rejected(setup,phase):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 with pytest.raises(ValueError,match='two_call_limit'):
  reports.call(s.conn,rid,phase,'fixture',{}, {},complete=lambda _:pytest.fail('transport attempted'))
 s.conn.rollback();assert not s.calls and pointer(s)==s.old


def test_legacy_enrollment_cannot_bypass_dormant_autonomous_policy(setup,monkeypatch):
 s=setup
 s.conn.execute('CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)')
 s.conn.execute("INSERT INTO settings VALUES('event.source_report.enrolled','[\"evt_test\"]','2026-10-06')");s.conn.commit()
 monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_ID','legacy-enrollment')
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_TOKENS','64000')
 assert legacy.tick(s.conn)==[]
 assert not s.calls and pointer(s)==s.old
 assert s.conn.execute("SELECT count(*) FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'='v2-fixture'").fetchone()[0]==0


def test_live_legacy_and_v2_successors_have_independent_capacity_and_transport(setup,monkeypatch):
 from sempervigil import event_source_report_pilot as pilot,event_source_report_publication as v1_publication
 from test_event_source_reports_postgres import generated as v1_generated
 s=setup
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL','gpt-5.6-sol')
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_PHASE_CONFIG',json.dumps({'writer':{'reasoning_effort':'none','max_completion_tokens':6000},'review':{'reasoning_effort':'low','max_completion_tokens':2400}}))
 monkeypatch.setattr(legacy,'configuration',lambda _:({'id':'m','model_name':'gpt-5.6-sol'},{'id':'p'},'a'*64))
 s.conn.execute("INSERT INTO events VALUES('evt_ms','Synthetic Microsoft','active','confirmed'),('evt_ast','Synthetic Astrana','active','confirmed'); INSERT INTO event_articles VALUES('evt_ms',1),('evt_ast',1)");s.conn.commit()
 def execute_legacy(rid):
  replies=iter([v1_generated(),{'ready':True,'issues':[],'locator_warnings':[]}])
  job=SimpleNamespace(job_type=legacy.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':rid})
  return legacy.run(s.conn,job,complete=lambda _:response(next(replies)))
 for event in ('evt_ms','evt_ast'):
  rid=legacy.submit(s.conn,event,debounce_seconds=0)['run_id'];assert execute_legacy(rid)['status']=='accepted'
  record=legacy._load(s.conn,rid);qualification=v1_publication.qualification(record,rid)
  bundle=v1_publication.bundle_for(record,rid,qualification,None);qid=legacy._version(qualification);revision=legacy._version(bundle)
  s.conn.execute('INSERT INTO event_quote_qualifications VALUES(%s,%s,%s,%s,NULL)',(event,qid,json.dumps(qualification),'now'))
  s.conn.execute('INSERT INTO event_public_revisions VALUES(%s,%s,%s,NULL,%s,%s)',(event,revision,qid,json.dumps(bundle),'now'))
  s.conn.execute('INSERT INTO event_public_pointers VALUES(%s,%s,%s)',(event,revision,'now'));s.conn.commit()
 now=datetime.now(timezone.utc)
 old={'id':'legacy-live','limit':32000,'events':['evt_ms','evt_ast'],'starts_at':(now-timedelta(minutes=1)).isoformat(),'expires_at':(now+timedelta(hours=1)).isoformat(),'max_runs':1,'max_concurrent':1,'run_tokens':32000}
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_PILOT_POLICY',json.dumps(old))
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_ID',old['id']);monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_TOKENS',str(old['limit']))
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_EVENT_IDS','evt_ms,evt_ast')
 v2_policy=policy.policy();old=pilot.policy();policy.active(v2_policy);pilot.active(old)
 s.conn.execute("INSERT INTO articles SELECT 3,'Genuine synthetic new source','https://example.org/successor',content_text||' Genuine synthetic successor change.','2026-10-03','2026-10-03','{}' FROM articles WHERE id=1; INSERT INTO event_articles VALUES('evt_ms',3),('evt_ast',3),('evt_test',3)");s.conn.commit()
 old_rid=legacy.submit(s.conn,'evt_ms')['run_id'];assert execute_legacy(old_rid)['status']=='accepted'
 new_rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(new_rid)['status']=='accepted'
 assert pilot.locked_rows(s.conn,old)[0][0]==old_rid
 assert policy.locked_rows(s.conn,v2_policy)[0][0]==new_rid
 for rid,identity,marker in [(old_rid,'legacy-live','pilot'),(new_rid,'v2-fixture','autonomous_policy')]:
  record=reports._load(s.conn,rid)
  assert record['snapshot']['cohort']['id']==identity
  assert marker in record['snapshot'] and record['budget_tokens']==32000
  assert record['charged_tokens']==600 and record['reserved_tokens']==0
  assert s.conn.execute('SELECT count(*) FROM event_source_report_calls WHERE run_id=%s',(rid,)).fetchone()[0]==2
  assert ('autonomous_policy' if marker=='pilot' else 'pilot') not in record['snapshot']
 assert sum(r[3] for r in pilot.locked_rows(s.conn,old))==32000
 assert sum(r[3] for r in policy.locked_rows(s.conn,v2_policy))==32000
 s.conn.execute("UPDATE articles SET content_text=content_text||' Another material change.' WHERE id=3");s.conn.commit()
 with pytest.raises(ValueError,match='pilot_run_limit'):legacy.submit(s.conn,'evt_ast')
 s.conn.rollback()
 # Exhausting legacy's lifetime slot does not consume v2's second admission.
 second=reports.submit(s.conn,'evt_test')['run_id']
 assert second!=new_rid and len(policy.locked_rows(s.conn,v2_policy))==2
 assert len(pilot.locked_rows(s.conn,old))==1 and pilot.policy()==old
 s.conn.commit()


def test_editorial_warning_is_nonblocking_in_native_publication(setup):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 warning={'item_id':'P01','reason':'Nonmaterial classification preference; visible prose preserves the source qualification.','source_ids':['S1']}
 review={'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[warning]}
 assert s.execute(rid,review=review)['status']=='accepted'
 _,result=s.publish(rid,automatic=True)
 assert pointer(s)==result['revision_id']
 assert reports._load(s.conn,rid)['review']['editorial_warnings']==[warning]


def test_v2_cohort_identity_changes_stop_queued_transport_without_affecting_legacy(setup,monkeypatch):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 captured=reports._load(s.conn,rid)['snapshot']['autonomous_policy']
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_ID','fresh-other')
 assert s.execute(rid)['status']=='held' and not s.calls and pointer(s)==s.old
 assert reports._load(s.conn,rid)['snapshot']['autonomous_policy']==captured


@pytest.mark.parametrize('stage',['admission','queued','promotion'])
@pytest.mark.parametrize('reason',['revoked','terminal_held'])
def test_unqualified_predecessor_never_revived_by_novel_sources(setup,stage,reason):
 s=setup;rid=None
 if stage!='admission':rid=reports.submit(s.conn,'evt_test')['run_id']
 if stage=='promotion':assert s.execute(rid)['status']=='accepted'
 baseline,_=reports.published_baseline(s.conn,'evt_test',reports.snapshot(s.conn,'evt_test'))
 assert baseline is not None and reports.meaningful_change(baseline,reports.snapshot(s.conn,'evt_test'))
 if reason=='revoked':s.conn.execute("UPDATE event_quote_qualifications SET revoked_at='now' WHERE event_id='evt_test'")
 else:s.conn.execute("UPDATE event_source_report_runs SET status='held' WHERE run_id=(SELECT bundle_json::jsonb->>'run_id' FROM event_public_revisions WHERE event_id='evt_test' AND revision_id=%s)",(s.old,))
 s.conn.commit()
 if stage=='admission':
  with pytest.raises(ValueError,match='qualified_predecessor_required'):reports.submit(s.conn,'evt_test')
  s.conn.rollback();assert not s.calls
 elif stage=='queued':assert s.execute(rid)['status']=='held' and not s.calls
 else:
  with pytest.raises(ValueError,match='qualified_predecessor_required'):s.publish(rid,automatic=True)
  assert len(s.calls)==2
 assert pointer(s)==s.old
