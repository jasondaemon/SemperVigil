"""Real transactions and separated roles, synthetic provider replies only."""
import copy,json
from contextlib import contextmanager
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
import pytest
from test_event_source_reports_postgres import database,generated,response
from sempervigil import event_source_reports as legacy,event_source_reports_v2 as reports
from sempervigil import event_source_report_publication_v2 as publication,event_report_v2_policy as policy


@pytest.fixture
def setup(database,monkeypatch):
 conn,factory,namespace=database
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
   return response(replies[len(local)-1])
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
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_ID','v2-fixture');monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_TOKENS','64000');monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p));monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','1')
 conn.execute("UPDATE articles SET content_text=content_text||' The investigation continued.' WHERE id=1");conn.commit()
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
 if change=='sources':s.conn.execute("UPDATE articles SET content_text=content_text||' A correction.' WHERE id=1");s.conn.commit()
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
 if limit=='capacity':monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_TOKENS','32000')
 first=reports.submit(s.conn,'evt_test')['run_id']
 if limit!='busy':s.conn.execute("UPDATE event_source_report_runs SET status='held' WHERE run_id=%s",(first,));s.conn.commit()
 if limit=='policy_conflict':p=copy.deepcopy(s.p);p['debounce_seconds']=301;monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 s.conn.execute("UPDATE articles SET content_text=content_text||' Further changed evidence.' WHERE id=1");s.conn.commit()
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
 for citation in item['citations']:citation['source_id']='S1'
 extra=' '.join(c['quote'] for c in item['citations'])
 s.conn.execute('UPDATE articles SET content_text=content_text||%s WHERE id=1',(' '+extra,));s.conn.commit()
 s.value['items'].append(item)
 rid=reports.submit(s.conn,'evt_test')['run_id']
 review={'ready':False,'issues':[{'item_id':identifier,'reason':case['expected_issue'],'source_ids':['S1']}],'locator_warnings':[]}
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
 s.conn.execute("UPDATE articles SET content_text=content_text||' New evidence for concurrent admission.' WHERE id=1");s.conn.commit()
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
 s.conn.execute("UPDATE articles SET content_text=content_text||' Additional evidence.' WHERE id=1");s.conn.commit()
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
   if change=='source':s.conn.execute("UPDATE articles SET content_text=content_text||' Midrun correction.' WHERE id=1");s.conn.commit()
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
 for citation in item['citations']:citation['source_id']='S1'
 s.conn.execute('UPDATE articles SET content_text=content_text||%s WHERE id=1',(' '+' '.join(c['quote'] for c in item['citations']),));s.conn.commit()
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
 assert legacy.tick(s.conn)==[]
 assert not s.calls and pointer(s)==s.old
 assert s.conn.execute("SELECT count(*) FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'='v2-fixture'").fetchone()[0]==0
