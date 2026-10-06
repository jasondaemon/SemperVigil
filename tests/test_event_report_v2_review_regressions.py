"""Independent-review defect reproductions on real disposable PostgreSQL."""
import copy,json,time
from datetime import datetime,timedelta,timezone
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import pytest
from test_event_source_reports_postgres import database
from test_event_report_v2_policy_postgres import setup,pointer
from sempervigil import event_source_reports_v2 as reports,event_source_report_publication_v2 as publication


def test_scheduler_race_keeps_published_run_accepted(setup,monkeypatch):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 queued=publication.submit(s.conn,rid,automatic=True,factory=lambda:s.factory(s.admission))
 native=publication.submit;promoted=[]
 def compete(conn,run_id,**kw):
  from sempervigil.event_approval import run
  promoted.append(run({'approval_id':queued['approval_id']},factory=lambda:s.factory(s.promotion)))
  return native(conn,run_id,factory=lambda:s.factory(s.admission),**kw)
 monkeypatch.setattr(publication,'submit',compete)
 results=reports.tick(s.conn)
 from sempervigil.event_publication_store import load_export
 def dedicated():c=s.factory(s.promotion);c.commit();return c
 exported=load_export(dedicated,['evt_test']);state=reports._load(s.conn,rid)['status']
 print(json.dumps({'run_state':state,'pointer_is_promoted':pointer(s)==promoted[0]['revision_id'],'withdrawn':exported['withdrawn'],'tick':results}))
 assert state=='accepted'
 assert exported['qualified_revisions']['evt_test']['run_id']==rid


def test_benign_locator_warning_publishes(setup):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 review={'ready':True,'issues':[],'locator_warnings':[{'item_id':'P01','reason':'Locator quotes a clause; full cited source supplies the complete qualification.','source_ids':['S1']}]}
 assert s.execute(rid,review=review)['status']=='accepted'
 _,result=s.publish(rid,automatic=True)
 assert pointer(s)==result['revision_id']


@pytest.mark.parametrize('change',['expiry','autonomous_disabled','generation_disabled'])
def test_manual_default_cannot_drop_autonomous_policy(setup,monkeypatch,change):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 if change=='expiry':
  p=copy.deepcopy(s.p);p['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat();monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 if change=='autonomous_disabled':monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
 if change=='generation_disabled':monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','0')
 with pytest.raises((ValueError,PermissionError)):
  s.publish(rid) # Intrinsic origin is guarded even with automatic=False.
 assert pointer(s)==s.old


@pytest.mark.parametrize("lock_kind", ["run", "cohort"])
@pytest.mark.parametrize("change", ["expiry", "sources", "predecessor"])
def test_real_lock_wait_drift_has_no_transport(setup,monkeypatch,lock_kind,change):
 s=setup;p=copy.deepcopy(s.p);expires=datetime.now(timezone.utc)+timedelta(seconds=3 if change=='expiry' else 60);p['expires_at']=expires.isoformat();monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 rid=reports.submit(s.conn,'evt_test')['run_id'];holder=s.factory();
 if lock_kind=='run':holder.execute('SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR UPDATE',(rid,))
 else:holder.execute('SELECT pg_advisory_xact_lock(hashtext(%s))',('source-report-cohort:v2-fixture',))
 ids=Queue();sent=[]
 def attempt():
  from test_event_source_reports_postgres import response
  with s.factory() as c:
   ids.put(c.execute('SELECT pg_backend_pid()').fetchone()[0])
   try:return reports.call(c,rid,'writer','fixture',{}, {},complete=lambda q:sent.append(q) or response(s.value))
   except Exception as e:c.rollback();return type(e).__name__+':'+str(e)
 with ThreadPoolExecutor(max_workers=1) as pool:
  f=pool.submit(attempt);pid=ids.get(timeout=5);blocked=False;deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   with s.factory() as observer:
    row=observer.execute('SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s',(pid,)).fetchone()
   if row and row[0]=='Lock':blocked=True;break
   if f.done():break
   time.sleep(.02)
  try:
   assert blocked,'Did not observe a real PostgreSQL lock wait'
   if change=='expiry':
    while datetime.now(timezone.utc)<=expires:time.sleep(.02)
   else:
    with s.factory() as drift:
     if change=='sources':drift.execute("UPDATE articles SET content_text=content_text||' New correction.' WHERE id=1")
     else:drift.execute("DELETE FROM event_public_pointers WHERE event_id='evt_test'")
  finally:holder.commit();holder.close()
  result=f.result(timeout=10)
 print(json.dumps({'actual_pg_lock_wait':blocked,'transport_count':len(sent),'expired':datetime.now(timezone.utc)>expires,'result_type':type(result).__name__}))
 assert not sent
 assert not s.conn.execute('SELECT 1 FROM event_source_report_calls WHERE run_id=%s',(rid,)).fetchone()


@pytest.mark.parametrize('stage',['journal','credentials'])
def test_durable_journal_pretransport_guard_proves_zero_http(setup,monkeypatch,stage):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];sent=[]
 from sempervigil.llm import router
 monkeypatch.setattr(router,'_http_request',lambda *a,**k:sent.append(a))
 if stage=='credentials':
  def ready(conn):
   monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
   return {'base_url':'https://invalid.example'},{}
  monkeypatch.setattr(reports,'ready_client',ready)
 else:
  original=reports._fresh;count=[]
  def fresh(conn,record):
   count.append(True)
   if len(count)==3:monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
   return original(conn,record)
  monkeypatch.setattr(reports,'_fresh',fresh)
 with pytest.raises(reports.PreTransportFailure):reports.call(s.conn,rid,'writer','fixture',{}, {})
 assert not sent
 row=s.conn.execute('SELECT status,error,reservation FROM event_source_report_calls WHERE run_id=%s',(rid,)).fetchone()
 assert row[0]=='failed' and json.loads(row[1])['proof']['http_attempted'] is False
 assert reports._load(s.conn,rid)['reserved_tokens']==0
 assert row[2]>0 # Spent lifetime call allowance is never reset/replayed.
 with pytest.raises(ValueError):reports.call(s.conn,rid,'writer','fixture',{}, {},complete=lambda p:sent.append(p))
 assert not sent


@pytest.mark.parametrize('change',['expiry','autonomous_disabled','generation_disabled'])
def test_promotion_rechecks_intrinsic_policy(setup,monkeypatch,change):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 queued=publication.submit(s.conn,rid,factory=lambda:s.factory(s.admission))
 assert json.loads(s.conn.execute('SELECT approval_json FROM event_review_approvals WHERE approval_id=%s',(queued['approval_id'],)).fetchone()[0])['automatic_approval']
 if change=='expiry':
  p=copy.deepcopy(s.p);p['expires_at']=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat();monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 elif change=='autonomous_disabled':monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
 else:monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','0')
 from sempervigil.event_approval import run
 with pytest.raises(ValueError):run({'approval_id':queued['approval_id']},factory=lambda:s.factory(s.promotion))
 assert pointer(s)==s.old


@pytest.mark.parametrize('warning',[{'item_id':'NOPE','reason':'Bad locator','source_ids':['S1']},{'item_id':'P01','reason':'Bad locator','source_ids':['NOPE']}])
def test_malformed_locator_warning_cannot_publish(setup,warning):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 review={'ready':True,'issues':[],'locator_warnings':[warning]}
 assert s.execute(rid,review=review)['status']=='held'
 with pytest.raises(ValueError):s.publish(rid)
 assert pointer(s)==s.old


@pytest.mark.parametrize('field,value', [('snapshot_json', '{}'),('budget_tokens',31000),('predecessor','changed'),('request_key','changed'),('event_id','evt_test2'),('generator_version','c'*64),('source_version','changed'),('run_id','changed'),('trigger_kind','generator_upgrade'),('created_at','changed'),('correction_enabled',True)])
def test_existing_role_cannot_rewrite_admission(setup,field,value):
 import psycopg
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 with s.factory(s.admission) as c:
  with pytest.raises(psycopg.errors.CheckViolation):
   c.execute(f'UPDATE event_source_report_runs SET {field}=%s WHERE run_id=%s',(value,rid))
  c.rollback()
 assert reports._load(s.conn,rid)['snapshot']['autonomous_policy']


def test_existing_role_cannot_delete_no_call_admission(setup):
 import psycopg
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 # The existing scheduler connection has DELETE; do not change grants.
 with s.factory(s.admission) as c:
  assert c.execute("SELECT has_table_privilege(current_user,'event_source_report_runs','DELETE')").fetchone()[0]
  with pytest.raises(psycopg.errors.CheckViolation):c.execute('DELETE FROM event_source_report_runs WHERE run_id=%s',(rid,))
  c.rollback()
 assert reports._load(s.conn,rid)['status']=='queued'
 assert not s.conn.execute('SELECT 1 FROM event_source_report_calls WHERE run_id=%s',(rid,)).fetchone()


@pytest.mark.parametrize('proof',['missing','mismatched'])
def test_saved_autonomous_approval_requires_policy_proof(setup,proof):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 queued=publication.submit(s.conn,rid,factory=lambda:s.factory(s.admission))
 approval=json.loads(s.conn.execute('SELECT approval_json FROM event_review_approvals WHERE approval_id=%s',(queued['approval_id'],)).fetchone()[0])
 if proof=='missing':approval.pop('automatic_approval')
 else:approval['automatic_approval']['source_version']='wrong'
 from sempervigil.investigation import _version
 with pytest.raises(ValueError,match='autonomous_approval_required|automatic_approval_changed'):
  publication.run_approval(approval,qualification_id=_version(approval['qualification']),factory=lambda:s.factory(s.promotion))
 assert pointer(s)==s.old


def test_admission_requires_installed_integrity_guards(setup):
 s=setup
 s.conn.execute('DROP TRIGGER event_report_v2_admission_guard ON event_source_report_runs');s.conn.commit()
 with pytest.raises(ValueError,match='admission_integrity_required'):reports.submit(s.conn,'evt_test')
 assert not s.conn.execute("SELECT 1 FROM event_source_report_runs WHERE snapshot_json::jsonb ? 'autonomous_policy'").fetchone()


def test_autonomous_lifetime_ledger_cannot_be_truncated(setup):
 import psycopg
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id']
 with pytest.raises(psycopg.errors.CheckViolation):s.conn.execute('TRUNCATE event_source_report_runs CASCADE')
 s.conn.rollback()
 assert reports._load(s.conn,rid)['status']=='queued'


def test_publication_failure_preserves_unpublished_accepted_content(setup,monkeypatch):
 s=setup;rid=reports.submit(s.conn,'evt_test')['run_id'];assert s.execute(rid)['status']=='accepted'
 def failure(*a,**k):raise ValueError('temporary admission hold')
 monkeypatch.setattr(publication,'submit',failure)
 assert any(x.get('status')=='publication_held' for x in reports.tick(s.conn))
 assert reports._load(s.conn,rid)['status']=='accepted'
 assert pointer(s)==s.old and len(s.calls)==2
