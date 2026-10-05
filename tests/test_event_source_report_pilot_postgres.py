"""Real PostgreSQL policy/queue/roles; synthetic provider only, never HTTP."""
import copy,json
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace
from threading import Barrier
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_event_source_reports_postgres import database,generated,response,execute
from sempervigil import event_source_reports as r,event_source_report_publication as pub,event_source_report_pilot as pilot


@pytest.fixture
def enrolled(database,monkeypatch):
    c,factory,namespace=database
    initial,_,_,_=execute(c,[generated(),{'ready':True,'issues':[]}])
    record=r._load(c,initial['run_id']);q=pub.qualification(record,initial['run_id'])
    bundle=pub.bundle_for(record,initial['run_id'],q,None);revision=r._version(bundle);qid=r._version(q)
    c.execute('INSERT INTO event_quote_qualifications VALUES(%s,%s,%s,%s,NULL)',('evt_test',qid,json.dumps(q),'2026-10-01T00:00:00Z'))
    c.execute('INSERT INTO event_public_revisions VALUES(%s,%s,%s,NULL,%s,%s)',('evt_test',revision,qid,json.dumps(bundle),'2026-10-01T00:00:00Z'))
    c.execute('INSERT INTO event_public_pointers VALUES(%s,%s,%s)',('evt_test',revision,'2026-10-01T00:00:00Z'))
    c.execute("CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT); CREATE TABLE llm_providers(id TEXT PRIMARY KEY,name TEXT,type TEXT,is_enabled INT); CREATE TABLE llm_models(id TEXT PRIMARY KEY,provider_id TEXT,model_name TEXT,is_enabled INT);")
    c.execute("INSERT INTO llm_providers VALUES('p','OpenAI','openai_compatible',1);INSERT INTO llm_models VALUES('luna','p','gpt-5.6-luna',1)")
    c.execute("INSERT INTO events VALUES('evt_second','Second incident','active','confirmed');INSERT INTO event_articles VALUES('evt_second',1)")
    # A separately qualified public predecessor for the second event.
    second_run=r.submit(c,'evt_second',debounce_seconds=0)
    replies=iter([generated(),{'ready':True,'issues':[],'locator_warnings':[]}])
    assert r.run(c,job(second_run),complete=lambda _:response(next(replies)))['status']=='accepted'
    second_record=r._load(c,second_run['run_id'])
    second=pub.bundle_for(second_record,second_run['run_id'],pub.qualification(second_record,second_run['run_id']),None)
    sq=r._version(second['qualification']);sv=r._version(second)
    c.execute('INSERT INTO event_quote_qualifications VALUES(%s,%s,%s,%s,NULL)',('evt_second',sq,json.dumps(second['qualification']),'2026-10-01T00:00:00Z'))
    c.execute('INSERT INTO event_public_revisions VALUES(%s,%s,%s,NULL,%s,%s)',('evt_second',sv,sq,json.dumps(second),'2026-10-01T00:00:00Z'))
    c.execute('INSERT INTO event_public_pointers VALUES(%s,%s,%s)',('evt_second',sv,'2026-10-01T00:00:00Z'))
    # published_baseline's saved run needs the second event's snapshot; use the
    # first event for publication tests, independent article membership for concurrency.
    c.commit()
    now=datetime.now(timezone.utc)
    p={'id':'bounded-fixture','limit':64000,'events':['evt_test','evt_second'],'starts_at':(now-timedelta(minutes=1)).isoformat(),'expires_at':(now+timedelta(hours=1)).isoformat(),'max_runs':2,'max_concurrent':1,'run_tokens':32000}
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_EVENT_IDS','evt_test,evt_second')
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_ID',p['id']);monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_TOKENS','64000')
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_PILOT_POLICY',json.dumps(p))
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL','gpt-5.6-sol')
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_PHASE_CONFIG',json.dumps({'writer':{'reasoning_effort':'none','max_completion_tokens':6000},'review':{'reasoning_effort':'low','max_completion_tokens':2400}}))
    monkeypatch.setattr(r,'configuration',lambda _:({'id':'sol','model_name':'gpt-5.6-sol'},{},'a'*64))
    monkeypatch.setattr('sempervigil.services.ai_service.get_model',lambda c,id:{'model_name':'gpt-5.6-luna'})
    admission='adm_'+namespace;promotion='pro_'+namespace
    for role in (admission,promotion):
        c.execute(f'CREATE ROLE "{role}"');c.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{role}"');c.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{role}"');c.execute(f'GRANT UPDATE ON events,articles,event_articles,event_source_report_runs TO "{role}"')
        c.execute(f'REVOKE SELECT ON llm_models,llm_providers FROM "{role}"')
    c.execute(f'GRANT INSERT ON jobs,event_quote_qualifications,event_review_approvals TO "{admission}"')
    c.execute(f'GRANT INSERT,UPDATE ON event_public_revisions,event_public_pointers TO "{promotion}"');c.commit()
    monkeypatch.setattr(pub,'connection_factory',lambda _:lambda:factory(admission))
    yield c,factory,p,admission,promotion,revision
    c.rollback()
    for role in (admission,promotion):c.execute(f'DROP OWNED BY "{role}"');c.execute(f'DROP ROLE "{role}"')
    c.commit()


def material(c):
    c.execute("UPDATE articles SET content_text=content_text||' Acme confirms investigation continues.' WHERE id=1");c.commit()


def job(receipt):
    return SimpleNamespace(job_type=r.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':receipt['run_id']})


def ready(c,receipt,mode='ready'):
    values=[generated(),{'ready':mode=='ready','issues':[] if mode=='ready' else [{'item_id':'P01','reason':'Fixture unsupported certainty','source_ids':['S1']}],'locator_warnings':[]}]
    calls=[]
    def complete(payload):
        calls.append(payload)
        if mode=='refusal':return {'choices':[{'finish_reason':'stop','message':{'refusal':'fixture'}}],'usage':{'total_tokens':300}}
        if mode=='unknown':raise TimeoutError('Fixture unknown transport')
        return response(values[len(calls)-1])
    outcome=r.run(c,job(receipt),complete=complete)
    return outcome,calls


def test_tick_accept_autoapprove_restricted_promote_and_build_handoff(enrolled,monkeypatch):
    c,f,p,adm,pro,prior=enrolled
    assert all(x['status']=='unchanged' for x in r.tick(c))
    material(c);queued=r.submit(c,'evt_test')
    result,calls=ready(c,queued);assert result['status']=='accepted' and len(calls)==2
    receipts=r.tick(c);approval=next(x for x in receipts if 'approval_id' in x)
    raw=c.execute('SELECT approval_json FROM event_review_approvals WHERE approval_id=%s',(approval['approval_id'],)).fetchone()[0]
    audit=json.loads(raw)['automatic_approval'];assert audit['policy']==pilot.policy()
    assert c.execute('SELECT count(*) FROM event_review_approvals').fetchone()[0]==1
    r.tick(c);assert c.execute('SELECT count(*) FROM event_review_approvals').fetchone()[0]==1
    from sempervigil import worker,event_approval
    import logging
    monkeypatch.setattr(event_approval,'connection_factory',lambda _:lambda:f(pro))
    monkeypatch.setattr(worker,'_log_job_claimed',lambda *args:None)
    monkeypatch.setenv('SV_EVENT_PUBLICATION_ENABLED','1')
    claimed=SimpleNamespace(id=approval['job_id'],job_type='event_promote_reviewed',payload={'approval_id':approval['approval_id']})
    result=worker.run_claimed_job(c,None,claimed,logging.getLogger('fixture'));assert result['status']=='promoted'
    # Normal approval marks dirty for the existing scheduler build admission.
    from sempervigil.storage import get_build_state
    assert get_build_state(c)['dirty']
    assert c.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0]!=prior
    assert pilot.status(c,'evt_test')['cohort']['runs']==2
    material(c)
    with pytest.raises(ValueError,match='run_limit'):r.submit(c,'evt_test')


@pytest.mark.parametrize('mode',['issues','refusal','unknown'])
def test_failure_never_autoapproves_or_retries(enrolled,mode):
    c,_,_,_,_,prior=enrolled;material(c);receipt=r.submit(c,'evt_test')
    result,calls=ready(c,receipt,mode);assert result['status']=='held'
    r.tick(c);r.tick(c)
    assert c.execute('SELECT count(*) FROM event_review_approvals').fetchone()[0]==0
    assert c.execute('SELECT count(*) FROM event_source_report_runs WHERE event_id=\'evt_test\' AND snapshot_json::jsonb ? \'pilot\'').fetchone()[0]==1
    assert c.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0]==prior
    assert next(x for x in pilot.status(c,'evt_test')['runs'] if x['run_id']==receipt['run_id'])['alerts']


@pytest.mark.parametrize('change',['source','report','expiry','budget','predecessor'])
def test_autoapproval_rejects_stale_tampered_expired_overbudget(enrolled,monkeypatch,change):
    c,_,p,_,_,_=enrolled;material(c);receipt=r.submit(c,'evt_test');assert ready(c,receipt)[0]['status']=='accepted'
    if change=='source':material(c)
    if change=='report':c.execute("UPDATE event_source_report_runs SET report_json='{}' WHERE run_id=%s",(receipt['run_id'],));c.commit()
    if change=='budget':c.execute('UPDATE event_source_report_runs SET budget_tokens=31999 WHERE run_id=%s',(receipt['run_id'],));c.commit()
    if change=='predecessor':
        c.execute("INSERT INTO event_public_revisions SELECT event_id,%s,qualification_id,predecessor,bundle_json,recorded_at FROM event_public_revisions WHERE event_id='evt_test' LIMIT 1",('b'*64,))
        c.execute("UPDATE event_public_pointers SET revision_id=%s WHERE event_id='evt_test'",('b'*64,));c.commit()
    if change=='expiry':monkeypatch.setattr(pilot,'active',lambda _:(_ for _ in ()).throw(ValueError('event_source_report_pilot_expired_or_not_started')))
    with pytest.raises(Exception):pub.submit(c,receipt['run_id'],automatic=True)
    assert c.execute('SELECT count(*) FROM event_review_approvals').fetchone()[0]==0


def test_atomic_admission_one_slot_noncohort_out_and_no_generator_backfill(enrolled):
    c,f,_,_,_,_=enrolled;material(c);barrier=Barrier(2)
    def admit(event):
        with f() as other:
            barrier.wait(timeout=5)
            try:return r.submit(other,event)
            except ValueError as exc:other.rollback();return {'reason':str(exc)}
    with ThreadPoolExecutor(max_workers=2) as pool:out=list(pool.map(admit,['evt_test','evt_second']))
    assert sum(x.get('status')=='queued' for x in out)==1
    assert any(x.get('reason')=='event_source_report_pilot_busy' for x in out)
    with pytest.raises(PermissionError):r.submit(c,'evt_unenrolled')
    with pytest.raises(ValueError,match='successor_required'):r.submit(c,'evt_test',trigger='generator_upgrade')


def test_restart_keeps_unknown_reservation_and_never_replays(enrolled):
    c,f,_,_,_,_=enrolled;material(c);receipt=r.submit(c,'evt_test')
    c.execute("UPDATE event_source_report_runs SET status='running',reserved_tokens=100 WHERE run_id=%s",(receipt['run_id'],));c.commit()
    with f() as restarted:
        assert r.run(restarted,job(receipt),complete=lambda _:pytest.fail('replay'))['reused']
        assert r._load(restarted,receipt['run_id'])['reserved_tokens']==100
    with pytest.raises(ValueError,match='repair_disabled'):r.recover_pretransport(c,receipt['run_id'])


@pytest.mark.parametrize('change',['sourceburst','expiry','budget','generation'])
def test_before_transport_holds_and_tick_isolates_failure(enrolled,monkeypatch,change):
    c,_,p,_,_,_=enrolled;material(c);receipt=r.submit(c,'evt_test')
    if change=='sourceburst':material(c)
    if change=='expiry':monkeypatch.setattr(pilot,'active',lambda _:(_ for _ in ()).throw(ValueError('event_source_report_pilot_expired_or_not_started')))
    if change=='budget':c.execute('UPDATE event_source_report_runs SET budget_tokens=1 WHERE run_id=%s',(receipt['run_id'],));c.commit()
    if change=='generation':monkeypatch.setattr(r,'configuration',lambda _:({'model_name':'gpt-5.6-sol'},{},'b'*64))
    outcome=r.run(c,job(receipt),complete=lambda _:pytest.fail('Pretransport hold reached provider'))
    assert outcome['status']=='held'
    assert c.execute('SELECT count(*) FROM event_source_report_calls WHERE run_id=%s',(receipt['run_id'],)).fetchone()[0]==0
    assert isinstance(r.tick(c),list)
    assert c.execute('SELECT 1').fetchone()[0]==1


def test_promotion_binds_generation_identity_without_provider_access(enrolled):
    c,f,_,adm,pro,_=enrolled;material(c);receipt=r.submit(c,'evt_test');assert ready(c,receipt)[0]['status']=='accepted'
    approval=pub.submit(c,receipt['run_id'],automatic=True)
    c.execute('UPDATE event_source_report_runs SET generator_version=%s WHERE run_id=%s',('b'*64,receipt['run_id']));c.commit()
    from sempervigil.event_approval import run
    with pytest.raises(ValueError,match='automatic_approval_changed'):
        run({'approval_id':approval['approval_id']},factory=lambda:f(pro))
