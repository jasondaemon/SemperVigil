"""Actual persistence, budget and separated publication roles; no hosted requests."""
import copy
import json
import os
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest

from sempervigil import event_source_reports as reports
from sempervigil import event_source_report_publication as publication
from sempervigil import event_report_contract as contract
from sempervigil.event_publication_store import SCHEMA as PUBLIC_SCHEMA
from sempervigil.event_approval import SCHEMA as APPROVAL_SCHEMA


@pytest.fixture
def database(monkeypatch):
    url=os.environ["SV_TEST_DB_URL"]
    namespace="report_test_"+uuid4().hex[:12]
    admin=psycopg.connect(url)
    admin.execute(f'CREATE SCHEMA "{namespace}"')
    admin.execute(f'SET search_path TO "{namespace}"')
    admin.execute("""CREATE TABLE events(id TEXT PRIMARY KEY,title TEXT,visibility TEXT,lifecycle TEXT);
      CREATE TABLE articles(id INTEGER PRIMARY KEY,title TEXT,original_url TEXT,content_text TEXT,
        published_at TEXT,ingested_at TEXT,meta_json TEXT);
      CREATE TABLE event_articles(event_id TEXT REFERENCES events(id),article_id INTEGER REFERENCES articles(id),
        PRIMARY KEY(event_id,article_id));
      CREATE TABLE jobs(id TEXT PRIMARY KEY,job_type TEXT,status TEXT,priority INTEGER,payload_json TEXT,result_json TEXT,
        requested_at TEXT,started_at TEXT,finished_at TEXT,locked_by TEXT,locked_at TEXT,error TEXT,queue_name TEXT,
        attempt_count INTEGER,max_attempts INTEGER,available_at TEXT,heartbeat_at TEXT,lease_expires_at TEXT,
        parent_job_id TEXT,dedupe_key TEXT);
      INSERT INTO events VALUES('evt_test','Acme incident','active','confirmed');
      INSERT INTO articles VALUES(1,'Acme incident','https://example.org/incident',
       'Acme said certain patient records may be affected. The company rotated credentials.',
       '2026-10-01','2026-10-01', '{}');
      INSERT INTO event_articles VALUES('evt_test',1);""")
    admin.execute(reports.SCHEMA);admin.execute(PUBLIC_SCHEMA);admin.execute(APPROVAL_SCHEMA)
    admin.commit()
    monkeypatch.setenv("SV_EVENT_SOURCE_REPORT_ENABLED","1")
    monkeypatch.setenv("SV_EVENT_HUMAN_APPROVAL_ENABLED","1")
    monkeypatch.setattr(reports,"configuration",lambda conn:({"id":"m","model_name":"test-model"},{"id":"p"},"a"*64))
    def factory(role=None):
        c=psycopg.connect(url)
        c.execute(f'SET search_path TO "{namespace}"')
        if role:c.execute(f'SET ROLE "{role}"')
        return c
    yield admin,factory,namespace
    admin.rollback();admin.close()
    with psycopg.connect(url) as cleanup:cleanup.execute(f'DROP SCHEMA "{namespace}" CASCADE')


def generated(kind="breach"):
    return {"title":"Acme incident","kind":kind,"items":[
        {"id":"P01","section":"overview","text":"Acme reported possible patient-record exposure.",
         "claim_type":"finding","confidence":None,"rationale":"","date_label":"","date_sort":None,
         "citations":[{"source_id":"S1","quote":"certain patient records may be affected"}]},
        {"id":"P02","section":"response_recovery","text":"The company rotated credentials.",
         "claim_type":"finding","confidence":None,"rationale":"","date_label":"","date_sort":None,
         "citations":[{"source_id":"S1","quote":"The company rotated credentials."}]}]}


def test_audited_final_two_call_allowance(database,monkeypatch,tmp_path):
    conn,_,_=database
    issues={'ready':False,'issues':[{'item_id':'P02','reason':'Response needs qualified wording.','source_ids':['S1']}]}
    submitted,result,_,_=execute(conn,[generated(),issues])
    rid=submitted['run_id'];record=reports._load(conn,rid)
    reports.grant_correction_allowance(conn,rid,22000,authority='fixture-explicit-final-two')
    reports.grant_correction_allowance(conn,rid,22000,authority='fixture-explicit-final-two')
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute('UPDATE event_source_report_allowances SET tokens=1 WHERE run_id=%s',(rid,))
    conn.rollback()
    monkeypatch.setattr(reports,'ready_client',lambda _:None)
    from sempervigil.event_source_report_executor import JournaledExecutor
    replies=[{'items':[generated()['items'][1]]},{'ready':True,'issues':[],'locator_warnings':[]}]
    calls=[]
    def complete(payload):calls.append(payload);return response(replies[len(calls)-1])
    executor=JournaledExecutor(conn,rid,tmp_path,ceiling=22000,phases=('correction','verification'),complete=complete)
    revised,verified=reports.correct_and_verify(conn,rid,contract.context(record['snapshot']),record['report'],record['review'],complete=executor,compact=True)
    assert revised==generated() and verified['ready'] and len(calls)==2
    assert [v[0] for v in conn.execute('SELECT phase FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(rid,)).fetchall()]==['writer','review','correction','verification']
    conn.execute("UPDATE event_source_report_runs SET status='accepted',review_json=%s WHERE run_id=%s",(contract.encode(verified),rid));conn.commit()
    assert publication.current_material(conn,rid)['report']==revised
    tampered=copy.deepcopy(revised);tampered['items'][0]['text']='Unchecked replacement prose.'
    conn.execute('UPDATE event_source_report_runs SET report_json=%s WHERE run_id=%s',(contract.encode(tampered),rid));conn.commit()
    with pytest.raises(ValueError,match='response_integrity'):publication.current_material(conn,rid)
    with pytest.raises(ValueError,match='not_eligible'):
        reports.grant_correction_allowance(conn,rid,22000,authority='fixture-explicit-final-two')
    assert conn.execute('SELECT count(*) FROM event_source_report_allowances').fetchone()[0]==1


def response(value):
    if 'ready' in value and 'locator_warnings' not in value:
        value={**value,'locator_warnings':[]}
    return {"choices":[{"finish_reason":"stop","message":{"content":json.dumps(value)}}],
            "usage":{"prompt_tokens":200,"completion_tokens":100,"total_tokens":300,
                     "completion_tokens_details":{"reasoning_tokens":25}}}


def test_manual_quality_correction_uses_immutable_scope_and_phase_options(database,monkeypatch,tmp_path):
    conn,_,_=database
    submitted,_,_,_=execute(conn,[generated(),{'ready':True,'issues':[]}]);rid=submitted['run_id']
    conn.execute("UPDATE event_source_report_runs SET status='held',reason='manual_quality_fixture' WHERE run_id=%s",(rid,));conn.commit()
    manual=[{'item_id':'P02','reason':'Preserve the source qualification in this response item.','source_ids':['S1']}]
    options={'correction':{'reasoning_effort':'none','max_completion_tokens':1000},'verification':{'reasoning_effort':'low','max_completion_tokens':2400}}
    reports.grant_correction_allowance(conn,rid,24000,authority='explicit-final-pair',manual_issues=manual,phase_options=options)
    before=reports._load(conn,rid);monkeypatch.setattr(reports,'ready_client',lambda _:None)
    from sempervigil.event_source_report_executor import JournaledExecutor
    seen=[];values=[{'items':[generated()['items'][1]]},{'ready':True,'issues':[],'locator_warnings':[]}]
    def fixture(payload):seen.append(payload);return response(values[len(seen)-1])
    executor=JournaledExecutor(conn,rid,tmp_path,ceiling=24000,phases=('correction','verification'),complete=fixture)
    revised,verified=reports.correct_and_verify(conn,rid,contract.context(before['snapshot']),before['report'],{'ready':False,'issues':manual,'locator_warnings':[]},complete=executor,compact=True)
    assert [(q['reasoning_effort'],q['max_completion_tokens']) for q in seen]==[('none',1000),('low',2400)]
    conn.execute("UPDATE event_source_report_runs SET status='accepted',review_json=%s WHERE run_id=%s",(contract.encode(verified),rid));conn.commit()
    assert publication.current_material(conn,rid)['report']==revised
    raw_review=conn.execute("SELECT response_json FROM event_source_report_calls WHERE run_id=%s AND phase='review'",(rid,)).fetchone()[0]
    assert json.loads(json.loads(raw_review)['choices'][0]['message']['content'])['ready']


def test_legacy_span_representation_keeps_immutable_derivative(database):
    conn,_,_=database
    original=generated()
    original['items'].append({**original['items'][1],'id':'P03','section':'what_changed','text':'Newly added metadata.'})
    submitted,_,_,_=execute(conn,[original,{'ready':True,'issues':[]}])
    rid=submitted['run_id'];record=reports._load(conn,rid)
    snap=contract.update_context(record['snapshot'],'generator_upgrade',record['snapshot'])
    old_spans={k:[{key:value for key,value in s.items() if key!='passage_anchor'} for s in vs] for k,vs in record['spans'].items()}
    conn.execute("UPDATE event_source_report_runs SET snapshot_json=%s,spans_json=%s,status='held',reason='manual_quality_revision_metadata' WHERE run_id=%s",(contract.encode(snap),contract.encode(old_spans),rid))
    derivative=contract.publication_projection(record['report'],record['review'],contract.context(snap))
    derivative['spans']={k:[{key:value for key,value in s.items() if key!='passage_anchor'} for s in vs] for k,vs in derivative['spans'].items()}
    conn.execute('INSERT INTO event_source_report_derivatives VALUES(%s,%s,%s)',(rid,contract.encode(derivative),'fixture'));conn.commit()
    assert publication.current_material(conn,rid)['spans']==derivative['spans']
    altered=copy.deepcopy(derivative);altered['spans']['P01'][0]['start']+=1
    with pytest.raises(ValueError,match='derivative_integrity'):
        publication.current_material(conn,rid,derivative=altered)


def execute(conn,values,*,allow_correction=False):
    submitted=reports.submit(conn,"evt_test",debounce_seconds=0,allow_correction=allow_correction)
    calls=[]
    def complete(payload):calls.append(payload);return response(values[len(calls)-1])
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name="openai",status="running",
                        max_attempts=1,payload={"run_id":submitted["run_id"]})
    result=reports.run(conn,job,complete=complete)
    return submitted,result,calls,job


@pytest.mark.parametrize("kind",["breach","compromise","law_enforcement","vulnerability"])
def test_two_call_path_persists_responses_spans_and_replay(database,kind):
    conn,_,_=database
    submitted,result,calls,job=execute(conn,[generated(kind),{"ready":True,"issues":[]}])
    assert result["status"]=="accepted" and len(calls)==2
    record=reports._load(conn,submitted["run_id"])
    assert record["charged_tokens"]==600 and record["reserved_tokens"]==0
    rows=conn.execute("SELECT response_json,usage_json FROM event_source_report_calls WHERE run_id=%s",(submitted["run_id"],)).fetchall()
    assert len(rows)==2 and all(json.loads(u)["completion_tokens_details"]["reasoning_tokens"]==25 for _,u in rows)
    assert reports.run(conn,job,complete=lambda _:pytest.fail("replayed paid call"))["reused"]
    assert reports.submit(conn,"evt_test",debounce_seconds=0)["status"]=="unchanged"
    assert contract.validate(record["report"],record["snapshot"])==record["spans"]


def test_four_call_correction_is_targeted_and_terminal(database):
    conn,_,_=database
    first=generated();corrected=copy.deepcopy(first)
    corrected["items"][0]["text"]="Acme said certain patient records may be affected."
    issue={"ready":False,"issues":[{"item_id":"P01","reason":"Preserve the specific uncertainty wording.","source_ids":["S1"]}]}
    submitted,result,calls,job=execute(conn,[first,issue,corrected,{"ready":True,"issues":[]}],allow_correction=True)
    assert result["status"]=="accepted" and len(calls)==4
    assert reports._load(conn,submitted["run_id"])["charged_tokens"]==1200
    assert reports.run(conn,job,complete=lambda _:pytest.fail("fifth call"))["reused"]


@pytest.mark.parametrize("failure",["invalid_confidence","quote_in_previous_only","source_changed","transport","budget"])
def test_holds_preserve_old_pointer_and_responses(database,failure):
    conn,_,_=database
    value=generated()
    if failure=="invalid_confidence":value["items"][0]["confidence"]="high"
    if failure=="quote_in_previous_only":value["items"][0]["citations"][0]["quote"]="Unsupported prior report statement."
    submitted=reports.submit(conn,"evt_test",budget_tokens=1 if failure=="budget" else 24000,debounce_seconds=0)
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name="openai",status="running",max_attempts=1,
                        payload={"run_id":submitted["run_id"]})
    calls=[]
    def complete(payload):
        calls.append(payload)
        if failure=="transport":raise TimeoutError("fixture transport failure")
        if failure=="source_changed":
            conn.execute("UPDATE articles SET content_text=content_text||' Correction.' WHERE id=1");conn.commit()
        return response(value)
    result=reports.run(conn,job,complete=complete)
    assert result["status"]=="held"
    assert conn.execute("SELECT count(*) FROM event_public_pointers").fetchone()[0]==0
    assert len(calls)==(0 if failure=="budget" else 1)
    assert reports.submit(conn,"evt_test",debounce_seconds=0)["reused"] if failure!="source_changed" else True
    assert reports.run(conn,job,complete=lambda _:pytest.fail("retry"))["reused"]


@pytest.mark.parametrize("derivative",[False,True])
def test_job_to_review_to_restricted_atomic_publication(database,derivative):
    conn,factory,namespace=database
    original=generated()
    if derivative:
        original["items"].append({**original["items"][1],"id":"P03","section":"what_changed","text":"New evidence shows credential rotation."})
    submitted,result,_,_=execute(conn,[original,{"ready":True,"issues":[]}])
    if derivative:
        record=reports._load(conn,submitted["run_id"])
        snap=contract.update_context(record["snapshot"],"generator_upgrade",record["snapshot"])
        conn.execute("UPDATE event_source_report_runs SET snapshot_json=%s,status='held',reason='manual_quality_revision_metadata' WHERE run_id=%s",(contract.encode(snap),submitted["run_id"]));conn.commit()
        receipt=publication.qualify_generator_refresh(conn,submitted["run_id"])
        assert receipt["removed_item_ids"] == ["P03"]
        assert reports._load(conn,submitted["run_id"])["status"]=="held"
        assert reports._load(conn,submitted["run_id"])["report"]==original
    admission="adm_"+namespace;promotion="pro_"+namespace
    conn.execute(f'CREATE ROLE "{admission}"');conn.execute(f'CREATE ROLE "{promotion}"')
    for role in (admission,promotion):
        conn.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{role}"')
        conn.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{role}"')
        conn.execute(f'GRANT UPDATE ON events,articles,event_articles,event_source_report_runs TO "{role}"')
    conn.execute(f'GRANT INSERT ON jobs,event_quote_qualifications,event_review_approvals TO "{admission}"')
    conn.execute(f'GRANT INSERT,UPDATE ON event_public_revisions,event_public_pointers TO "{promotion}"')
    conn.commit()
    try:
        queued=publication.submit(conn,submitted["run_id"],factory=lambda:factory(admission))
        from sempervigil.event_approval import run
        result=run({"approval_id":queued["approval_id"]},factory=lambda:factory(promotion))
        assert result["status"]=="promoted"
        raw=conn.execute("SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s",(result["revision_id"],)).fetchone()[0]
        from sempervigil.event_render import render
        metadata,html=render(json.loads(raw),event_id="evt_test",expected_revision=result["revision_id"])
        assert "possible patient-record" in html and 'href="https://example.org/incident"' in html
        assert metadata["event_report_format"]==contract.PUBLIC_WORKFLOW
        if derivative:
            assert contract.GENERATOR_NOTICE in html and "New evidence shows" not in html
            assert '"raw_report_version"' in raw and '"removed_item_ids":["P03"]' in raw
            bundle=json.loads(raw);tampered=copy.deepcopy(bundle)
            tampered["revision_provenance"]["notice"]="New event developments."
            with pytest.raises(ValueError,match="revision_notice_invalid"):
                publication.validate_bundle(tampered,event_id="evt_test")
        assert "fact_ids" not in raw and '"text":"Acme said certain' not in raw
        baseline,generation=reports.published_baseline(conn,"evt_test",reports.snapshot(conn,"evt_test"))
        assert baseline==reports._load(conn,submitted["run_id"])["snapshot"]
        assert generation==reports._load(conn,submitted["run_id"])["generator_version"]
        with pytest.raises(PermissionError):publication.submit(conn,submitted["run_id"],factory=lambda:factory(promotion))
        with pytest.raises(PermissionError):run({"approval_id":queued["approval_id"]},factory=lambda:factory(admission))
    finally:
        conn.rollback()
        for role in (admission,promotion):
            conn.execute(f'DROP OWNED BY "{role}"');conn.execute(f'DROP ROLE "{role}"')
        conn.commit()


def test_completed_model_artifacts_are_immutable(database):
    conn,_,_=database
    submitted,_,_,_=execute(conn,[generated(),{"ready":True,"issues":[]}])
    with pytest.raises(psycopg.errors.CheckViolation,match="immutable"):
        conn.execute("UPDATE event_source_report_calls SET response_json='{}' WHERE run_id=%s",(submitted["run_id"],))
    conn.rollback()


def test_cohort_reservations_survive_unknown_transport_and_block_other_runs(database,monkeypatch):
    conn,_,_=database
    monkeypatch.setenv("SV_EVENT_SOURCE_REPORT_COHORT_ID","small-test")
    monkeypatch.setenv("SV_EVENT_SOURCE_REPORT_COHORT_TOKENS","6500")
    submitted=reports.submit(conn,"evt_test",debounce_seconds=0)
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name="openai",status="running",max_attempts=1,payload={"run_id":submitted["run_id"]})
    def uncertain(_):raise TimeoutError("Unknown usage stays reserved")
    assert reports.run(conn,job,complete=uncertain)["status"]=="held"
    held=reports._load(conn,submitted["run_id"])
    assert held["reserved_tokens"]>0
    conn.execute("UPDATE articles SET content_text=content_text||' Additional material facts.' WHERE id=1");conn.commit()
    submitted=reports.submit(conn,"evt_test",debounce_seconds=0)
    job.payload={"run_id":submitted["run_id"]}
    result=reports.run(conn,job,complete=lambda _:pytest.fail("Cohort cap must precede HTTP"))
    assert result["status"]=="held" and result["reason"]=="event_source_report_cohort_exhausted"


def test_debounce_source_burst_holds_before_any_http_and_readmits_latest(database):
    conn,_,_=database
    admitted=reports.submit(conn,"evt_test",debounce_seconds=300)
    conn.execute("UPDATE articles SET content_text=content_text||' Acme confirms a new affected population.' WHERE id=1");conn.commit()
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name="openai",status="running",max_attempts=1,payload={"run_id":admitted["run_id"]})
    result=reports.run(conn,job,complete=lambda _:pytest.fail("Stale debounce spent tokens"))
    assert result["reason"]=="event_source_report_sources_changed"
    assert reports._load(conn,admitted["run_id"])["reserved_tokens"]==0
    successor=reports.submit(conn,"evt_test",debounce_seconds=300)
    assert successor["run_id"]!=admitted["run_id"]
    assert "new affected population" in reports._load(conn,successor["run_id"])["snapshot"]["sources"][0]["text"]


def test_legacy_public_baseline_requires_exact_original_evidence_version(database,monkeypatch):
    conn,_,_=database
    from sempervigil.article_evidence import source_for
    from sempervigil import storage
    from sempervigil import event_render
    # Isolate legacy source-version reconstruction; real workflow projections are
    # independently exercised by the retained publication/render contract tests.
    monkeypatch.setattr(event_render,"resolve",lambda b,**kw: ({},{}))
    article={"id":1,"title":"Acme incident","content_text":reports.snapshot(conn,"evt_test")["sources"][0]["text"]}
    monkeypatch.setattr(storage,"get_article_by_id",lambda c,aid:article)
    conn.execute("CREATE TABLE article_evidence_revisions(revision_id TEXT,article_id INTEGER,source_version TEXT)")
    conn.execute("INSERT INTO article_evidence_revisions VALUES('aer_original',1,%s)",(source_for(article)["source_version"],))
    bundle={"workflow":"legacy","sources":[{"article_id":1,"evidence_revision_id":"aer_original"}]}
    revision=reports._version(bundle)
    qid=reports._version({})
    conn.execute("INSERT INTO event_quote_qualifications VALUES('evt_test',%s,'{}','now',NULL)",(qid,))
    conn.execute("INSERT INTO event_public_revisions VALUES('evt_test',%s,%s,NULL,%s,'now')",(revision,qid,contract.encode(bundle)))
    conn.execute("INSERT INTO event_public_pointers VALUES('evt_test',%s,'now')",(revision,));conn.commit()
    baseline,_=reports.published_baseline(conn,"evt_test",reports.snapshot(conn,"evt_test"))
    assert baseline["sources"][0]["article_id"]==1
    article["content_text"]+=" A subsequent correction."
    assert reports.published_baseline(conn,"evt_test",reports.snapshot(conn,"evt_test"))==(None,None)


def pretransport_parent(conn,error):
    admitted=reports.submit(conn,'evt_test',debounce_seconds=0)
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':admitted['run_id']})
    def failure(p):raise error
    assert reports.run(conn,job,complete=failure)['status']=='held'
    conn.execute("UPDATE jobs SET status='failed',error=%s WHERE id=%s",(str(error),admitted['job_id']));conn.commit()
    return admitted


def test_pretransport_recovery_is_parent_linked_idempotent_and_audited(database,monkeypatch):
    conn,_,_=database
    monkeypatch.setattr(reports,'ready_client',lambda c:({},{}))
    parent=pretransport_parent(conn,reports.PreTransportFailure())
    original=conn.execute('SELECT * FROM event_source_report_calls WHERE run_id=%s',(parent['run_id'],)).fetchone()
    reservation=reports._load(conn,parent['run_id'])['reserved_tokens'];assert reservation>0
    child=reports.recover_pretransport(conn,parent['run_id'],debounce_seconds=0)
    assert child['released_tokens']==reservation
    assert reports._load(conn,parent['run_id'])['status']=='held' and reports._load(conn,parent['run_id'])['reserved_tokens']==0
    assert conn.execute('SELECT * FROM event_source_report_calls WHERE run_id=%s',(parent['run_id'],)).fetchone()==original
    assert conn.execute('SELECT parent_job_id FROM jobs WHERE id=%s',(child['job_id'],)).fetchone()[0]==parent['job_id']
    assert reports.recover_pretransport(conn,parent['run_id'])['reused']
    journal=[]
    def fixture(p):journal.append(p);return response(generated() if len(journal)==1 else {'ready':True,'issues':[]})
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':child['run_id']})
    assert reports.run(conn,job,complete=fixture)['status']=='accepted' and len(journal)==2
    with pytest.raises(ValueError,match='already_used'):reports.recover_pretransport(conn,child['run_id'])
    conn.rollback()
    with pytest.raises(psycopg.errors.CheckViolation,match='immutable'):
        conn.execute("UPDATE event_source_report_recoveries SET released_tokens=1")
    conn.rollback()


@pytest.mark.parametrize('error',[TimeoutError('Uncertain HTTP'),ValueError('Unclassified local or remote failure')])
def test_recovery_refuses_unproven_transport_without_releasing_reservation(database,monkeypatch,error):
    conn,_,_=database
    monkeypatch.setattr(reports,'ready_client',lambda c:pytest.fail('Unknown outcome reached readiness'))
    parent=pretransport_parent(conn,error);reserved=reports._load(conn,parent['run_id'])['reserved_tokens']
    with pytest.raises(ValueError,match='proof_required'):reports.recover_pretransport(conn,parent['run_id'])
    conn.rollback()
    assert reports._load(conn,parent['run_id'])['reserved_tokens']==reserved
    assert conn.execute('SELECT count(*) FROM event_source_report_recoveries').fetchone()[0]==0


def test_legacy_proof_and_recovered_executor_journal_end_to_end(database,monkeypatch,tmp_path,capsys):
    conn,_,_=database
    from sempervigil.event_source_report_executor import JournaledExecutor
    monkeypatch.setattr(reports,'ready_client',lambda c:({},{}))
    message='Master key is not set. Set SEMPERVIGIL_MASTER_KEY (preferred) or legacy SEMPERIVGIL_MASTER_KEY.'
    parent=pretransport_parent(conn,ValueError(message))
    request=json.loads(conn.execute('SELECT request_json FROM event_source_report_calls WHERE run_id=%s',(parent['run_id'],)).fetchone()[0])
    proof={'workflow':'event-source-report-pretransport-proof-v1','kind':'legacy_missing_master_key',
     'http_attempted':False,'failure_stage':'provider_credentials','request_version':reports._version(request),
     'master_key_present':False,'failure_reproduced':True,'reason_code':'missing_master_key',
     'audit_reason':'Offline fixture verifies the authorized legacy pre-transport reconciliation contract.',
     **{k:'a'*64 for k in ('complete_code_version','loader_code_version','executor_instance_version','journal_version')}}
    bad={**proof,'http_attempted':True}
    with pytest.raises(ValueError,match='proof_invalid'):reports.recover_pretransport(conn,parent['run_id'],legacy_proof=bad)
    conn.rollback()
    admitted=reports.recover_pretransport(conn,parent['run_id'],legacy_proof=proof,debounce_seconds=0)
    values=iter([generated(),{'ready':True,'issues':[]}])
    def fixture(p):print('Fixture router stdout cannot corrupt journal');return response(next(values))
    executor=JournaledExecutor(conn,admitted['run_id'],tmp_path,complete=fixture)
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':admitted['run_id']})
    assert reports.run(conn,job,complete=executor)['status']=='accepted'
    assert len(executor.reservations)==2 and sum(executor.reservations)<=24000
    receipts=[json.loads(p.read_text()) for p in (tmp_path/admitted['run_id']).glob('*.json')]
    saved=[json.loads(row[0]) for row in conn.execute('SELECT response_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(admitted['run_id'],)).fetchall()]
    assert all(r['status']=='completed' and r['response'] in saved for r in receipts)
    assert len(receipts)==2 and 'Fixture router stdout' in capsys.readouterr().out
