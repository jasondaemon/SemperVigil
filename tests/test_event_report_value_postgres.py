"""Per-update economics retain failed cost and distinguish promotion from activation."""
import json
from types import SimpleNamespace
import pytest
from test_event_report_v2_policy_postgres import setup
from test_event_source_reports_postgres import database
from sempervigil import event_source_reports_v2 as reports,event_report_value as value
from sempervigil.event_report_v2_policy import policy
from sempervigil.storage import get_setting

@pytest.fixture
def measured(setup):
    s=setup;s.conn.execute('CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)');s.conn.commit();return s

def queued(s):return next(r['run_id'] for r in reports.tick(s.conn) if r['status']=='queued')

def receipt(s,rid):return get_setting(s.conn,'event.report_v2.value.'+rid,{})

def test_zero_run_cohort_waits_without_model_calls(measured):
    s=measured;a=value.record_cohort(s.conn,policy());assert a['runs']==0 and a['known_actual_tokens']==0 and a['accepted_activated_updates']==0
    assert a['status']=='waiting_for_genuine_evidence_change' and a['estimated_usd'] is None and not s.calls

def test_accepted_promoted_and_activated_are_distinct_durable_states(measured,tmp_path):
    s=measured;rid=queued(s);s.execute(rid);value.record_cohort(s.conn,policy());m=receipt(s,rid)
    assert m['known_actual_tokens']==600 and m['promoted_revisions']==[] and m['activation'] is None and m['rates'] is None
    _,promoted=s.publish(rid,automatic=True);value.record_cohort(s.conn,policy());assert receipt(s,rid)['accepted_activated_updates']==0
    release=tmp_path/'synthetic-guarded-release';release.mkdir();rev=promoted['revision_id']
    (release/'.sempervigil-events-activation.json').write_text(json.dumps({'workflow':'event-release-authorization-v2','revisions':{'evt_test':rev},'withdrawn':{},'pages':{'evt_test':'evt_test'},'index_sha256':'0'*64,'fragments':{'evt_test':'a'*64}}))
    # Match the real manifest filename without weakening its parser.
    from sempervigil.event_activation import MANIFEST
    (release/'.sempervigil-events-activation.json').rename(release/MANIFEST)
    a=value.record_cohort(s.conn,policy(),release=release);m=receipt(s,rid)
    assert a['accepted_activated_updates']==1 and m['activation']['revision']==rev and m['known_actual_tokens']==600
    old=m['activation'];value.record_cohort(s.conn,policy());assert receipt(s,rid)['activation']==old

def test_unknown_transport_keeps_reservation_separate(measured):
    s=measured;rid=queued(s);s.execute(rid,transport=ValueError('synthetic unknown transport'));value.record_cohort(s.conn,policy());m=receipt(s,rid)
    assert m['status']=='held' and m['known_actual_tokens']==0 and m['unknown_or_outstanding_reserved_tokens']>0
    assert m['lifetime_admission_tokens']==32000 and m['estimated_usd'] is None and not m['activation']

def test_empty_length_output_cost_is_not_omitted(measured):
    s=measured;rid=queued(s)
    def complete(q):
        s.calls.append(q);return {'id':'synthetic-length','choices':[{'finish_reason':'length','message':{'content':''}}],'usage':{'prompt_tokens':100,'completion_tokens':3200,'total_tokens':3300}}
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':rid})
    assert reports.run(s.conn,job,complete=complete)['status']=='held';value.record_cohort(s.conn,policy());m=receipt(s,rid)
    assert m['known_actual_tokens']==3300 and m['paragraphs']==0 and not m['model_review_ready'] and m['failed_call_cost_included']
    assert m['unknown_or_outstanding_reserved_tokens']==0 and m['accepted_activated_updates']==0
    reports.tick(s.conn);assert len(s.calls)==1

def test_failed_second_call_does_not_count_retained_draft_as_value(measured):
    from test_event_source_reports_postgres import response
    s=measured;rid=queued(s)
    def complete(q):
        s.calls.append(q)
        if len(s.calls)==1:return response(s.value)
        return {'id':'synthetic-empty-review','choices':[{'finish_reason':'length','message':{'content':''}}],'usage':{'prompt_tokens':9,'completion_tokens':1,'total_tokens':10}}
    job=SimpleNamespace(job_type=reports.JOB_TYPE,queue_name='openai',status='running',max_attempts=1,payload={'run_id':rid})
    assert reports.run(s.conn,job,complete=complete)['status']=='held';value.record_cohort(s.conn,policy());m=receipt(s,rid)
    assert m['words']>0 and m['final_accepted_words']==0 and m['content_state']=='unapproved_draft_or_absent'
    assert m['known_actual_tokens']==310 and m['accepted_activated_updates']==0 and len(m['calls'])==2
