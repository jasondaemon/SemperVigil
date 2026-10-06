"""Durable per-update usage/value receipts; unknown pricing is never zero cost."""
import json
from datetime import datetime
from pathlib import Path
from .storage import get_setting,set_setting
from .utils import utc_now_iso

WORKFLOW='event-report-update-value-v1'


def record_cohort(conn,policy,*,release=None):
    rows=conn.execute("SELECT run_id,event_id,status,reason,source_version,predecessor,snapshot_json,report_json,review_json,created_at,budget_tokens,reserved_tokens FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'=%s ORDER BY created_at,run_id",(policy['id'],)).fetchall()
    activation=None
    if release is not None:
        from .event_activation import read_manifest
        path=Path(release).resolve(strict=True);activation=(path.name,read_manifest(path))
    receipts=[]
    for rid,eid,status,reason,source,predecessor,sraw,raw,reviewraw,created,budget,reserved in rows:
        snap=json.loads(sraw);report=json.loads(raw) if raw else None;review=json.loads(reviewraw) if reviewraw else None
        calls=[]
        for ordinal,phase,state,qraw,rraw,uraw,start in conn.execute('SELECT ordinal,phase,status,request_json,response_json,usage_json,created_at FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(rid,)):
            q=json.loads(qraw);response=json.loads(rraw) if rraw else {};usage=json.loads(uraw) if uraw else None
            known=usage if isinstance(usage,dict) and type(usage.get('total_tokens')) is int and usage['total_tokens']>=0 else None
            choice=(response.get('choices') or [{}])[0]
            calls.append({'ordinal':ordinal,'phase':phase,'status':state,'created_at':str(start),'response_id':response.get('id'),'model':q.get('model'),'reasoning_effort':q.get('reasoning_effort'),'completion_cap':q.get('max_completion_tokens'),'finish_reason':choice.get('finish_reason'),'visible_content_chars':len(choice.get('message',{}).get('content') or ''),'usage':known,'transport_elapsed_ms':response.get('transport_elapsed_ms')})
        revisions=conn.execute("SELECT revision_id FROM event_public_revisions WHERE event_id=%s AND bundle_json::jsonb->>'run_id'=%s ORDER BY recorded_at",(eid,rid)).fetchall()
        old=get_setting(conn,'event.report_v2.value.'+rid,{})
        active=old.get('activation')
        if activation and activation[1]['revisions'].get(eid) in {v[0] for v in revisions}:
            active=active or {'release':activation[0],'revision':activation[1]['revisions'][eid],'fragment_sha256':activation[1]['fragments'][eid],'observed_at':utc_now_iso(),'evidence':'guarded builder success and current release authorization manifest'}
        jobs=conn.execute("SELECT started_at,finished_at FROM jobs WHERE payload_json::jsonb->>'run_id'=%s ORDER BY requested_at DESC LIMIT 1",(rid,)).fetchone();duration=None
        if jobs and all(jobs):
            duration=(datetime.fromisoformat(str(jobs[1]).replace('Z','+00:00'))-datetime.fromisoformat(str(jobs[0]).replace('Z','+00:00'))).total_seconds()
        known=sum(c['usage']['total_tokens'] for c in calls if c['usage'])
        from .event_source_reports_v2 import safe_reason
        receipt={'workflow':WORKFLOW,'run_id':rid,'event_id':eid,'cohort_id':policy['id'],'cost_scope':'ongoing bounded successor cohort; excludes retained imports and R&D','observed_at':utc_now_iso(),'created_at':str(created),'status':status,'reason':safe_reason(reason) if reason else None,'source_version':source,'predecessor':predecessor,'evidence_delta':snap.get('evidence_delta'),'calls':calls,'known_actual_tokens':known,'unknown_or_outstanding_reserved_tokens':reserved,'lifetime_admission_tokens':budget,'failed_call_cost_included':True,'rates':None,'estimated_usd':None,'invoice_usd':None,'pricing_status':'unverified; unavailable, not zero','job_duration_seconds':duration,'paragraphs':len(report['items']) if report else 0,'words':sum(len(i['text'].split()) for i in report['items']) if report else 0,'citations':sum(len(i['citations']) for i in report['items']) if report else 0,'source_count':len(snap.get('sources',[])),'content_state':'final_accepted' if status=='accepted' and review and review.get('ready') else 'unapproved_draft_or_absent','final_accepted_words':sum(len(i['text'].split()) for i in report['items']) if report and status=='accepted' and review and review.get('ready') else 0,'model_review_ready':bool(review and review.get('ready')),'review_independence':'same-model separate source-checking passes' if snap.get('final_editor') else 'see pinned requests','promoted_revisions':[v[0] for v in revisions],'activation':active,'accepted_activated_updates':1 if active else 0}
        set_setting(conn,'event.report_v2.value.'+rid,receipt);receipts.append(receipt)
    aggregate={'workflow':WORKFLOW,'cohort_id':policy['id'],'scope':'ongoing cohort only','observed_at':utc_now_iso(),'runs':len(receipts),'known_actual_tokens':sum(r['known_actual_tokens'] for r in receipts),'unknown_or_outstanding_reserved_tokens':sum(r['unknown_or_outstanding_reserved_tokens'] for r in receipts),'lifetime_admission_tokens':sum(r['lifetime_admission_tokens'] for r in receipts),'accepted_activated_updates':sum(r['accepted_activated_updates'] for r in receipts),'rates':None,'estimated_usd':None,'expiry':policy['expires_at'],'status':'waiting_for_genuine_evidence_change' if not receipts else 'see per-update receipts'}
    set_setting(conn,'event.report_v2.value.cohort.'+policy['id'],aggregate);conn.commit();return aggregate
