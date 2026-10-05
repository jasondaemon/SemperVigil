"""Default-off bounded successor-only policy; no new publication authority."""
from datetime import datetime, timezone
import json
import os
import re

from .investigation import _version


def utc(value):
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('event_source_report_pilot_timezone_required')
    return parsed.astimezone(timezone.utc)


def policy():
    raw=os.environ.get('SV_EVENT_SOURCE_REPORT_PILOT_POLICY','').strip()
    if not raw:return None
    if len(raw)>4096:raise ValueError('event_source_report_pilot_invalid')
    p=json.loads(raw)
    keys={'id','limit','events','starts_at','expires_at','max_runs','max_concurrent','run_tokens'}
    if (type(p) is not dict or set(p)!=keys or not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}',p.get('id',''))
        or type(p['events']) is not list or not 1<=len(p['events'])<=2
        or len(set(p['events']))!=len(p['events'])
        or any(type(e) is not str or not re.fullmatch(r'evt_[A-Za-z0-9_-]{1,128}',e) for e in p['events'])
        or any(type(p[k]) is not int for k in ('limit','max_runs','max_concurrent','run_tokens'))
        or not 1<=p['max_runs']<=2 or p['max_concurrent']!=1
        or not 1<=p['run_tokens']<=32000 or not 1<=p['limit']<=64000
        or p['max_runs']*p['run_tokens']>p['limit']):
        raise ValueError('event_source_report_pilot_invalid')
    start,end=utc(p['starts_at']),utc(p['expires_at'])
    if not 0<(end-start).total_seconds()<=172800:raise ValueError('event_source_report_pilot_window_invalid')
    if os.environ.get('SV_EVENT_SOURCE_REPORT_COHORT_ID')!=p['id'] or os.environ.get('SV_EVENT_SOURCE_REPORT_COHORT_TOKENS')!=str(p['limit']):
        raise ValueError('event_source_report_pilot_cohort_mismatch')
    scope={e.strip() for e in os.environ.get('SV_EVENT_SOURCE_REPORT_EVENT_IDS','').split(',') if e.strip()}
    if scope!=set(p['events']):raise ValueError('event_source_report_pilot_scope_mismatch')
    p={**p,'events':sorted(p['events'])}
    return p


def active(p, now=None):
    current=now or datetime.now(timezone.utc)
    if not utc(p['starts_at'])<=current<utc(p['expires_at']):
        raise ValueError('event_source_report_pilot_expired_or_not_started')


def locked_rows(conn,p):
    conn.execute('SELECT pg_advisory_xact_lock(hashtext(%s))',('source-report-cohort:'+p['id'],))
    rows=conn.execute("""SELECT run_id,event_id,status,budget_tokens,snapshot_json FROM event_source_report_runs
      WHERE snapshot_json::jsonb->'cohort'->>'id'=%s ORDER BY run_id""",(p['id'],)).fetchall()
    if any(json.loads(r[4]).get('pilot')!=p for r in rows):
        raise ValueError('event_source_report_pilot_policy_changed')
    return rows


def admit(conn,p,event_id,predecessor,old,trigger):
    active(p)
    if event_id not in p['events'] or trigger!='evidence_change' or not predecessor or old is None:
        raise ValueError('event_source_report_pilot_successor_required')
    rows=locked_rows(conn,p)
    if len(rows)>=p['max_runs'] or any(r[1]==event_id for r in rows):
        raise ValueError('event_source_report_pilot_run_limit')
    if sum(r[2] in ('queued','running') for r in rows)>=p['max_concurrent']:
        raise ValueError('event_source_report_pilot_busy')
    # Full lifetime admission capacity is reserved even if a held attempt used
    # fewer tokens. There is no rotation, retry or reclamation of pilot slots.
    if sum(r[3] for r in rows)+p['run_tokens']>p['limit']:
        raise ValueError('event_source_report_pilot_capacity_exhausted')


def check_run(conn,record,run_id,p,*,publication=False):
    active(p)
    if record['snapshot'].get('pilot')!=p or record['event_id'] not in p['events']:
        raise ValueError('event_source_report_pilot_policy_changed')
    rows=locked_rows(conn,p)
    if not any(r[0]==run_id for r in rows) or len(rows)>p['max_runs'] or sum(r[3] for r in rows)>p['limit']:
        raise ValueError('event_source_report_pilot_capacity_exhausted')
    if record['budget_tokens']!=p['run_tokens'] or record['charged_tokens']+record['reserved_tokens']>p['run_tokens']:
        raise ValueError('event_source_report_pilot_budget_invalid')
    if publication:
        config=json.loads(os.environ.get('SV_EVENT_SOURCE_REPORT_PHASE_CONFIG','{}'))
        if (os.environ.get('SV_EVENT_SOURCE_REPORT_WRITER_MODEL')!='gpt-5.6-sol'
            or config.get('writer')!={'reasoning_effort':'none','max_completion_tokens':6000}
            or config.get('review')!={'reasoning_effort':'low','max_completion_tokens':2400}):
            raise ValueError('event_source_report_pilot_models_invalid')
        calls=conn.execute('SELECT phase,status,reservation,request_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(run_id,)).fetchall()
        if (record['status']!='accepted' or record.get('derivation') or record['reserved_tokens']!=0
            or record['review']!={'ready':True,'issues':[],'locator_warnings':[]}
            or [r[:2] for r in calls]!=[('writer','completed'),('review','completed')]
            or sum(r[2] for r in calls)>p['run_tokens']):
            raise ValueError('event_source_report_pilot_not_independently_ready')
        expected=[('gpt-5.6-sol','none',6000),('gpt-5.6-luna','low',2400)]
        actual=[(q['model'],q['reasoning_effort'],q['max_completion_tokens']) for q in (json.loads(r[3]) for r in calls)]
        if actual!=expected:raise ValueError('event_source_report_pilot_models_invalid')
        delta=record['snapshot'].get('evidence_delta',{})
        if delta.get('baseline')!='known' or not any(delta.get(k) for k in ('new','changed','removed')):
            raise ValueError('event_source_report_pilot_no_evidence_delta')
        return {'workflow':'event-source-report-automatic-approval-v1','policy':p,'policy_version':_version(p),'run_id':run_id,
                'generator_version':record['generator_version'],'source_version':record['source_version'],
                'report_version':_version(record['report']),'review_version':_version(record['review'])}


def safe_reason(value):
    text=str(value or '')
    match=re.match(r'event_source_report_[a-z_]+',text)
    return match.group(0) if match else 'unclassified_failure'


def status(conn,event_id=None):
    from .storage import get_setting
    p=policy()
    params=() if event_id is None else (event_id,)
    where='' if event_id is None else ' WHERE event_id=%s'
    rows=conn.execute('SELECT run_id,event_id,status,reason,charged_tokens,reserved_tokens,budget_tokens,created_at,snapshot_json FROM event_source_report_runs'+where+' ORDER BY created_at DESC LIMIT 100',params).fetchall()
    runs=[]
    for rid,e,state,reason,charged,reserved,budget,created,raw in rows:
        snap=json.loads(raw)
        calls=conn.execute('SELECT phase,status,usage_json,request_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(rid,)).fetchall()
        phases=[]
        for phase,cs,usage,request in calls:
            q=json.loads(request);u=json.loads(usage) if usage else {}
            phases.append({'phase':phase,'status':cs,'model':q.get('model'),'usage':u})
        approval=conn.execute("SELECT j.id,j.status FROM event_review_approvals a JOIN jobs j ON j.id=a.job_id WHERE a.approval_json::jsonb->>'run_id'=%s",(rid,)).fetchone()
        published=conn.execute('SELECT revision_id FROM event_public_revisions WHERE event_id=%s AND bundle_json::jsonb->>\'run_id\'=%s',(e,rid)).fetchone()
        alerts=[]
        if state=='held':alerts.append(safe_reason(reason))
        if reserved:alerts.append('unknown_or_outstanding_reservation')
        if state=='accepted' and not published and (datetime.now(timezone.utc)-utc(created)).total_seconds()>900:alerts.append('accepted_unpublished_over_15_minutes')
        if approval and approval[1]=='failed':alerts.append('promotion_failed')
        runs.append({'run_id':rid,'event_id':e,'status':state,'reason':safe_reason(reason) if reason else None,'charged_tokens':charged,'reserved_tokens':reserved,'budget_tokens':budget,'created_at':created,'cohort':snap.get('cohort'),'phases':phases,'call_count':len(calls),'call_limit':2 if snap.get('pilot') else 4,'estimated_max_nominal_usd':round((charged+reserved)*.00002,6),'approval':approval,'revision':published[0] if published else None,'alerts':alerts,'alert_key':_version({'run_id':rid,'alerts':alerts}) if alerts else None})
    cohort_rows=[] if not p else conn.execute("SELECT charged_tokens,reserved_tokens,budget_tokens,status FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'=%s",(p['id'],)).fetchall()
    used=sum(r[0]+r[1] for r in cohort_rows)
    capacity=sum(r[2] for r in cohort_rows)
    last_tick=get_setting(conn,'event.source_report.last_tick',{})
    phase_alerts=[]
    if p:
        try:active(p)
        except ValueError:phase_alerts.append('pilot_expired_or_not_started')
    builds=conn.execute("SELECT id,status FROM jobs WHERE job_type='build_site' ORDER BY requested_at DESC LIMIT 3").fetchall()
    if builds and builds[0][1]=='failed':phase_alerts.append('latest_build_failed')
    return {'enabled':os.environ.get('SV_EVENT_SOURCE_REPORT_ENABLED')=='1','policy':p,'checked_at':datetime.now(timezone.utc).isoformat(),'last_tick':last_tick,'operational_alerts':phase_alerts,'recent_builds':builds,'runs':runs,
        'cohort':None if not p else {'actual_plus_reserved':used,'admission_capacity_reserved':capacity,'limit':p['limit'],'remaining_admission_tokens':p['limit']-capacity,'runs':len(cohort_rows),'active':sum(r[3] in ('queued','running') for r in cohort_rows),'estimated_max_capacity_usd':round(p['limit']*.00002,6),'cost_scope':'report-only conservative public-rate estimate, not an invoice','alerts':['cohort_80_percent'] if used>=.8*p['limit'] or capacity>=.8*p['limit'] else []}}
