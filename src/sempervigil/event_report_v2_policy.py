"""Explicit, expiring autonomous v2 policy; no legacy pilot or production defaults."""
import json
import os
from datetime import datetime, timezone
from .investigation import _version

WORKFLOW = 'event-report-v2-autonomous-policy-v1'


def policy():
    enabled = os.environ.get('SV_EVENT_REPORT_V2_AUTONOMOUS', '0')
    if enabled not in {'0', '1'}:
        raise ValueError('event_report_v2_autonomous_invalid')
    if enabled == '0':
        return None
    from .attack_catalog_runtime import scope
    from .event_source_reports_v2 import cohort_configuration
    cohort = cohort_configuration()
    p = json.loads(os.environ.get('SV_EVENT_REPORT_V2_POLICY', '{}'))
    keys = {'starts_at', 'expires_at', 'max_runs', 'max_concurrent', 'run_tokens',
            'debounce_seconds', 'generator_version'}
    if not cohort or not isinstance(p, dict) or set(p) != keys:
        raise ValueError('event_report_v2_policy_required')
    from .event_source_report_pilot import policy as legacy_policy
    legacy = legacy_policy()
    if legacy and legacy['id'] == cohort['id']:
        raise ValueError('event_report_v2_pilot_cohort_overlap')
    events = sorted(scope())
    if not events:
        raise ValueError('event_report_v2_scope_required')
    for key, low, high in [('max_runs', 1, 100), ('max_concurrent', 1, 10),
                           ('run_tokens', 1, 200000), ('debounce_seconds', 0, 86400)]:
        if type(p[key]) is not int or not low <= p[key] <= high:
            raise ValueError('event_report_v2_policy_invalid')
    if p['max_concurrent'] > p['max_runs'] or p['run_tokens'] > cohort['limit']:
        raise ValueError('event_report_v2_policy_invalid')
    import re
    if not isinstance(p['generator_version'], str) or not re.fullmatch('[0-9a-f]{64}', p['generator_version']):
        raise ValueError('event_report_v2_generation_pin_required')
    start, end = utc(p['starts_at']), utc(p['expires_at'])
    if end <= start or (end-start).total_seconds() > 7*86400:
        raise ValueError('event_report_v2_expiry_invalid')
    return {**p, **cohort, 'events': events, 'workflow': WORKFLOW}


def utc(value):
    if not isinstance(value, str):
        raise ValueError('event_report_v2_expiry_invalid')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('event_report_v2_expiry_invalid')
    return result.astimezone(timezone.utc)


def active(p):
    if os.environ.get('SV_EVENT_REPORT_V2_GENERATION_ENABLED', '0') != '1':
        raise ValueError('event_report_v2_generation_disabled')
    if not utc(p['starts_at']) <= datetime.now(timezone.utc) < utc(p['expires_at']):
        raise ValueError('event_report_v2_policy_inactive')


def locked_rows(conn, p, *, lock=True):
    if lock:
        conn.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', ('source-report-cohort:'+p['id'],))
    rows = conn.execute("""SELECT run_id,event_id,status,budget_tokens,snapshot_json
       FROM event_source_report_runs WHERE snapshot_json::jsonb->'cohort'->>'id'=%s""", (p['id'],)).fetchall()
    if any(json.loads(row[4]).get('autonomous_policy') != p for row in rows):
        raise ValueError('event_report_v2_policy_conflict')
    overrun = conn.execute("""SELECT EXISTS(SELECT 1 FROM event_source_report_calls c
        JOIN event_source_report_runs r USING(run_id)
        WHERE r.snapshot_json::jsonb->'cohort'->>'id'=%s AND c.status='completed'
          AND (c.usage_json::jsonb->>'total_tokens') ~ '^[0-9]+$'
          AND (c.usage_json::jsonb->>'total_tokens')::bigint > c.reservation)""", (p['id'],)).fetchone()[0]
    if overrun:
        raise ValueError('event_report_v2_provider_budget_overrun')
    return rows


def require_integrity(conn):
    guards = conn.execute("""SELECT count(*) FROM pg_trigger
        WHERE tgrelid='event_source_report_runs'::regclass AND NOT tgisinternal
          AND tgenabled IN ('O','A') AND tgname IN
          ('event_report_v2_admission_guard','event_report_v2_admission_truncate_guard')""").fetchone()[0]
    if guards != 2:
        raise ValueError('event_report_v2_admission_integrity_required')


def admit(conn, p, event_id, generation, predecessor, baseline):
    require_integrity(conn)
    active(p)
    if event_id not in p['events'] or generation != p['generator_version']:
        raise ValueError('event_report_v2_policy_changed')
    # Initial enrollments/backfill and generator-only upgrades are deliberately absent.
    if not predecessor or baseline is None:
        raise ValueError('event_report_v2_successor_required')
    rows = locked_rows(conn, p)
    if len(rows) >= p['max_runs']:
        raise ValueError('event_report_v2_run_limit')
    if sum(r[2] in ('queued', 'running') for r in rows) >= p['max_concurrent']:
        raise ValueError('event_report_v2_busy')
    # Held/failed attempts retain their lifetime admission capacity. No retry rotation.
    if sum(r[3] for r in rows) + p['run_tokens'] > p['limit']:
        raise ValueError('event_report_v2_capacity_exhausted')


def check_run(conn, record, run_id, *, publication=False):
    require_integrity(conn)
    p = policy()
    if not p or record['snapshot'].get('autonomous_policy') != p:
        raise ValueError('event_report_v2_policy_changed')
    active(p)
    from .event_source_reports_v2 import runtime_identity
    if record['snapshot'].get('autonomous_runtime_version') != runtime_identity():
        raise ValueError('event_report_v2_runtime_changed')
    # Verification does not reserve capacity. Taking the admission advisory lock
    # here would deadlock the separate approval connection against its caller.
    # Immutable policy snapshots and serialized lifetime admission enforce bounds.
    rows = locked_rows(conn, p, lock=False)
    if (record['event_id'] not in p['events'] or record['generator_version'] != p['generator_version']
        or record['budget_tokens'] != p['run_tokens'] or len(rows) > p['max_runs']
        or sum(r[3] for r in rows) > p['limit'] or not any(r[0] == run_id for r in rows)
        or record['charged_tokens'] + record['reserved_tokens'] > p['run_tokens']):
        raise ValueError('event_report_v2_policy_changed')
    if publication:
        calls = conn.execute('SELECT phase,status,request_json FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal', (run_id,)).fetchall()
        requests = [json.loads(r[2]) for r in calls]
        if (record['status'] != 'accepted' or record['reserved_tokens'] != 0
            or not record['review'] or record['review'].get('ready') is not True or record['review'].get('issues') != []
            or [(r[0], r[1]) for r in calls] != [('writer', 'completed'), ('review', 'completed')]
            or requests[0]['model'] == requests[1]['model']):
            raise ValueError('event_report_v2_not_independently_ready')
        from . import event_report_contract_v2 as contract
        contract.validate_review(record['review'], record['report'], contract.context(record['snapshot']))
        from .event_source_reports_v2 import MODEL, phase_settings
        models = [os.environ.get('SV_EVENT_SOURCE_REPORT_WRITER_MODEL'), MODEL]
        prompts = [contract.WRITER + contract.ATTACK_WRITER, contract.REVIEWER + contract.ATTACK_REVIEWER]
        for request, model, prompt, phase in zip(requests, models, prompts, ['writer', 'review']):
            options = phase_settings()[phase]
            if (request['model'] != model or request['messages'][0] != {'role': 'system', 'content': prompt}
                or any(request.get(k) != value for k, value in options.items())):
                raise ValueError('event_report_v2_request_identity_changed')
        delta = record['snapshot'].get('evidence_delta', {})
        if delta.get('baseline') != 'known' or not any(delta.get(k) for k in ('new', 'changed', 'removed')):
            raise ValueError('event_report_v2_no_evidence_delta')
        return {'workflow': WORKFLOW, 'policy': p, 'policy_version': _version(p),
                'run_id': run_id, 'generator_version': record['generator_version'],
                'source_version': record['source_version'], 'report_version': _version(record['report']),
                'review_version': _version(record['review'])}
