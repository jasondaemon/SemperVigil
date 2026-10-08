"""Explicit first-report authority, separate from qualified-report successors."""
import json
import os
from .investigation import _version

WORKFLOW = 'event-report-initial-admission-v1'


def manages_legacy(event_id):
    """Resolve first-report policy only for explicitly selected events."""
    if (os.environ.get('SV_EVENT_REPORT_V2_AUTONOMOUS', '0') != '1'
            or os.environ.get('SV_EVENT_REPORT_V2_ENABLED', '0') != '1'
            or event_id not in {e.strip() for e in os.environ.get(
                'SV_EVENT_REPORT_V2_EVENT_IDS', '').split(',') if e.strip()}):
        return False
    from .event_report_v2_policy import policy
    selected = policy()
    return bool(selected and selected.get('admission_kind') == 'initial_report')


def validate(identity, events):
    keys = {'event_id', 'entity', 'system', 'incident_window', 'source_anchors'}
    if (not isinstance(identity, dict) or set(identity) != keys
            or events != [identity.get('event_id')]
            or any(not isinstance(identity.get(k), str) or not identity[k].strip()
                   for k in ('entity', 'system', 'incident_window'))
            or len(json.dumps(identity).encode()) > 6000):
        raise ValueError('event_report_initial_identity_required')
    anchors = identity['source_anchors']
    if (not isinstance(anchors, list) or not 1 <= len(anchors) <= 10
            or any(not isinstance(a, dict) or set(a) != {'source_id', 'quote'}
                   or any(not isinstance(a.get(k), str) or not a[k].strip()
                          for k in ('source_id', 'quote')) for a in anchors)):
        raise ValueError('event_report_initial_identity_required')
    return identity


def require_draft(conn, policy, event_id):
    identity = validate(policy['incident_identity'], policy['events'])
    row = conn.execute('SELECT entity,visibility,lifecycle,publish_state FROM events WHERE id=%s',
                       (event_id,)).fetchone()
    pointer = conn.execute('SELECT revision_id FROM event_public_pointers WHERE event_id=%s',
                           (event_id,)).fetchone()
    history = conn.execute('SELECT 1 FROM event_public_revisions WHERE event_id=%s LIMIT 1',
                           (event_id,)).fetchone()
    if (event_id != identity['event_id'] or not row or pointer or history
            or row != (identity['entity'], 'active', 'confirmed', 'draft')):
        raise ValueError('event_report_initial_draft_required')
    from .event_source_reports_v2 import snapshot
    snap = snapshot(conn, event_id)
    sources = {s['id']: s['text'] for s in snap['sources']}
    for anchor in identity['source_anchors']:
        if ' '.join(anchor['quote'].split()) not in ' '.join(sources.get(anchor['source_id'], '').split()):
            raise ValueError('event_report_initial_identity_source_changed')
    # Locator integrity is not semantic verification. The entire original source
    # remains in both passes; independent review must check the proposed identity.
    return _version(identity)
