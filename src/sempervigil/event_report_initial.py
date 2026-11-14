"""Explicit first-report authority, separate from qualified-report successors."""
import json
import os
import re
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
    keys = {'event_id', 'entity', 'system', 'incident_window', 'source_anchors',
            'query_terms', 'incident_year'}
    if (not isinstance(identity, dict) or set(identity) != keys
            or events != [identity.get('event_id')]
            or any(not isinstance(identity.get(k), str) or not identity[k].strip()
                   for k in ('entity', 'system', 'incident_window'))
            or len(json.dumps(identity).encode()) > 6000):
        raise ValueError('event_report_initial_identity_required')
    terms = identity['query_terms']
    if (type(identity['incident_year']) is not int or not 1900 <= identity['incident_year'] <= 2100
            or str(identity['incident_year']) not in identity['incident_window']
            or type(terms) is not list or not 1 <= len(terms) <= 4
            or any(type(t) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9 ._-]{0,60}', t) for t in terms)
            or len(set(terms)) != len(terms)):
        raise ValueError('event_report_initial_query_identity_required')
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
    require_source_identity(identity, snap['sources'])
    ids = [s['article_id'] for s in snap['sources']]
    rows = conn.execute('''SELECT id,COALESCE(to_jsonb(articles)->>'has_full_content','unknown'),
        to_jsonb(articles)->>'content_error' FROM articles WHERE id=ANY(%s)''', (ids,)).fetchall()
    if {r[0] for r in rows} != set(ids) or any(r[1] not in ('1', 'true') or r[2] for r in rows):
        raise ValueError('event_report_initial_full_sources_required')
    # Locator integrity is not semantic verification. The entire original source
    # remains in both passes; independent review must check the proposed identity.
    return _version(identity)


def require_source_identity(identity, articles):
    sources = {s['id']: s['text'] for s in articles}
    for anchor in identity['source_anchors']:
        if ' '.join(anchor['quote'].split()) not in ' '.join(sources.get(anchor['source_id'], '').split()):
            raise ValueError('event_report_initial_identity_source_changed')
    body = ' '.join(sources.values()).casefold()
    if (any(term.casefold() not in body for term in identity['query_terms'])
            or str(identity['incident_year']) not in ' '.join(a['quote'] for a in identity['source_anchors'])):
        raise ValueError('event_report_initial_query_identity_source_changed')


def research_identity(conn, event_id):
    """Only explicit source-checked policy or qualified historical evidence is trusted."""
    row = conn.execute('''SELECT p.revision_id,r.bundle_json FROM event_public_pointers p
        LEFT JOIN event_public_revisions r USING(event_id,revision_id) WHERE p.event_id=%s''', (event_id,)).fetchone()
    seen = set()
    while row and len(seen) < 32:
        revision, raw = row
        if raw is None: raise ValueError('event_report_initial_identity_history_unavailable')
        if revision in seen: raise ValueError('event_report_initial_identity_history_invalid')
        seen.add(revision)
        bundle = json.loads(raw) if isinstance(raw, str) else raw
        if bundle.get('workflow') != 'event-source-report-public-v2': break
        from .event_source_report_publication_v2 import published_material, validate_bundle
        validate_bundle(bundle, event_id=event_id, expected_revision=revision)
        material = published_material(conn, bundle)
        identity = material['snapshot'].get('incident_identity')
        if identity:
            validate(identity, [event_id]); require_source_identity(identity, material['snapshot']['sources'])
            return identity
        predecessor = bundle['predecessor']
        row = conn.execute('SELECT revision_id,bundle_json FROM event_public_revisions '
                           'WHERE event_id=%s AND revision_id=%s', (event_id, predecessor)).fetchone() if predecessor else None
        if predecessor and not row:
            raise ValueError('event_report_initial_identity_history_unavailable')
    if row and len(seen) >= 32:
        raise ValueError('event_report_initial_identity_history_limit')
    if manages_legacy(event_id):
        from .event_report_v2_policy import policy, active
        selected = policy(); active(selected); require_draft(conn, selected, event_id)
        return selected['incident_identity']
    return None
