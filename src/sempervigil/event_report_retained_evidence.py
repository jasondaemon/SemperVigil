"""Verify retained evidence independently of ordinary live article refreshes."""
import json
from .investigation import _version
from .event_report_contract import digest


def require_retained(conn, record, *, lock=False):
    packet = record['snapshot']
    keys = ('event_id', 'title', 'sources', 'membership', 'excluded_article_ids')
    try:
        core = {k: packet[k] for k in keys}
        for key in ('capture_policy', 'capture_history'):
            if key in packet: core[key] = packet[key]
        if (packet['source_version'] != record['source_version']
                or _version(core) != record['source_version']
                or packet['event_id'] != record['event_id'] or not packet['sources']):
            raise ValueError('event_source_report_retained_snapshot_integrity')
        members = {m['article_id']: m for m in packet['membership']}
        if len(members) != len(packet['membership']):
            raise ValueError('event_source_report_retained_snapshot_integrity')
        for source in packet['sources']:
            member = members[source['article_id']]
            if (not source['text'] or source['id'] != 'S'+str(source['article_id'])
                    or source['content_hash'] != digest(' '.join(source['text'].split()))
                    or member['text_hash'] != digest(source['text'])
                    or member['suppressed'] or member['url'] != source['url']):
                raise ValueError('event_source_report_retained_snapshot_integrity')
    except (KeyError, TypeError) as exc:
        raise ValueError('event_source_report_retained_snapshot_integrity') from exc
    suffix = ' FOR SHARE NOWAIT' if lock else ''
    row = conn.execute('SELECT id,visibility,lifecycle FROM events WHERE id=%s'+suffix,
                       (record['event_id'],)).fetchone()
    if not row or row != (record['event_id'], 'active', 'confirmed'):
        raise ValueError('event_source_report_event_ineligible')
    # Unlinking/replacing membership is ordinary evolving evidence, not revocation.
    # Missing original article identity or explicit suppression remains fail closed.
    ids = [s['article_id'] for s in packet['sources']]
    rows = conn.execute('SELECT id,meta_json FROM articles WHERE id=ANY(%s) ORDER BY id'+suffix,
                        (ids,)).fetchall()
    if {r[0] for r in rows} != set(ids):
        raise ValueError('event_source_report_retained_evidence_unavailable')
    for _, raw in rows:
        meta = json.loads(raw) if isinstance(raw, str) and raw else (raw or {})
        if not isinstance(meta, dict) or meta.get('suppressed'):
            raise ValueError('event_source_report_evidence_changed')
    return packet
