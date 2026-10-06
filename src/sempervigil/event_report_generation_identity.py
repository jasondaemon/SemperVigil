"""Original generation identity for historical reads, current prompts for admission."""
import hashlib,json
from pathlib import Path


def identity(writer, reviewer, generation):
    return {'workflow':'source-report-generation-prompts-v1','generator_version':generation,
            'writer_sha256':hashlib.sha256(writer.encode()).hexdigest(),
            'review_sha256':hashlib.sha256(reviewer.encode()).hexdigest()}


def validate(conn, record, writer_request, review_request, *, published, current_writer, current_reviewer):
    from .investigation import _version
    writer=writer_request['messages'][0]['content'];reviewer=review_request['messages'][0]['content']
    actual=identity(writer,reviewer,record['generator_version'])
    saved=record['snapshot'].get('generation_prompt_identity')
    if saved is not None and saved!=actual:
        raise ValueError('event_source_report_original_prompt_integrity')
    if not published:
        if writer!=current_writer or reviewer!=current_reviewer:
            raise ValueError('event_source_report_prompt_integrity')
        return actual
    if saved is not None:
        return actual
    # Pre-pin generations have no original prompt manifest in their snapshot.
    # Accept only exact trusted repository-ancestor pairs AND an authentic,
    # non-revoked existing publication; this grants no new admission authority.
    history=json.loads((Path(__file__).parent/'data/event_report_v2_prompt_history.json').read_text())
    pair=(actual['writer_sha256'],actual['review_sha256'])
    if pair not in {(p['writer_sha256'],p['review_sha256']) for p in history['pairs']}:
        raise ValueError('event_source_report_historical_prompt_unknown')
    rows=conn.execute("""SELECT r.bundle_json,r.qualification_id,q.qualification_json,q.revoked_at
        FROM event_public_revisions r JOIN event_quote_qualifications q USING(event_id,qualification_id)
        WHERE r.event_id=%s AND r.bundle_json::jsonb->>'run_id'=%s""",(record['event_id'],record['run_id'])).fetchall()
    for raw,qid,qraw,revoked in rows:
        bundle=json.loads(raw);q=json.loads(qraw)
        if revoked is None and _version(q)==qid and bundle.get('qualification')==q and q.get('run_id')==record['run_id']:
            return actual
    raise ValueError('event_source_report_historical_publication_required')
