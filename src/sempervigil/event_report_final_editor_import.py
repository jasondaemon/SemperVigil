"""Operator-only intake of authentic retained final editing, never a new paid call.

Historical writing and final editing keep their own exact requests/receipts.
Independent content approval binds the new narrative and full frozen evidence.
Separated qualification/promotion/build gates still own publication.
"""
import hashlib
import json

from . import event_report_contract_v2 as contract, event_report_final_editor as editor
from .investigation import _version
from .utils import utc_now_iso

WORKFLOW = 'retained-whole-source-final-editor-import-v1'


def validate(audit):
    wm, em = audit['writer_manifest'], audit['editor_manifest']
    w, e = audit['writer_journal'], audit['editor_journal']
    if (wm.get('workflow') != 'materiality-calibrated-report-pairs-v1'
        or wm.get('id') != 'pairs_' + _version({k:v for k,v in wm.items() if k != 'id'})[:24]
        or wm.get('publication') is not False or wm.get('database_transaction') != 'READ ONLY'
        or em.get('workflow') != 'retained-narrative-editor-evaluation-v1'
        or em.get('id') != 'editcal_' + _version({k:v for k,v in em.items() if k != 'id'})[:24]
        or em.get('publication') is not False or em.get('database_transaction') != 'READ ONLY'
        or em.get('max_calls') != 2 or em.get('ceiling') != 49906
        or em.get('scoped_model_policy') != editor.SOURCE_CHECKING_PASSES
        or w.get('manifest_id') != wm['id'] or w.get('phase') != w.get('case', '') + '_writer'
        or e.get('manifest_id') != em['id']):
        raise ValueError('event_final_editor_retained_manifest_invalid')
    wc = wm['writers'][w['case']]
    ec = next(c for c in em['cases'] if c['case'] == e['case'])
    if wc['event_id'] != ec['event_id']:
        raise ValueError('event_final_editor_retained_event_mismatch')
    for journal, expected in ((w, wc), (e, ec)):
        q, response = journal['request'], journal['response']
        request_version = expected.get('request_version', expected.get('request_sha256'))
        if (journal.get('status') != 'completed' or _version(q) != request_version
            or journal.get('request_version', journal.get('request_sha256')) != request_version
            or q.get('model') != 'gpt-5.6-sol'
            or journal['reservation'] != contract.tokens(contract.encode(q)) + 512 + q['max_completion_tokens']
            or journal['reservation'] != expected['reservation']
            or journal['usage'] != response.get('usage')
            or type(journal['usage'].get('total_tokens')) is not int
            or not 0 <= journal['usage']['total_tokens'] <= journal['reservation']
            or response['choices'][0].get('finish_reason') != 'stop'
            or response['choices'][0]['message'].get('refusal')):
            raise ValueError('event_final_editor_retained_call_invalid')
    if _version(w['request']['messages'][0]['content']) != wm['systems']['writer']:
        raise ValueError('event_final_editor_retained_writer_prompt_invalid')
    raw = json.loads(w['response']['choices'][0]['message']['content'])
    writer_packet = json.loads(w['request']['messages'][1]['content'])
    draft_spans = contract.validate(editor.narrative(raw), writer_packet)
    packet = {k:v for k,v in writer_packet.items() if k != 'attack_reference'}
    packet['final_editor'] = ec['profile']
    previous = packet.get('previous_report')
    if isinstance(previous, dict) and 'items' in previous:
        packet['previous_report'] = editor.narrative(previous)
    q = e['request']
    if (_version(raw) != ec['original_draft_version']
        or _version(editor.narrative(raw)) != ec['draft_version']
        or packet['source_version'] != ec['source_version']):
        raise ValueError('event_final_editor_retained_draft_identity_invalid')
    expected_input = editor.editor_input(packet, raw, draft_spans)
    if (q['messages'][0] != {'role':'system','content':editor.PROMPT}
        or json.loads(q['messages'][1]['content']) != expected_input
        or any(q.get(k) != ec['profile'][k] for k in ('model','reasoning_effort','max_completion_tokens'))
        or q['response_format'] != {'type':'json_schema','json_schema':{
            'name':'event_source_report','strict':True,'schema':editor.schema(packet)}}):
        raise ValueError('event_final_editor_retained_input_invalid')
    expected_sources = {s['id']: hashlib.sha256(s['text'].encode()).hexdigest() for s in packet['sources']}
    if expected_sources != {s['id']:s['text_sha256'] for s in ec['sources']}:
        raise ValueError('event_final_editor_retained_sources_invalid')
    result = json.loads(e['response']['choices'][0]['message']['content'])
    spans, resolved = editor.validate_result(result, packet)
    approval = audit['content_review']
    if (approval.get('ready') is not True or approval.get('scope') != 'content_only'
        or not approval.get('authority') or not approval.get('reviewer') or not approval.get('source_review')
        or approval.get('report_version') != _version(result['report'])
        or approval.get('evidence_version') != _version(packet)
        or approval.get('source_version') != packet['source_version']
        or approval.get('response_version') != _version(e['response'])
        or not result['review']['ready']):
        raise ValueError('event_final_editor_retained_content_approval_required')
    if (packet['update_reason'] != 'generator_upgrade'
        or packet['evidence_delta'] != {'baseline':'known','new':[],'changed':[],'removed':[]}):
        raise ValueError('event_final_editor_retained_revision_reason_invalid')
    return {'packet':packet, 'raw':raw, 'report':editor.empty_mappings(result['report']),
            'review':result['review'], 'spans':spans, 'resolved_mappings':resolved,
            'approved_narrative_version':_version(result['report']),
            'generator_version':_version({'workflow':WORKFLOW,'audit':_version(audit)})}


def derive(audit, snapshot):
    final = validate(audit)
    lineage = {'workflow':editor.WORKFLOW, 'import_workflow':WORKFLOW,
               'snapshot_version':_version(snapshot), 'audit_version':_version(audit),
               'writer_request_version':_version(audit['writer_journal']['request']),
               'writer_response_version':_version(audit['writer_journal']['response']),
               'editor_request_version':_version(audit['editor_journal']['request']),
               'editor_response_version':_version(audit['editor_journal']['response']),
               'draft_version':_version(editor.empty_mappings(final['raw'])),
               'report_version':_version(final['report']), 'review_version':_version(final['review']),
               'evidence_version':_version(final['packet']),
               'approved_narrative_version':final['approved_narrative_version'],
               'independent_content_review_version':_version(audit['content_review']),
               'independent_content_review_scope':'content_only',
               'independence':'none; same-model separate source-checking invocations',
               'invocation_policy':editor.SOURCE_CHECKING_PASSES,
               'review_scope':'source-checking final editing', 'profile':final['packet']['final_editor']}
    lineage['artifact_version'] = _version(lineage)
    return {k:final[k] for k in ('report','review','spans','resolved_mappings')} | {'lineage':lineage}


def import_reviewed(conn, audit):
    from . import event_source_reports_v2 as reports
    final = validate(audit);packet=final['packet'];eid=packet['event_id'];reports.check_scope(eid)
    conn.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', ('source-report:'+eid,))
    current=reports.snapshot(conn,eid,lock=True);predecessor=reports.previous(conn,eid)[0]
    if predecessor != audit['predecessor']:
        raise ValueError('publication_predecessor_conflict')
    reports.require_qualified_predecessor(conn,eid,predecessor)
    if current['source_version'] != packet['source_version']:
        raise ValueError('event_final_editor_retained_sources_changed')
    baseline,_=reports.published_baseline(conn,eid,current)
    if baseline is None or reports.meaningful_change(baseline,current):
        raise ValueError('event_final_editor_retained_baseline_changed')
    runtime=reports.configuration(conn)[2]
    snap={**packet,'final_editor_model_policy':editor.SOURCE_CHECKING_PASSES,
          'retained_final_editor':True,'publication_freshness_source_version':current['source_version'],
          'runtime_generation_at_import':runtime}
    derived=derive(audit,snap);rid='esr_'+_version({'workflow':WORKFLOW,'audit':_version(audit)})
    existing=conn.execute('SELECT status FROM event_source_report_runs WHERE run_id=%s',(rid,)).fetchone()
    if existing:
        from .event_source_report_publication_v2 import current_material
        current_material(conn,rid);conn.commit()
        return {'run_id':rid,'status':existing[0],'reused':True}
    journals=[audit['writer_journal'],audit['editor_journal']]
    budget=sum(j['reservation'] for j in journals);charged=sum(j['usage']['total_tokens'] for j in journals)
    conn.execute("""INSERT INTO event_source_report_runs(run_id,event_id,request_key,trigger_kind,snapshot_json,source_version,generator_version,predecessor,status,report_json,review_json,spans_json,budget_tokens,charged_tokens,created_at)
      VALUES(%s,%s,%s,'generator_upgrade',%s,%s,%s,%s,'accepted',%s,%s,%s,%s,%s,%s)""",
      (rid,eid,rid,contract.encode(snap),packet['source_version'],final['generator_version'],predecessor,
       contract.encode(derived['report']),contract.encode(derived['review']),contract.encode(derived['spans']),budget,charged,utc_now_iso()))
    for ordinal,j in enumerate(journals,1):
        conn.execute("""INSERT INTO event_source_report_calls(run_id,ordinal,phase,request_json,response_json,status,reservation,usage_json,created_at)
          VALUES(%s,%s,%s,%s,%s,'completed',%s,%s,%s)""",(rid,ordinal,'writer' if ordinal==1 else 'review',contract.encode(j['request']),contract.encode(j['response']),j['reservation'],contract.encode(j['usage']),j.get('claimed_at',utc_now_iso())))
    artifact={'workflow':editor.WORKFLOW,'final':derived,'retained_final_editor':audit}
    conn.execute('INSERT INTO event_source_report_derivatives VALUES(%s,%s,%s)',(rid,contract.encode(artifact),utc_now_iso()))
    from .event_source_report_publication_v2 import current_material
    current_material(conn,rid,lock=True);conn.commit()
    return {'run_id':rid,'status':'accepted','imported_calls':2,'new_model_calls':0,
            'charged_tokens':charged,'publication':'not_requested'}
