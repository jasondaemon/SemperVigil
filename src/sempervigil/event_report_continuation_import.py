"""Operator-only import of an already reviewed immutable two-call continuation.

No network calls, enqueueing generation, resetting jobs or rewriting raw outputs.
The normal separated approval/promotion/build gates still own publication.
"""
import json
from . import event_report_contract_v2 as contract
from .private_report_canary import identity
from .investigation import _version
from .utils import utc_now_iso


def validate_retained(audit,writer_request,writer_response,review_request,review_response):
    parent=audit['parent_manifest'];child=audit['continuation_manifest']
    writer=audit['parent_writer_journal'];review=audit['review_journal'];packet=audit['packet']
    if (parent['id']!='canary_'+identity({k:v for k,v in parent.items() if k!='id'})[:24] or
        parent['workflow']!='private-manifest-bound-report-canary-v1' or parent['publication'] is not False or
        parent['database_transaction']!='READ ONLY' or parent['max_calls']!=2 or parent['ceiling']!=32000 or
        child['id']!='continuation_'+identity({k:v for k,v in child.items() if k!='id'})[:24] or
        child['workflow']!='private-report-continuation-v1' or child['parent_id']!=parent['id'] or
        child['parent_manifest_sha256']!=identity(parent) or child['parent_writer_journal_sha256']!=identity(writer) or
        child['publication'] is not False or child['database_transaction']!='READ ONLY' or child['max_calls']!=2 or child['ceiling']!=32000):
        raise ValueError('retained_continuation_identity_invalid')
    import hashlib
    codes=audit['candidate_codes']
    if set(codes)!=set(child['code_sha256']) or any(hashlib.sha256(v.encode()).hexdigest()!=child['code_sha256'][n] for n,v in codes.items()):
        raise ValueError('retained_continuation_code_identity_invalid')
    if (writer['manifest_id']!=parent['id'] or writer['status']!='completed' or writer['phase']!='writer' or
        review['manifest_id']!=parent['id'] or review['continuation_id']!=child['id'] or review['status']!='completed' or review['phase']!='review' or
        writer['request']!=writer_request or writer['response']!=writer_response or review['request']!=review_request or review['response']!=review_response or
        identity(writer_request)!=parent['writer_request_sha256'] or identity(packet)!=parent['packet_sha256'] or
        json.loads(writer_request['messages'][1]['content'])!=packet or identity(review_request)!=child['review_request_sha256'] or
        identity(writer_request['messages'][0]['content'])!=parent['writer_system_sha256'] or identity(review_request['messages'][0]['content'])!=parent['review_system_sha256']):
        raise ValueError('retained_continuation_call_identity_invalid')
    for phase,call,request,model,effort,cap in [('writer',writer,writer_request,'gpt-5.6-sol','none',6000),('review',review,review_request,'gpt-5.6-luna','low',2400)]:
        if request['model']!=model or request['reasoning_effort']!=effort or request['max_completion_tokens']!=cap or call['reservation']!=contract.tokens(contract.encode(request))+512+cap:
            raise ValueError('retained_continuation_reservation_invalid')
        if call['usage']!=call['response']['usage'] or type(call['usage'].get('total_tokens')) is not int or not 0<=call['usage']['total_tokens']<=call['reservation']:
            raise ValueError('retained_continuation_usage_invalid')
        choice=call['response']['choices'][0]
        if choice['finish_reason']!='stop' or choice['message'].get('refusal'):raise ValueError('retained_continuation_incomplete')
    if writer['reservation']+review['reservation']>32000:raise ValueError('retained_continuation_budget_invalid')
    raw=json.loads(writer_response['choices'][0]['message']['content'])
    from .attack_catalog import project_optional_mappings
    from .attack_catalog_runtime import catalog_for
    report,evidence,spans,resolved,removed=project_optional_mappings(raw,packet,catalog_for(packet['attack_reference']['catalog']),contract_override=contract)
    projection={'report':report,'evidence':evidence,'spans':spans,'removed':removed}
    if identity(projection)!=child['projection_sha256'] or json.loads(review_request['messages'][1]['content'])!={'report':report,'evidence':evidence,'citation_provenance':spans}:
        raise ValueError('retained_continuation_projection_invalid')
    parsed=json.loads(review_response['choices'][0]['message']['content']);contract.validate_review(parsed,report,evidence)
    if not parsed['ready']:raise ValueError('retained_continuation_review_held')
    return {'generator_version':_version({'workflow':'retained-reviewed-continuation-v1','parent':parent['id'],'continuation':child['id'],'code_sha256':child['code_sha256']}),
            'report':report,'review':parsed,'spans':spans,'projection':projection,'raw':raw}


def import_reviewed(conn,audit,*,manual_review,editorial=None):
    from . import event_source_reports_v2 as reports
    parent=audit['parent_manifest'];event_id=parent['event_id'];reports.check_scope(event_id)
    validated=validate_retained(audit,audit['parent_writer_journal']['request'],audit['parent_writer_journal']['response'],audit['review_journal']['request'],audit['review_journal']['response'])
    if not isinstance(manual_review,dict) or not manual_review.get('authority') or not manual_review.get('source_review'):
        raise ValueError('retained_continuation_manual_review_required')
    if editorial is None and manual_review.get('ready') is not True:
        raise ValueError('retained_continuation_manual_review_required')
    if editorial is not None:
        from .event_source_report_publication_v2 import apply_editorial
        apply_editorial({'report':validated['report'],'review':validated['review']},
                       editorial,validated['projection']['evidence'])
    conn.execute('SELECT pg_advisory_xact_lock(hashtext(%s))',('source-report:'+event_id,))
    current=reports.snapshot(conn,event_id,lock=True);predecessor=reports.previous(conn,event_id)[0]
    if predecessor!=parent['predecessor']:raise ValueError('publication_predecessor_conflict')
    original=audit['packet'];members={v['article_id']:v for v in current['membership']}
    for s in original['sources']:
        m=members.get(s['article_id'])
        if not m or m['suppressed'] or m['text_hash']!=contract.digest(s['text']) or m['title']!=s['title'] or m['url']!=s['url']:
            raise ValueError('retained_continuation_source_changed')
    # Every source membership/version of the public predecessor is reconstructed;
    # no private premise or unsupported prior-report prose is promoted into evidence.
    baseline,_=reports.published_baseline(conn,event_id,current)
    if baseline is None or reports.meaningful_change(baseline,current):raise ValueError('retained_continuation_baseline_changed')
    if original['evidence_delta']!={'baseline':'known','new':[],'changed':[],'removed':[]} or original['update_reason']!='generator_upgrade':
        raise ValueError('retained_continuation_revision_reason_invalid')
    run_id='esr_'+_version({'workflow':'retained-reviewed-continuation-import-v1','continuation':audit['continuation_manifest']['id']})
    existing=conn.execute('SELECT status FROM event_source_report_runs WHERE run_id=%s',(run_id,)).fetchone()
    if existing:
        from .event_source_report_publication_v2 import current_material
        current_material(conn,run_id,editorial=editorial);return {'run_id':run_id,'status':existing[0],'reused':True}
    runtime=reports.configuration(conn)[2]
    snap={**original,'report_contract':contract.WORKFLOW,'publication_freshness_source_version':current['source_version'],'runtime_generation_at_import':runtime}
    writer=audit['parent_writer_journal'];review=audit['review_journal'];charged=writer['usage']['total_tokens']+review['usage']['total_tokens']
    conn.execute("""INSERT INTO event_source_report_runs(run_id,event_id,request_key,trigger_kind,snapshot_json,source_version,generator_version,predecessor,status,report_json,review_json,spans_json,budget_tokens,charged_tokens,created_at)
      VALUES(%s,%s,%s,'generator_upgrade',%s,%s,%s,%s,'accepted',%s,%s,%s,32000,%s,%s)""",(run_id,event_id,run_id,contract.encode(snap),original['source_version'],validated['generator_version'],predecessor,contract.encode(validated['report']),contract.encode(validated['review']),contract.encode(validated['spans']),charged,utc_now_iso()))
    for ordinal,journal in enumerate([writer,review],1):
        conn.execute("""INSERT INTO event_source_report_calls(run_id,ordinal,phase,request_json,response_json,status,reservation,usage_json,created_at)
          VALUES(%s,%s,%s,%s,%s,'completed',%s,%s,%s)""",(run_id,ordinal,journal['phase'],contract.encode(journal['request']),contract.encode(journal['response']),journal['reservation'],contract.encode(journal['usage']),utc_now_iso()))
    artifact={'workflow':'optional-mapping-projection-v1','input_version':_version(validated['raw']),'snapshot_version':_version(snap),'projection':validated['projection'],
              'continuation':audit,'manual_review':manual_review,'runtime_generation_at_import':runtime,
              'continuation_lineage':{'workflow':'retained-reviewed-continuation-v1','parent_manifest_id':parent['id'],'continuation_id':audit['continuation_manifest']['id'],'generator_version':validated['generator_version'],'raw_writer_version':_version(validated['raw']),'projection_version':_version(validated['projection']),'removed_mapping_reasons':[v['reason'] for v in validated['projection']['removed']]}}
    if editorial is not None:
        artifact['required_editorial_proposal_version']=_version(editorial['proposal'])
    conn.execute('INSERT INTO event_source_report_derivatives VALUES(%s,%s,%s)',(run_id,contract.encode(artifact),utc_now_iso()))
    from .event_source_report_publication_v2 import current_material
    current_material(conn,run_id,lock=True,editorial=editorial)
    conn.commit()
    return {'run_id':run_id,'status':'accepted','imported_calls':2,'new_model_calls':0,'charged_tokens':charged,'publication':'not_requested'}
