"""Source-report adapter to the existing separated approval/promotion boundary."""
import json

from . import event_report_contract_v2 as contract
from .event_approval import connection_factory, JOB_TYPE
from .event_source_reports_v2 import _load, snapshot
from .investigation import _version
from .storage import enqueue_job
from .utils import utc_now_iso

APPROVAL_WORKFLOW = "event-source-report-approval-v2"


def current_material(conn, run_id, *, lock=False, published=False, derivative=None):
    if lock:
        conn.execute('SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR SHARE NOWAIT',(run_id,))
    record=_load(conn,run_id)
    if record['snapshot'].get('report_contract')!=contract.WORKFLOW or record['status']!='accepted' or not record['review']:
        raise ValueError('event_source_report_not_accepted')
    current=snapshot(conn,record['event_id'],lock=lock)
    if not published and current['source_version']!=record['snapshot'].get('publication_freshness_source_version',record['source_version']):
        raise ValueError('event_source_report_sources_changed')
    if published:
        membership={m['article_id']:m for m in current['membership']}
        for source in record['snapshot']['sources']:
            m=membership.get(source['article_id'])
            if not m or m['suppressed'] or m['title']!=source['title'] or m['url']!=source['url'] or m['text_hash']!=contract.digest(source['text']):
                raise ValueError('event_source_report_evidence_changed')
    calls=conn.execute('SELECT phase,request_json,response_json,status FROM event_source_report_calls WHERE run_id=%s ORDER BY ordinal',(run_id,)).fetchall()
    if [(v[0],v[3]) for v in calls]!=[('writer','completed'),('review','completed')]:
        raise ValueError('event_source_report_response_integrity')
    values={phase:(json.loads(req),json.loads(res)) for phase,req,res,status in calls}
    writer_request,writer_response=values['writer'];review_request,review_response=values['review']
    def body(response):
        choice=response['choices'][0]
        if choice.get('finish_reason')!='stop' or choice['message'].get('refusal'):
            raise ValueError('event_source_report_incomplete_response')
        return json.loads(choice['message']['content'])
    raw=body(writer_response);input_packet=json.loads(writer_request['messages'][1]['content'])
    if input_packet['sources']!=record['snapshot']['sources']:
        raise ValueError('event_source_report_input_integrity')
    from .attack_catalog_runtime import catalog_for
    from .attack_catalog import project_optional_mappings
    cat=catalog_for(input_packet['attack_reference']['catalog'])
    report,evidence,spans,resolved,removed=project_optional_mappings(raw,input_packet,cat,contract_override=contract)
    saved=conn.execute('SELECT projection_json FROM event_source_report_derivatives WHERE run_id=%s',(run_id,)).fetchone()
    if not saved:raise ValueError('event_source_report_projection_required')
    audit=json.loads(saved[0]);expected={'report':report,'evidence':evidence,'spans':spans,'removed':removed}
    if audit.get('snapshot_version')!=_version(record['snapshot']):raise ValueError('event_source_report_snapshot_integrity')
    if audit['workflow']!='optional-mapping-projection-v1' or audit['input_version']!=_version(raw) or audit['projection']!=expected:
        raise ValueError('event_source_report_derivative_integrity')
    if json.loads(review_request['messages'][1]['content'])!={'evidence':evidence,'report':report,'citation_provenance':spans}:
        raise ValueError('event_source_report_review_input_integrity')
    schema={'type':'json_schema','json_schema':{'name':'event_source_report','strict':True,'schema':contract.review_schema(report,[s['id'] for s in evidence['sources']])}}
    if review_request['response_format']!=schema:
        raise ValueError('event_source_report_review_schema_changed')
    review=body(review_response);contract.validate_review(review,report,evidence)
    if not review['ready'] or record['report']!=report or record['review']!=review or record['spans']!=spans:
        raise ValueError('event_source_report_response_integrity')
    if audit.get('continuation'):
        from .event_report_continuation_import import validate_retained
        validated=validate_retained(audit['continuation'],writer_request,writer_response,review_request,review_response)
        if record['generator_version']!=validated['generator_version'] or record['snapshot'].get('runtime_generation_at_import')!=audit['runtime_generation_at_import']:
            raise ValueError('event_source_report_continuation_identity_changed')
    elif writer_request['messages'][0]['content']!=contract.WRITER+contract.ATTACK_WRITER or review_request['messages'][0]['content']!=contract.REVIEWER+contract.ATTACK_REVIEWER:
        raise ValueError('event_source_report_prompt_integrity')
    return {**record,'resolved_mappings':resolved,'mapping_derivation':{'workflow':'optional-mapping-projection-v1',
            'input_version':audit['input_version'],'removed':[{'item_id':v['item_id'],'technique_id':v['mapping']['technique_id'],'reason':v['reason']} for v in removed]},'derivation':audit.get('continuation_lineage')}


def qualify_generator_refresh(conn, run_id):
    """Persist an immutable derivative without changing raw report/review or hold."""
    raw = _load(conn,run_id)
    projected = contract.publication_projection(raw["report"],raw["review"],contract.context(raw["snapshot"]))
    current_material(conn,run_id,lock=True,derivative=projected)
    conn.execute("INSERT INTO event_source_report_derivatives VALUES(%s,%s,%s) ON CONFLICT(run_id) DO NOTHING",
                 (run_id,contract.encode(projected),utc_now_iso()))
    conn.commit()
    current_material(conn,run_id)  # Recheck persisted artifact, including on reuse.
    return {"run_id":run_id,"projection_version":_version(projected),"removed_item_ids":projected["lineage"]["removed_item_ids"]}


def qualification(record, run_id):
    result = {"workflow":"event-source-report-qualification-v1","event_id":record["event_id"],
            "run_id":run_id,"source_version":record["source_version"],
            "report_version":_version(record["report"]),"review_version":_version(record["review"]),
            "policy":contract.WORKFLOW}
    if record.get("derivation"):
        result.update(workflow="event-source-report-derivative-qualification-v1",derivation=record["derivation"])
    return result


def submit(conn, run_id, *, factory=None, automatic=False):
    from .event_source_reports_v2 import enabled,check_scope
    if not enabled():
        raise PermissionError("event_source_report_disabled")
    initial = current_material(conn,run_id)
    check_scope(initial["event_id"])
    if automatic:
        from .event_source_reports_v2 import _fresh
        _fresh(conn,initial)
    factory = factory or connection_factory("SV_EVENT_APPROVAL_DB_URL")
    with factory() as authority:
        authority.execute("SET LOCAL statement_timeout='3s'")
        writable = authority.execute("""SELECT has_table_privilege(current_user,'event_public_pointers','INSERT,UPDATE,DELETE,TRUNCATE')
          OR has_any_column_privilege(current_user,'event_public_pointers','INSERT,UPDATE')
          OR has_table_privilege(current_user,'event_public_revisions','INSERT,UPDATE,DELETE,TRUNCATE')""").fetchone()[0]
        if writable:
            raise PermissionError("approval_admission_role_required")
        authority.execute("SELECT id FROM events WHERE id=%s FOR UPDATE NOWAIT",(initial["event_id"],))
        record = current_material(authority,run_id,lock=True)
        q = qualification(record,run_id)
        approval = {"workflow":APPROVAL_WORKFLOW,"event_id":record["event_id"],"run_id":run_id,
                    "qualification":q,"predecessor":record["predecessor"]}
        if automatic:
            from .event_source_report_pilot import policy,check_run
            p=policy()
            if not p:raise ValueError('event_source_report_pilot_required')
            approval['automatic_approval']=check_run(authority,record,run_id,p,publication=True)
        aid,qid = _version(approval),_version(q)
        prior = authority.execute("SELECT job_id FROM event_review_approvals WHERE approval_id=%s",(aid,)).fetchone()
        if prior:
            return {"status":"reused","job_id":prior[0],"approval_id":aid}
        pointer = authority.execute("SELECT revision_id FROM event_public_pointers WHERE event_id=%s",(record["event_id"],)).fetchone()
        if (pointer[0] if pointer else None)!=record["predecessor"]:
            raise ValueError("publication_predecessor_conflict")
        authority.execute("""INSERT INTO event_quote_qualifications(event_id,qualification_id,qualification_json,recorded_at)
          VALUES(%s,%s,%s,%s) ON CONFLICT(event_id,qualification_id) DO NOTHING""",
          (record["event_id"],qid,contract.encode(q),utc_now_iso()))
        job_id = enqueue_job(authority,JOB_TYPE,{"approval_id":aid},queue_name="fetch",max_attempts=1,commit=False)
        authority.execute("""INSERT INTO event_review_approvals(approval_id,event_id,qualification_id,approval_json,job_id,recorded_at)
          VALUES(%s,%s,%s,%s,%s,%s)""",(aid,record["event_id"],qid,contract.encode(approval),job_id,utc_now_iso()))
    return {"status":"queued","job_id":job_id,"approval_id":aid,"public_eligible":False}


def bundle_for(record,run_id,q,predecessor):
    packet = record["snapshot"]
    delta = packet.get("evidence_delta",{})
    notice = None
    if packet.get("update_reason") == "generator_upgrade":
        notice = (contract.GENERATOR_NOTICE if delta == {"baseline":"known","new":[],"changed":[],"removed":[]}
                  else "Report revised using the supplied source set; the prior evidence-change baseline is unavailable.")
    provenance = {"workflow":"event-report-revision-provenance-v1","update_reason":packet.get("update_reason","evidence_change"),
                  "evidence_delta":delta,"notice":notice,"derivation":record.get("derivation")}
    return {"workflow":contract.PUBLIC_WORKFLOW,"event_id":record["event_id"],"run_id":run_id,
            "report":record["report"],"spans":record["spans"],
            "sources":[{k:v for k,v in s.items() if k not in {"text","duplicates"}}
                       for s in contract.context(record["snapshot"])["sources"]],
            "coverage":contract.context(record["snapshot"])["coverage"],
            "qualification":q,"predecessor":predecessor,"revision_provenance":provenance,
            "attack":{"catalog":packet["attack_reference"]["catalog"],"mappings":record["resolved_mappings"],"derivation":record["mapping_derivation"]}}


def validate_bundle(bundle, *, event_id, expected_revision=None):
    required = {"workflow","event_id","run_id","report","spans","sources","coverage","qualification","predecessor","attack"}
    if set(bundle) not in (required,required|{"revision_provenance"}) or bundle["workflow"]!=contract.PUBLIC_WORKFLOW or bundle["event_id"]!=event_id:
        raise ValueError("event_source_report_bundle_invalid")
    q = bundle["qualification"]
    if q["event_id"]!=event_id or q["run_id"]!=bundle["run_id"] or q["report_version"]!=_version(bundle["report"]):
        raise ValueError("event_source_report_qualification_invalid")
    import jsonschema
    from .attack_catalog_runtime import catalog_for
    from .attack_catalog import generation_schema
    cat=catalog_for(bundle['attack']['catalog'])
    if bundle['attack']['catalog']!=cat.identity:raise ValueError('event_source_report_catalog_identity_mismatch')
    tids=[m['technique_id'] for i in bundle['report']['items'] for m in i['attack_mappings']]
    jsonschema.validate(bundle["report"],generation_schema([s["id"] for s in bundle["sources"]],tids,{}))
    resolved={}
    for item in bundle['report']['items']:
        resolved[item['id']]=[]
        if item['attack_mappings'] and item['section']!='attack_path':
            raise ValueError('attack_mapping_outside_attack_path')
        for mapping in item['attack_mappings']:
            # Public bundles omit full bodies. Original ID attribution is checked
            # against the immutable full input in current_material, not here.
            structural={**mapping,'origin':'analyst_applied'}
            value=cat.validate_mapping(structural,item=item,sources={s['id']:{'text':''} for s in bundle['sources']},allowed_ids=tids)
            value['origin']=mapping['origin'];resolved[item['id']].append(value)
    if resolved!=bundle['attack']['mappings']:raise ValueError('event_source_report_mapping_metadata_invalid')
    if set(bundle["spans"])!={x["id"] for x in bundle["report"]["items"]}:
        raise ValueError("event_source_report_spans_invalid")
    provenance = bundle.get("revision_provenance")
    if provenance:
        if provenance.get("workflow") != "event-report-revision-provenance-v1":
            raise ValueError("event_source_report_provenance_invalid")
        if provenance["update_reason"] == "generator_upgrade":
            if any(x["section"]=="what_changed" for x in bundle["report"]["items"]):
                raise ValueError("event_source_report_generated_upgrade_metadata")
            if provenance.get("derivation") != q.get("derivation"):
                raise ValueError("event_source_report_derivative_integrity")
            delta = provenance["evidence_delta"]
            expected = (contract.GENERATOR_NOTICE if delta == {"baseline":"known","new":[],"changed":[],"removed":[]}
                        else "Report revised using the supplied source set; the prior evidence-change baseline is unavailable.")
            if provenance["notice"] != expected:
                raise ValueError("event_source_report_revision_notice_invalid")
    revision = _version(bundle)
    if expected_revision and expected_revision!=revision:
        raise ValueError("event_publication_pointer_mismatch")
    return {**bundle,"revision_id":revision}


def run_approval(approval, *, qualification_id, factory=None):
    if approval.get("workflow")!=APPROVAL_WORKFLOW:
        raise ValueError("event_source_report_approval_invalid")
    factory = factory or connection_factory("SV_EVENT_PROMOTION_DB_URL")
    with factory() as conn:
        conn.execute("SET LOCAL statement_timeout='3s'")
        writable = conn.execute("""SELECT has_table_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE,DELETE,TRUNCATE')
          OR has_any_column_privilege(current_user,'event_quote_qualifications','INSERT,UPDATE')""").fetchone()[0]
        if writable:
            raise PermissionError("qualification_read_only_role_required")
        guarded = conn.execute("""SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='event_quote_qualifications'::regclass
          AND tgname='event_qualification_guard' AND tgenabled IN ('O','A') AND NOT tgisinternal)""").fetchone()[0]
        if not guarded:
            raise ValueError("qualification_revocation_guard_required")
        conn.execute("SELECT id FROM events WHERE id=%s FOR UPDATE NOWAIT",(approval["event_id"],))
        record = current_material(conn,approval["run_id"],lock=True)
        if 'automatic_approval' in approval:
            from .event_source_report_pilot import policy,check_run
            p=policy()
            if not p or approval['automatic_approval']!=check_run(conn,record,approval['run_id'],p,publication=True):
                raise ValueError('event_source_report_automatic_approval_changed')
        q = qualification(record,approval["run_id"])
        row = conn.execute("SELECT qualification_json,revoked_at FROM event_quote_qualifications WHERE event_id=%s AND qualification_id=%s",
                           (approval["event_id"],qualification_id)).fetchone()
        if not row or row[1] is not None or json.loads(row[0])!=q or approval["qualification"]!=q:
            raise ValueError("qualification_unavailable")
        bundle = bundle_for(record,approval["run_id"],q,approval["predecessor"])
        revision = validate_bundle(bundle,event_id=approval["event_id"])["revision_id"]
        ptr = conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id=%s FOR UPDATE NOWAIT",(approval["event_id"],)).fetchone()
        if (ptr[0] if ptr else None) not in {approval["predecessor"],revision}:
            raise ValueError("publication_predecessor_conflict")
        conn.execute("""INSERT INTO event_public_revisions(event_id,revision_id,qualification_id,predecessor,bundle_json,recorded_at)
          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(event_id,revision_id) DO NOTHING""",
          (approval["event_id"],revision,qualification_id,approval["predecessor"],contract.encode(bundle),utc_now_iso()))
        conn.execute("""INSERT INTO event_public_pointers(event_id,revision_id,updated_at) VALUES(%s,%s,%s)
          ON CONFLICT(event_id) DO UPDATE SET revision_id=EXCLUDED.revision_id,updated_at=EXCLUDED.updated_at""",
          (approval["event_id"],revision,utc_now_iso()))
    return {"status":"promoted","event_id":approval["event_id"],"revision_id":revision,
            "publication_status":"awaiting_export","public_eligible":False}
