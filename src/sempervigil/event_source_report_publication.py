"""Source-report adapter to the existing separated approval/promotion boundary."""
import json

from . import event_report_contract as contract
from .event_approval import connection_factory, JOB_TYPE
from .event_source_reports import _load, snapshot
from .investigation import _version
from .storage import enqueue_job
from .utils import utc_now_iso

APPROVAL_WORKFLOW = "event-source-report-approval-v1"


def current_material(conn, run_id, *, lock=False, published=False, derivative=None):
    if lock:
        conn.execute("SELECT run_id FROM event_source_report_runs WHERE run_id=%s FOR SHARE NOWAIT", (run_id,))
    record = _load(conn,run_id)
    if not record["review"]:
        raise ValueError("event_source_report_not_reviewed")
    if published:
        from .event_report_retained_evidence import require_retained
        require_retained(conn, record, lock=lock)
    else:
        current = snapshot(conn,record["event_id"],lock=lock)
        if current["source_version"] != record["source_version"]:
            raise ValueError("event_source_report_sources_changed")
    packet = contract.context(record["snapshot"])
    spans = contract.validate(record["report"],packet)
    # Preserve exact legacy span storage/bundle identity. Only the newly added
    # anchor may be absent; offsets, source ID and original quote still must match.
    if all(set(s)=={'source_id','start','end','quote'} for values in record['spans'].values() for s in values):
        spans={k:[{key:value for key,value in s.items() if key!='passage_anchor'} for s in values]
               for k,values in spans.items()}
    if spans != record["spans"]:
        raise ValueError("event_source_report_span_integrity")
    contract.validate_review(record["review"],record["report"],packet)
    # Verify persisted model output matches the accepted report; no unchecked
    # report_json edits may substitute for the independently reviewed response.
    calls = conn.execute("""SELECT phase,response_json,request_json FROM event_source_report_calls
        WHERE run_id=%s AND status='completed' ORDER BY ordinal""",(run_id,)).fetchall()
    responses = {phase:json.loads(raw) for phase,raw,_ in calls}
    writer = next((json.loads(req) for phase,_,req in calls if phase == 'writer'), None)
    original = json.loads(writer['messages'][1]['content']) if writer else {}
    # Authorized legacy derivatives may change application-owned revision
    # metadata. Bind immutable evidence, not that later continuity metadata.
    evidence_keys = ('event_id', 'title', 'sources', 'membership',
                     'excluded_article_ids', 'source_version')
    if not writer or any(original.get(k) != packet.get(k) for k in evidence_keys):
        raise ValueError('event_source_report_input_integrity')
    def body(phase):
        return json.loads(responses[phase]['choices'][0]['message']['content'])
    write_phase = "correction" if "correction" in responses else "writer"
    review_phase = "verification" if "correction" in responses else "review"
    reconstructed = body(write_phase) if write_phase in responses else None
    if write_phase=='correction' and reconstructed is not None and set(reconstructed)=={'items'}:
        if 'writer' not in responses or 'review' not in responses:
            raise ValueError('event_source_report_response_integrity')
        flagged={issue['item_id'] for issue in body('review')['issues']}
        if not flagged and body('review')['ready']:
            allowance=conn.execute('SELECT audit_json FROM event_source_report_allowances WHERE run_id=%s',(run_id,)).fetchone()
            if not allowance:raise ValueError('event_source_report_response_integrity')
            audit=json.loads(allowance[0]);manual=audit.get('manual_issues',[])
            contract.validate_review({'ready':False,'issues':manual,'locator_warnings':[]},body('writer'),packet)
            if audit.get('report_version')!=_version(body('writer')) or audit.get('review_version')!=_version(body('review')):
                raise ValueError('event_source_report_response_integrity')
            flagged={issue['item_id'] for issue in manual}
        reconstructed=contract.apply_correction(body('writer'),reconstructed,flagged)
    for phase,actual,expected in ((write_phase,reconstructed,record['report']),
                                  (review_phase,body(review_phase) if review_phase in responses else None,record['review'])):
        if phase not in responses or actual!=expected:
            raise ValueError("event_source_report_response_integrity")
    saved = conn.execute("SELECT projection_json FROM event_source_report_derivatives WHERE run_id=%s",(run_id,)).fetchone()
    derivative = derivative if derivative is not None else (json.loads(saved[0]) if saved else None)
    if derivative is not None:
        expected = contract.publication_projection(record["report"],record["review"],packet)
        if all(set(s)=={'source_id','start','end','quote'} for values in derivative.get('spans',{}).values() for s in values):
            expected['spans']={k:[{key:value for key,value in s.items() if key!='passage_anchor'} for s in values]
                               for k,values in expected['spans'].items()}
        if derivative != expected:
            raise ValueError("event_source_report_derivative_integrity")
        reason = conn.execute("SELECT reason FROM event_source_report_runs WHERE run_id=%s",(run_id,)).fetchone()[0]
        allowed_hold = reason == "manual_quality_revision_metadata" or reason in {
            "manual_quality_false_evidence_novelty_"+item for item in expected["lineage"]["removed_item_ids"]}
        if record["status"] != "accepted" and not (record["status"] == "held" and allowed_hold):
            raise ValueError("event_source_report_derivative_hold_ineligible")
        record = {**record,"report":expected["report"],"review":expected["review_projection"],
                  "spans":expected["spans"],"derivation":expected["lineage"]}
    elif record["status"] != "accepted" or not record["review"]["ready"]:
        raise ValueError("event_source_report_not_accepted")
    return record


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
    from .event_source_reports import enabled,check_scope
    if not enabled():
        raise PermissionError("event_source_report_disabled")
    initial = current_material(conn,run_id)
    check_scope(initial["event_id"])
    if automatic:
        from .event_source_reports import _fresh
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
            "qualification":q,"predecessor":predecessor,"revision_provenance":provenance}


def validate_bundle(bundle, *, event_id, expected_revision=None):
    required = {"workflow","event_id","run_id","report","spans","sources","coverage","qualification","predecessor"}
    if set(bundle) not in (required,required|{"revision_provenance"}) or bundle["workflow"]!=contract.PUBLIC_WORKFLOW or bundle["event_id"]!=event_id:
        raise ValueError("event_source_report_bundle_invalid")
    q = bundle["qualification"]
    if q["event_id"]!=event_id or q["run_id"]!=bundle["run_id"] or q["report_version"]!=_version(bundle["report"]):
        raise ValueError("event_source_report_qualification_invalid")
    import jsonschema
    jsonschema.validate(bundle["report"],contract.schema([s["id"] for s in bundle["sources"]]))
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
