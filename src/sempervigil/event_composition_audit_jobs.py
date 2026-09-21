"""Single-attempt hosted support audits for generated Event compositions."""
import json
import os
import time

from . import event_composition_audit as audit
from .investigation import _version
from .services.ai_service import get_model, get_provider, load_provider_secret
from .storage import enqueue_job, insert_llm_run, update_job_result

JOB_TYPE = "event_composition_audit"
MODEL_NAME = "gpt-5.6-luna"
PARAMS = {"max_completion_tokens": 4800, "reasoning_effort": "low"}
REVIEWER = "policy:event-composition-audit-v1"


def require_enabled() -> None:
    if os.environ.get("SV_EVENT_REASSESSMENT_AUTOMATION_ENABLED", "0") != "1":
        raise PermissionError("event_reassessment_automation_disabled")


def configuration(conn):
    row = conn.execute(
        """SELECT p.id,m.id FROM llm_providers p JOIN llm_models m ON m.provider_id=p.id
             WHERE lower(p.name)='openai' AND lower(p.type)='openai_compatible'
               AND p.is_enabled=1 AND m.is_enabled=1 AND m.model_name=%s
             ORDER BY p.name,m.id LIMIT 1""", (MODEL_NAME,),
    ).fetchone()
    if not row:
        raise ValueError("event_composition_audit_openai_model_missing")
    provider, model = get_provider(conn, row[0]) or {}, get_model(conn, row[1]) or {}
    if not provider.get("base_url") or model.get("model_name") != MODEL_NAME:
        raise ValueError("event_composition_audit_openai_model_invalid")
    generation = _version({"workflow": audit.WORKFLOW, "params": PARAMS,
        "model": model.get("id"), "provider": provider.get("id"),
        "base_url": provider.get("base_url"), "system": audit.SYSTEM_PROMPT})
    return model, provider, generation


def material(conn, composition_id: str) -> tuple[dict, dict, str]:
    row = conn.execute(
        """SELECT c.status,c.composition_json,r.status,r.ledger_json
             FROM event_ledger_compositions c JOIN event_ledger_revisions r
               ON r.revision_id=c.ledger_revision_id
            WHERE c.composition_id=%s""", (composition_id,),
    ).fetchone()
    if not row or row[0] not in {"unreviewed", "held"} or row[2] != "accepted":
        raise ValueError("event_composition_audit_material_unavailable")
    return json.loads(row[1]), json.loads(row[3]), row[0]


def submit(conn, composition_id: str) -> str:
    require_enabled()
    _, _, generation = configuration(conn)
    composition, ledger, _ = material(conn, composition_id)
    req = audit.request(composition_id, composition, ledger, generation)
    key = "event-composition-audit:" + req["request_version"]
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
    prior = conn.execute("SELECT id FROM jobs WHERE job_type=%s AND dedupe_key=%s ORDER BY requested_at LIMIT 1",
                         (JOB_TYPE, key)).fetchone()
    if prior:
        conn.commit()
        return prior[0]
    payload = {"workflow": audit.WORKFLOW, "composition_id": composition_id,
               "generation": generation, "request_version": req["request_version"]}
    return enqueue_job(conn, JOB_TYPE, payload, priority=-10, queue_name="openai",
                       max_attempts=1, dedupe_key=key)


def complete(conn, job_id: str, req: dict) -> str:
    from .llm.router import _http_request, _auth_headers, _join_url, _read_openai
    model, provider, generation = configuration(conn)
    if generation != req["generation"]:
        raise ValueError("event_composition_audit_configuration_changed")
    payload = {"model": model["model_name"], **PARAMS,
        "messages": [{"role": "system", "content": audit.SYSTEM_PROMPT},
                     {"role": "user", "content": req["input"]}],
        "response_format": {"type": "json_schema", "json_schema": {
            "name": "event_composition_audit", "strict": True, "schema": req["schema"]}}}
    started, raw, error = time.monotonic(), "", None
    try:
        raw = _read_openai(_http_request("POST", _join_url(provider["base_url"], "/chat/completions"),
            _auth_headers(provider["type"], load_provider_secret(conn, provider["id"])), payload,
            provider, context={"stage": JOB_TYPE, "job_id": job_id}))
        if configuration(conn)[2] != generation:
            raise ValueError("event_composition_audit_configuration_changed")
        return raw
    except Exception as exc:
        error = type(exc).__name__
        raise
    finally:
        insert_llm_run(conn, job_id=job_id, provider_id=provider["id"], model_id=model["id"],
            prompt_name="event-composition-audit", input_chars=len(req["input"]),
            output_chars=len(raw), latency_ms=int((time.monotonic() - started) * 1000),
            ok=error is None, error=error)


def run(conn, job, *, generate=None) -> dict:
    require_enabled()
    payload = job.payload or {}
    if (job.job_type != JOB_TYPE or job.result or job.attempt_count != 0 or job.max_attempts != 1
            or job.queue_name != "openai" or job.status != "running"
            or set(payload) != {"workflow", "composition_id", "generation", "request_version"}
            or payload["workflow"] != audit.WORKFLOW):
        raise ValueError("event_composition_audit_invalid_or_replayed_job")
    composition, ledger, _ = material(conn, payload["composition_id"])
    req = audit.request(payload["composition_id"], composition, ledger, payload["generation"])
    if req["request_version"] != payload["request_version"]:
        raise ValueError("event_composition_audit_baseline_changed")
    result = {"workflow": audit.WORKFLOW, "composition_id": payload["composition_id"],
              "status": "started", "public_eligible": False}
    if not update_job_result(conn, job.id, result):
        raise ValueError("event_composition_audit_job_not_running")
    raw = (generate or (lambda request: complete(conn, job.id, request)))(req)
    decision = audit.validate(raw.encode(), req)
    result.update(status="audited", audit=decision, raw=raw[:audit.MAX_OUTPUT_BYTES])
    if not update_job_result(conn, job.id, result):
        raise ValueError("event_composition_audit_job_not_running")
    from .event_composition import review
    if decision["ready"]:
        applied = review(conn, payload["composition_id"], "accept", reason="", reviewer=REVIEWER)
        remediation = None
    else:
        failures = [row for row in decision["audits"] if row["verdict"] != "supported"]
        reason = "; ".join(f'{row["id"]}: {row["verdict"]} - {row["reason"]}' for row in failures)[:1000]
        applied = review(conn, payload["composition_id"], "hold", reason=reason, reviewer=REVIEWER)
        remediation = None
        from .event_composition_jobs import configuration as composition_configuration
        if composition.get("generation_version") == composition_configuration(conn)[2]:
            item_sections = {item["id"]: item["section"]
                             for item in json.loads(req["input"])["items"]}
            overview_failed = any(item_sections[row["id"]] == "overview"
                                  for row in failures)
            if not overview_failed:
                from .event_composition import store_unreviewed
                filtered = audit.filtered_record(
                    payload["composition_id"], composition, ledger, decision)
                filtered_id = store_unreviewed(conn, filtered)
                accepted = review(conn, filtered_id, "accept", reason="", reviewer=REVIEWER)
                remediation = {"status": "accepted", "composition_id": filtered_id,
                               "application": accepted,
                               "workflow": audit.FILTER_WORKFLOW}
            else:
                try:
                    from .event_composition_repair_jobs import submit
                    remediation = {"status": "queued",
                                   "job_id": submit(conn, payload["composition_id"], decision),
                                   "workflow": "event-composition-repair-v1"}
                except ValueError as exc:
                    remediation = {"status": "held", "reason": str(exc),
                                   "workflow": "event-composition-repair-v1"}
    result.update(status="applied", application=applied, remediation=remediation)
    if not update_job_result(conn, job.id, result):
        raise ValueError("event_composition_audit_job_not_running")
    return result
