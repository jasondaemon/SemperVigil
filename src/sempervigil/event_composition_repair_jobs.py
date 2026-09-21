"""Single-attempt hosted corrective rewrites for audited Event compositions."""
import os
import time

from . import event_composition_repair as repair
from .investigation import _version
from .services.ai_service import get_model, get_provider, load_provider_secret
from .storage import enqueue_job, insert_llm_run, update_job_result

JOB_TYPE = "event_composition_repair"
MODEL_NAME = "gpt-5.6-sol"
PARAMS = {"max_completion_tokens": 1800, "reasoning_effort": "low"}


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
        raise ValueError("event_composition_repair_openai_model_missing")
    provider, model = get_provider(conn, row[0]) or {}, get_model(conn, row[1]) or {}
    if not provider.get("base_url") or model.get("model_name") != MODEL_NAME:
        raise ValueError("event_composition_repair_openai_model_invalid")
    generation = _version({"workflow": repair.WORKFLOW, "params": PARAMS,
        "model": model.get("id"), "provider": provider.get("id"),
        "base_url": provider.get("base_url"), "system": repair.SYSTEM_PROMPT})
    return model, provider, generation


def material(conn, composition_id: str):
    import json
    row = conn.execute(
        """SELECT composition_json,ledger_revision_id,status,reviewed_by
             FROM event_ledger_compositions WHERE composition_id=%s""", (composition_id,),
    ).fetchone()
    if not row or row[2] != "held" or row[3] != "policy:event-composition-audit-v1":
        raise ValueError("event_composition_repair_material_unavailable")
    from .event_ledger import get_revision
    return json.loads(row[0]), get_revision(conn, row[1], require_status="accepted")


def submit(conn, composition_id: str, decision: dict) -> str:
    require_enabled()
    _, _, generation = configuration(conn)
    composition, ledger = material(conn, composition_id)
    req = repair.request(composition_id, composition, ledger, decision, generation)
    key = "event-composition-repair:" + req["request_version"]
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
    prior = conn.execute("SELECT id FROM jobs WHERE job_type=%s AND dedupe_key=%s",
                         (JOB_TYPE, key)).fetchone()
    if prior:
        conn.commit()
        return prior[0]
    payload = {"workflow": repair.WORKFLOW, "composition_id": composition_id,
               "audit": decision, "generation": generation,
               "request_version": req["request_version"]}
    return enqueue_job(conn, JOB_TYPE, payload, priority=-10, queue_name="openai",
                       max_attempts=1, dedupe_key=key)


def complete(conn, job_id: str, req: dict) -> str:
    from .llm.router import _http_request, _auth_headers, _join_url, _read_openai
    model, provider, generation = configuration(conn)
    if generation != req["generation"]:
        raise ValueError("event_composition_repair_configuration_changed")
    payload = {"model": model["model_name"], **PARAMS,
        "messages": [{"role": "system", "content": repair.SYSTEM_PROMPT},
                     {"role": "user", "content": req["input"]}],
        "response_format": {"type": "json_schema", "json_schema": {
            "name": "event_composition_repair", "strict": True, "schema": req["schema"]}}}
    started, raw, error = time.monotonic(), "", None
    try:
        raw = _read_openai(_http_request("POST", _join_url(provider["base_url"], "/chat/completions"),
            _auth_headers(provider["type"], load_provider_secret(conn, provider["id"])), payload,
            provider, context={"stage": JOB_TYPE, "job_id": job_id}))
        if configuration(conn)[2] != generation:
            raise ValueError("event_composition_repair_configuration_changed")
        return raw
    except Exception as exc:
        error = type(exc).__name__
        raise
    finally:
        insert_llm_run(conn, job_id=job_id, provider_id=provider["id"], model_id=model["id"],
            prompt_name="event-composition-repair", input_chars=len(req["input"]),
            output_chars=len(raw), latency_ms=int((time.monotonic()-started)*1000),
            ok=error is None, error=error)


def run(conn, job, *, generate=None) -> dict:
    require_enabled()
    payload = job.payload or {}
    if (job.job_type != JOB_TYPE or job.result or job.attempt_count != 0 or job.max_attempts != 1
            or job.queue_name != "openai" or job.status != "running"
            or set(payload) != {"workflow", "composition_id", "audit", "generation", "request_version"}
            or payload["workflow"] != repair.WORKFLOW):
        raise ValueError("event_composition_repair_invalid_or_replayed_job")
    composition, ledger = material(conn, payload["composition_id"])
    req = repair.request(payload["composition_id"], composition, ledger,
                         payload["audit"], payload["generation"])
    if req["request_version"] != payload["request_version"]:
        raise ValueError("event_composition_repair_baseline_changed")
    result = {"workflow": repair.WORKFLOW, "composition_id": payload["composition_id"],
              "status": "started", "public_eligible": False}
    if not update_job_result(conn, job.id, result):
        raise ValueError("event_composition_repair_job_not_running")
    raw = (generate or (lambda request: complete(conn, job.id, request)))(req)
    record = repair.validate(raw.encode(), req, composition, ledger)
    from .event_composition import store_unreviewed
    result.update(status="review_required", repaired_composition_id=store_unreviewed(conn, record),
                  raw=raw[:repair.MAX_OUTPUT_BYTES])
    if not update_job_result(conn, job.id, result):
        raise ValueError("event_composition_repair_job_not_running")
    return result
