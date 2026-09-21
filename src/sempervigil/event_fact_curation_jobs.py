"""Single-attempt hosted curation jobs for Event-scoped article facts."""
import json
import os
import time

from . import event_fact_curation as curation
from .incident_candidates import projection
from .investigation import _version
from .services.ai_service import get_model, get_provider, load_provider_secret
from .storage import enqueue_job, get_article_by_id, insert_llm_run, update_job_result

JOB_TYPE = "event_fact_curate"
MODEL_NAME = "gpt-5.6-luna"
PARAMS = {"max_completion_tokens": 2400, "reasoning_effort": "low"}
REVIEWER = "policy:event-fact-curation-v2"


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
        raise ValueError("event_fact_curation_openai_model_missing")
    provider, model = get_provider(conn, row[0]) or {}, get_model(conn, row[1]) or {}
    if not provider.get("base_url") or model.get("model_name") != MODEL_NAME:
        raise ValueError("event_fact_curation_openai_model_invalid")
    generation = _version({"workflow": curation.WORKFLOW, "params": PARAMS,
        "model": model["id"], "provider": provider["id"], "base_url": provider["base_url"],
        "timeout_s": provider.get("timeout_s"), "system": curation.SYSTEM_PROMPT})
    return model, provider, generation


def material(conn, event_id: str, revision_id: str) -> tuple[dict, dict, dict, dict]:
    case = conn.execute(
        "SELECT snapshot_version,snapshot_json,status FROM event_reassessment_cases WHERE event_id=%s",
        (event_id,),
    ).fetchone()
    row = conn.execute(
        "SELECT article_id,status,evidence_json,source_version FROM article_evidence_revisions WHERE revision_id=%s",
        (revision_id,),
    ).fetchone()
    if not case or case[2] != "active" or not row or row[1] not in {"unreviewed", "held", "accepted"}:
        raise ValueError("event_fact_curation_material_unavailable")
    snapshot = json.loads(case[1])
    source = next((item for item in snapshot["articles"] if item["article_id"] == row[0]), None)
    if not source or source.get("source_version") != row[3]:
        raise ValueError("event_fact_curation_source_stale")
    article = get_article_by_id(conn, int(row[0]))
    if not article:
        raise ValueError("event_fact_curation_source_missing")
    evidence = json.loads(row[2])
    evidence["revision_id"] = revision_id
    return snapshot["event"], article, evidence, {"status": row[1], "snapshot_version": case[0]}


def submit(conn, event_id: str, revision_id: str) -> str:
    require_enabled()
    _, _, generation = configuration(conn)
    event, article, evidence, state = material(conn, event_id, revision_id)
    signals = projection(evidence, str(article.get("title") or ""))
    anchors = set(signals["supporting_fact_ids"] if signals else [])
    req = curation.request(event, article, evidence, generation, anchors)
    key = "event-fact-curate:" + _version({"request": req["request_version"],
                                             "snapshot": state["snapshot_version"]})
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
    prior = conn.execute("SELECT id FROM jobs WHERE job_type=%s AND dedupe_key=%s ORDER BY requested_at LIMIT 1",
                         (JOB_TYPE, key)).fetchone()
    if prior:
        conn.commit()
        return prior[0]
    payload = {"workflow": curation.WORKFLOW, "event_id": event_id,
               "revision_id": revision_id, "generation": generation,
               "request_version": req["request_version"],
               "snapshot_version": state["snapshot_version"]}
    return enqueue_job(conn, JOB_TYPE, payload, priority=-10, queue_name="openai",
                       max_attempts=1, dedupe_key=key)


def complete(conn, job_id: str, req: dict) -> str:
    from .llm.router import _http_request, _auth_headers, _join_url, _read_openai
    model, provider, generation = configuration(conn)
    if generation != req["generation"]:
        raise ValueError("event_fact_curation_configuration_changed")
    payload = {"model": model["model_name"], **PARAMS,
        "messages": [{"role": "system", "content": curation.SYSTEM_PROMPT},
                     {"role": "user", "content": req["input"]}],
        "response_format": {"type": "json_schema", "json_schema": {
            "name": "event_fact_curation", "strict": True, "schema": req["schema"]}}}
    started, raw, error = time.monotonic(), "", None
    try:
        raw = _read_openai(_http_request("POST", _join_url(provider["base_url"], "/chat/completions"),
            _auth_headers(provider["type"], load_provider_secret(conn, provider["id"])), payload,
            provider, context={"stage": JOB_TYPE, "job_id": job_id}))
        if configuration(conn)[2] != generation:
            raise ValueError("event_fact_curation_configuration_changed")
        return raw
    except Exception as exc:
        error = type(exc).__name__
        raise
    finally:
        insert_llm_run(conn, job_id=job_id, provider_id=provider["id"], model_id=model["id"],
            prompt_name="event-fact-curation", input_chars=len(req["input"]),
            output_chars=len(raw), latency_ms=int((time.monotonic() - started) * 1000),
            ok=error is None, error=error)


def _apply(conn, event_id: str, revision_id: str, result: dict) -> dict:
    from .article_evidence_store import review as review_evidence
    from .incident_candidates import project, review as review_candidate, refine_selection

    _, _, _, state = material(conn, event_id, revision_id)
    if result["evidence_verdict"] == "hold":
        if state["status"] in {"unreviewed", "held"}:
            review_evidence(conn, revision_id, "hold", reason=result["reason"], reviewer=REVIEWER)
        return {"status": "held", "reason": result["reason"]}
    if state["status"] in {"unreviewed", "held"}:
        review_evidence(conn, revision_id, "accept", reason="", reviewer=REVIEWER)
    projected = project(conn, revision_id, event_id=event_id)
    if projected["status"] == "skipped":
        return {"status": "no_incident_signal"}
    candidate_id = projected["candidate_id"]
    row = conn.execute(
        "SELECT status,selected_fact_ids_json FROM incident_candidates WHERE candidate_id=%s",
        (candidate_id,),
    ).fetchone()
    decision = result["incident_verdict"]
    if row[0] == "enrolled":
        if decision != "same_incident":
            raise ValueError("event_fact_curation_enrollment_conflict")
        refined = refine_selection(conn, candidate_id, result["selected_fact_ids"],
                                   reason=result["reason"], reviewer=REVIEWER)
        return {"status": "enrolled", "candidate_id": candidate_id,
                "selected_fact_ids": refined["selected_fact_ids"]}
    if row[0] in {"rejected", "held"} and decision == "same_incident":
        if row[0] == "rejected":
            raise ValueError("event_fact_curation_candidate_already_rejected")
    if row[0] in {"suggested", "held"}:
        mapped = {"same_incident": "enroll", "unrelated": "reject", "ambiguous": "hold"}[decision]
        reviewed = review_candidate(conn, candidate_id, mapped, reason=result["reason"], reviewer=REVIEWER,
                                    selected_fact_ids=result["selected_fact_ids"] if mapped == "enroll" else None)
        return {"status": reviewed["status"], "candidate_id": candidate_id,
                "selected_fact_ids": reviewed["selected_fact_ids"]}
    return {"status": row[0], "candidate_id": candidate_id}


def run(conn, job, *, generate=None) -> dict:
    require_enabled()
    payload = job.payload or {}
    required = {"workflow", "event_id", "revision_id", "generation", "request_version", "snapshot_version"}
    if (job.job_type != JOB_TYPE or job.result or job.attempt_count != 0 or job.max_attempts != 1
            or job.queue_name != "openai" or job.status != "running" or set(payload) != required
            or payload["workflow"] != curation.WORKFLOW):
        raise ValueError("event_fact_curation_invalid_or_replayed_job")
    event, article, evidence, state = material(conn, payload["event_id"], payload["revision_id"])
    if state["snapshot_version"] != payload["snapshot_version"]:
        raise ValueError("event_fact_curation_baseline_changed")
    signals = projection(evidence, str(article.get("title") or ""))
    anchors = set(signals["supporting_fact_ids"] if signals else [])
    req = curation.request(event, article, evidence, payload["generation"], anchors)
    if req["request_version"] != payload["request_version"]:
        raise ValueError("event_fact_curation_baseline_changed")
    state_result = {"workflow": curation.WORKFLOW, "event_id": payload["event_id"],
                    "revision_id": payload["revision_id"], "status": "started",
                    "public_eligible": False}
    if not update_job_result(conn, job.id, state_result):
        raise ValueError("event_fact_curation_job_not_running")
    raw = (generate or (lambda request: complete(conn, job.id, request)))(req)
    result = curation.validate(raw.encode(), req, anchors)
    state_result.update(status="curated", curation=result, raw=raw[:curation.MAX_OUTPUT_BYTES])
    if not update_job_result(conn, job.id, state_result):
        raise ValueError("event_fact_curation_job_not_running")
    state_result.update(application=_apply(conn, payload["event_id"], payload["revision_id"], result),
                        status="applied")
    if not update_job_result(conn, job.id, state_result):
        raise ValueError("event_fact_curation_job_not_running")
    return state_result
