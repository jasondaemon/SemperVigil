"""Single-attempt private composition jobs for accepted Event ledgers."""
import os
import time

from . import event_composition as composition
from .investigation import _version
from .services.ai_service import get_active_profile_for_stage, get_model, get_provider, load_provider_secret
from .storage import enqueue_job, insert_llm_run, update_job_result

JOB_TYPE = "event_ledger_compose"
PARAMS = {"temperature": 0, "max_tokens": 4096}


def require_enabled() -> None:
    if os.environ.get("SV_EVENT_LEDGER_COMPOSITION_ENABLED", "0") != "1":
        raise PermissionError("event_composition_disabled")


def configuration(conn) -> tuple[dict, dict, dict, str]:
    profile = get_active_profile_for_stage(conn, "article_context_pack")
    if isinstance(profile, tuple):
        profile = profile[0]
    if not profile:
        raise ValueError("event_composition_profile_missing")
    model = get_model(conn, profile["primary_model_id"]) or {}
    provider = get_provider(conn, profile["primary_provider_id"]) or {}
    if (not str(model.get("model_name", "")).startswith("ollama/")
            or provider.get("type") not in {"openai_compatible", "ollama_native"}
            or not provider.get("base_url")):
        raise ValueError("event_composition_requires_existing_local_model")
    generation = _version({"workflow": composition.WORKFLOW, "params": PARAMS,
        "profile": profile["id"], "model": model["id"], "model_name": model["model_name"],
        "provider": provider["id"], "base_url": provider["base_url"],
        "timeout_s": provider.get("timeout_s"),
        "revisions": [str(x.get("updated_at") or "") for x in (profile, model, provider)],
        "system": composition.SYSTEM_PROMPT, "schema": composition.schema()})
    return profile, model, provider, generation


def ledger_revision(conn, revision_id: str) -> dict:
    from .event_ledger import get_revision
    try:
        return get_revision(conn, revision_id, require_status="accepted")
    except ValueError as exc:
        raise ValueError("event_composition_ledger_missing") from exc


def submit(conn, revision_id: str) -> str:
    require_enabled()
    _, _, _, generation = configuration(conn)
    ledger = ledger_revision(conn, revision_id)
    req = composition.request(ledger, generation)
    key = "event-ledger-compose:" + _version({"revision": revision_id,
                                               "request": req["request_version"]})
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
    existing = conn.execute(
        "SELECT id FROM jobs WHERE job_type=%s AND dedupe_key=%s ORDER BY requested_at LIMIT 1",
        (JOB_TYPE, key),
    ).fetchone()
    if existing:
        conn.commit()
        return existing[0]
    payload = {"workflow": composition.WORKFLOW, "ledger_revision_id": revision_id,
               "generation": generation, "request_version": req["request_version"]}
    return enqueue_job(conn, JOB_TYPE, payload, priority=-10, queue_name="llm_local",
                       max_attempts=1, dedupe_key=key)


def complete(conn, job_id: str, req: dict) -> str:
    from .llm.router import _call_ollama_native, _http_request, _auth_headers, _join_url, _read_openai
    require_enabled()
    profile, model, provider, generation = configuration(conn)
    if generation != req["generation"]:
        raise ValueError("event_composition_configuration_changed")
    secret = load_provider_secret(conn, provider["id"])
    messages = [{"role": "system", "content": composition.SYSTEM_PROMPT},
                {"role": "user", "content": req["input"]}]
    response_format = {"type": "json_schema", "json_schema": {
        "name": "event_ledger_composition", "strict": True, "schema": composition.schema()}}
    started, raw, error = time.monotonic(), "", None
    try:
        context = {"stage": JOB_TYPE, "job_id": job_id}
        if provider["type"] == "ollama_native":
            raw = _call_ollama_native(provider["base_url"], model["model_name"], messages,
                                      PARAMS, provider, context=context,
                                      response_format=response_format)
        else:
            payload = {"model": model["model_name"], **PARAMS, "messages": messages,
                       "response_format": response_format}
            raw = _read_openai(_http_request("POST", _join_url(provider["base_url"], "/chat/completions"),
                               _auth_headers(provider["type"], secret), payload, provider,
                               context=context))
        if configuration(conn)[3] != generation:
            raise ValueError("event_composition_configuration_changed")
        return raw
    except Exception as exc:
        error = type(exc).__name__
        raise
    finally:
        insert_llm_run(conn, job_id=job_id, provider_id=provider["id"], model_id=model["id"],
            prompt_name="event-ledger-composition", input_chars=len(req["input"]),
            output_chars=len(raw), latency_ms=int((time.monotonic()-started)*1000),
            ok=error is None, error=error)


def run(conn, job, *, generate=None) -> dict:
    require_enabled()
    payload = job.payload or {}
    if (job.job_type != JOB_TYPE or job.result or job.attempt_count != 0 or job.max_attempts != 1
            or job.queue_name != "llm_local" or job.status != "running"
            or set(payload) != {"workflow", "ledger_revision_id", "generation", "request_version"}
            or payload["workflow"] != composition.WORKFLOW):
        raise ValueError("event_composition_invalid_or_replayed_job")
    ledger = ledger_revision(conn, payload["ledger_revision_id"])
    req = composition.request(ledger, payload["generation"])
    if req["request_version"] != payload["request_version"]:
        raise ValueError("event_composition_baseline_changed")
    result = {"workflow": composition.WORKFLOW, "ledger_revision_id": ledger["revision_id"],
              "generation": payload["generation"], "request_version": req["request_version"],
              "attempts": 1, "status": "started", "public_eligible": False}
    if not update_job_result(conn, job.id, result):
        raise ValueError("event_composition_job_not_running")
    raw = (generate or (lambda request: complete(conn, job.id, request)))({
        "generation": payload["generation"], "input": req["input"]})
    current = ledger_revision(conn, payload["ledger_revision_id"])
    if current["revision_id"] != ledger["revision_id"] or not current["lineage_current"]:
        raise ValueError("event_composition_baseline_changed")
    record = composition.validate(raw.encode(), current, payload["generation"])
    result.update(status="review_required", composition_id=composition.store_unreviewed(conn, record),
                  output_chars=len(raw))
    if not update_job_result(conn, job.id, result):
        raise ValueError("event_composition_job_not_running")
    return result
