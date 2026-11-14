"""Bounded private Qwen comparison; never updates articles or publication state."""

import os
import time

from . import article_applicability as contract
from .investigation import _version
from .services.ai_service import get_active_profile_for_stage, get_model, get_provider
from .storage import enqueue_job, get_article_by_id, insert_llm_run, update_job_result

JOB_TYPE = "article_applicability_private"
PARAMS = {"temperature": 0, "max_tokens": 2048}


def require_enabled() -> None:
    if os.environ.get("SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED", "0") != "1":
        raise PermissionError("article_applicability_review_disabled")


def configuration(conn) -> tuple[dict, dict, dict, str]:
    profile = get_active_profile_for_stage(conn, "article_context_pack")
    if isinstance(profile, tuple):
        profile = profile[0]
    if not profile:
        raise ValueError("article_applicability_profile_missing")
    model = get_model(conn, profile["primary_model_id"]) or {}
    provider = get_provider(conn, profile["primary_provider_id"]) or {}
    if (not str(model.get("model_name", "")).startswith("ollama/")
            or provider.get("type") != "ollama_native" or not provider.get("base_url")):
        raise ValueError("article_applicability_requires_local_model")
    generation = _version({
        "workflow": contract.WORKFLOW, "params": PARAMS,
        "profile": profile["id"], "model": model["id"],
        "model_name": model["model_name"], "provider": provider["id"],
        "base_url": provider["base_url"],
        "revisions": [str(x.get("updated_at") or "") for x in (profile, model, provider)],
        "prompt": contract.PROMPT, "schema": contract.schema(),
    })
    return profile, model, provider, generation


def snapshot(conn, article_id: int) -> dict:
    article = get_article_by_id(conn, article_id)
    if not article:
        raise ValueError("article_applicability_source_missing")
    return {key: article.get(key) for key in ("id", "title", "content_text")}


def submit(conn, article_ids: list[int]) -> str:
    require_enabled()
    if (type(article_ids) is not list or not 1 <= len(article_ids) <= 3
            or any(type(item) is not int or item <= 0 for item in article_ids)
            or len(set(article_ids)) != len(article_ids)):
        raise ValueError("article_applicability_invalid_ids")
    generation = configuration(conn)[3]
    articles = [snapshot(conn, article_id) for article_id in sorted(article_ids)]
    for article in articles:
        contract.request(article, generation)
    payload = {"workflow": contract.WORKFLOW, "generation": generation,
               "articles": articles}
    key = "article-applicability:" + _version(payload)
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
    existing = conn.execute(
        "SELECT id FROM jobs WHERE job_type=%s AND dedupe_key=%s ORDER BY requested_at LIMIT 1",
        (JOB_TYPE, key),
    ).fetchone()
    if existing:
        conn.commit()
        return existing[0]
    return enqueue_job(conn, JOB_TYPE, payload, priority=-10, queue_name="llm_local",
                       max_attempts=1, dedupe_key=key)


def complete(conn, job_id: str, request: dict) -> str:
    from .llm.router import _call_ollama_native

    require_enabled()
    _, model, provider, generation = configuration(conn)
    if generation != request["generation"]:
        raise ValueError("article_applicability_configuration_changed")
    started = time.monotonic()
    raw = ""
    error = None
    try:
        raw = _call_ollama_native(
            provider["base_url"], model["model_name"],
            [{"role": "system", "content": request["prompt"]},
             {"role": "user", "content": request["input"]}],
            PARAMS, provider,
            context={"stage": JOB_TYPE, "job_id": job_id, "no_retry": True},
            response_format={"type": "json_schema", "json_schema": {
                "name": "article_applicability", "strict": True, "schema": request["schema"]}},
        )
        if configuration(conn)[3] != generation:
            raise ValueError("article_applicability_configuration_changed")
        return raw
    except Exception as exc:
        error = type(exc).__name__
        raise
    finally:
        insert_llm_run(
            conn, job_id=job_id, provider_id=provider["id"], model_id=model["id"],
            prompt_name=contract.WORKFLOW, input_chars=len(request["input"]),
            output_chars=len(raw), latency_ms=int((time.monotonic() - started) * 1000),
            ok=error is None, error=error,
        )


def run(conn, job, *, generate=None) -> dict:
    require_enabled()
    payload = job.payload or {}
    if (job.job_type != JOB_TYPE or job.result or job.attempt_count != 0
            or job.max_attempts != 1 or job.queue_name != "llm_local"
            or job.status != "running"
            or set(payload) != {"workflow", "generation", "articles"}
            or payload["workflow"] != contract.WORKFLOW):
        raise ValueError("article_applicability_invalid_or_replayed_job")
    articles = payload["articles"]
    if (type(articles) is not list or not 1 <= len(articles) <= 3
            or len({article["id"] for article in articles}) != len(articles)):
        raise ValueError("article_applicability_invalid_cohort")
    generation = configuration(conn)[3]
    if generation != payload["generation"]:
        raise ValueError("article_applicability_configuration_changed")
    for article in articles:
        contract.request(article, generation)
        if snapshot(conn, article["id"]) != article:
            raise ValueError("article_applicability_baseline_changed")
    result = {"workflow": contract.WORKFLOW, "public_eligible": False,
              "generation": generation, "attempts": 0, "articles": []}
    generate = generate or (lambda request: complete(conn, job.id, request))
    started = time.monotonic()
    if not update_job_result(conn, job.id, result):
        raise ValueError("article_applicability_job_not_running")
    for article in articles:
        require_enabled()
        if time.monotonic() - started >= 600:
            result["status"] = "time_budget_exhausted"
            if not update_job_result(conn, job.id, result):
                raise ValueError("article_applicability_job_not_running")
            return result
        request = contract.request(article, generation)
        result["attempts"] += 1
        if not update_job_result(conn, job.id, result):
            raise ValueError("article_applicability_job_not_running")
        try:
            raw = generate(request)
        except Exception as exc:
            row = {"article_id": article["id"], "source_version": request["source_version"],
                   "status": "transport_failed", "error": type(exc).__name__}
            result["articles"].append(row)
            result["status"] = "stopped"
            if not update_job_result(conn, job.id, result):
                raise ValueError("article_applicability_job_not_running")
            return result
        try:
            row = contract.validate(raw, article, generation)
        except Exception as exc:
            row = {"article_id": article["id"], "source_version": request["source_version"],
                   "status": "held", "error": str(exc) if isinstance(exc, ValueError)
                   else type(exc).__name__}
        else:
            row["status"] = "private_comparison"
        result["articles"].append(row)
        if not update_job_result(conn, job.id, result):
            raise ValueError("article_applicability_job_not_running")
    return result
