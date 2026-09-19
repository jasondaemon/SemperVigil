"""Operator-only stored-article comparison. No article, event or build writes."""
import json
import os
import time

from . import article_evidence as evidence
from .investigation import _version
from .services.ai_service import get_active_profile_for_stage, get_model, get_provider, load_provider_secret
from .storage import enqueue_job, get_article_by_id, insert_llm_run, update_job_result

JOB_TYPE = "article_review_private"
PARAMS = {"temperature": 0, "max_tokens": 2048}


def require_enabled() -> None:
    if os.environ.get("SV_ARTICLE_REVIEW_ENABLED", "0") != "1":
        raise PermissionError("article_review_disabled")


def configuration(conn) -> tuple[dict, dict, dict, str]:
    profile = get_active_profile_for_stage(conn, "article_context_pack")
    if isinstance(profile, tuple):
        profile = profile[0]
    if not profile:
        raise ValueError("article_review_profile_missing")
    model = get_model(conn, profile["primary_model_id"]) or {}
    provider = get_provider(conn, profile["primary_provider_id"]) or {}
    if (not str(model.get("model_name", "")).startswith("ollama/")
            or provider.get("type") != "openai_compatible" or not provider.get("base_url")):
        raise ValueError("article_review_requires_existing_local_model")
    version = _version({"workflow": evidence.WORKFLOW, "params": PARAMS,
        "profile": profile["id"], "model": model["id"], "model_name": model["model_name"],
        "provider": provider["id"], "base_url": provider["base_url"],
        "timeout_s": provider.get("timeout_s"),
        "revisions": [str(x.get("updated_at") or "") for x in (profile, model, provider)],
        "context_prompt": evidence.CONTEXT_PROMPT, "summary_prompt": evidence.SUMMARY_PROMPT,
        "context_schema": evidence.context_schema(), "summary_schema": evidence.summary_schema()})
    return profile, model, provider, version


def snapshot(conn, article_id: int) -> dict:
    article = get_article_by_id(conn, article_id)
    if not article or not article.get("summary_llm") or not article.get("context_llm"):
        raise ValueError("article_review_baseline_missing")
    return {k: article.get(k) for k in ("id", "title", "content_text", "summary_llm", "context_llm")}


def submit(conn, article_ids: list[int]) -> str:
    require_enabled()
    if (type(article_ids) is not list or not 1 <= len(article_ids) <= 3
            or any(type(x) is not int or x <= 0 for x in article_ids)
            or len(set(article_ids)) != len(article_ids)):
        raise ValueError("article_review_invalid_ids")
    _, _, _, generation = configuration(conn)
    articles = [snapshot(conn, n) for n in sorted(article_ids)]
    for article in articles:
        evidence.context_request(article, generation)
    payload = {"workflow": evidence.WORKFLOW, "generation": generation, "articles": articles}
    key = "article-review:" + _version(payload)
    # Keep completed/failed jobs deduplicated too: retries must not spend calls again.
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
    existing = conn.execute("SELECT id FROM jobs WHERE job_type=%s AND dedupe_key=%s ORDER BY requested_at LIMIT 1",
                            (JOB_TYPE, key)).fetchone()
    if existing:
        conn.commit()
        return existing[0]
    return enqueue_job(conn, JOB_TYPE, payload, priority=-10, queue_name="llm_local",
                       max_attempts=1, dedupe_key=key)


def complete(conn, job_id: str, request: dict) -> str:
    """One provider attempt, no router repair/fallback. Only called inside the LLM job."""
    from .llm.router import _http_request, _auth_headers, _join_url, _read_openai
    require_enabled()
    profile, model, provider, generation = configuration(conn)
    if generation != request["generation_version"]:
        raise ValueError("article_review_configuration_changed")
    secret = load_provider_secret(conn, provider["id"])
    payload = {"model": model["model_name"], **PARAMS,
        "messages": [{"role": "system", "content": request["system"]},
                     {"role": "user", "content": request["input"]}],
        "response_format": {"type": "json_schema", "json_schema": {
            "name": "article_" + request["phase"], "strict": True, "schema": request["schema"]}}}
    started, raw, error = time.monotonic(), "", None
    try:
        raw = _read_openai(_http_request("POST", _join_url(provider["base_url"], "/chat/completions"),
            _auth_headers(provider["type"], secret), payload, provider,
            context={"stage": JOB_TYPE, "job_id": job_id}))
        if configuration(conn)[3] != generation:
            raise ValueError("article_review_configuration_changed")
        return raw
    except Exception as exc:
        error = type(exc).__name__
        raise
    finally:
        insert_llm_run(conn, job_id=job_id, provider_id=provider["id"], model_id=model["id"],
            prompt_name="article-review-" + request["phase"], input_chars=len(request["input"]),
            output_chars=len(raw), latency_ms=int((time.monotonic()-started)*1000),
            ok=error is None, error=error)


def run(conn, job, *, generate=None) -> dict:
    require_enabled()
    payload = job.payload or {}
    if (job.job_type != JOB_TYPE or job.result or job.attempt_count != 0 or job.max_attempts != 1
            or job.queue_name != "llm_local" or job.status != "running"
            or set(payload) != {"workflow", "generation", "articles"}
            or payload["workflow"] != evidence.WORKFLOW):
        raise ValueError("article_review_invalid_or_replayed_job")
    articles = payload["articles"]
    if (type(articles) is not list or not 1 <= len(articles) <= 3
            or len({a["id"] for a in articles}) != len(articles)):
        raise ValueError("article_review_invalid_cohort")
    generation = configuration(conn)[3]
    if generation != payload["generation"]:
        raise ValueError("article_review_configuration_changed")
    for article in articles:
        evidence.context_request(article, generation)
        if snapshot(conn, article["id"]) != article:
            raise ValueError("article_review_baseline_changed")
    result = {"workflow": evidence.WORKFLOW, "public_eligible": False,
              "generation": generation, "attempts": 0, "articles": []}
    generate = generate or (lambda request: complete(conn, job.id, request))
    started = time.monotonic()

    def persist():
        if not update_job_result(conn, job.id, result):
            raise ValueError("article_review_job_not_running")

    persist()
    for article in articles:
        row = {"article_id": article["id"], "source_version": evidence.source_for(article)["source_version"],
               "baseline_summary": article["summary_llm"], "baseline_context": article["context_llm"],
               "phases": []}
        result["articles"].append(row)
        context = None
        for phase in ("context", "summary"):
            require_enabled()
            if time.monotonic() - started >= 600:
                result["status"] = "time_budget_exhausted"
                persist()
                return result
            if snapshot(conn, article["id"]) != article:
                raise ValueError("article_review_baseline_changed")
            request = (evidence.context_request(article, generation) if phase == "context" else
                       evidence.summary_request(article, context, generation))
            attempt = {"phase": phase, "request_version": request["request_version"], "status": "started"}
            row["phases"].append(attempt)
            result["attempts"] += 1
            persist()  # Reserve before sending. An interrupted job cannot replay the request.
            tick = time.monotonic()
            try:
                raw = generate(request)
            except Exception as exc:
                attempt.update(status="transport_failed", error=type(exc).__name__)
                result["status"] = "stopped"
                persist()
                return result
            attempt["latency_ms"] = int((time.monotonic()-tick)*1000)
            attempt["raw"] = raw[:evidence.MAX_OUTPUT_BYTES]
            try:
                if phase == "context":
                    context = evidence.validate_context(raw.encode(), article, generation)
                    attempt["candidate"] = context
                    if not context["facts"]:
                        attempt["status"] = "abstained"
                        persist()
                        break
                else:
                    candidate = evidence.validate_summary(raw.encode(), article, context, generation)
                    attempt["candidate"] = candidate
                    row["feed_preview"] = {"summary": " ".join(x["text"] for x in candidate["summary_sentences"]),
                                           "summary_bullets": [x["text"] for x in candidate["bullets"]]}
                attempt["status"] = "structurally_valid_unreviewed"
            except ValueError as exc:
                attempt.update(status="invalid", error=str(exc))
                persist()
                break
            persist()
    result["status"] = "comparison_ready"
    persist()
    return result
