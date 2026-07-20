from __future__ import annotations

import json
import logging
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import jsonschema

from ..services.ai_service import (
    get_active_profile_for_stage,
    get_model,
    get_profile,
    get_provider,
    get_prompt,
    get_schema,
    load_provider_secret,
)
from ..config import load_runtime_config
from ..utils import build_json_handler, build_json_formatter, log_event

STAGE_NAMES = [
    "summarize_article",
    "article_context_pack",
    "daily_brief_overall_synthesis",
    "cve_enrich_products",
    "cve_enrich_threat_actors",
    "article_enrich_products",
    "article_enrich_threat_actors",
    "derive_events_from_articles",
    "event_web_validate",
    "event_report_llm",
]


def _commit_before_provider_io(conn) -> None:
    try:
        conn.commit()
    except Exception:
        pass


def run_pipeline_stage(
    conn,
    stage_name: str,
    input_payload: str,
    logger: logging.Logger,
    profile_id: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = None
    reason = ""
    if profile_id:
        profile = get_profile(conn, profile_id)
        if not profile:
            raise ValueError("profile_not_found")
    else:
        profile, reason = get_active_profile_for_stage(conn, stage_name)
        if not profile:
            raise ValueError(f"llm_stage_{reason}")
        profile_id = profile["id"]
    ctx = dict(context or {})
    ctx["stage"] = stage_name
    result = run_profile(conn, profile_id, input_payload, logger, context=ctx)
    return {"profile_id": profile_id, "stage": stage_name, **result}


def test_provider(conn, provider_id: str, logger: logging.Logger) -> dict[str, Any]:
    provider = get_provider(conn, provider_id)
    if not provider:
        raise ValueError("provider_not_found")
    api_key = load_provider_secret(conn, provider_id)
    base_url = provider.get("base_url") or _default_base_url(provider["type"])
    if provider["type"] == "openai_compatible":
        path = _join_url(base_url, "/models")
        headers = _auth_headers(provider["type"], api_key)
        response = _http_request("GET", path, headers, None, provider)
        return {"ok": True, "response": response}
    if provider["type"] == "anthropic":
        path = _join_url(base_url, "/messages")
        payload = {
            "model": "claude-3-5-sonnet-20240620",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "ping"}],
        }
        headers = _auth_headers(provider["type"], api_key)
        response = _http_request("POST", path, headers, payload, provider)
        return {"ok": True, "response": response}
    if provider["type"] == "google":
        model_name = "gemini-1.5-flash"
        path = _join_url(
            base_url,
            f"/models/{urllib.parse.quote(model_name)}:generateContent",
        )
        path = _append_key(path, api_key)
        payload = {"contents": [{"parts": [{"text": "ping"}]}]}
        response = _http_request("POST", path, {}, payload, provider)
        return {"ok": True, "response": response}
    raise ValueError("unsupported_provider_type")


def test_profile(
    conn, profile_id: str, text: str, logger: logging.Logger
) -> dict[str, Any]:
    result = run_profile(conn, profile_id, text, logger)
    return result


def test_model(
    conn,
    provider_id: str,
    model_id: str,
    text: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    provider = get_provider(conn, provider_id)
    if not provider:
        raise ValueError("provider_not_found")
    model = get_model(conn, model_id)
    if not model:
        raise ValueError("model_not_found")
    api_key = load_provider_secret(conn, provider_id)
    base_url = provider.get("base_url") or _default_base_url(provider["type"])
    messages = [
        {"role": "system", "content": "You are a concise assistant."},
        {"role": "user", "content": text},
    ]
    start = time.time()
    raw = _call_provider(
        provider["type"],
        base_url,
        api_key,
        model["model_name"],
        messages,
        {},
        provider,
    )
    latency_ms = int((time.time() - start) * 1000)
    return {"ok": True, "latency_ms": latency_ms, "output": raw[:800]}


def run_profile(
    conn, profile_id: str, text: str, logger: logging.Logger, context: dict[str, Any] | None = None
) -> dict[str, Any]:
    profile = get_profile(conn, profile_id)
    if not profile:
        raise ValueError("profile_not_found")
    safe_text = str(text or "")
    params = profile.get("params") or {}
    max_input_chars = params.get("max_input_chars") or os.environ.get("SV_LLM_MAX_INPUT_CHARS", "50000")
    try:
        max_input_chars = int(max_input_chars)
    except Exception:
        max_input_chars = 50000
    if max_input_chars <= 0:
        max_input_chars = 50000
    max_input_chars = min(max_input_chars, 800000)
    logger.debug("llm_input_limit profile_id=%s max_input_chars=%s", profile_id, max_input_chars)
    if len(safe_text) > max_input_chars:
        safe_text = safe_text[:max_input_chars] + f"\n\n[TRUNCATED: input exceeded {max_input_chars} chars]"
    prompt = get_prompt(conn, profile["prompt_id"])
    if not prompt:
        raise ValueError("prompt_not_found")
    schema = get_schema(conn, profile["schema_id"]) if profile.get("schema_id") else None
    attempts = _resolve_profile_chain(profile)
    errors: list[str] = []
    for attempt in attempts:
        try:
            attempt_ctx = dict(context or {})
            attempt_ctx.setdefault("profile_name", profile.get("name") or "")
            output = _call_with_profile(
                conn,
                profile_id,
                attempt["provider_id"],
                attempt["model_id"],
                prompt,
                schema,
                params,
                safe_text,
                logger,
                context=attempt_ctx,
            )
            return output
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
    raise ValueError("all_providers_failed: " + "; ".join(errors))


def _call_with_profile(
    conn,
    profile_id: str,
    provider_id: str,
    model_id: str,
    prompt: dict[str, Any],
    schema: dict[str, Any] | None,
    params: dict[str, Any],
    text: str,
    logger: logging.Logger,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    provider = get_provider(conn, provider_id)
    if not provider:
        raise ValueError("provider_not_found")
    model = get_model(conn, model_id)
    if not model:
        raise ValueError("model_not_found")
    api_key = load_provider_secret(conn, provider_id)
    base_url = provider.get("base_url") or _default_base_url(provider["type"])
    messages = _render_messages(prompt, text)
    start = time.time()
    prompt_name = prompt.get("name") if isinstance(prompt, dict) else ""
    ctx = dict(context or {})
    ctx.setdefault("profile_name", "")
    ctx.setdefault("prompt_name", prompt_name)
    ctx.setdefault("provider_name", provider.get("name") or "")
    ctx.setdefault("model_name", model.get("model_name") or "")
    if "openai_background_enabled" not in ctx:
        try:
            runtime = load_runtime_config(conn)
            llm_cfg = runtime.llm or {}
            ctx["openai_background_enabled"] = bool(llm_cfg.get("openai_background_enabled"))
            ctx["openai_background_poll_seconds"] = llm_cfg.get("openai_background_poll_seconds")
            ctx["openai_background_max_seconds"] = llm_cfg.get("openai_background_max_seconds")
            stage_name = str(ctx.get("stage") or "")
            json_mode_enabled = bool(llm_cfg.get("json_response_format_enabled", True))
            json_mode_stages_raw = llm_cfg.get("json_response_format_stages") or [
                "cve_enrich_products"
            ]
            json_mode_stages = {
                str(item).strip()
                for item in json_mode_stages_raw
                if str(item).strip()
            }
            ctx["json_response_format_enabled"] = json_mode_enabled and (
                stage_name in json_mode_stages
            )
        except Exception:
            pass
    log_event(
        logger,
        logging.INFO,
        "llm_request_start",
        profile_name=ctx.get("profile_name") or "",
        prompt_name=ctx.get("prompt_name") or "",
        provider_name=ctx.get("provider_name") or "",
        model_name=ctx.get("model_name") or "",
        stage=ctx.get("stage") or "",
        job_type=ctx.get("job_type") or "",
    )
    _commit_before_provider_io(conn)
    raw = _call_provider(
        provider["type"],
        base_url,
        api_key,
        model["model_name"],
        messages,
        params,
        provider,
        context=ctx,
    )
    latency_ms = int((time.time() - start) * 1000)
    log_event(
        logger,
        logging.INFO,
        "llm_request_done",
        profile_name=ctx.get("profile_name") or "",
        prompt_name=ctx.get("prompt_name") or "",
        provider_name=ctx.get("provider_name") or "",
        model_name=ctx.get("model_name") or "",
        stage=ctx.get("stage") or "",
        job_type=ctx.get("job_type") or "",
        latency_ms=latency_ms,
    )
    parsed = _maybe_parse_json(raw)
    if schema:
        validation = _validate_json(schema["json_schema"], parsed)
        if not validation["ok"]:
            repair_messages = _render_messages(
                prompt,
                text + "\n\nReturn valid JSON only. Fix schema violations.",
            )
            _commit_before_provider_io(conn)
            raw = _call_provider(
                provider["type"],
                base_url,
                api_key,
                model["model_name"],
                repair_messages,
                params,
                provider,
                context=ctx,
            )
            parsed = _maybe_parse_json(raw)
            validation = _validate_json(schema["json_schema"], parsed)
        return {
            "raw": raw,
            "parsed": parsed,
            "schema_valid": validation["ok"],
            "schema_error": validation.get("error"),
        }
    return {"raw": raw, "parsed": parsed, "schema_valid": True, "schema_error": None}


def _render_messages(prompt: dict[str, Any], text: str) -> list[dict[str, str]]:
    system = prompt["system_template"].replace("{{input}}", text)
    user = prompt["user_template"].replace("{{input}}", text)
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _resolve_profile_chain(profile: dict[str, Any]) -> list[dict[str, str]]:
    chain = [
        {"provider_id": profile["primary_provider_id"], "model_id": profile["primary_model_id"]}
    ]
    fallback = profile.get("fallback") or []
    for item in fallback:
        if isinstance(item, dict) and item.get("provider_id") and item.get("model_id"):
            chain.append({"provider_id": item["provider_id"], "model_id": item["model_id"]})
    return chain


def _call_provider(
    provider_type: str,
    base_url: str,
    api_key: str | None,
    model_name: str,
    messages: list[dict[str, str]],
    params: dict[str, Any],
    provider: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> str:
    if provider_type == "openai_compatible":
        if _use_openai_background(provider, base_url, context):
            response = _call_openai_responses_background(
                provider,
                base_url,
                api_key,
                model_name,
                messages,
                params,
                context=context,
            )
            return _read_openai(response)
        path = _join_url(base_url, "/chat/completions")
        payload = {
            "model": model_name,
            "messages": messages,
            **_filter_params(params),
        }
        if bool((context or {}).get("json_response_format_enabled")):
            payload["response_format"] = {"type": "json_object"}
        headers = _auth_headers(provider_type, api_key)
        response = _http_request("POST", path, headers, payload, provider, context=context)
        return _read_openai(response)
    if provider_type == "anthropic":
        path = _join_url(base_url, "/messages")
        payload = {
            "model": model_name,
            "max_tokens": int(params.get("max_tokens", 256)),
            "system": messages[0]["content"],
            "messages": [{"role": "user", "content": messages[1]["content"]}],
        }
        headers = _auth_headers(provider_type, api_key)
        response = _http_request("POST", path, headers, payload, provider, context=context)
        return _read_anthropic(response)
    if provider_type == "google":
        path = _join_url(
            base_url,
            f"/models/{urllib.parse.quote(model_name)}:generateContent",
        )
        path = _append_key(path, api_key)
        payload = {
            "contents": [{"parts": [{"text": messages[1]["content"]}]}],
            "generationConfig": _filter_params(params),
        }
        response = _http_request("POST", path, {}, payload, provider, context=context)
        return _read_google(response)
    raise ValueError("unsupported_provider_type")


def _http_request(
    method: str,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any] | None,
    provider: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)
    timeout = int(provider.get("timeout_s", 1200))
    backoff = [1, 2]
    attempts = 0
    provider_name = str(provider.get("name") or "").lower()
    is_openai = str(provider.get("type") or "").lower() == "openai_compatible"
    if is_openai:
        base_url = str(provider.get("base_url") or "").lower()
        if "api.openai.com" not in base_url and "openai" not in provider_name:
            is_openai = False
    logger = _ensure_openai_http_logger()
    log_http = bool(os.environ.get("SV_LLM_LOG_HTTP"))
    if is_openai:
        log_http = True
    provider_label = provider.get("name") or provider.get("id") or "provider"
    started_at = time.time()
    context = dict(context or {})
    request_metrics = _estimate_request_metrics(payload)
    if log_http:
        log_event(
            logger,
            logging.INFO,
            "llm_http_start",
            provider=provider_label,
            provider_name=context.get("provider_name") or provider_label,
            model_name=context.get("model_name") or "",
            profile_name=context.get("profile_name") or "",
            prompt_name=context.get("prompt_name") or "",
            stage=context.get("stage") or "",
            job_type=context.get("job_type") or "",
            url=url,
            timeout_s=timeout,
            payload_bytes=len(data) if data else 0,
            request_chars=request_metrics["request_chars"],
            request_tokens_estimate=request_metrics["request_tokens_estimate"],
        )
    while True:
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
                status_code = response.getcode()
                resp_headers = dict(response.headers.items())
            break
        except urllib.error.HTTPError as exc:
            if exc.code in {429, 503} and attempts < len(backoff):
                time.sleep(backoff[attempts])
                attempts += 1
                continue
            raw = exc.read().decode("utf-8", errors="ignore")
            status_code = exc.code
            resp_headers = dict(exc.headers.items()) if exc.headers else {}
            if log_http:
                redacted_headers = {
                    k: ("REDACTED" if k.lower() == "authorization" else v)
                    for k, v in headers.items()
                }
                log_event(
                    logger,
                    logging.INFO,
                    "OpenAI_prompts",
                    method=method,
                    url=url,
                    model=(payload or {}).get("model") if isinstance(payload, dict) else "",
                    provider_name=context.get("provider_name") or provider_label,
                    model_name=context.get("model_name") or "",
                    profile_name=context.get("profile_name") or "",
                    prompt_name=context.get("prompt_name") or "",
                    stage=context.get("stage") or "",
                    job_type=context.get("job_type") or "",
                    request_headers=json.dumps(redacted_headers, ensure_ascii=False),
                    request_body=json.dumps(payload, ensure_ascii=False) if payload is not None else "",
                    response_status=status_code,
                    response_headers=json.dumps(resp_headers, ensure_ascii=False),
                    response_body=raw,
                    elapsed_ms=int((time.time() - started_at) * 1000),
                )
                log_event(
                    logger,
                    logging.INFO,
                    "llm_http_error",
                    provider=provider_label,
                    url=url,
                    status=status_code,
                    elapsed_ms=int((time.time() - started_at) * 1000),
                )
            raise ValueError(f"http_error {exc.code}: {raw[:500]}") from exc
        except urllib.error.URLError as exc:
            is_timeout = isinstance(exc.reason, (TimeoutError, socket.timeout))
            if is_timeout and attempts < len(backoff):
                time.sleep(backoff[attempts])
                attempts += 1
                continue
            if log_http:
                redacted_headers = {
                    k: ("REDACTED" if k.lower() == "authorization" else v)
                    for k, v in headers.items()
                }
                log_event(
                    logger,
                    logging.INFO,
                    "OpenAI_prompts",
                    method=method,
                    url=url,
                    model=(payload or {}).get("model") if isinstance(payload, dict) else "",
                    provider_name=context.get("provider_name") or provider_label,
                    model_name=context.get("model_name") or "",
                    profile_name=context.get("profile_name") or "",
                    prompt_name=context.get("prompt_name") or "",
                    stage=context.get("stage") or "",
                    job_type=context.get("job_type") or "",
                    request_headers=json.dumps(redacted_headers, ensure_ascii=False),
                    request_body=json.dumps(payload, ensure_ascii=False) if payload is not None else "",
                    response_status="network_error",
                    response_headers="{}",
                    response_body=str(exc),
                    elapsed_ms=int((time.time() - started_at) * 1000),
                )
                log_event(
                    logger,
                    logging.INFO,
                    "llm_http_error",
                    provider=provider_label,
                    url=url,
                    status="network_error",
                    elapsed_ms=int((time.time() - started_at) * 1000),
                )
            raise ValueError(f"network_error: {exc}") from exc
        except socket.timeout as exc:
            if attempts < len(backoff):
                time.sleep(backoff[attempts])
                attempts += 1
                continue
            if log_http:
                redacted_headers = {
                    k: ("REDACTED" if k.lower() == "authorization" else v)
                    for k, v in headers.items()
                }
                log_event(
                    logger,
                    logging.INFO,
                    "OpenAI_prompts",
                    method=method,
                    url=url,
                    model=(payload or {}).get("model") if isinstance(payload, dict) else "",
                    provider_name=context.get("provider_name") or provider_label,
                    model_name=context.get("model_name") or "",
                    profile_name=context.get("profile_name") or "",
                    prompt_name=context.get("prompt_name") or "",
                    stage=context.get("stage") or "",
                    job_type=context.get("job_type") or "",
                    request_headers=json.dumps(redacted_headers, ensure_ascii=False),
                    request_body=json.dumps(payload, ensure_ascii=False) if payload is not None else "",
                    response_status="timeout",
                    response_headers="{}",
                    response_body=str(exc),
                    elapsed_ms=int((time.time() - started_at) * 1000),
                )
                log_event(
                    logger,
                    logging.INFO,
                    "llm_http_error",
                    provider=provider_label,
                    url=url,
                    status="timeout",
                    elapsed_ms=int((time.time() - started_at) * 1000),
                )
            raise ValueError(f"timeout: {exc}") from exc
    response_metrics = _extract_response_metrics(raw)
    if log_http:
        redacted_headers = {
            k: ("REDACTED" if k.lower() == "authorization" else v) for k, v in headers.items()
        }
        log_event(
            logger,
            logging.INFO,
            "OpenAI_prompts",
            method=method,
            url=url,
            model=(payload or {}).get("model") if isinstance(payload, dict) else "",
            provider_name=context.get("provider_name") or provider_label,
            model_name=context.get("model_name") or "",
            profile_name=context.get("profile_name") or "",
            prompt_name=context.get("prompt_name") or "",
            stage=context.get("stage") or "",
            job_type=context.get("job_type") or "",
            request_headers=json.dumps(redacted_headers, ensure_ascii=False),
            request_body=json.dumps(payload, ensure_ascii=False) if payload is not None else "",
            response_status=status_code,
            response_headers=json.dumps(resp_headers, ensure_ascii=False),
            response_body=raw,
            elapsed_ms=int((time.time() - started_at) * 1000),
            request_chars=request_metrics["request_chars"],
            request_tokens_estimate=request_metrics["request_tokens_estimate"],
            response_prompt_tokens=response_metrics["prompt_tokens"],
            response_completion_tokens=response_metrics["completion_tokens"],
            response_total_tokens=response_metrics["total_tokens"],
        )
        log_event(
            logger,
            logging.INFO,
            "llm_http_done",
            provider=provider_label,
            provider_name=context.get("provider_name") or provider_label,
            model_name=context.get("model_name") or "",
            profile_name=context.get("profile_name") or "",
            prompt_name=context.get("prompt_name") or "",
            stage=context.get("stage") or "",
            job_type=context.get("job_type") or "",
            url=url,
            status=status_code,
            elapsed_ms=int((time.time() - started_at) * 1000),
            response_bytes=len(raw.encode("utf-8", errors="ignore")),
            request_tokens_estimate=request_metrics["request_tokens_estimate"],
            response_prompt_tokens=response_metrics["prompt_tokens"],
            response_completion_tokens=response_metrics["completion_tokens"],
            response_total_tokens=response_metrics["total_tokens"],
        )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def _estimate_request_metrics(payload: dict[str, Any] | None) -> dict[str, int]:
    text_parts: list[str] = []
    if isinstance(payload, dict):
        messages = payload.get("messages")
        if isinstance(messages, list):
            for item in messages:
                if isinstance(item, dict):
                    content = item.get("content")
                    if isinstance(content, str) and content.strip():
                        text_parts.append(content)
        contents = payload.get("contents")
        if isinstance(contents, list):
            for item in contents:
                if not isinstance(item, dict):
                    continue
                parts = item.get("parts")
                if isinstance(parts, list):
                    for part in parts:
                        if isinstance(part, dict):
                            text = part.get("text")
                            if isinstance(text, str) and text.strip():
                                text_parts.append(text)
    text = "\n".join(text_parts)
    request_chars = len(text)
    if not text:
        return {"request_chars": 0, "request_tokens_estimate": 0}
    request_tokens_estimate = max(1, int(round(request_chars / 4)))
    return {
        "request_chars": request_chars,
        "request_tokens_estimate": request_tokens_estimate,
    }


def _extract_response_metrics(raw: str) -> dict[str, int]:
    try:
        response = json.loads(raw)
    except json.JSONDecodeError:
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    if not isinstance(response, dict):
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    usage = response.get("usage")
    if not isinstance(usage, dict):
        return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    return {
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": int(usage.get("completion_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
    }


def _read_openai(response: dict[str, Any]) -> str:
    if "output" in response:
        text = _extract_openai_output_text(response)
        if not text:
            raise ValueError("openai_missing_output_text")
        return text
    choices = response.get("choices") or []
    if not choices:
        raise ValueError("openai_missing_choices")
    return choices[0]["message"]["content"]


def _extract_openai_output_text(response: dict[str, Any]) -> str:
    output_text = response.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    output = response.get("output") or []
    parts: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "message":
            content = item.get("content")
            if isinstance(content, str):
                if content.strip():
                    parts.append(content)
                continue
            if isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    part_type = part.get("type")
                    if part_type in {"output_text", "text"}:
                        text = part.get("text")
                        if isinstance(text, str) and text.strip():
                            parts.append(text)
    return "\n".join(parts).strip()


def _use_openai_background(
    provider: dict[str, Any],
    base_url: str,
    context: dict[str, Any] | None = None,
) -> bool:
    if str(provider.get("type") or "").lower() != "openai_compatible":
        return False
    if "api.openai.com" not in str(base_url or "").lower():
        return False
    ctx = context or {}
    if isinstance(ctx.get("openai_background_enabled"), bool):
        return bool(ctx.get("openai_background_enabled"))
    flag = os.environ.get("SV_OPENAI_BACKGROUND", "").strip().lower()
    return flag in {"1", "true", "yes", "on"}


def _call_openai_responses_background(
    provider: dict[str, Any],
    base_url: str,
    api_key: str | None,
    model_name: str,
    messages: list[dict[str, str]],
    params: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = _auth_headers(provider["type"], api_key)
    response_params = _filter_params(params)
    response_params.pop("max_tokens", None)
    payload = {
        "model": model_name,
        "input": messages,
        "background": True,
        "store": True,
        **response_params,
    }
    if "max_tokens" in params and "max_output_tokens" not in payload:
        try:
            payload["max_output_tokens"] = int(params["max_tokens"])
        except Exception:
            pass
    create_path = _join_url(base_url, "/responses")
    response = _http_request("POST", create_path, headers, payload, provider, context=context)
    resp_id = response.get("id")
    if not resp_id:
        return response
    status = response.get("status") or ""
    poll_seconds = _get_int_env("SV_OPENAI_BACKGROUND_POLL_SECONDS", 2, minimum=1)
    max_wait = _get_int_env("SV_OPENAI_BACKGROUND_MAX_SECONDS", 3600, minimum=60)
    ctx = context or {}
    poll_override = ctx.get("openai_background_poll_seconds")
    max_override = ctx.get("openai_background_max_seconds")
    if isinstance(poll_override, int) and poll_override > 0:
        poll_seconds = poll_override
    if isinstance(max_override, int) and max_override >= 60:
        max_wait = max_override
    started_at = time.time()
    while status in {"queued", "in_progress"}:
        if time.time() - started_at > max_wait:
            raise ValueError("openai_background_timeout")
        time.sleep(poll_seconds)
        get_path = _join_url(base_url, f"/responses/{resp_id}")
        response = _http_request("GET", get_path, headers, None, provider, context=context)
        status = response.get("status") or ""
    return response


def _get_int_env(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except Exception:
        value = int(default)
    if minimum is not None and value < minimum:
        value = minimum
    return value


def _read_anthropic(response: dict[str, Any]) -> str:
    content = response.get("content") or []
    if not content:
        raise ValueError("anthropic_missing_content")
    return content[0].get("text") or ""


def _read_google(response: dict[str, Any]) -> str:
    candidates = response.get("candidates") or []
    if not candidates:
        raise ValueError("google_missing_candidates")
    parts = candidates[0].get("content", {}).get("parts", [])
    if not parts:
        raise ValueError("google_missing_parts")
    return parts[0].get("text") or ""


def _ensure_openai_http_logger() -> logging.Logger:
    logger = logging.getLogger("sempervigil.llm.http")
    if getattr(logger, "_sv_openai_log_ready", False):
        return logger
    log_path = os.environ.get("SV_OPENAI_LOG_FILE", "").strip()
    logger.setLevel(logging.INFO)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    if log_path:
        file_handler = build_json_handler(log_path)
        stdout_handler.setFormatter(file_handler.formatter)
        logger.addHandler(file_handler)
    else:
        stdout_handler.setFormatter(build_json_formatter())
    logger.addHandler(stdout_handler)
    logger.propagate = False
    logger._sv_openai_log_ready = True
    return logger


def _filter_params(params: dict[str, Any]) -> dict[str, Any]:
    allowed = {"temperature", "max_tokens", "top_p", "seed"}
    return {key: value for key, value in params.items() if key in allowed}


def _auth_headers(provider_type: str, api_key: str | None) -> dict[str, str]:
    if not api_key:
        return {}
    if provider_type == "openai_compatible":
        return {"Authorization": f"Bearer {api_key}"}
    if provider_type == "anthropic":
        return {"x-api-key": api_key, "anthropic-version": "2023-06-01"}
    return {}


def _default_base_url(provider_type: str) -> str:
    if provider_type == "openai_compatible":
        return "https://api.openai.com/v1"
    if provider_type == "anthropic":
        return "https://api.anthropic.com/v1"
    if provider_type == "google":
        return "https://generativelanguage.googleapis.com/v1beta"
    return ""


def _append_key(url: str, api_key: str | None) -> str:
    if not api_key:
        return url
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("key", api_key))
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query), parsed.fragment)
    )


def _join_url(base: str, path: str) -> str:
    return base.rstrip("/") + path


def _maybe_parse_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        extracted = _extract_json_payload(raw)
        if extracted:
            try:
                return json.loads(extracted)
            except json.JSONDecodeError:
                return raw
        return raw


def _extract_json_payload(raw: str) -> str | None:
    if not raw:
        return None
    text = raw.strip()
    if not text:
        return None
    start_obj = text.find("{")
    start_arr = text.find("[")
    starts = [s for s in (start_obj, start_arr) if s != -1]
    if not starts:
        return None
    start = min(starts)
    end_obj = text.rfind("}")
    end_arr = text.rfind("]")
    end = max(end_obj, end_arr)
    if end == -1 or end <= start:
        return None
    return text[start : end + 1].strip()


def _validate_json(schema: dict[str, Any], payload: Any) -> dict[str, Any]:
    try:
        jsonschema.validate(payload, schema)
        return {"ok": True}
    except jsonschema.ValidationError as exc:
        return {"ok": False, "error": str(exc)}
