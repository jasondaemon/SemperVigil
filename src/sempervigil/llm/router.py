from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import jsonschema

from ..services.ai_service import (
    get_model,
    get_profile,
    get_provider,
    get_prompt,
    get_schema,
    load_provider_secret,
)

STAGE_NAMES = [
    "summarize_article",
    "extract_facts",
    "merge_story",
    "exec_brief",
    "classify_topic",
]


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


def run_profile(
    conn, profile_id: str, text: str, logger: logging.Logger
) -> dict[str, Any]:
    profile = get_profile(conn, profile_id)
    if not profile:
        raise ValueError("profile_not_found")
    prompt = get_prompt(conn, profile["prompt_id"])
    if not prompt:
        raise ValueError("prompt_not_found")
    schema = get_schema(conn, profile["schema_id"]) if profile.get("schema_id") else None
    attempts = _resolve_profile_chain(profile)
    errors: list[str] = []
    for attempt in attempts:
        try:
            output = _call_with_profile(
                conn,
                attempt["provider_id"],
                attempt["model_id"],
                prompt,
                schema,
                profile.get("params") or {},
                text,
                logger,
            )
            return output
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
    raise ValueError("all_providers_failed: " + "; ".join(errors))


def _call_with_profile(
    conn,
    provider_id: str,
    model_id: str,
    prompt: dict[str, Any],
    schema: dict[str, Any] | None,
    params: dict[str, Any],
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
    messages = _render_messages(prompt, text)
    raw = _call_provider(
        provider["type"],
        base_url,
        api_key,
        model["model_name"],
        messages,
        params,
        provider,
    )
    parsed = _maybe_parse_json(raw)
    if schema:
        validation = _validate_json(schema["json_schema"], parsed)
        if not validation["ok"]:
            repair_messages = _render_messages(
                prompt,
                text + "\n\nReturn valid JSON only. Fix schema violations.",
            )
            raw = _call_provider(
                provider["type"],
                base_url,
                api_key,
                model["model_name"],
                repair_messages,
                params,
                provider,
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
) -> str:
    if provider_type == "openai_compatible":
        path = _join_url(base_url, "/chat/completions")
        payload = {
            "model": model_name,
            "messages": messages,
            **_filter_params(params),
        }
        headers = _auth_headers(provider_type, api_key)
        response = _http_request("POST", path, headers, payload, provider)
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
        response = _http_request("POST", path, headers, payload, provider)
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
        response = _http_request("POST", path, {}, payload, provider)
        return _read_google(response)
    raise ValueError("unsupported_provider_type")


def _http_request(
    method: str,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any] | None,
    provider: dict[str, Any],
) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)
    timeout = int(provider.get("timeout_s", 30))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        raise ValueError(f"http_error {exc.code}: {raw[:500]}") from exc
    except urllib.error.URLError as exc:
        raise ValueError(f"network_error: {exc}") from exc
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


def _read_openai(response: dict[str, Any]) -> str:
    choices = response.get("choices") or []
    if not choices:
        raise ValueError("openai_missing_choices")
    return choices[0]["message"]["content"]


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
        return raw


def _validate_json(schema: dict[str, Any], payload: Any) -> dict[str, Any]:
    try:
        jsonschema.validate(payload, schema)
        return {"ok": True}
    except jsonschema.ValidationError as exc:
        return {"ok": False, "error": str(exc)}
