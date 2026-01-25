from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

from ..utils import log_event


def summarize_with_llm(
    *,
    title: str,
    source: str,
    published_at: str | None,
    url: str,
    content: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    base_url = os.environ.get("SV_LLM_BASE_URL", "").strip()
    api_key = os.environ.get("SV_LLM_API_KEY", "").strip()
    model = os.environ.get("SV_LLM_MODEL", "ollama/llama3").strip()
    temperature = float(os.environ.get("SV_LLM_TEMPERATURE", "0.2"))
    max_tokens = int(os.environ.get("SV_LLM_MAX_TOKENS", "256"))
    if not base_url:
        raise ValueError("SV_LLM_BASE_URL not set")
    if not api_key:
        raise ValueError("SV_LLM_API_KEY not set")
    url_base = base_url.rstrip("/")
    endpoint = f"{url_base}/chat/completions"

    system = (
        "You summarize cybersecurity news for a security program. "
        "Be concise, factual, and avoid speculation. "
        "Return JSON with keys: summary, bullets, why, cves."
    )
    user = (
        f"Title: {title}\n"
        f"Source: {source}\n"
        f"Published: {published_at or 'unknown'}\n"
        f"URL: {url}\n\n"
        f"Content:\n{content}\n\n"
        "Respond with JSON only."
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
    except Exception as exc:  # noqa: BLE001
        log_event(logger, logging.WARNING, "llm_call_failed", error=str(exc))
        raise
    data = json.loads(raw)
    content_text = data["choices"][0]["message"]["content"]
    try:
        parsed = json.loads(content_text)
    except json.JSONDecodeError:
        parsed = {"summary": content_text, "bullets": [], "why": "", "cves": []}
    parsed["model"] = model
    return parsed
