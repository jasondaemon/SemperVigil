from __future__ import annotations

import time
import requests


class SearxngError(RuntimeError):
    pass


def searxng_search(
    query: str,
    *,
    url: str,
    timeout_s: int = 20,
    categories: str | None = None,
    engines: str | None = None,
    language: str | None = None,
    safesearch: int = 0,
    max_results: int = 10,
) -> list[dict[str, object]]:
    if not url:
        raise SearxngError("SV_SEARXNG_URL not set")
    params = {
        "q": query,
        "format": "json",
        "safesearch": str(safesearch),
    }
    if categories:
        params["categories"] = categories
    if engines:
        params["engines"] = engines
    if language:
        params["language"] = language
    req_url = url.rstrip("/") + "/search"
    attempts = 0
    last_error = None
    headers = {
        "User-Agent": "SemperVigil/1.0",
        # SearxNG bot detection expects a client IP header.
        "X-Forwarded-For": "127.0.0.1",
        "X-Real-IP": "127.0.0.1",
    }
    while attempts < 2:
        attempts += 1
        try:
            response = requests.get(
                req_url,
                params=params,
                timeout=timeout_s,
                headers=headers,
            )
        except requests.RequestException as exc:
            last_error = exc
            if attempts < 2:
                time.sleep(1)
                continue
            raise SearxngError(f"Searxng connection error: {exc}") from exc
        if response.status_code >= 400:
            last_error = SearxngError(f"Searxng HTTP error {response.status_code}")
            if attempts < 2:
                time.sleep(1)
                continue
            raise last_error
        try:
            data = response.json()
        except ValueError as exc:
            last_error = exc
            if attempts < 2:
                time.sleep(1)
                continue
            raise SearxngError("Searxng returned invalid JSON") from exc
        break
    else:
        raise SearxngError(f"Searxng error: {last_error}")
    results = []
    for item in data.get("results", [])[: max_results or 10]:
        results.append(
            {
                "url": item.get("url"),
                "title": item.get("title"),
                "snippet": item.get("content") or item.get("snippet"),
                "engine": item.get("engine"),
                "category": item.get("category"),
                "published_at": item.get("publishedDate") or item.get("publishedDateTimestamp"),
            }
        )
    return results
