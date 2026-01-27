from __future__ import annotations

import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


class SearxngError(RuntimeError):
    pass


def searxng_search(
    query: str,
    *,
    url: str,
    timeout_s: int = 10,
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
    req_url = url.rstrip("/") + "/search?" + urlencode(params)
    request = Request(req_url, headers={"User-Agent": "SemperVigil/1.0"})
    try:
        with urlopen(request, timeout=timeout_s) as response:
            body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise SearxngError(f"Searxng HTTP error {exc.code}") from exc
    except URLError as exc:
        raise SearxngError(f"Searxng connection error: {exc}") from exc
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SearxngError("Searxng returned invalid JSON") from exc
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
