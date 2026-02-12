from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from typing import Any

from bs4 import BeautifulSoup

from ..source_overrides import normalize_source_overrides
from ..utils import log_event


def fetch_article_content(
    url: str,
    *,
    timeout_seconds: int,
    user_agent: str,
    logger: logging.Logger,
    source_id: str | None = None,
    source_name: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    fetch_cfg = normalize_source_overrides(overrides or {}).get("fetch", {})
    use_vpn = bool(fetch_cfg.get("use_vpn", True))
    try:
        with _open_request(request, timeout_seconds, use_vpn=use_vpn) as response:
            status_code = response.getcode()
            raw = response.read()
        log_event(
            logger,
            logging.INFO,
            "content_fetch_done",
            url=url,
            status_code=status_code,
            vpn_used=use_vpn,
            source_id=source_id,
            source_name=source_name,
        )
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        snippet = ""
        try:
            snippet = (exc.read() or b"").decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            snippet = ""
        log_event(
            logger,
            logging.WARNING,
            "content_fetch_failed",
            url=url,
            status_code=status_code,
            vpn_used=use_vpn,
            source_id=source_id,
            source_name=source_name,
            error=str(exc),
        )
        if status_code == 403:
            log_event(
                logger,
                logging.WARNING,
                "content_fetch_forbidden",
                url=url,
                status_code=status_code,
                vpn_used=use_vpn,
                source_id=source_id,
                source_name=source_name,
                body_snippet=(snippet or "")[:200],
            )
        raise
    except Exception as exc:  # noqa: BLE001
        log_event(
            logger,
            logging.WARNING,
            "content_fetch_failed",
            url=url,
            status_code=None,
            vpn_used=use_vpn,
            source_id=source_id,
            source_name=source_name,
            error=str(exc),
        )
        raise
    html = raw.decode("utf-8", errors="replace")
    extracted = extract_content_from_html(html, overrides=overrides, logger=logger)
    return {"content_text": extracted["content_text"], "content_html": html, "method": extracted["method"]}


def _open_request(request: urllib.request.Request, timeout_seconds: int, *, use_vpn: bool):
    if use_vpn:
        return urllib.request.urlopen(request, timeout=timeout_seconds)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(request, timeout=timeout_seconds)


def extract_content_from_html(
    html: str, *, overrides: dict[str, Any] | None, logger: logging.Logger
) -> dict[str, Any]:
    cfg = normalize_source_overrides(overrides or {}).get("content", {})
    mode = str(cfg.get("mode") or "default")
    min_chars = int(cfg.get("min_chars") or 800)
    include_selectors = list(cfg.get("include_selectors") or [])
    exclude_selectors = list(cfg.get("exclude_selectors") or [])
    strip_patterns = list(cfg.get("strip_patterns") or [])
    allow_fallback = bool(cfg.get("allow_fallback_to_default", True))

    method = "default"
    text = ""

    if mode == "jsonld_articlebody":
        method = "jsonld_articlebody"
        text = _extract_jsonld_article_body(html)
    elif mode == "css_selectors":
        method = "css_selectors"
        text = _extract_css_selectors(html, include_selectors, exclude_selectors)
    elif mode == "readability":
        method = "readability"
        text = _extract_readability(html)
    elif mode == "trafilatura":
        method = "trafilatura"
        text = _extract_trafilatura(html)
    else:
        method = "default"
        text = extract_readable_text(html)

    if strip_patterns:
        text = _strip_patterns(text, strip_patterns)

    if len(text or "") < min_chars and mode != "default" and allow_fallback:
        fallback = extract_readable_text(html)
        if strip_patterns:
            fallback = _strip_patterns(fallback, strip_patterns)
        if len(fallback or "") >= min_chars or mode in {"readability", "trafilatura"}:
            text = fallback
            method = f"{method}:fallback_default"

    return {"content_text": text, "method": method}


def extract_readable_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()
    article = soup.find("article")
    if article:
        return _normalize_text(article.get_text(" ", strip=True))
    best = None
    best_len = 0
    for div in soup.find_all("div"):
        text = div.get_text(" ", strip=True)
        if len(text) > best_len:
            best_len = len(text)
            best = text
    if best:
        return _normalize_text(best)
    return _normalize_text(soup.get_text(" ", strip=True))


def _extract_readability(html: str) -> str:
    try:
        from readability import Document  # type: ignore
    except Exception:  # noqa: BLE001
        return ""
    try:
        doc = Document(html)
        summary_html = doc.summary(html_partial=True)
    except Exception:  # noqa: BLE001
        return ""
    return extract_readable_text(summary_html or "")


def _extract_trafilatura(html: str) -> str:
    try:
        import trafilatura  # type: ignore
    except Exception:  # noqa: BLE001
        return ""
    try:
        extracted = trafilatura.extract(html) or ""
    except Exception:  # noqa: BLE001
        return ""
    return _normalize_text(extracted)


def _extract_jsonld_article_body(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    bodies: list[str] = []
    for script in soup.find_all("script", type=re.compile(r"application/ld\\+json", re.I)):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        bodies.extend(_collect_article_bodies(parsed))
    combined = " ".join(body for body in bodies if body)
    return _normalize_text(combined)


def _collect_article_bodies(payload: Any) -> list[str]:
    bodies: list[str] = []
    if isinstance(payload, list):
        for item in payload:
            bodies.extend(_collect_article_bodies(item))
        return bodies
    if isinstance(payload, dict):
        body = payload.get("articleBody")
        if isinstance(body, str) and body.strip():
            bodies.append(body.strip())
        graph = payload.get("@graph")
        if graph is not None:
            bodies.extend(_collect_article_bodies(graph))
        for value in payload.values():
            if isinstance(value, (dict, list)):
                bodies.extend(_collect_article_bodies(value))
        return bodies
    return bodies


def _extract_css_selectors(
    html: str, include_selectors: list[str], exclude_selectors: list[str]
) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in exclude_selectors:
        try:
            for node in soup.select(selector):
                node.decompose()
        except Exception:  # noqa: BLE001
            continue
    if not include_selectors:
        return ""
    chunks: list[str] = []
    for selector in include_selectors:
        try:
            nodes = soup.select(selector)
        except Exception:  # noqa: BLE001
            continue
        for node in nodes:
            text = node.get_text(" ", strip=True)
            if text:
                chunks.append(text)
    return _normalize_text(" ".join(chunks))


def _strip_patterns(text: str, patterns: list[str]) -> str:
    cleaned = text
    for pattern in patterns:
        try:
            cleaned = re.sub(pattern, " ", cleaned)
        except re.error:
            continue
    return _normalize_text(cleaned)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
