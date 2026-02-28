from __future__ import annotations

import json
import logging
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests
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
    fetch_cfg = normalize_source_overrides(overrides or {}).get("fetch", {})
    request_headers: dict[str, str] = {"User-Agent": user_agent}
    raw_headers = fetch_cfg.get("http_headers", {}) if isinstance(fetch_cfg, dict) else {}
    if isinstance(raw_headers, dict):
        for key, value in raw_headers.items():
            request_headers[str(key)] = str(value)
    request = urllib.request.Request(url, headers=request_headers)
    use_vpn = bool(fetch_cfg.get("use_vpn", True))
    used_vpn = use_vpn
    try:
        with _open_request(request, timeout_seconds, use_vpn=used_vpn) as response:
            status_code = response.getcode()
            raw = response.read()
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
            vpn_used=used_vpn,
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
                vpn_used=used_vpn,
                source_id=source_id,
                source_name=source_name,
                body_snippet=(snippet or "")[:200],
            )
            # Some sites (including Cloudflare-protected origins) block urllib TLS fingerprints
            # but allow requests/urllib3. Retry once with requests before failing the job.
            try:
                status_code, raw = _fetch_with_requests(
                    url,
                    request_headers,
                    timeout_seconds,
                    use_vpn=used_vpn,
                )
                log_event(
                    logger,
                    logging.INFO,
                    "content_fetch_retry_requests_ok",
                    url=url,
                    status_code=status_code,
                    vpn_used=used_vpn,
                    source_id=source_id,
                    source_name=source_name,
                )
            except Exception as retry_exc:  # noqa: BLE001
                log_event(
                    logger,
                    logging.WARNING,
                    "content_fetch_retry_requests_failed",
                    url=url,
                    status_code=status_code,
                    vpn_used=used_vpn,
                    source_id=source_id,
                    source_name=source_name,
                    error=str(retry_exc),
                )
                raise
            else:
                html = raw.decode("utf-8", errors="replace")
                extracted = extract_content_from_html(html, overrides=overrides, logger=logger)
                published_at, published_at_source = extract_published_at_from_html(html)
                return {
                    "content_text": extracted["content_text"],
                    "content_html": html,
                    "method": f'{extracted["method"]}:requests_retry',
                    "published_at": published_at,
                    "published_at_source": published_at_source,
                }
        raise
    except Exception as exc:  # noqa: BLE001
        # Some sources intermittently time out over VPN; retry once directly.
        if use_vpn and _is_timeout_error(exc):
            log_event(
                logger,
                logging.INFO,
                "content_fetch_retry_no_vpn",
                url=url,
                source_id=source_id,
                source_name=source_name,
            )
            used_vpn = False
            with _open_request(request, timeout_seconds, use_vpn=used_vpn) as response:
                status_code = response.getcode()
                raw = response.read()
            log_event(
                logger,
                logging.INFO,
                "content_fetch_done",
                url=url,
                status_code=status_code,
                vpn_used=used_vpn,
                source_id=source_id,
                source_name=source_name,
            )
        else:
            log_event(
                logger,
                logging.WARNING,
                "content_fetch_failed",
                url=url,
                status_code=None,
                vpn_used=used_vpn,
                source_id=source_id,
                source_name=source_name,
                error=str(exc),
            )
            raise
    else:
        log_event(
            logger,
            logging.INFO,
            "content_fetch_done",
            url=url,
            status_code=status_code,
            vpn_used=used_vpn,
            source_id=source_id,
            source_name=source_name,
        )
    html = raw.decode("utf-8", errors="replace")
    extracted = extract_content_from_html(html, overrides=overrides, logger=logger)
    published_at, published_at_source = extract_published_at_from_html(html)
    return {
        "content_text": extracted["content_text"],
        "content_html": html,
        "method": extracted["method"],
        "published_at": published_at,
        "published_at_source": published_at_source,
    }


def _open_request(request: urllib.request.Request, timeout_seconds: int, *, use_vpn: bool):
    if use_vpn:
        return urllib.request.urlopen(request, timeout=timeout_seconds)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(request, timeout=timeout_seconds)


def _fetch_with_requests(
    url: str, headers: dict[str, str], timeout_seconds: int, *, use_vpn: bool
) -> tuple[int, bytes]:
    session = requests.Session()
    # Respect container proxy env for VPN path; bypass env proxy when VPN is disabled.
    session.trust_env = use_vpn
    response = session.get(url, headers=headers, timeout=timeout_seconds)
    response.raise_for_status()
    return int(response.status_code), response.content


def _is_timeout_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "timed out" in message or "timeout" in message


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


def _normalize_published_candidate(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000.0
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
        except Exception:  # noqa: BLE001
            return None
    text = str(value).strip()
    if not text:
        return None
    if not re.search(r"\b\d{4}\b", text):
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except Exception:  # noqa: BLE001
        return None


def _find_jsonld_dates(payload: Any) -> list[str]:
    dates: list[str] = []
    if isinstance(payload, list):
        for item in payload:
            dates.extend(_find_jsonld_dates(item))
        return dates
    if isinstance(payload, dict):
        for key in ("datePublished", "dateCreated", "dateModified", "uploadDate"):
            value = payload.get(key)
            if value is not None:
                dates.append(str(value))
        graph = payload.get("@graph")
        if graph is not None:
            dates.extend(_find_jsonld_dates(graph))
        for value in payload.values():
            if isinstance(value, (dict, list)):
                dates.extend(_find_jsonld_dates(value))
    return dates


def extract_published_at_from_html(html: str) -> tuple[str | None, str | None]:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type=re.compile(r"application/ld\\+json", re.I)):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:  # noqa: BLE001
            continue
        for candidate in _find_jsonld_dates(payload):
            normalized = _normalize_published_candidate(candidate)
            if normalized:
                return normalized, "html_jsonld"

    meta_fields = [
        ("property", "article:published_time"),
        ("property", "og:published_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "date"),
        ("name", "datePublished"),
        ("name", "dc.date"),
        ("name", "article:published_time"),
        ("itemprop", "datePublished"),
        ("itemprop", "dateModified"),
    ]
    for attr, name in meta_fields:
        node = soup.find("meta", attrs={attr: name})
        if not node:
            continue
        candidate = node.get("content") or node.get("value")
        normalized = _normalize_published_candidate(candidate)
        if normalized:
            return normalized, f"html_meta_{name}"

    for node in soup.find_all("time"):
        candidate = node.get("datetime") or node.get_text(" ", strip=True)
        normalized = _normalize_published_candidate(candidate)
        if normalized:
            return normalized, "html_time"

    # Fallback for JS blobs where datePublished is embedded but not valid JSON.
    regex_candidates = [
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'"dateModified"\s*:\s*"([^"]+)"',
        r'"published_time"\s*:\s*"([^"]+)"',
        r'"publishDate"\s*:\s*"([^"]+)"',
    ]
    for pattern in regex_candidates:
        match = re.search(pattern, html, re.IGNORECASE)
        if not match:
            continue
        normalized = _normalize_published_candidate(match.group(1))
        if normalized:
            return normalized, "html_regex"
    return None, None
