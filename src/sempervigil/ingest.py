from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import feedparser

from .config import Config
from .models import Article, Source
from .storage import article_exists
from .utils import log_event, normalize_url, parse_entry_date, stable_id_from_url, utc_now_iso


@dataclass(frozen=True)
class SourceResult:
    source_id: str
    status: str
    http_status: int | None
    found_count: int
    accepted_count: int
    error: str | None
    articles: list[Article]
    preview: list[dict[str, Any]]


def _fetch_url(
    url: str,
    headers: dict[str, str],
    timeout: int,
    max_retries: int,
    backoff_seconds: int,
) -> tuple[int | None, bytes | None, str | None]:
    attempt = 0
    while attempt <= max_retries:
        try:
            request = Request(url, headers=headers)
            with urlopen(request, timeout=timeout) as response:
                status = response.getcode()
                content = response.read()
            return status, content, None
        except HTTPError as exc:
            return exc.code, exc.read(), str(exc)
        except URLError as exc:
            if attempt >= max_retries:
                return None, None, str(exc)
            time.sleep(backoff_seconds * (attempt + 1))
            attempt += 1
        except Exception as exc:  # noqa: BLE001
            return None, None, str(exc)
    return None, None, "Unknown fetch error"


def _entry_summary(entry: Any, prefer_entry_summary: bool) -> str | None:
    if prefer_entry_summary:
        return entry.get("summary") or entry.get("description")
    return entry.get("description") or entry.get("summary")


def _keyword_match(text: str, keywords: list[str]) -> list[str]:
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword.lower() in lowered]


def process_source(
    source: Source,
    config: Config,
    logger: logging.Logger,
    conn,
    test_mode: bool = False,
) -> SourceResult:
    if source.type == "html":
        log_event(logger, logging.WARNING, "source_not_implemented", source_id=source.id)
        return SourceResult(
            source_id=source.id,
            status="not_implemented",
            http_status=None,
            found_count=0,
            accepted_count=0,
            error="HTML sources not implemented",
            articles=[],
            preview=[],
        )

    overrides = source.overrides or {}
    headers = overrides.get("http_headers") or {}
    if not isinstance(headers, dict):
        headers = {}

    http_cfg = config.ingest.http
    request_headers = {"User-Agent": http_cfg.user_agent}
    request_headers.update({str(k): str(v) for k, v in headers.items()})

    http_status, content, error = _fetch_url(
        source.url,
        headers=request_headers,
        timeout=http_cfg.timeout_seconds,
        max_retries=http_cfg.max_retries,
        backoff_seconds=http_cfg.backoff_seconds,
    )

    if error or not content:
        log_event(
            logger,
            logging.ERROR,
            "source_fetch_failed",
            source_id=source.id,
            error=error or "empty response",
        )
        return SourceResult(
            source_id=source.id,
            status="error",
            http_status=http_status,
            found_count=0,
            accepted_count=0,
            error=error or "empty response",
            articles=[],
            preview=[],
        )

    parsed = feedparser.parse(content)
    entries = parsed.entries or []
    if parsed.bozo:
        log_event(
            logger,
            logging.WARNING,
            "feed_parse_warning",
            source_id=source.id,
            error=str(parsed.bozo_exception),
        )

    allow_keywords = config.ingest.filters.allow_keywords
    deny_keywords = config.ingest.filters.deny_keywords
    prefer_entry_summary = bool(
        (overrides.get("parse") or {}).get("prefer_entry_summary", True)
    )

    url_norm_cfg = config.per_source_tweaks.url_normalization
    prefer_updated = config.per_source_tweaks.date_parsing.prefer_updated_if_published_missing

    accepted: list[Article] = []
    preview: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for entry in entries:
        title = (entry.get("title") or "").strip()
        link = entry.get("link") or entry.get("id")
        summary = _entry_summary(entry, prefer_entry_summary)
        combined_text = f"{title} {summary or ''}".strip()
        reasons: list[str] = []

        if not link:
            reasons.append("missing_url")
            preview.append({
                "title": title,
                "url": None,
                "accepted": False,
                "reasons": reasons,
            })
            continue

        normalized_url = normalize_url(
            link,
            strip_tracking_params=url_norm_cfg.strip_tracking_params,
            tracking_params=url_norm_cfg.tracking_params,
        )
        article_id = stable_id_from_url(normalized_url)

        denied_matches = _keyword_match(combined_text, deny_keywords)
        allowed_matches = _keyword_match(combined_text, allow_keywords) if allow_keywords else []

        if denied_matches:
            reasons.append(f"deny_keywords:{','.join(denied_matches)}")
        if allow_keywords and not allowed_matches:
            reasons.append("allow_keywords:miss")

        if article_id in seen_ids:
            reasons.append("dedupe_run")
        elif config.ingest.dedupe.enabled and article_exists(conn, article_id):
            reasons.append("dedupe")

        accepted_flag = not reasons
        if accepted_flag:
            seen_ids.add(article_id)
            article = Article(
                id=article_id,
                title=title or normalized_url,
                url=normalized_url,
                source_id=source.id,
                published_at=parse_entry_date(entry, prefer_updated),
                fetched_at=utc_now_iso(),
                summary=summary,
                tags=source.tags,
            )
            accepted.append(article)

        preview.append(
            {
                "title": title,
                "url": normalized_url,
                "accepted": accepted_flag,
                "reasons": reasons,
            }
        )

    log_event(
        logger,
        logging.INFO,
        "source_parsed",
        source_id=source.id,
        found_count=len(entries),
        accepted_count=len(accepted),
    )

    if test_mode:
        log_event(
            logger,
            logging.INFO,
            "source_preview",
            source_id=source.id,
            preview=json.dumps(preview[:20]),
        )

    return SourceResult(
        source_id=source.id,
        status="ok",
        http_status=http_status,
        found_count=len(entries),
        accepted_count=len(accepted),
        error=None,
        articles=accepted,
        preview=preview,
    )
