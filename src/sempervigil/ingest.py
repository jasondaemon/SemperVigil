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
from .models import Article, Decision, Source
from .tagger import derive_tags
from .storage import article_exists
from .utils import extract_published_at, log_event, normalize_url, stable_id_from_url, utc_now_iso


@dataclass(frozen=True)
class SourceResult:
    source_id: str
    status: str
    http_status: int | None
    found_count: int
    accepted_count: int
    skipped_duplicates: int
    skipped_filters: int
    skipped_missing_url: int
    already_seen_count: int
    error: str | None
    articles: list[Article]
    decisions: list[Decision]
    raw_entry: dict[str, Any] | None


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


def evaluate_entry(
    entry: Any,
    source: Source,
    config: Config,
    conn,
    seen_ids: set[str],
    fetched_at: str,
    ignore_dedupe: bool = False,
) -> tuple[Decision, Article | None]:
    title = (entry.get("title") or "").strip()
    link = entry.get("link") or entry.get("id")
    summary = _entry_summary(
        entry, bool((source.overrides.get("parse") or {}).get("prefer_entry_summary", True))
    )
    derived_tags = derive_tags(source, title, summary)
    combined_text = f"{title} {summary or ''}".strip()
    reasons: list[str] = []
    skip_reasons: list[str] = []
    if not link:
        published_at, published_at_source = extract_published_at(entry, fetched_at)
        decision = Decision(
            decision="SKIP",
            reasons=["missing_url"],
            normalized_url=None,
            stable_id=None,
            published_at=published_at,
            published_at_source=published_at_source,
            title=title,
            original_url=None,
            tags=derived_tags,
        )
        return decision, None

    url_norm_cfg = config.per_source_tweaks.url_normalization
    normalized_url = normalize_url(
        link,
        strip_tracking_params=url_norm_cfg.strip_tracking_params,
        tracking_params=url_norm_cfg.tracking_params,
    )
    stable_id = stable_id_from_url(normalized_url)

    denied_matches = _keyword_match(combined_text, config.ingest.filters.deny_keywords)
    allowed_matches = (
        _keyword_match(combined_text, config.ingest.filters.allow_keywords)
        if config.ingest.filters.allow_keywords
        else []
    )

    if denied_matches:
        reason = f"deny_keywords:{','.join(denied_matches)}"
        reasons.append(reason)
        skip_reasons.append(reason)
    if config.ingest.filters.allow_keywords and not allowed_matches:
        reasons.append("allow_keywords:miss")
        skip_reasons.append("allow_keywords:miss")

    if stable_id in seen_ids:
        if ignore_dedupe:
            reasons.append("already_seen")
        else:
            reasons.append("duplicate")
            skip_reasons.append("duplicate")
    elif config.ingest.dedupe.enabled and article_exists(conn, stable_id):
        if ignore_dedupe:
            reasons.append("already_seen")
        else:
            reasons.append("duplicate")
            skip_reasons.append("duplicate")

    accepted = not skip_reasons
    published_at, published_at_source = extract_published_at(entry, fetched_at)
    decision = Decision(
        decision="ACCEPT" if accepted else "SKIP",
        reasons=reasons,
        normalized_url=normalized_url,
        stable_id=stable_id,
        published_at=published_at,
        published_at_source=published_at_source,
        title=title or normalized_url,
        original_url=link,
        tags=derived_tags,
    )

    if not accepted:
        return decision, None

    article = Article(
        id=stable_id,
        title=title or normalized_url,
        url=normalized_url,
        source_id=source.id,
        published_at=published_at,
        published_at_source=published_at_source,
        fetched_at=fetched_at,
        summary=summary,
        tags=derived_tags,
    )
    return decision, article


def process_source(
    source: Source,
    config: Config,
    logger: logging.Logger,
    conn,
    test_mode: bool = False,
    ignore_dedupe: bool = False,
) -> SourceResult:
    if source.type == "html":
        log_event(logger, logging.WARNING, "source_not_implemented", source_id=source.id)
        return SourceResult(
            source_id=source.id,
            status="not_implemented",
            http_status=None,
            found_count=0,
            accepted_count=0,
            skipped_duplicates=0,
            skipped_filters=0,
            skipped_missing_url=0,
            already_seen_count=0,
            error="HTML sources not implemented",
            articles=[],
            decisions=[],
            raw_entry=None,
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
            skipped_duplicates=0,
            skipped_filters=0,
            skipped_missing_url=0,
            already_seen_count=0,
            error=error or "empty response",
            articles=[],
            decisions=[],
            raw_entry=None,
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

    accepted: list[Article] = []
    decisions: list[Decision] = []
    seen_ids: set[str] = set()
    skipped_duplicates = 0
    skipped_filters = 0
    skipped_missing_url = 0
    already_seen_count = 0
    raw_entry = dict(entries[0]) if test_mode and entries else None

    fetched_at = utc_now_iso()
    for entry in entries:
        decision, article = evaluate_entry(
            entry, source, config, conn, seen_ids, fetched_at, ignore_dedupe=ignore_dedupe
        )
        decisions.append(decision)
        if "already_seen" in decision.reasons:
            already_seen_count += 1
        if decision.decision == "ACCEPT" and article:
            seen_ids.add(article.id)
            accepted.append(article)
            continue
        if decision.decision == "SKIP" and "missing_url" in decision.reasons:
            skipped_missing_url += 1
        if decision.decision == "SKIP" and (
            any(reason.startswith("deny_keywords") for reason in decision.reasons)
            or "allow_keywords:miss" in decision.reasons
        ):
            skipped_filters += 1
        if decision.decision == "SKIP" and "duplicate" in decision.reasons:
            skipped_duplicates += 1

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
            preview=json.dumps([decision.__dict__ for decision in decisions[:20]]),
        )

    return SourceResult(
        source_id=source.id,
        status="ok",
        http_status=http_status,
        found_count=len(entries),
        accepted_count=len(accepted),
        skipped_duplicates=skipped_duplicates,
        skipped_filters=skipped_filters,
        skipped_missing_url=skipped_missing_url,
        already_seen_count=already_seen_count,
        error=None,
        articles=accepted,
        decisions=decisions,
        raw_entry=raw_entry,
    )
