from __future__ import annotations

import json
import logging
import time
import re
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen

import feedparser
from bs4 import BeautifulSoup

from .config import Config
from .http_fetch import fetch_bytes
from .models import Article, Decision, Source, SourceTactic
from .policy import resolve_policy
from .source_overrides import (
    compile_pattern,
    get_http_fetch_compressed,
    get_http_fetch_range_chunks,
    get_http_fetch_settings,
    normalize_source_overrides,
    should_allow_url,
)
from .tagger import derive_tags
from .storage import article_exists, list_tactics, list_tactics_for_source, upsert_tactic
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
    notes: list[dict[str, Any]] | None = None


def _fetch_url(
    url: str,
    headers: dict[str, str],
    timeout: int,
    max_retries: int,
    backoff_seconds: int,
    *,
    use_vpn: bool = True,
) -> tuple[int | None, bytes | None, str | None]:
    attempt = 0
    while attempt <= max_retries:
        try:
            request = Request(url, headers=headers)
            if use_vpn:
                response_ctx = urlopen(request, timeout=timeout)
            else:
                opener = build_opener(ProxyHandler({}))
                response_ctx = opener.open(request, timeout=timeout)
            with response_ctx as response:
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


def _looks_like_url(text: str) -> bool:
    lowered = text.lower().strip()
    return lowered.startswith("http://") or lowered.startswith("https://")


def _title_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = (parsed.path or "/").strip("/")
    if not path:
        return parsed.netloc or url
    slug = path.split("/")[-1].strip()
    if not slug:
        return parsed.netloc or url
    slug = unquote(slug)
    if slug.lower() in {"index", "rss", "feed"} and len(path.split("/")) > 1:
        slug = path.split("/")[-2]
    text = re.sub(r"[_\-]+", " ", slug).strip()
    text = re.sub(r"\s+", " ", text)
    return text.title() if text else (parsed.netloc or url)


def _normalize_entry_title(title: str, link: str | None) -> str:
    clean = (title or "").strip()
    if not clean:
        return clean
    if _looks_like_url(clean):
        candidate = link or clean
        return _title_from_url(candidate)
    return clean


def evaluate_entry(
    entry: Any,
    source: Source,
    policy: dict[str, Any],
    config: Config,
    conn,
    seen_ids: set[str],
    fetched_at: str,
    ignore_dedupe: bool = False,
) -> tuple[Decision, Article | None]:
    title = (entry.get("title") or "").strip()
    link = entry.get("link") or entry.get("id")
    title = _normalize_entry_title(title, link)
    prefer_entry_summary = bool(policy.get("parse", {}).get("prefer_entry_summary", True))
    summary = _entry_summary(entry, prefer_entry_summary)
    derived_tags = derive_tags(policy.get("tags", {}), title, summary)
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

    url_norm_cfg = policy.get("canonical_url", {})
    normalized_url = normalize_url(
        link,
        strip_tracking_params=bool(url_norm_cfg.get("strip_tracking_params", True)),
        tracking_params=list(url_norm_cfg.get("tracking_params", [])),
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
    elif policy.get("dedupe", {}).get("enabled", True) and article_exists(
        conn, source.id, stable_id
    ):
        if ignore_dedupe:
            reasons.append("already_seen")
        else:
            reasons.append("duplicate")
            skip_reasons.append("duplicate")

    accepted = not skip_reasons
    date_cfg = policy.get("date", {})
    published_at, published_at_source = extract_published_at(
        entry,
        fetched_at,
        strategy=str(date_cfg.get("strategy", "published_then_updated")),
        allow_dc_date=bool(date_cfg.get("allow_dc_date", True)),
    )
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
        id=None,
        source_id=source.id,
        stable_id=stable_id,
        original_url=link,
        normalized_url=normalized_url,
        title=title or normalized_url,
        published_at=published_at,
        published_at_source=published_at_source,
        ingested_at=fetched_at,
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
    tactics = list_tactics(conn, source.id)
    overrides = normalize_source_overrides(
        source.overrides, logger=logger, source_id=source.id, source_name=source.name
    )
    discovery_cfg = overrides.get("discovery", {})
    allow_re = compile_pattern(discovery_cfg.get("allowlist_regex"), logger, "discovery.allowlist_regex")
    block_re = compile_pattern(discovery_cfg.get("blocklist_regex"), logger, "discovery.blocklist_regex")
    if not tactics:
        existing_tactics = list_tactics_for_source(conn, source.id)
        if not existing_tactics:
            source_kind = source.kind or "rss"
            base_url = source.url or source.base_url
            if base_url:
                tactic_type = "rss" if source_kind == "rss" else "html_index"
                config = {"feed_url": base_url}
                tactic = SourceTactic(
                    id=None,
                    source_id=source.id,
                    tactic_type=tactic_type,
                    enabled=True,
                    priority=0,
                    config=config,
                    last_success_at=None,
                    last_error_at=None,
                    error_streak=0,
                )
                upsert_tactic(conn, tactic)
                tactics = [tactic]
        if not tactics:
            return SourceResult(
                source_id=source.id,
                status="error",
                http_status=None,
                found_count=0,
                accepted_count=0,
                skipped_duplicates=0,
                skipped_filters=0,
                skipped_missing_url=0,
                already_seen_count=0,
                error="No enabled tactics for source",
                articles=[],
                decisions=[],
                raw_entry=None,
                notes=[{"tactic_type": "none", "status": "error", "error": "no tactics"}],
            )

    source_kind = getattr(source, "kind", None)
    if source_kind is None and isinstance(source.overrides, dict):
        source_kind = source.overrides.get("kind")
    if source_kind == "rss":
        tactics = sorted(
            tactics,
            key=lambda tactic: 0 if tactic.tactic_type == "rss" else 1,
        )

    notes: list[dict[str, Any]] = []
    final_result: SourceResult | None = None
    for tactic in tactics:
        result, note = _run_tactic(
            source,
            tactic,
            config,
            logger,
            conn,
            overrides,
            discovery_cfg=discovery_cfg,
            allow_re=allow_re,
            block_re=block_re,
            test_mode=test_mode,
            ignore_dedupe=ignore_dedupe,
        )
        notes.append(note)
        if result.status == "ok":
            final_result = result
            if result.accepted_count > 0:
                break
            continue
        final_result = result

    if final_result is None:
        final_result = result
    return SourceResult(
        **{**final_result.__dict__, "notes": notes},
    )


def _run_tactic(
    source: Source,
    tactic: SourceTactic,
    config: Config,
    logger: logging.Logger,
    conn,
    overrides: dict[str, Any] | None,
    discovery_cfg: dict[str, Any],
    allow_re: re.Pattern | None,
    block_re: re.Pattern | None,
    test_mode: bool,
    ignore_dedupe: bool,
) -> tuple[SourceResult, dict[str, Any]]:
    def _looks_like_feed_payload(payload: bytes, content_type: str | None) -> bool:
        # Some sites intermittently return HTML challenge/marketing pages at feed URLs.
        # Treat those as fetch errors so we do not ingest navigation/footer links as articles.
        content_type_lc = (content_type or "").lower()
        if any(
            token in content_type_lc
            for token in ("application/rss+xml", "application/atom+xml", "application/xml", "text/xml")
        ):
            return True
        probe = payload[:16384].lstrip().lower()
        return b"<rss" in probe or b"<feed" in probe or b"<rdf" in probe

    if overrides is None:
        overrides = {}
    policy = resolve_policy(tactic.config or {}, logger)
    prefer_entry_summary = bool(policy.get("parse", {}).get("prefer_entry_summary", True))
    dedupe_strategy = policy.get("dedupe", {}).get("strategy")
    if dedupe_strategy and dedupe_strategy != "canonical_url_hash":
        log_event(
            logger,
            logging.DEBUG,
            "dedupe_strategy_unsupported",
            source_id=source.id,
            strategy=dedupe_strategy,
        )

    feed_url = tactic.config.get("feed_url") if tactic.config else None
    if not feed_url:
        return (
            SourceResult(
                source_id=source.id,
                status="error",
                http_status=None,
                found_count=0,
                accepted_count=0,
                skipped_duplicates=0,
                skipped_filters=0,
                skipped_missing_url=0,
                already_seen_count=0,
                error="Missing feed_url in tactic config",
                articles=[],
                decisions=[],
                raw_entry=None,
            ),
            {"tactic_type": tactic.tactic_type, "status": "error", "error": "missing feed_url"},
        )

    headers = policy.get("fetch", {}).get("headers") or {}
    if not isinstance(headers, dict):
        headers = {}

    http_cfg = config.ingest.http
    fetcher, fetch_timeout_seconds, fetch_headers = get_http_fetch_settings(
        overrides, http_cfg.timeout_seconds
    )
    fetch_compressed = get_http_fetch_compressed(overrides)
    fetch_range_chunks = get_http_fetch_range_chunks(overrides)
    fetch_cfg = overrides.get("fetch", {}) if isinstance(overrides, dict) else {}
    use_vpn = bool(fetch_cfg.get("use_vpn", True)) if isinstance(fetch_cfg, dict) else True
    request_headers = {"User-Agent": http_cfg.user_agent}
    request_headers.update({str(k): str(v) for k, v in headers.items()})
    request_headers.update(fetch_headers)

    if tactic.tactic_type == "html_index":
        http_status, content, error = _fetch_url(
            feed_url,
            headers=request_headers,
            timeout=fetch_timeout_seconds,
            max_retries=http_cfg.max_retries,
            backoff_seconds=http_cfg.backoff_seconds,
            use_vpn=use_vpn,
        )
        if error or not content:
            return (
                SourceResult(
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
                ),
                {
                    "tactic_type": tactic.tactic_type,
                    "status": "error",
                    "http_status": http_status,
                    "error": error or "empty response",
                },
            )
        entries: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        content_text = ""
        try:
            content_text = content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                content_text = content.decode("latin-1")
            except Exception:  # noqa: BLE001
                content_text = ""

        json_payload: dict[str, Any] | None = None
        if content_text:
            stripped = content_text.lstrip()
            if stripped.startswith("{") or stripped.startswith("["):
                try:
                    parsed_payload = json.loads(content_text)
                    if isinstance(parsed_payload, dict):
                        json_payload = parsed_payload
                except Exception:  # noqa: BLE001
                    json_payload = None

        if isinstance(json_payload, dict) and isinstance(json_payload.get("results"), list):
            for item in json_payload["results"]:
                if not isinstance(item, dict):
                    continue
                href = item.get("permalink") or item.get("readMoreLink") or item.get("url")
                if not isinstance(href, str) or not href.strip():
                    continue
                href = href.strip()
                if href.startswith("#"):
                    continue
                parsed = urlparse(href)
                if parsed.scheme in {"mailto", "javascript"}:
                    continue
                if parsed.scheme in {"http", "https"}:
                    url = href
                else:
                    if "msstoreapiprod/api/msrc" in feed_url and href.startswith("/"):
                        url = urljoin("https://www.microsoft.com/en-us/msrc/", href.lstrip("/"))
                    else:
                        url = urljoin(feed_url, href)
                parsed_url = urlparse(url)
                if parsed_url.scheme not in {"http", "https"}:
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                title_text = item.get("title") if isinstance(item.get("title"), str) else url
                summary_text = (
                    item.get("summary")
                    if isinstance(item.get("summary"), str)
                    else item.get("blurb") if isinstance(item.get("blurb"), str) else ""
                )
                entry: dict[str, Any] = {"link": url, "title": title_text, "summary": summary_text}
                if isinstance(item.get("publishedDate"), str):
                    entry["published"] = item["publishedDate"]
                if isinstance(item.get("updatedDate"), str):
                    entry["updated"] = item["updatedDate"]
                entries.append(entry)
        else:
            soup = BeautifulSoup(content, "html.parser")
            entry_selector = tactic.config.get("entry_selector") if tactic.config else None
            link_selector = tactic.config.get("link_selector") if tactic.config else None
            containers = soup.select(entry_selector) if entry_selector else [soup]
            for container in containers:
                anchors = (
                    container.select(link_selector) if link_selector else container.find_all("a")
                )
                for anchor in anchors:
                    href = anchor.get("href")
                    if not href:
                        continue
                    if href.startswith("#"):
                        continue
                    parsed = urlparse(href)
                    if parsed.scheme in {"mailto", "javascript"}:
                        continue
                    url = urljoin(feed_url, href)
                    parsed_url = urlparse(url)
                    if parsed_url.scheme not in {"http", "https"}:
                        continue
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    text = anchor.get_text(" ", strip=True) or url
                    entries.append({"link": url, "title": text, "summary": ""})
        accepted: list[Article] = []
        decisions: list[Decision] = []
        seen_ids: set[str] = set()
        skipped_duplicates = 0
        skipped_filters = 0
        skipped_missing_url = 0
        already_seen_count = 0
        raw_entry = dict(entries[0]) if test_mode and entries else None
        fetched_at = utc_now_iso()
        total_entries = len(entries)
        for index, entry in enumerate(entries, start=1):
            link = entry.get("link") or entry.get("id")
            if link:
                allowed, reason = should_allow_url(link, allow_re, block_re)
                if not allowed:
                    decision = _skip_override_decision(
                        entry,
                        policy,
                        fetched_at,
                        reason or "override_filter",
                        prefer_entry_summary,
                    )
                    decisions.append(decision)
                    skipped_filters += 1
                    continue
            decision, article = evaluate_entry(
                entry,
                source,
                policy,
                config,
                conn,
                seen_ids,
                fetched_at,
                ignore_dedupe=ignore_dedupe,
            )
            if logger.isEnabledFor(logging.DEBUG) and total_entries:
                log_event(
                    logger,
                    logging.DEBUG,
                    "ingest_progress",
                    source_id=source.id,
                    source_name=source.name,
                    i=index,
                    total=total_entries,
                )
            decisions.append(decision)
            if "already_seen" in decision.reasons:
                already_seen_count += 1
            if decision.decision == "ACCEPT" and article:
                seen_ids.add(article.stable_id)
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
        return (
            SourceResult(
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
            ),
            {
                "tactic_type": tactic.tactic_type,
                "status": "ok",
                "http_status": http_status,
                "found_count": len(entries),
                "accepted_count": len(accepted),
            },
        )

    if tactic.tactic_type in {"sitemap", "article_html", "jsonfeed"}:
        return (
            SourceResult(
                source_id=source.id,
                status="not_implemented",
                http_status=None,
                found_count=0,
                accepted_count=0,
                skipped_duplicates=0,
                skipped_filters=0,
                skipped_missing_url=0,
                already_seen_count=0,
                error=f"Tactic {tactic.tactic_type} not implemented",
                articles=[],
                decisions=[],
                raw_entry=None,
            ),
            {"tactic_type": tactic.tactic_type, "status": "not_implemented"},
        )

    if tactic.tactic_type == "rss":
        headers_dict: dict[str, str] = {}
        try:
            http_status, _final_url, headers_dict, content, fetcher_used = fetch_bytes(
                feed_url,
                headers=request_headers,
                timeout_seconds=fetch_timeout_seconds,
                fetcher=fetcher,
                compressed=fetch_compressed,
                range_chunks=fetch_range_chunks,
            )
        except Exception as exc:  # noqa: BLE001
            http_status = None
            content = b""
            error = str(exc)
            fetcher_used = fetcher
        else:
            error = None
        if fetcher_used:
            log_event(
                logger,
                logging.DEBUG,
                "rss_fetcher_used",
                source_id=source.id,
                tactic_type=tactic.tactic_type,
                fetcher_used=fetcher_used,
            )
        content_type = None
        if isinstance(headers_dict, dict):
            content_type = headers_dict.get("content-type")
        if content and not _looks_like_feed_payload(content, content_type):
            return (
                SourceResult(
                    source_id=source.id,
                    status="error",
                    http_status=http_status,
                    found_count=0,
                    accepted_count=0,
                    skipped_duplicates=0,
                    skipped_filters=0,
                    skipped_missing_url=0,
                    already_seen_count=0,
                    error=f"rss_non_feed_payload content_type={content_type or 'unknown'}",
                    articles=[],
                    decisions=[],
                    raw_entry=None,
                ),
                {
                    "tactic_type": tactic.tactic_type,
                    "status": "error",
                    "http_status": http_status,
                    "error": f"rss_non_feed_payload content_type={content_type or 'unknown'}",
                },
            )
    else:
        http_status, content, error = _fetch_url(
            feed_url,
            headers=request_headers,
            timeout=http_cfg.timeout_seconds,
            max_retries=http_cfg.max_retries,
            backoff_seconds=http_cfg.backoff_seconds,
            use_vpn=use_vpn,
        )

    if error or not content:
        return (
            SourceResult(
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
            ),
            {
                "tactic_type": tactic.tactic_type,
                "status": "error",
                "http_status": http_status,
                "error": error or "empty response",
            },
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
    total_entries = len(entries)
    for index, entry in enumerate(entries, start=1):
        link = entry.get("link") or entry.get("id")
        if link:
            allowed, reason = should_allow_url(link, allow_re, block_re)
            if not allowed:
                decision = _skip_override_decision(
                    entry,
                    policy,
                    fetched_at,
                    reason or "override_filter",
                    prefer_entry_summary,
                )
                decisions.append(decision)
                skipped_filters += 1
                continue
        decision, article = evaluate_entry(
            entry,
            source,
            policy,
            config,
            conn,
            seen_ids,
            fetched_at,
            ignore_dedupe=ignore_dedupe,
        )
        if logger.isEnabledFor(logging.DEBUG) and total_entries:
            log_event(
                logger,
                logging.DEBUG,
                "ingest_progress",
                source_id=source.id,
                source_name=source.name,
                i=index,
                total=total_entries,
            )
        decisions.append(decision)
        if "already_seen" in decision.reasons:
            already_seen_count += 1
        if decision.decision == "ACCEPT" and article:
            seen_ids.add(article.stable_id)
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

    return (
        SourceResult(
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
        ),
        {
            "tactic_type": tactic.tactic_type,
            "status": "ok",
            "http_status": http_status,
            "found_count": len(entries),
            "accepted_count": len(accepted),
        },
    )


def _skip_override_decision(
    entry: Any,
    policy: dict[str, Any],
    fetched_at: str,
    reason: str,
    prefer_entry_summary: bool,
) -> Decision:
    title = (entry.get("title") or "").strip()
    link = entry.get("link") or entry.get("id")
    title = _normalize_entry_title(title, link)
    summary = _entry_summary(entry, prefer_entry_summary)
    derived_tags = derive_tags(policy.get("tags", {}), title, summary)
    normalized_url = None
    stable_id = None
    if link:
        url_norm_cfg = policy.get("canonical_url", {})
        normalized_url = normalize_url(
            link,
            strip_tracking_params=bool(url_norm_cfg.get("strip_tracking_params", True)),
            tracking_params=list(url_norm_cfg.get("tracking_params", [])),
        )
        stable_id = stable_id_from_url(normalized_url)
    published_at, published_at_source = extract_published_at(entry, fetched_at)
    return Decision(
        decision="SKIP",
        reasons=[reason],
        normalized_url=normalized_url,
        stable_id=stable_id,
        published_at=published_at,
        published_at_source=published_at_source,
        title=title or normalized_url or (link or ""),
        original_url=link,
        tags=derived_tags,
    )
