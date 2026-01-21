from __future__ import annotations

import calendar
import hashlib
import logging
import re
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    parts = [f"event={event}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    logger.log(level, " ".join(parts))


def normalize_url(url: str, strip_tracking_params: bool, tracking_params: list[str]) -> str:
    if not url:
        return url
    split = urlsplit(url)
    scheme = split.scheme.lower() if split.scheme else "http"
    netloc = split.netloc.lower()
    path = split.path or "/"
    query_params = parse_qsl(split.query, keep_blank_values=True)
    if strip_tracking_params:
        tracking_set = {param.lower() for param in tracking_params}
        query_params = [
            (key, value)
            for key, value in query_params
            if key.lower() not in tracking_set
        ]
    query = urlencode(sorted(query_params)) if query_params else ""
    normalized = urlunsplit((scheme, netloc, path, query, ""))
    return normalized


def stable_id_from_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def slugify(text: str, max_length: int = 80) -> str:
    if not text:
        return "untitled"
    normalized = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", normalized).strip("-").lower()
    cleaned = cleaned or "untitled"
    return cleaned[:max_length].strip("-") or "untitled"


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_date_value(value: Any) -> datetime | None:
    if value is None:
        return None
    if hasattr(value, "tm_year"):
        return datetime.fromtimestamp(calendar.timegm(value), tz=timezone.utc)
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if isinstance(value, str):
        try:
            parsed = parsedate_to_datetime(value)
            return _normalize_datetime(parsed)
        except (TypeError, ValueError):
            try:
                parsed = datetime.fromisoformat(value)
                return _normalize_datetime(parsed)
            except ValueError:
                return None
    return None


def extract_published_at(entry: Any, fetched_at: str) -> tuple[str, str]:
    published = _parse_date_value(entry.get("published_parsed") or entry.get("published"))
    if published:
        return published.isoformat(), "published"

    updated = _parse_date_value(entry.get("updated_parsed") or entry.get("updated"))
    if updated:
        return updated.isoformat(), "updated"

    dc_date = _parse_date_value(
        entry.get("dc_date") or entry.get("dc:date") or entry.get("dc_date_parsed")
    )
    if dc_date:
        return dc_date.isoformat(), "dc_date"

    return fetched_at, "fallback_fetched_at"


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
