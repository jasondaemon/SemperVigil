from __future__ import annotations

import calendar
import dataclasses
import hashlib
import json
import logging
import sys
import os
import re
import unicodedata
from datetime import date, datetime, timezone, timedelta
from enum import Enum
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    parts = [f"event={event}"]
    for key, value in fields.items():
        parts.append(f"{key}={value}")
    logger.log(level, " ".join(parts))


def configure_logging(logger_name: str, default_level: str = "INFO") -> logging.Logger:
    level_name = os.environ.get("SV_LOG_LEVEL", default_level).upper()
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=getattr(logging, level_name, logging.INFO),
            format="%(asctime)s %(levelname)s %(message)s",
        )
    _ensure_stdout_handler(level_name)
    _maybe_add_file_handler(level_name)
    _apply_log_overrides()
    return logging.getLogger(logger_name)


def _apply_log_overrides() -> None:
    overrides = os.environ.get("SV_LOG_LEVELS", "")
    if not overrides:
        return
    for item in overrides.split(","):
        if not item.strip() or "=" not in item:
            continue
        name, level = item.split("=", 1)
        logger = logging.getLogger(name.strip())
        logger.setLevel(getattr(logging, level.strip().upper(), logging.INFO))


def _maybe_add_file_handler(level_name: str) -> None:
    log_path = os.environ.get("SV_LOG_FILE")
    if not log_path:
        return
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == log_path:
            return
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    handler = logging.FileHandler(log_path)
    handler.setLevel(getattr(logging, level_name, logging.INFO))
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(handler)


def _ensure_stdout_handler(level_name: str) -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream is sys.stdout:
            return
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level_name, logging.INFO))
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(handler)


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, sort_keys=True)


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()
        except TypeError:
            return value.model_dump(mode="json")
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


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


def extract_published_at(
    entry: Any,
    fetched_at: str,
    strategy: str = "published_then_updated",
    allow_dc_date: bool = True,
) -> tuple[str, str]:
    published = _parse_date_value(entry.get("published_parsed") or entry.get("published"))
    updated = _parse_date_value(entry.get("updated_parsed") or entry.get("updated"))

    if strategy == "updated_then_published":
        if updated:
            return updated.isoformat(), "modified"
        if published:
            return published.isoformat(), "published"
    elif strategy == "published_only":
        if published:
            return published.isoformat(), "published"
    elif strategy == "updated_only":
        if updated:
            return updated.isoformat(), "modified"
    else:
        if published:
            return published.isoformat(), "published"
        if updated:
            return updated.isoformat(), "modified"

    if allow_dc_date:
        dc_date = _parse_date_value(
            entry.get("dc_date") or entry.get("dc:date") or entry.get("dc_date_parsed")
        )
        if dc_date:
            return dc_date.isoformat(), "guessed"

    return fetched_at, "guessed"


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def utc_now_iso_offset(*, seconds: int) -> str:
    return (datetime.now(tz=timezone.utc) + timedelta(seconds=seconds)).isoformat()
