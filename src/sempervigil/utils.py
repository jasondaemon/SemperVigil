from __future__ import annotations

import calendar
import dataclasses
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import sys
import os
import re
import unicodedata
from datetime import date, datetime, timezone, timedelta
import time
from zoneinfo import ZoneInfo
from enum import Enum
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID, uuid4

_LOG_KV_PATTERN = re.compile(r"(?P<key>[A-Za-z0-9_]+)=(?P<value>\"[^\"]*\"|'[^']*'|\\S+)")


def _coerce_log_value(value: str) -> Any:
    text = str(value or "").strip()
    if not text:
        return ""
    if text[0] == text[-1] and text[0] in {'"', "'"} and len(text) >= 2:
        text = text[1:-1]
    lowered = text.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def parse_log_line(line: str) -> dict[str, Any]:
    raw = str(line or "").rstrip("\n")
    if not raw:
        return {"raw": ""}
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict):
            payload.setdefault("raw", raw)
            return payload
    except Exception:
        pass
    event: dict[str, Any] = {"raw": raw, "message": raw}
    matches = list(_LOG_KV_PATTERN.finditer(raw))
    if matches:
        for match in matches:
            key = match.group("key")
            value = match.group("value")
            event[key] = _coerce_log_value(value)
        if "event" in event and "message" in event:
            event["message"] = str(event.get("event") or raw)
    return event


def log_event(logger: logging.Logger | None, level: int, event: str, **fields: Any) -> None:
    if logger is None:
        return
    payload: dict[str, Any] = {"event": event}
    hide_source_id = bool(fields.get("source_name"))
    for key, value in fields.items():
        if value is None or value == "":
            continue
        if hide_source_id and key == "source_id":
            continue
        payload[key] = value
    logger.log(level, json.dumps(payload, default=_json_default, sort_keys=True))


def configure_logging(logger_name: str, default_level: str = "INFO") -> logging.Logger:
    level_name = os.environ.get("SV_LOG_LEVEL", default_level).upper()
    root = logging.getLogger()
    root.setLevel(getattr(logging, level_name, logging.INFO))
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
    max_bytes = int(os.environ.get("SV_LOG_MAX_BYTES", 25 * 1024 * 1024))
    backup_count = int(os.environ.get("SV_LOG_BACKUPS", 3))
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == log_path:
            return
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    handler = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count)
    handler.setLevel(getattr(logging, level_name, logging.INFO))
    handler.setFormatter(_log_formatter())
    root.addHandler(handler)


def _ensure_stdout_handler(level_name: str) -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        if isinstance(handler, logging.StreamHandler):
            if handler.stream is sys.stdout:
                return
            root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level_name, logging.INFO))
    handler.setFormatter(_log_formatter())
    root.addHandler(handler)


def _log_formatter() -> logging.Formatter:
    return _JsonLogFormatter()


class _JsonLogFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__()
        tz_name = os.environ.get("SV_LOG_TZ", "America/New_York")
        try:
            self._zone = ZoneInfo(tz_name)
        except Exception:
            self._zone = None

    def format(self, record: logging.LogRecord) -> str:
        if self._zone is None:
            timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
        else:
            timestamp = datetime.now(self._zone).isoformat(timespec="milliseconds")
        message = record.getMessage()
        parsed = parse_log_line(message)
        payload: dict[str, Any] = {
            "ts": timestamp,
            "level": record.levelname,
            "logger": record.name,
        }
        if isinstance(parsed, dict):
            payload.update({key: value for key, value in parsed.items() if key != "raw"})
            payload.setdefault("message", message)
        else:
            payload["message"] = message
        return json.dumps(payload, default=_json_default, sort_keys=True)


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, sort_keys=True)


def atomic_write_text(path: str | Path, content: str, *, encoding: str = "utf-8") -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    suffix = f".tmp.{os.getpid()}.{uuid4().hex}"
    temp_path = target.with_name(f"{target.name}{suffix}")
    try:
        with temp_path.open("w", encoding=encoding) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        raise


def atomic_write_json(
    path: str | Path,
    obj: Any,
    *,
    indent: int = 2,
    encoding: str = "utf-8",
) -> None:
    payload = json.dumps(obj, indent=indent, default=_json_default)
    atomic_write_text(path, payload, encoding=encoding)


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


def _is_reasonable_publish_time(value: datetime, *, skew_hours: int = 18) -> bool:
    now_utc = datetime.now(tz=timezone.utc)
    return value <= now_utc + timedelta(hours=skew_hours)


def extract_published_at(
    entry: Any,
    fetched_at: str,
    strategy: str = "published_then_updated",
    allow_dc_date: bool = True,
) -> tuple[str, str]:
    published = _parse_date_value(entry.get("published_parsed") or entry.get("published"))
    updated = _parse_date_value(entry.get("updated_parsed") or entry.get("updated"))
    if published and not _is_reasonable_publish_time(published):
        published = None
    if updated and not _is_reasonable_publish_time(updated):
        updated = None

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
        if dc_date and _is_reasonable_publish_time(dc_date):
            return dc_date.isoformat(), "guessed"

    return fetched_at, "guessed"


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def utc_now_iso_offset(*, seconds: int) -> str:
    return (datetime.now(tz=timezone.utc) + timedelta(seconds=seconds)).isoformat()
