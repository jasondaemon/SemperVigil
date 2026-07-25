from __future__ import annotations

import json
import logging
import re
from typing import Any


DEFAULT_DISCOVERY = {
    "mode": "default",
    "allowlist_regex": None,
    "blocklist_regex": None,
}

DEFAULT_CONTENT = {
    "mode": "default",
    "min_chars": 800,
    "include_selectors": [],
    "exclude_selectors": [],
    "strip_patterns": [],
    "allow_fallback_to_default": True,
}

DEFAULT_FETCH = {
    "use_vpn": True,
    "http_fetcher": "python_then_curl",
    "http_timeout_seconds": None,
    "http_compressed": True,
    "http_range_chunks": True,
    "http_version": None,
    "http_headers": {},
}


def normalize_source_overrides(
    raw: Any,
    logger: logging.Logger | None = None,
    source_id: str | None = None,
    source_name: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str):
        candidate = raw.strip()
        if candidate:
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                data = parsed
    discovery_raw = data.get("discovery") if isinstance(data.get("discovery"), dict) else {}
    content_raw = data.get("content") if isinstance(data.get("content"), dict) else {}
    fetch_raw = data.get("fetch") if isinstance(data.get("fetch"), dict) else {}

    discovery = {
        **DEFAULT_DISCOVERY,
        "mode": normalize_discovery_mode(
            discovery_raw.get("mode"), logger=logger, source_id=source_id, source_name=source_name
        ),
        "allowlist_regex": _normalize_optional_str(discovery_raw.get("allowlist_regex")),
        "blocklist_regex": _normalize_optional_str(discovery_raw.get("blocklist_regex")),
    }
    content = {
        **DEFAULT_CONTENT,
        "mode": str(content_raw.get("mode") or DEFAULT_CONTENT["mode"]),
        "min_chars": _normalize_int(content_raw.get("min_chars"), DEFAULT_CONTENT["min_chars"]),
        "include_selectors": _normalize_list(content_raw.get("include_selectors")),
        "exclude_selectors": _normalize_list(content_raw.get("exclude_selectors")),
        "strip_patterns": _normalize_list(content_raw.get("strip_patterns")),
        "allow_fallback_to_default": _normalize_bool(
            content_raw.get("allow_fallback_to_default"), DEFAULT_CONTENT["allow_fallback_to_default"]
        ),
    }
    fetch = {
        **DEFAULT_FETCH,
        "use_vpn": _normalize_bool(fetch_raw.get("use_vpn"), DEFAULT_FETCH["use_vpn"]),
        "http_fetcher": _normalize_fetcher(fetch_raw.get("http_fetcher")),
        "http_timeout_seconds": _normalize_optional_int(fetch_raw.get("http_timeout_seconds")),
        "http_compressed": _normalize_bool(
            fetch_raw.get("http_compressed"), DEFAULT_FETCH["http_compressed"]
        ),
        "http_range_chunks": _normalize_bool(
            fetch_raw.get("http_range_chunks"), DEFAULT_FETCH["http_range_chunks"]
        ),
        "http_version": _normalize_http_version(fetch_raw.get("http_version")),
        "http_headers": _normalize_dict(fetch_raw.get("http_headers")),
    }
    return {"discovery": discovery, "content": content, "fetch": fetch}


def get_http_fetch_settings(
    overrides: dict[str, Any] | None,
    default_timeout_seconds: int,
) -> tuple[str, int, dict[str, str]]:
    fetch_cfg = overrides.get("fetch", {}) if isinstance(overrides, dict) else {}
    fetcher = fetch_cfg.get("http_fetcher") if isinstance(fetch_cfg, dict) else None
    timeout = fetch_cfg.get("http_timeout_seconds") if isinstance(fetch_cfg, dict) else None
    headers = fetch_cfg.get("http_headers") if isinstance(fetch_cfg, dict) else None
    fetcher = _normalize_fetcher(fetcher)
    timeout_seconds = _normalize_optional_int(timeout)
    if timeout_seconds is None:
        timeout_seconds = default_timeout_seconds
    headers_dict = _normalize_dict(headers)
    return fetcher, timeout_seconds, headers_dict


def get_http_fetch_compressed(overrides: dict[str, Any] | None) -> bool:
    fetch_cfg = overrides.get("fetch", {}) if isinstance(overrides, dict) else {}
    compressed = fetch_cfg.get("http_compressed") if isinstance(fetch_cfg, dict) else None
    return _normalize_bool(compressed, DEFAULT_FETCH["http_compressed"])


def get_http_fetch_range_chunks(overrides: dict[str, Any] | None) -> bool:
    fetch_cfg = overrides.get("fetch", {}) if isinstance(overrides, dict) else {}
    range_chunks = fetch_cfg.get("http_range_chunks") if isinstance(fetch_cfg, dict) else None
    return _normalize_bool(range_chunks, DEFAULT_FETCH["http_range_chunks"])


def get_http_fetch_version(overrides: dict[str, Any] | None) -> str | None:
    fetch_cfg = overrides.get("fetch", {}) if isinstance(overrides, dict) else {}
    version = fetch_cfg.get("http_version") if isinstance(fetch_cfg, dict) else None
    return _normalize_http_version(version)


def normalize_discovery_mode(
    mode: Any,
    logger: logging.Logger | None = None,
    source_id: str | None = None,
    source_name: str | None = None,
) -> str:
    raw = str(mode).strip() if mode is not None else ""
    if not raw:
        return DEFAULT_DISCOVERY["mode"]
    if raw == "rss_only":
        if logger:
            logger.warning(
                "legacy_discovery_mode source_id=%s source_name=%s mode=%s",
                source_id,
                source_name,
                raw,
            )
        return DEFAULT_DISCOVERY["mode"]
    if raw not in {"default"}:
        if logger:
            logger.warning(
                "unknown_discovery_mode source_id=%s source_name=%s mode=%s",
                source_id,
                source_name,
                raw,
            )
        return DEFAULT_DISCOVERY["mode"]
    return raw


def compile_pattern(pattern: str | None, logger: logging.Logger | None, field: str) -> re.Pattern | None:
    if not pattern:
        return None
    try:
        return re.compile(pattern)
    except re.error as exc:
        if logger:
            logger.warning(
                "invalid_override_regex field=%s error=%s pattern=%s", field, str(exc), pattern
            )
        return None


def should_allow_url(
    url: str, allowlist: re.Pattern | None, blocklist: re.Pattern | None
) -> tuple[bool, str | None]:
    if allowlist and not allowlist.search(url):
        return False, "allowlist_miss"
    if blocklist and blocklist.search(url):
        return False, "blocklist_match"
    return True, None


def _normalize_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        items: list[str] = []
        for raw in value.replace(",", "\n").splitlines():
            item = raw.strip()
            if item:
                items.append(item)
        return items
    return []


def _normalize_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _normalize_fetcher(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"python", "curl", "python_then_curl"}:
        return raw
    return DEFAULT_FETCH["http_fetcher"]


def _normalize_http_version(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if raw in {"1", "1.1", "http1", "http1.1", "http/1.1"}:
        return "1.1"
    if raw in {"2", "2.0", "http2", "http/2", "http/2.0"}:
        return "2"
    return None


def _normalize_dict(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    return {}
