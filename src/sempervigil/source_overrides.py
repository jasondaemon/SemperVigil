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


def normalize_source_overrides(raw: Any) -> dict[str, Any]:
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

    discovery = {
        **DEFAULT_DISCOVERY,
        "mode": str(discovery_raw.get("mode") or DEFAULT_DISCOVERY["mode"]),
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
    return {"discovery": discovery, "content": content}


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


def _normalize_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)
