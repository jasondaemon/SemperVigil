from __future__ import annotations

import copy
import logging
from typing import Any

from .utils import log_event


POLICY_DEFAULTS: dict[str, Any] = {
    "fetch": {
        "headers": {},
    },
    "parse": {
        "prefer_entry_summary": True,
    },
    "canonical_url": {
        "strip_tracking_params": True,
        "tracking_params": [
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_term",
            "utm_content",
        ],
    },
    "date": {
        "strategy": "published_then_updated",
        "allow_dc_date": True,
    },
    "dedupe": {
        "enabled": True,
        "strategy": "canonical_url_hash",
    },
    "tags": {
        "tag_defaults": [],
        "tag_normalize": {},
        "tag_rules": {
            "include_if": {},
            "exclude_if": {},
        },
    },
    "publish": {},
}


def resolve_policy(overrides: dict[str, Any] | None, logger: logging.Logger) -> dict[str, Any]:
    base = copy.deepcopy(POLICY_DEFAULTS)
    if not overrides:
        return base
    normalized = _normalize_overrides(overrides)
    merged = _deep_merge(base, normalized, logger, path="policy")
    return merged


def _normalize_overrides(overrides: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(overrides)
    if "tags" not in normalized:
        tags = {}
        if "tag_defaults" in normalized:
            tags["tag_defaults"] = normalized.pop("tag_defaults")
        if "tag_normalize" in normalized:
            tags["tag_normalize"] = normalized.pop("tag_normalize")
        if "tag_rules" in normalized:
            tags["tag_rules"] = normalized.pop("tag_rules")
        if tags:
            normalized["tags"] = tags
    return normalized


def _deep_merge(
    base: dict[str, Any],
    overrides: dict[str, Any],
    logger: logging.Logger,
    path: str,
) -> dict[str, Any]:
    for key, value in overrides.items():
        if key not in base:
            log_event(logger, logging.DEBUG, "policy_unknown_key", path=f"{path}.{key}")
            continue
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_merge(base[key], value, logger, path=f"{path}.{key}")
        else:
            base[key] = value
    return base
