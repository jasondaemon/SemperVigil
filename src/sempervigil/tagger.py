from __future__ import annotations

import re
from typing import Iterable

from .models import Source


def normalize_tag(tag: str) -> str:
    cleaned = tag.strip().lower()
    cleaned = re.sub(r"\s+", "-", cleaned)
    cleaned = re.sub(r"[^a-z0-9\-]", "-", cleaned)
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")
    return cleaned


def _normalize_tags(tags: Iterable[str], alias_map: dict[str, str]) -> list[str]:
    normalized: list[str] = []
    for tag in tags:
        if not tag:
            continue
        value = normalize_tag(str(tag))
        if not value:
            continue
        value = alias_map.get(value, value)
        normalized.append(value)
    return normalized


def derive_tags(
    source: Source, policy: dict[str, object], title: str | None, summary: str | None
) -> list[str]:
    tags_cfg = policy.get("tags") if isinstance(policy, dict) else {}
    if not tags_cfg and isinstance(policy, dict):
        tags_cfg = policy
    text = f"{title or ''}\n{summary or ''}"

    tag_defaults = tags_cfg.get("tag_defaults") or []
    tag_normalize = tags_cfg.get("tag_normalize") or {}
    tag_rules = tags_cfg.get("tag_rules") or {}
    include_rules = tag_rules.get("include_if") or {}
    exclude_rules = tag_rules.get("exclude_if") or {}

    alias_map = {
        normalize_tag(str(key)): normalize_tag(str(value))
        for key, value in tag_normalize.items()
        if key and value
    }

    tags: list[str] = []
    tags.extend(_normalize_tags(tag_defaults, alias_map))

    for pattern, include_tags in include_rules.items():
        if not pattern:
            continue
        if re.search(pattern, text, flags=re.IGNORECASE):
            tags.extend(_normalize_tags(include_tags, alias_map))

    for pattern, exclude_tags in exclude_rules.items():
        if not pattern:
            continue
        if re.search(pattern, text, flags=re.IGNORECASE):
            for tag in _normalize_tags(exclude_tags, alias_map):
                tags = [value for value in tags if value != tag]

    return sorted(set(tags))
