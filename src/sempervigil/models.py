from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Article:
    id: str
    title: str
    url: str
    source_id: str
    published_at: str | None
    published_at_source: str | None
    fetched_at: str
    summary: str | None
    tags: list[str]


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    kind: str
    url: str
    enabled: bool
    section: str
    policy: dict[str, object]


@dataclass(frozen=True)
class Decision:
    decision: str
    reasons: list[str]
    normalized_url: str | None
    stable_id: str | None
    published_at: str | None
    published_at_source: str | None
    title: str
    original_url: str | None
    tags: list[str]
