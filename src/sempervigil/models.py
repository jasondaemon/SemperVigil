from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Article:
    id: int | None
    stable_id: str
    original_url: str
    normalized_url: str
    title: str
    source_id: str
    published_at: str | None
    published_at_source: str | None
    ingested_at: str
    summary: str | None
    tags: list[str]


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    enabled: bool
    base_url: str | None
    topic_key: str | None
    default_frequency_minutes: int
    pause_until: str | None
    paused_reason: str | None
    robots_notes: str | None


@dataclass(frozen=True)
class SourceTactic:
    id: int | None
    source_id: str
    tactic_type: str
    enabled: bool
    priority: int
    config: dict[str, object]
    last_success_at: str | None
    last_error_at: str | None
    error_streak: int


@dataclass(frozen=True)
class Job:
    id: str
    job_type: str
    status: str
    payload: dict[str, object]
    requested_at: str
    started_at: str | None
    finished_at: str | None
    locked_by: str | None
    locked_at: str | None
    error: str | None


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
