from __future__ import annotations

from dataclasses import dataclass

# Schema stable as of v0.1 — future changes via migrations only.


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
    overrides: dict[str, object] | None = None
    kind: str | None = None
    url: str | None = None


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
    priority: int
    payload: dict[str, object]
    result: dict[str, object] | None
    requested_at: str
    started_at: str | None
    finished_at: str | None
    locked_by: str | None
    locked_at: str | None
    error: str | None
    queue_name: str | None = None
    attempt_count: int = 0
    max_attempts: int = 0
    available_at: str | None = None
    heartbeat_at: str | None = None
    lease_expires_at: str | None = None
    parent_job_id: str | None = None
    dedupe_key: str | None = None


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
