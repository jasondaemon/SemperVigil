from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import yaml

from .models import Source

class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class AppConfig:
    name: str
    timezone: str


@dataclass(frozen=True)
class PathsConfig:
    data_dir: str
    output_dir: str
    state_db: str
    run_reports_dir: str


@dataclass(frozen=True)
class PublishingConfig:
    format: str
    hugo_section: str
    write_json_index: bool
    json_index_path: str


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: int
    user_agent: str
    max_retries: int
    backoff_seconds: int


@dataclass(frozen=True)
class DedupeConfig:
    enabled: bool
    strategy: str


@dataclass(frozen=True)
class FiltersConfig:
    allow_keywords: list[str]
    deny_keywords: list[str]


@dataclass(frozen=True)
class IngestConfig:
    http: HttpConfig
    dedupe: DedupeConfig
    filters: FiltersConfig


@dataclass(frozen=True)
class UrlNormalizationConfig:
    strip_tracking_params: bool
    tracking_params: list[str]


@dataclass(frozen=True)
class DateParsingConfig:
    prefer_updated_if_published_missing: bool


@dataclass(frozen=True)
class PerSourceTweaks:
    url_normalization: UrlNormalizationConfig
    date_parsing: DateParsingConfig


@dataclass(frozen=True)
class Config:
    app: AppConfig
    paths: PathsConfig
    publishing: PublishingConfig
    ingest: IngestConfig
    llm: dict[str, Any]
    sources: list[Source]
    per_source_tweaks: PerSourceTweaks


DEFAULT_CONFIG: dict[str, Any] = {
    "app": {
        "name": "SemperVigil",
        "timezone": "UTC",
    },
    "paths": {
        "data_dir": "/data",
        "output_dir": "/site/content/posts",
        "state_db": "/data/state.sqlite3",
        "run_reports_dir": "/data/reports",
    },
    "publishing": {
        "format": "hugo_markdown",
        "hugo_section": "posts",
        "write_json_index": True,
        "json_index_path": "/site/static/sempervigil/index.json",
    },
    "ingest": {
        "http": {
            "timeout_seconds": 20,
            "user_agent": "SemperVigil/0.1",
            "max_retries": 2,
            "backoff_seconds": 2,
        },
        "dedupe": {
            "enabled": True,
            "strategy": "canonical_url_hash",
        },
        "filters": {
            "allow_keywords": [],
            "deny_keywords": [],
        },
    },
    "llm": {
        "enabled": False,
    },
    "per_source_tweaks": {
        "date_parsing": {
            "prefer_updated_if_published_missing": True,
        },
        "url_normalization": {
            "strip_tracking_params": True,
            "tracking_params": [
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
            ],
        },
    },
}


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merged[key] = _merge_dicts(base[key], value)
        else:
            merged[key] = value
    return merged


def _require(value: Any, path: str) -> Any:
    if value is None:
        raise ConfigError(f"Missing required config value: {path}")
    return value


def _require_str(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"Expected non-empty string at {path}")
    return value


def _as_list(value: Any, path: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    raise ConfigError(f"Expected list of strings at {path}")


def _parse_sources_list(sources_raw: Any) -> list[Source]:
    if not isinstance(sources_raw, list):
        raise ConfigError("sources must be a list of source entries")
    source_ids: set[str] = set()
    sources: list[Source] = []
    for index, source in enumerate(sources_raw):
        if not isinstance(source, dict):
            raise ConfigError(f"Source entry at index {index} must be a mapping")
        source_id = _require(source.get("id"), f"sources[{index}].id")
        if not isinstance(source_id, str) or not source_id.strip():
            raise ConfigError(f"sources[{index}].id must be a non-empty string")
        if source_id in source_ids:
            raise ConfigError(f"Duplicate source id '{source_id}'")
        source_ids.add(source_id)
        source_kind = _require(source.get("type"), f"sources[{index}].type")
        if source_kind not in {"rss", "atom", "html"}:
            raise ConfigError(
                f"sources[{index}].type must be one of rss, atom, html (got {source_kind})"
            )
        url = _require(source.get("url"), f"sources[{index}].url")
        if not isinstance(url, str) or not url.strip():
            raise ConfigError(f"sources[{index}].url must be a non-empty string")
        enabled = bool(source.get("enabled", True))
        name = source.get("name") or source_id
        tags = _as_list(source.get("tags"), f"sources[{index}].tags")
        overrides = source.get("overrides") or {}
        if overrides and not isinstance(overrides, dict):
            raise ConfigError(f"sources[{index}].overrides must be a mapping")
        default_frequency_minutes = int(source.get("default_frequency_minutes", 60))
        sources.append(
            Source(
                id=source_id,
                name=name,
                enabled=enabled,
                base_url=url,
                topic_key=None,
                default_frequency_minutes=default_frequency_minutes,
                pause_until=None,
                paused_reason=None,
                robots_notes=None,
            )
        )
    return sources


def load_config(path: str | None = None) -> Config:
    config_path = path or os.environ.get("SV_CONFIG_PATH", "/config/config.yml")
    if not os.path.exists(config_path):
        raise ConfigError(f"Config file not found at {config_path}")

    with open(config_path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if not isinstance(raw, dict):
        raise ConfigError("Config file must contain a YAML mapping at the top level")

    merged = _merge_dicts(DEFAULT_CONFIG, raw)

    sources: list[Source] = []
    if "sources" in raw:
        sources_raw = raw.get("sources")
        sources = _parse_sources_list(sources_raw)

    app_cfg = merged.get("app") or {}
    paths_cfg = merged.get("paths") or {}
    publishing_cfg = merged.get("publishing") or {}
    ingest_cfg = merged.get("ingest") or {}
    tweaks_cfg = merged.get("per_source_tweaks") or {}

    app = AppConfig(
        name=str(app_cfg.get("name", DEFAULT_CONFIG["app"]["name"])),
        timezone=str(app_cfg.get("timezone", DEFAULT_CONFIG["app"]["timezone"])),
    )

    paths = PathsConfig(
        data_dir=_require_str(paths_cfg.get("data_dir"), "paths.data_dir"),
        output_dir=_require_str(paths_cfg.get("output_dir"), "paths.output_dir"),
        state_db=_require_str(paths_cfg.get("state_db"), "paths.state_db"),
        run_reports_dir=_require_str(paths_cfg.get("run_reports_dir"), "paths.run_reports_dir"),
    )

    publishing = PublishingConfig(
        format=str(publishing_cfg.get("format", DEFAULT_CONFIG["publishing"]["format"])),
        hugo_section=str(
            publishing_cfg.get(
                "hugo_section", DEFAULT_CONFIG["publishing"]["hugo_section"]
            )
        ),
        write_json_index=bool(
            publishing_cfg.get(
                "write_json_index", DEFAULT_CONFIG["publishing"]["write_json_index"]
            )
        ),
        json_index_path=str(
            publishing_cfg.get(
                "json_index_path", DEFAULT_CONFIG["publishing"]["json_index_path"]
            )
        ),
    )

    http_cfg = ingest_cfg.get("http") or {}
    dedupe_cfg = ingest_cfg.get("dedupe") or {}
    filters_cfg = ingest_cfg.get("filters") or {}

    http = HttpConfig(
        timeout_seconds=int(
            http_cfg.get("timeout_seconds", DEFAULT_CONFIG["ingest"]["http"]["timeout_seconds"])
        ),
        user_agent=str(
            http_cfg.get("user_agent", DEFAULT_CONFIG["ingest"]["http"]["user_agent"])
        ),
        max_retries=int(
            http_cfg.get("max_retries", DEFAULT_CONFIG["ingest"]["http"]["max_retries"])
        ),
        backoff_seconds=int(
            http_cfg.get("backoff_seconds", DEFAULT_CONFIG["ingest"]["http"]["backoff_seconds"])
        ),
    )

    dedupe = DedupeConfig(
        enabled=bool(dedupe_cfg.get("enabled", DEFAULT_CONFIG["ingest"]["dedupe"]["enabled"])),
        strategy=str(
            dedupe_cfg.get("strategy", DEFAULT_CONFIG["ingest"]["dedupe"]["strategy"])
        ),
    )

    filters = FiltersConfig(
        allow_keywords=_as_list(
            filters_cfg.get("allow_keywords", DEFAULT_CONFIG["ingest"]["filters"]["allow_keywords"]),
            "ingest.filters.allow_keywords",
        ),
        deny_keywords=_as_list(
            filters_cfg.get("deny_keywords", DEFAULT_CONFIG["ingest"]["filters"]["deny_keywords"]),
            "ingest.filters.deny_keywords",
        ),
    )

    ingest = IngestConfig(http=http, dedupe=dedupe, filters=filters)

    url_norm_cfg = (tweaks_cfg.get("url_normalization") or {})
    date_parsing_cfg = (tweaks_cfg.get("date_parsing") or {})

    url_norm = UrlNormalizationConfig(
        strip_tracking_params=bool(
            url_norm_cfg.get(
                "strip_tracking_params",
                DEFAULT_CONFIG["per_source_tweaks"]["url_normalization"]["strip_tracking_params"],
            )
        ),
        tracking_params=_as_list(
            url_norm_cfg.get(
                "tracking_params",
                DEFAULT_CONFIG["per_source_tweaks"]["url_normalization"]["tracking_params"],
            ),
            "per_source_tweaks.url_normalization.tracking_params",
        ),
    )

    date_parsing = DateParsingConfig(
        prefer_updated_if_published_missing=bool(
            date_parsing_cfg.get(
                "prefer_updated_if_published_missing",
                DEFAULT_CONFIG["per_source_tweaks"]["date_parsing"][
                    "prefer_updated_if_published_missing"
                ],
            )
        )
    )

    per_source_tweaks = PerSourceTweaks(
        url_normalization=url_norm,
        date_parsing=date_parsing,
    )

    return Config(
        app=app,
        paths=paths,
        publishing=publishing,
        ingest=ingest,
        llm=merged.get("llm") or DEFAULT_CONFIG["llm"],
        sources=sources,
        per_source_tweaks=per_source_tweaks,
    )


def load_sources_file(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        raise ConfigError(f"Sources file not found at {path}")
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if isinstance(raw, dict):
        raw = raw.get("sources")
    if not isinstance(raw, list):
        raise ConfigError("Sources file must contain a list of sources")
    sources: list[dict[str, Any]] = []
    for index, source in enumerate(raw):
        if not isinstance(source, dict):
            raise ConfigError(f"Source entry at index {index} must be a mapping")
        sources.append(source)
    return sources
