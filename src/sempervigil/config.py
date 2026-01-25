from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from .storage import get_setting, set_setting


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
class JobsConfig:
    lock_timeout_seconds: int


@dataclass(frozen=True)
class CveConfig:
    enabled: bool
    sync_interval_minutes: int
    results_per_page: int
    rate_limit_seconds: float
    backoff_seconds: float
    max_retries: int
    prefer_v4: bool


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
    jobs: JobsConfig
    cve: CveConfig
    llm: dict[str, Any]
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
    "jobs": {
        "lock_timeout_seconds": 600,
    },
    "cve": {
        "enabled": True,
        "sync_interval_minutes": 60,
        "results_per_page": 2000,
        "rate_limit_seconds": 1.0,
        "backoff_seconds": 2.0,
        "max_retries": 3,
        "prefer_v4": True,
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

CONFIG_KEY = "config.runtime"


def get_state_db_path() -> str:
    data_dir = os.environ.get("SV_DATA_DIR", DEFAULT_CONFIG["paths"]["data_dir"])
    return os.path.join(data_dir, "state.sqlite3")


def bootstrap_runtime_config(conn) -> dict[str, Any]:
    cfg = get_setting(conn, CONFIG_KEY, None)
    if cfg is None:
        set_setting(conn, CONFIG_KEY, _deep_copy(DEFAULT_CONFIG))
        cfg = get_setting(conn, CONFIG_KEY, None)
    if not isinstance(cfg, dict):
        raise ConfigError("config.runtime must be a JSON object")
    return cfg


def get_runtime_config(conn) -> dict[str, Any]:
    cfg = bootstrap_runtime_config(conn)
    errors = validate_runtime_config(cfg)
    if errors:
        raise ConfigError("Invalid config.runtime: " + "; ".join(errors))
    return cfg


def set_runtime_config(conn, cfg: dict[str, Any]) -> None:
    errors = validate_runtime_config(cfg)
    if errors:
        raise ConfigError("Invalid config.runtime: " + "; ".join(errors))
    set_setting(conn, CONFIG_KEY, _deep_copy(cfg))


def load_runtime_config(conn) -> Config:
    cfg = get_runtime_config(conn)
    return _build_config(cfg)


def validate_runtime_config(cfg: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    _validate_dict(cfg, DEFAULT_CONFIG, "config.runtime", errors)
    return errors


def _validate_dict(value: dict[str, Any], schema: dict[str, Any], path: str, errors: list[str]) -> None:
    if not isinstance(value, dict):
        errors.append(f"{path} must be an object")
        return
    for key in schema.keys():
        if key not in value:
            errors.append(f"missing {path}.{key}")
    for key in value.keys():
        if key not in schema:
            errors.append(f"unknown {path}.{key}")
    for key, default in schema.items():
        if key not in value:
            continue
        _validate_value(value[key], default, f"{path}.{key}", errors)


def _validate_value(value: Any, default: Any, path: str, errors: list[str]) -> None:
    if isinstance(default, dict):
        if not isinstance(value, dict):
            errors.append(f"{path} must be an object")
            return
        _validate_dict(value, default, path, errors)
        return
    if isinstance(default, list):
        if not isinstance(value, list):
            errors.append(f"{path} must be a list")
            return
        if default:
            sample = default[0]
            for item in value:
                if not isinstance(item, type(sample)):
                    errors.append(f"{path} must be a list of {type(sample).__name__}")
                    break
        else:
            for item in value:
                if not isinstance(item, str):
                    errors.append(f"{path} must be a list of strings")
                    break
        return
    if isinstance(default, bool):
        if not isinstance(value, bool):
            errors.append(f"{path} must be a boolean")
        return
    if isinstance(default, int) and not isinstance(default, bool):
        if not isinstance(value, int):
            errors.append(f"{path} must be an integer")
        return
    if isinstance(default, float):
        if not isinstance(value, (int, float)):
            errors.append(f"{path} must be a number")
        return
    if isinstance(default, str):
        if not isinstance(value, str):
            errors.append(f"{path} must be a string")
        return


def _build_config(cfg: dict[str, Any]) -> Config:
    app_cfg = cfg.get("app") or {}
    paths_cfg = cfg.get("paths") or {}
    publishing_cfg = cfg.get("publishing") or {}
    ingest_cfg = cfg.get("ingest") or {}
    jobs_cfg = cfg.get("jobs") or {}
    cve_cfg = cfg.get("cve") or {}
    tweaks_cfg = cfg.get("per_source_tweaks") or {}

    app = AppConfig(
        name=str(app_cfg.get("name")),
        timezone=str(app_cfg.get("timezone")),
    )

    paths = PathsConfig(
        data_dir=str(paths_cfg.get("data_dir")),
        output_dir=str(paths_cfg.get("output_dir")),
        state_db=str(paths_cfg.get("state_db")),
        run_reports_dir=str(paths_cfg.get("run_reports_dir")),
    )

    publishing = PublishingConfig(
        format=str(publishing_cfg.get("format")),
        hugo_section=str(publishing_cfg.get("hugo_section")),
        write_json_index=bool(publishing_cfg.get("write_json_index")),
        json_index_path=str(publishing_cfg.get("json_index_path")),
    )

    http_cfg = ingest_cfg.get("http") or {}
    dedupe_cfg = ingest_cfg.get("dedupe") or {}
    filters_cfg = ingest_cfg.get("filters") or {}

    http = HttpConfig(
        timeout_seconds=int(http_cfg.get("timeout_seconds")),
        user_agent=str(http_cfg.get("user_agent")),
        max_retries=int(http_cfg.get("max_retries")),
        backoff_seconds=int(http_cfg.get("backoff_seconds")),
    )

    dedupe = DedupeConfig(
        enabled=bool(dedupe_cfg.get("enabled")),
        strategy=str(dedupe_cfg.get("strategy")),
    )

    filters = FiltersConfig(
        allow_keywords=list(filters_cfg.get("allow_keywords")),
        deny_keywords=list(filters_cfg.get("deny_keywords")),
    )

    ingest = IngestConfig(http=http, dedupe=dedupe, filters=filters)
    jobs = JobsConfig(lock_timeout_seconds=int(jobs_cfg.get("lock_timeout_seconds")))

    cve = CveConfig(
        enabled=bool(cve_cfg.get("enabled")),
        sync_interval_minutes=int(cve_cfg.get("sync_interval_minutes")),
        results_per_page=int(cve_cfg.get("results_per_page")),
        rate_limit_seconds=float(cve_cfg.get("rate_limit_seconds")),
        backoff_seconds=float(cve_cfg.get("backoff_seconds")),
        max_retries=int(cve_cfg.get("max_retries")),
        prefer_v4=bool(cve_cfg.get("prefer_v4")),
    )

    url_norm_cfg = tweaks_cfg.get("url_normalization") or {}
    date_parsing_cfg = tweaks_cfg.get("date_parsing") or {}

    url_norm = UrlNormalizationConfig(
        strip_tracking_params=bool(url_norm_cfg.get("strip_tracking_params")),
        tracking_params=list(url_norm_cfg.get("tracking_params")),
    )

    date_parsing = DateParsingConfig(
        prefer_updated_if_published_missing=bool(
            date_parsing_cfg.get("prefer_updated_if_published_missing")
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
        jobs=jobs,
        cve=cve,
        llm=dict(cfg.get("llm") or {}),
        per_source_tweaks=per_source_tweaks,
    )


def _deep_copy(value: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(value))
