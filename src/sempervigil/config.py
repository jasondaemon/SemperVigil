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
    run_reports_dir: str


@dataclass(frozen=True)
class PublishingConfig:
    format: str
    hugo_section: str
    write_json_index: bool
    json_index_path: str
    public_base_url: str


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
    build_debounce_seconds: int


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
class ScopeConfig:
    min_cvss: float | None


@dataclass(frozen=True)
class PersonalizationConfig:
    watchlist_enabled: bool
    watchlist_exposure_mode: str
    rss_enabled: bool
    rss_private_token: str | None


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
    scope: ScopeConfig
    personalization: PersonalizationConfig
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
        "run_reports_dir": "/data/reports",
    },
    "publishing": {
        "format": "hugo_markdown",
        "hugo_section": "posts",
        "write_json_index": True,
        "json_index_path": "/site/static/sempervigil/index.json",
        "public_base_url": "",
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
        "build_debounce_seconds": 600,
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
    "scope": {
        "min_cvss": None,
    },
    "personalization": {
        "watchlist_enabled": False,
        "watchlist_exposure_mode": "private_only",
        "rss_enabled": False,
        "rss_private_token": None,
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
CVE_SETTINGS_KEY = "cve.settings"
EVENTS_SETTINGS_KEY = "events.settings"

DEFAULT_CVE_SETTINGS: dict[str, Any] = {
    "enabled": True,
    "schedule_minutes": 60,
    "nvd": {
        "api_base": "https://services.nvd.nist.gov/rest/json/cves/2.0",
        "results_per_page": 2000,
    },
    "filters": {
        "min_cvss": None,
        "severities": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        "require_known_score": False,
        "product_keywords": [],
        "vendor_keywords": [],
    },
    "retention_days": 365,
}

DEFAULT_EVENTS_SETTINGS: dict[str, Any] = {
    "enabled": True,
    "merge_window_days": 14,
    "min_shared_products_to_merge": 1,
    "product_burst_window_hours": 24,
    "product_burst_min_high_critical": 3,
}


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
    cfg = _upgrade_runtime_config(cfg)
    errors = validate_runtime_config(cfg)
    if errors:
        raise ConfigError("Invalid config.runtime: " + "; ".join(errors))
    return cfg


def set_runtime_config(conn, cfg: dict[str, Any]) -> None:
    errors = validate_runtime_config(cfg)
    if errors:
        raise ConfigError("Invalid config.runtime: " + "; ".join(errors))
    set_setting(conn, CONFIG_KEY, _deep_copy(cfg))


def apply_runtime_config_patch(conn, patch: dict[str, Any]) -> dict[str, Any]:
    current = get_runtime_config(conn)
    merged = _deep_copy(current)
    _merge_missing(merged, patch)
    errors = validate_runtime_config(merged)
    if errors:
        raise ConfigError("Invalid config.runtime: " + "; ".join(errors))
    set_setting(conn, CONFIG_KEY, _deep_copy(merged))
    return merged


def _upgrade_runtime_config(cfg: dict[str, Any]) -> dict[str, Any]:
    merged = _deep_copy(DEFAULT_CONFIG)
    _merge_missing(merged, cfg)
    return merged


def _merge_missing(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if key not in target:
            continue
        if isinstance(target[key], dict) and isinstance(value, dict):
            _merge_missing(target[key], value)
        else:
            target[key] = value


def bootstrap_cve_settings(conn) -> dict[str, Any]:
    cfg = get_setting(conn, CVE_SETTINGS_KEY, None)
    if cfg is None:
        set_setting(conn, CVE_SETTINGS_KEY, _deep_copy(DEFAULT_CVE_SETTINGS))
        cfg = get_setting(conn, CVE_SETTINGS_KEY, None)
    if not isinstance(cfg, dict):
        raise ConfigError("cve.settings must be a JSON object")
    return cfg


def bootstrap_events_settings(conn) -> dict[str, Any]:
    cfg = get_setting(conn, EVENTS_SETTINGS_KEY, None)
    if cfg is None:
        set_setting(conn, EVENTS_SETTINGS_KEY, _deep_copy(DEFAULT_EVENTS_SETTINGS))
        cfg = get_setting(conn, EVENTS_SETTINGS_KEY, None)
    if not isinstance(cfg, dict):
        raise ConfigError("events.settings must be a JSON object")
    return cfg


def get_cve_settings(conn) -> dict[str, Any]:
    cfg = bootstrap_cve_settings(conn)
    errors = validate_cve_settings(cfg)
    if errors:
        raise ConfigError("Invalid cve.settings: " + "; ".join(errors))
    return cfg


def get_events_settings(conn) -> dict[str, Any]:
    cfg = bootstrap_events_settings(conn)
    errors = validate_events_settings(cfg)
    if errors:
        raise ConfigError("Invalid events.settings: " + "; ".join(errors))
    return cfg


def set_cve_settings(conn, cfg: dict[str, Any]) -> None:
    errors = validate_cve_settings(cfg)
    if errors:
        raise ConfigError("Invalid cve.settings: " + "; ".join(errors))
    set_setting(conn, CVE_SETTINGS_KEY, _deep_copy(cfg))


def set_events_settings(conn, cfg: dict[str, Any]) -> None:
    errors = validate_events_settings(cfg)
    if errors:
        raise ConfigError("Invalid events.settings: " + "; ".join(errors))
    set_setting(conn, EVENTS_SETTINGS_KEY, _deep_copy(cfg))


def validate_cve_settings(cfg: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = ["enabled", "schedule_minutes", "nvd", "filters", "retention_days"]
    for key in required:
        if key not in cfg:
            errors.append(f"missing cve.settings.{key}")
    if "enabled" in cfg and not isinstance(cfg["enabled"], bool):
        errors.append("cve.settings.enabled must be a boolean")
    if "schedule_minutes" in cfg and not isinstance(cfg["schedule_minutes"], int):
        errors.append("cve.settings.schedule_minutes must be an integer")
    if "retention_days" in cfg and not isinstance(cfg["retention_days"], int):
        errors.append("cve.settings.retention_days must be an integer")
    nvd = cfg.get("nvd")
    if not isinstance(nvd, dict):
        errors.append("cve.settings.nvd must be an object")
    else:
        if "api_base" not in nvd or not isinstance(nvd.get("api_base"), str):
            errors.append("cve.settings.nvd.api_base must be a string")
        if "results_per_page" not in nvd or not isinstance(nvd.get("results_per_page"), int):
            errors.append("cve.settings.nvd.results_per_page must be an integer")
    filters = cfg.get("filters")
    if not isinstance(filters, dict):
        errors.append("cve.settings.filters must be an object")
    else:
        min_cvss = filters.get("min_cvss")
        if min_cvss is not None and not isinstance(min_cvss, (int, float)):
            errors.append("cve.settings.filters.min_cvss must be a number or null")
        severities = filters.get("severities")
        if not isinstance(severities, list) or not all(
            isinstance(item, str) for item in severities
        ):
            errors.append("cve.settings.filters.severities must be a list of strings")
        if "require_known_score" in filters and not isinstance(
            filters.get("require_known_score"), bool
        ):
            errors.append("cve.settings.filters.require_known_score must be a boolean")
        for key in ("product_keywords", "vendor_keywords"):
            value = filters.get(key)
            if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                errors.append(f"cve.settings.filters.{key} must be a list of strings")
    return errors


def validate_events_settings(cfg: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    required = [
        "enabled",
        "merge_window_days",
        "min_shared_products_to_merge",
        "product_burst_window_hours",
        "product_burst_min_high_critical",
    ]
    for key in required:
        if key not in cfg:
            errors.append(f"missing events.settings.{key}")
    if "enabled" in cfg and not isinstance(cfg["enabled"], bool):
        errors.append("events.settings.enabled must be a boolean")
    for key in (
        "merge_window_days",
        "min_shared_products_to_merge",
        "product_burst_window_hours",
        "product_burst_min_high_critical",
    ):
        if key in cfg and not isinstance(cfg[key], int):
            errors.append(f"events.settings.{key} must be an integer")
    return errors

def load_runtime_config(conn) -> Config:
    cfg = get_runtime_config(conn)
    config = _build_config(cfg)
    return _apply_hugo_path_overrides(config)


def _apply_hugo_path_overrides(config: Config) -> Config:
    source_dir = os.environ.get("SV_HUGO_SOURCE_DIR")
    if not source_dir:
        return config
    output_dir = os.path.join(source_dir, "content", "posts")
    json_index_path = os.path.join(source_dir, "static", "sempervigil", "index.json")
    paths = PathsConfig(
        data_dir=config.paths.data_dir,
        output_dir=output_dir,
        run_reports_dir=config.paths.run_reports_dir,
    )
    publishing = PublishingConfig(
        format=config.publishing.format,
        hugo_section=config.publishing.hugo_section,
        write_json_index=config.publishing.write_json_index,
        json_index_path=json_index_path,
        public_base_url=config.publishing.public_base_url,
    )
    return Config(
        app=config.app,
        paths=paths,
        publishing=publishing,
        ingest=config.ingest,
        jobs=config.jobs,
        cve=config.cve,
        scope=config.scope,
        personalization=config.personalization,
        llm=config.llm,
    )


def validate_runtime_config(cfg: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    _validate_dict(cfg, DEFAULT_CONFIG, "config.runtime", errors)
    personalization = cfg.get("personalization") if isinstance(cfg, dict) else None
    if isinstance(personalization, dict):
        exposure_mode = personalization.get("watchlist_exposure_mode")
        if exposure_mode not in ("private_only", "public_highlights"):
            errors.append(
                "config.runtime.personalization.watchlist_exposure_mode must be private_only or public_highlights"
            )
        if not isinstance(personalization.get("watchlist_enabled"), bool):
            errors.append("config.runtime.personalization.watchlist_enabled must be a boolean")
        if not isinstance(personalization.get("rss_enabled"), bool):
            errors.append("config.runtime.personalization.rss_enabled must be a boolean")
        rss_token = personalization.get("rss_private_token")
        if rss_token is not None and not isinstance(rss_token, str):
            errors.append("config.runtime.personalization.rss_private_token must be a string or null")
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
    scope_cfg = cfg.get("scope") or {}
    personalization_cfg = cfg.get("personalization") or {}
    tweaks_cfg = cfg.get("per_source_tweaks") or {}

    app = AppConfig(
        name=str(app_cfg.get("name")),
        timezone=str(app_cfg.get("timezone")),
    )

    paths = PathsConfig(
        data_dir=str(paths_cfg.get("data_dir")),
        output_dir=str(paths_cfg.get("output_dir")),
        run_reports_dir=str(paths_cfg.get("run_reports_dir")),
    )

    publishing = PublishingConfig(
        format=str(publishing_cfg.get("format")),
        hugo_section=str(publishing_cfg.get("hugo_section")),
        write_json_index=bool(publishing_cfg.get("write_json_index")),
        json_index_path=str(publishing_cfg.get("json_index_path")),
        public_base_url=str(publishing_cfg.get("public_base_url") or ""),
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
    build_debounce = jobs_cfg.get("build_debounce_seconds", 600)
    jobs = JobsConfig(
        lock_timeout_seconds=int(jobs_cfg.get("lock_timeout_seconds")),
        build_debounce_seconds=int(build_debounce),
    )

    cve = CveConfig(
        enabled=bool(cve_cfg.get("enabled")),
        sync_interval_minutes=int(cve_cfg.get("sync_interval_minutes")),
        results_per_page=int(cve_cfg.get("results_per_page")),
        rate_limit_seconds=float(cve_cfg.get("rate_limit_seconds")),
        backoff_seconds=float(cve_cfg.get("backoff_seconds")),
        max_retries=int(cve_cfg.get("max_retries")),
        prefer_v4=bool(cve_cfg.get("prefer_v4")),
    )

    scope = ScopeConfig(
        min_cvss=scope_cfg.get("min_cvss"),
    )

    personalization = PersonalizationConfig(
        watchlist_enabled=bool(personalization_cfg.get("watchlist_enabled")),
        watchlist_exposure_mode=str(personalization_cfg.get("watchlist_exposure_mode") or "private_only"),
        rss_enabled=bool(personalization_cfg.get("rss_enabled")),
        rss_private_token=personalization_cfg.get("rss_private_token"),
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
        scope=scope,
        personalization=personalization,
        llm=dict(cfg.get("llm") or {}),
        per_source_tweaks=per_source_tweaks,
    )


def _deep_copy(value: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(value))
