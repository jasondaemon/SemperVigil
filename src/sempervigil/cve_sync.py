from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .storage import (
    get_latest_cve_snapshot,
    insert_cve_change,
    insert_cve_snapshot,
    set_setting,
    upsert_cve,
)
from .utils import json_dumps, utc_now_iso

NVD_API_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"


@dataclass(frozen=True)
class CveSyncConfig:
    results_per_page: int
    rate_limit_seconds: float
    backoff_seconds: float
    max_retries: int
    prefer_v4: bool
    api_key: str | None = None


def sync_cves(
    conn,
    config: CveSyncConfig,
    last_modified_start: str,
    last_modified_end: str,
) -> dict[str, object]:
    start_index = 0
    total_processed = 0
    total_new = 0
    total_changes = 0
    errors = 0

    while True:
        payload = _fetch_page(
            config,
            last_modified_start=last_modified_start,
            last_modified_end=last_modified_end,
            start_index=start_index,
        )
        if payload is None:
            errors += 1
            break
        vulnerabilities = payload.get("vulnerabilities") or []
        if not vulnerabilities:
            break
        for item in vulnerabilities:
            cve_item = item.get("cve") or {}
            processed = process_cve_item(conn, cve_item, config.prefer_v4)
            total_processed += 1
            total_new += 1 if processed.new_snapshot else 0
            total_changes += processed.change_count

        start_index += int(payload.get("resultsPerPage", config.results_per_page))
        if start_index >= int(payload.get("totalResults", 0)):
            break
        time.sleep(config.rate_limit_seconds)

    if errors == 0:
        set_setting(conn, "cve.last_successful_sync_at", utc_now_iso())

    return {
        "processed": total_processed,
        "new_snapshots": total_new,
        "changes": total_changes,
        "errors": errors,
    }


@dataclass(frozen=True)
class ProcessResult:
    new_snapshot: bool
    change_count: int


def process_cve_item(conn, cve_item: dict[str, Any], prefer_v4: bool) -> ProcessResult:
    cve_id = cve_item.get("id")
    if not cve_id:
        return ProcessResult(new_snapshot=False, change_count=0)
    published_at = cve_item.get("published")
    last_modified_at = cve_item.get("lastModified")
    description = _extract_description(cve_item.get("descriptions") or [])

    metrics = cve_item.get("metrics") or {}
    v31 = _extract_cvss(metrics.get("cvssMetricV31"))
    v40 = _extract_cvss(metrics.get("cvssMetricV40"))

    preferred = _select_preferred_metrics(v31, v40, prefer_v4)
    snapshot_hash = _snapshot_hash(
        {
            "preferred": asdict(preferred),
            "v31": v31,
            "v40": v40,
            "last_modified_at": last_modified_at,
        }
    )

    prev_snapshot = get_latest_cve_snapshot(conn, cve_id)

    upsert_cve(
        conn,
        cve_id=cve_id,
        published_at=published_at,
        last_modified_at=last_modified_at,
        preferred_cvss_version=preferred.version,
        preferred_base_score=preferred.base_score,
        preferred_base_severity=preferred.base_severity,
        preferred_vector=preferred.vector,
        cvss_v40_json=v40,
        cvss_v31_json=v31,
        description_text=description,
    )

    observed_at = utc_now_iso()
    inserted = insert_cve_snapshot(
        conn,
        cve_id=cve_id,
        observed_at=observed_at,
        nvd_last_modified_at=last_modified_at,
        preferred_cvss_version=preferred.version,
        preferred_base_score=preferred.base_score,
        preferred_base_severity=preferred.base_severity,
        preferred_vector=preferred.vector,
        cvss_v40_json=v40,
        cvss_v31_json=v31,
        snapshot_hash=snapshot_hash,
    )
    if not inserted:
        return ProcessResult(new_snapshot=False, change_count=0)

    change_count = 0
    if prev_snapshot:
        change_count = _diff_snapshots(
            conn,
            cve_id,
            prev_snapshot=prev_snapshot,
            new_snapshot={
                "preferred": preferred,
                "v31": v31,
                "v40": v40,
            },
            observed_at=observed_at,
        )
    return ProcessResult(new_snapshot=True, change_count=change_count)


@dataclass(frozen=True)
class PreferredMetrics:
    version: str | None
    base_score: float | None
    base_severity: str | None
    vector: str | None


def _select_preferred_metrics(
    v31: dict[str, Any] | None, v40: dict[str, Any] | None, prefer_v4: bool
) -> PreferredMetrics:
    if prefer_v4 and v40:
        return PreferredMetrics(
            version="4.0",
            base_score=v40.get("baseScore"),
            base_severity=_normalize_severity(v40.get("baseSeverity")),
            vector=v40.get("vectorString"),
        )
    if v31:
        return PreferredMetrics(
            version="3.1",
            base_score=v31.get("baseScore"),
            base_severity=_normalize_severity(v31.get("baseSeverity")),
            vector=v31.get("vectorString"),
        )
    if v40:
        return PreferredMetrics(
            version="4.0",
            base_score=v40.get("baseScore"),
            base_severity=_normalize_severity(v40.get("baseSeverity")),
            vector=v40.get("vectorString"),
        )
    return PreferredMetrics(version=None, base_score=None, base_severity=None, vector=None)


def _diff_snapshots(
    conn,
    cve_id: str,
    prev_snapshot: dict[str, Any],
    new_snapshot: dict[str, Any],
    observed_at: str,
) -> int:
    changes = 0
    prev_pref = prev_snapshot.get("preferred_base_severity")
    new_pref = new_snapshot["preferred"].base_severity
    prev_score = prev_snapshot.get("preferred_base_score")
    new_score = new_snapshot["preferred"].base_score

    if prev_pref and new_pref and prev_pref != new_pref:
        change_type = (
            "severity_upgrade"
            if _severity_rank(new_pref) > _severity_rank(prev_pref)
            else "severity_downgrade"
        )
        insert_cve_change(
            conn,
            cve_id=cve_id,
            change_at=observed_at,
            cvss_version=new_snapshot["preferred"].version,
            change_type=change_type,
            from_score=prev_score,
            to_score=new_score,
            from_severity=prev_pref,
            to_severity=new_pref,
            vector_from=prev_snapshot.get("preferred_vector"),
            vector_to=new_snapshot["preferred"].vector,
            metrics_changed_json=_change_evidence(
                "rule.cve.cvss.band_change",
                {"from": prev_pref, "to": new_pref},
            ),
            note=None,
        )
        changes += 1

    if prev_snapshot.get("preferred_vector") != new_snapshot["preferred"].vector:
        insert_cve_change(
            conn,
            cve_id=cve_id,
            change_at=observed_at,
            cvss_version=new_snapshot["preferred"].version,
            change_type="vector_change",
            from_score=prev_score,
            to_score=new_score,
            from_severity=prev_pref,
            to_severity=new_pref,
            vector_from=prev_snapshot.get("preferred_vector"),
            vector_to=new_snapshot["preferred"].vector,
            metrics_changed_json=_change_evidence(
                "rule.cve.vector.changed",
                {"from": prev_snapshot.get("preferred_vector"), "to": new_snapshot["preferred"].vector},
            ),
            note=None,
        )
        changes += 1

    prev_v40 = prev_snapshot.get("cvss_v40_json")
    new_v40 = new_snapshot.get("v40")
    if not prev_v40 and new_v40:
        v31_band = _normalize_severity(
            (prev_snapshot.get("cvss_v31_json") or {}).get("baseSeverity")
        )
        v40_band = _normalize_severity(new_v40.get("baseSeverity"))
        insert_cve_change(
            conn,
            cve_id=cve_id,
            change_at=observed_at,
            cvss_version="4.0",
            change_type="cvss_version_added",
            from_score=None,
            to_score=new_v40.get("baseScore"),
            from_severity=None,
            to_severity=v40_band,
            vector_from=None,
            vector_to=new_v40.get("vectorString"),
            metrics_changed_json=_change_evidence(
                "rule.cve.cvss.v4_added",
                {"v31": v31_band, "v40": v40_band},
            ),
            note=None,
        )
        changes += 1
        if v31_band and v40_band and v31_band != v40_band:
            insert_cve_change(
                conn,
                cve_id=cve_id,
                change_at=observed_at,
                cvss_version="4.0",
                change_type="preferred_severity_diff",
                from_score=None,
                to_score=None,
                from_severity=v31_band,
                to_severity=v40_band,
                vector_from=None,
                vector_to=None,
                metrics_changed_json=_change_evidence(
                    "rule.cve.preferred_severity_diff",
                    {"v31": v31_band, "v40": v40_band},
                ),
                note=None,
            )
            changes += 1

    return changes


def _change_evidence(rule_id: str, fields: dict[str, object]) -> dict[str, object]:
    return {"reasons": [rule_id], "evidence": fields}


def _extract_description(descriptions: list[dict[str, Any]]) -> str | None:
    for entry in descriptions:
        if entry.get("lang") == "en":
            return entry.get("value")
    return None


def _extract_cvss(entries: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    if not entries:
        return None
    entry = entries[0]
    cvss = entry.get("cvssData") or {}
    return {
        "baseScore": cvss.get("baseScore"),
        "baseSeverity": cvss.get("baseSeverity"),
        "vectorString": cvss.get("vectorString"),
        "exploitabilityScore": entry.get("exploitabilityScore"),
        "impactScore": entry.get("impactScore"),
    }


def _normalize_severity(value: str | None) -> str | None:
    if not value:
        return None
    upper = str(value).upper()
    if upper in {"NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL"}:
        return upper
    return None


def _severity_rank(value: str | None) -> int:
    mapping = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
    return mapping.get(value or "", 0)


def _snapshot_hash(payload: dict[str, Any]) -> str:
    encoded = json_dumps(payload).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _fetch_page(
    config: CveSyncConfig,
    last_modified_start: str,
    last_modified_end: str,
    start_index: int,
) -> dict[str, Any] | None:
    params = {
        "lastModStartDate": last_modified_start,
        "lastModEndDate": last_modified_end,
        "startIndex": start_index,
        "resultsPerPage": config.results_per_page,
    }
    url = f"{NVD_API_URL}?{urlencode(params)}"
    headers = {"User-Agent": "SemperVigil/0.1"}
    if config.api_key:
        headers["apiKey"] = config.api_key
    attempt = 0
    while attempt <= config.max_retries:
        try:
            request = Request(url, headers=headers)
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in {429, 503} and attempt < config.max_retries:
                time.sleep(config.backoff_seconds * (attempt + 1))
                attempt += 1
                continue
            return None
        except URLError:
            if attempt < config.max_retries:
                time.sleep(config.backoff_seconds * (attempt + 1))
                attempt += 1
                continue
            return None
    return None


def isoformat_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
