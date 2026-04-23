from __future__ import annotations

import csv
import gzip
import io
import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .cve_filters import extract_signals, matches_filters, normalize_severity
from .storage import (
    get_setting,
    get_latest_cve_snapshot,
    list_cve_ids,
    insert_cve_change,
    insert_cve_snapshot,
    link_cve_products_from_signals,
    compute_scope_for_cves,
    set_setting,
    upsert_cve,
)
from .utils import json_dumps, log_event, utc_now_iso

@dataclass(frozen=True)
class CveSyncConfig:
    api_base: str
    results_per_page: int
    rate_limit_seconds: float
    backoff_seconds: float
    max_retries: int
    prefer_v4: bool
    scope_min_cvss: float | None = None
    watchlist_enabled: bool = False
    api_key: str | None = None
    filters: dict[str, Any] | None = None


def sync_cves(
    conn,
    config: CveSyncConfig,
    last_modified_start: str,
    last_modified_end: str,
    cve_id: str | None = None,
) -> dict[str, object]:
    logger = logging.getLogger("sempervigil.cve_sync")
    start_index = 0
    total_processed = 0
    total_new = 0
    total_changes = 0
    errors = 0
    filtered = 0

    while True:
        payload = _fetch_page(
            config,
            last_modified_start=last_modified_start,
            last_modified_end=last_modified_end,
            start_index=start_index,
            cve_id=cve_id,
        )
        if payload is None:
            errors += 1
            break
        vulnerabilities = payload.get("vulnerabilities") or []
        if not vulnerabilities:
            break
        for item in vulnerabilities:
            cve_item = item.get("cve") or {}
            processed = process_cve_item(
                conn,
                cve_item,
                config.prefer_v4,
                config.filters or {},
                config.scope_min_cvss,
                config.watchlist_enabled,
                logger,
            )
            if processed is None:
                filtered += 1
                continue
            total_processed += 1
            total_new += 1 if processed.new_snapshot else 0
            total_changes += processed.change_count

        start_index += int(payload.get("resultsPerPage", config.results_per_page))
        if cve_id:
            break
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
        "filtered": filtered,
    }


@dataclass(frozen=True)
class ProcessResult:
    new_snapshot: bool
    change_count: int


def process_cve_item(
    conn,
    cve_item: dict[str, Any],
    prefer_v4: bool,
    filters: dict[str, Any],
    scope_min_cvss: float | None,
    watchlist_enabled: bool,
    logger: logging.Logger | None = None,
) -> ProcessResult | None:
    cve_id = cve_item.get("id")
    if not cve_id:
        return ProcessResult(new_snapshot=False, change_count=0)
    published_at = cve_item.get("published")
    last_modified_at = cve_item.get("lastModified")
    description = _extract_description(cve_item.get("descriptions"))

    metrics = cve_item.get("metrics") or {}
    v31_list = _extract_cvss(metrics.get("cvssMetricV31"), "3.1")
    v40_list = _extract_cvss(metrics.get("cvssMetricV40"), "4.0")
    v31 = _pick_preferred_entry(v31_list)
    v40 = _pick_preferred_entry(v40_list)

    preferred = _select_preferred_metrics(v31_list, v40_list, prefer_v4)
    signals = extract_signals(cve_item)
    if logger:
        log_event(
            logger,
            logging.DEBUG,
            "cve_signals_extracted",
            cve_id=cve_id,
            description_len=len(description or ""),
            vendors=len(signals.vendors),
            products=len(signals.products),
            cpes=len(signals.cpes),
            domains=len(signals.reference_domains),
            has_v31=bool(v31_list),
            has_v40=bool(v40_list),
        )
    if filters and not matches_filters(
        preferred_score=preferred.base_score,
        preferred_severity=preferred.base_severity,
        description=description,
        signals=signals,
        filters=filters,
    ):
        return None
    preferred_dict = asdict(preferred)
    snapshot_hash = _snapshot_hash(
        {
            "preferred": preferred_dict,
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
        cvss_v40_list_json=v40_list,
        cvss_v31_list_json=v31_list,
        description_text=description,
        affected_products=signals.products,
        affected_cpes=signals.cpes,
        reference_domains=signals.reference_domains,
    )
    link_cve_products_from_signals(
        conn,
        cve_id=cve_id,
        products=signals.products,
        cpes=signals.cpes,
        product_versions=signals.product_versions,
        source="nvd",
    )
    if watchlist_enabled:
        compute_scope_for_cves(conn, [cve_id], min_cvss=scope_min_cvss)
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
                "preferred": preferred_dict,
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
    v31_list: list[dict[str, Any]],
    v40_list: list[dict[str, Any]],
    prefer_v4: bool,
) -> PreferredMetrics:
    v31 = _pick_preferred_entry(v31_list)
    v40 = _pick_preferred_entry(v40_list)
    if prefer_v4 and v40:
        return PreferredMetrics(
            version="4.0",
            base_score=v40.get("baseScore"),
            base_severity=normalize_severity(v40.get("baseSeverity")),
            vector=v40.get("vectorString"),
        )
    if v31:
        return PreferredMetrics(
            version="3.1",
            base_score=v31.get("baseScore"),
            base_severity=normalize_severity(v31.get("baseSeverity")),
            vector=v31.get("vectorString"),
        )
    if v40:
        return PreferredMetrics(
            version="4.0",
            base_score=v40.get("baseScore"),
            base_severity=normalize_severity(v40.get("baseSeverity")),
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
    new_pref = new_snapshot["preferred"].get("base_severity")
    prev_score = prev_snapshot.get("preferred_base_score")
    new_score = new_snapshot["preferred"].get("base_score")

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
            cvss_version=new_snapshot["preferred"].get("version"),
            change_type=change_type,
            from_score=prev_score,
            to_score=new_score,
            from_severity=prev_pref,
            to_severity=new_pref,
            vector_from=prev_snapshot.get("preferred_vector"),
            vector_to=new_snapshot["preferred"].get("vector"),
            metrics_changed_json=_change_evidence(
                "rule.cve.cvss.band_change",
                {"from": prev_pref, "to": new_pref},
            ),
            note=None,
        )
        changes += 1

    if prev_snapshot.get("preferred_vector") != new_snapshot["preferred"].get("vector"):
        insert_cve_change(
            conn,
            cve_id=cve_id,
            change_at=observed_at,
            cvss_version=new_snapshot["preferred"].get("version"),
            change_type="vector_change",
            from_score=prev_score,
            to_score=new_score,
            from_severity=prev_pref,
            to_severity=new_pref,
            vector_from=prev_snapshot.get("preferred_vector"),
            vector_to=new_snapshot["preferred"].get("vector"),
            metrics_changed_json=_change_evidence(
                "rule.cve.vector.changed",
                {
                    "from": prev_snapshot.get("preferred_vector"),
                    "to": new_snapshot["preferred"].get("vector"),
                },
            ),
            note=None,
        )
        changes += 1

    prev_v40 = prev_snapshot.get("cvss_v40_json")
    new_v40 = new_snapshot.get("v40")
    if not prev_v40 and new_v40:
        v31_band = normalize_severity(
            (prev_snapshot.get("cvss_v31_json") or {}).get("baseSeverity")
        )
        v40_band = normalize_severity(new_v40.get("baseSeverity"))
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


def _extract_description(descriptions: Any) -> str | None:
    if isinstance(descriptions, str):
        text = descriptions.strip()
        return text or None
    if isinstance(descriptions, dict):
        text = str(descriptions.get("value") or "").strip()
        return text or None
    if isinstance(descriptions, list):
        for entry in descriptions:
            if isinstance(entry, dict) and entry.get("lang") == "en":
                text = str(entry.get("value") or "").strip()
                if text:
                    return text
        for entry in descriptions:
            if isinstance(entry, dict):
                text = str(entry.get("value") or "").strip()
                if text:
                    return text
    return None


def _extract_cvss(entries: list[dict[str, Any]] | None, version_label: str) -> list[dict[str, Any]]:
    if not entries:
        return []
    normalized: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        cvss = entry.get("cvssData") or {}
        normalized.append(
            {
                "version": cvss.get("version") or version_label,
                "source": entry.get("source"),
                "type": entry.get("type"),
                "baseScore": cvss.get("baseScore"),
                "baseSeverity": cvss.get("baseSeverity"),
                "vectorString": cvss.get("vectorString"),
                "exploitabilityScore": entry.get("exploitabilityScore"),
                "impactScore": entry.get("impactScore"),
                "cvssData": cvss,
            }
        )
    return normalized


def _pick_preferred_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    for entry in entries:
        if str(entry.get("type") or "").lower() == "primary":
            return entry
    return entries[0] if entries else None


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
    cve_id: str | None = None,
) -> dict[str, Any] | None:
    params = {
        "lastModStartDate": last_modified_start,
        "lastModEndDate": last_modified_end,
        "startIndex": start_index,
        "resultsPerPage": config.results_per_page,
    }
    if cve_id:
        params["cveId"] = cve_id
    url = f"{config.api_base}?{urlencode(params)}"
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


def preview_cves(
    config: CveSyncConfig,
    last_modified_start: str,
    last_modified_end: str,
    limit: int = 5,
) -> dict[str, object]:
    payload = _fetch_page(
        config,
        last_modified_start=last_modified_start,
        last_modified_end=last_modified_end,
        start_index=0,
    )
    if payload is None:
        return {"status": "error", "error": "fetch_failed", "items": []}
    vulnerabilities = payload.get("vulnerabilities") or []
    found = 0
    accepted = 0
    filtered = 0
    items: list[dict[str, object]] = []
    for entry in vulnerabilities:
        cve_item = entry.get("cve") or {}
        cve_id = cve_item.get("id")
        if not cve_id:
            continue
        found += 1
        description = _extract_description(cve_item.get("descriptions"))
        metrics = cve_item.get("metrics") or {}
        v31_list = _extract_cvss(metrics.get("cvssMetricV31"), "3.1")
        v40_list = _extract_cvss(metrics.get("cvssMetricV40"), "4.0")
        preferred = _select_preferred_metrics(v31_list, v40_list, config.prefer_v4)
        signals = extract_signals(cve_item)
        is_match = True
        if config.filters:
            is_match = matches_filters(
                preferred_score=preferred.base_score,
                preferred_severity=preferred.base_severity,
                description=description,
                signals=signals,
                filters=config.filters,
            )
        if is_match:
            accepted += 1
        else:
            filtered += 1
        if len(items) < max(0, int(limit)):
            items.append(
                {
                    "cve_id": cve_id,
                    "decision": "accepted" if is_match else "filtered",
                    "preferred_severity": preferred.base_severity,
                    "preferred_score": preferred.base_score,
                    "description": (description or "")[:240],
                }
            )
    return {
        "status": "ok",
        "found": found,
        "accepted": accepted,
        "filtered": filtered,
        "items": items,
    }


@dataclass(frozen=True)
class EpssRow:
    cve_id: str
    epss_score: float
    epss_percentile: float
    epss_date: str


def sync_epss(conn, cve_ids: list[str] | None = None) -> dict[str, object]:
    logger = logging.getLogger("sempervigil.cve_sync")
    checked_at = utc_now_iso()
    if cve_ids:
        rows = _fetch_epss_api_rows(cve_ids)
        if not rows:
            return {"status": "skipped", "reason": "epss_not_found", "matched": 0, "updated": 0}
        updated = _apply_epss_rows(conn, rows, checked_at=checked_at)
        return {
            "status": "ok",
            "source": "api",
            "matched": len(rows),
            "updated": updated,
            "epss_date": rows[0].epss_date,
        }

    today = datetime.now(timezone.utc).date()
    today_utc = today.isoformat()
    last_sync_date = str(get_setting(conn, "cve.epss.last_successful_sync_date", "") or "").strip()
    if last_sync_date == today_utc:
        return {
            "status": "skipped",
            "reason": "already_synced_today",
            "matched": 0,
            "updated": 0,
            "epss_date": last_sync_date,
        }

    existing_ids = set(list_cve_ids(conn))
    if not existing_ids:
        return {"status": "skipped", "reason": "no_cves", "matched": 0, "updated": 0}

    epss_rows: list[EpssRow] | None = None
    epss_date = ""
    for candidate in _epss_date_candidates(today):
        epss_rows = _fetch_epss_csv_rows(candidate)
        if epss_rows is not None:
            epss_date = candidate.isoformat()
            break
    if epss_rows is None:
        return {"status": "error", "reason": "epss_fetch_failed", "matched": 0, "updated": 0}

    matched_rows = [row for row in epss_rows if row.cve_id in existing_ids]
    if not matched_rows:
        set_setting(conn, "cve.epss.last_successful_sync_date", epss_date)
        return {
            "status": "ok",
            "source": "csv",
            "matched": 0,
            "updated": 0,
            "epss_date": epss_date,
        }

    updated = _apply_epss_rows(conn, matched_rows, checked_at=checked_at)
    set_setting(conn, "cve.epss.last_successful_sync_date", epss_date)
    log_event(
        logger,
        logging.INFO,
        "epss_sync_complete",
        epss_date=epss_date,
        matched=len(matched_rows),
        updated=updated,
    )
    return {
        "status": "ok",
        "source": "csv",
        "matched": len(matched_rows),
        "updated": updated,
        "epss_date": epss_date,
    }


def sync_cve_id(conn, api_key: str | None, cve_id: str) -> bool:
    if not cve_id:
        return False
    api_base = "https://services.nvd.nist.gov/rest/json/cves/2.0"
    params = {"cveId": cve_id}
    url = f"{api_base}?{urlencode(params)}"
    headers = {"User-Agent": "SemperVigil/0.1"}
    if api_key:
        headers["apiKey"] = api_key
    try:
        request = Request(url, headers=headers)
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return False
    items = payload.get("vulnerabilities") or []
    if not items:
        return False
    logger = logging.getLogger("sempervigil.cve_sync")
    updated = False
    for item in items:
        cve_item = item.get("cve") or {}
        processed = process_cve_item(conn, cve_item, True, {}, None, False, logger)
        if processed:
            updated = True
    return updated


def _epss_date_candidates(today: date) -> list[date]:
    return [today, date.fromordinal(today.toordinal() - 1), date.fromordinal(today.toordinal() - 2)]


def _fetch_epss_api_rows(cve_ids: list[str]) -> list[EpssRow]:
    ids = [str(cve_id).strip() for cve_id in cve_ids if str(cve_id).strip()]
    if not ids:
        return []
    params = {"cve": ",".join(ids)}
    url = f"https://api.first.org/data/v1/epss?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "SemperVigil/0.1"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, json.JSONDecodeError):
        return []
    rows: list[EpssRow] = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        cve_id = str(item.get("cve") or "").strip()
        if not cve_id:
            continue
        try:
            epss_score = float(item.get("epss"))
            epss_percentile = float(item.get("percentile"))
        except (TypeError, ValueError):
            continue
        rows.append(
            EpssRow(
                cve_id=cve_id,
                epss_score=epss_score,
                epss_percentile=epss_percentile,
                epss_date=str(item.get("date") or "").strip(),
            )
        )
    return rows


def _fetch_epss_csv_rows(target_date: date) -> list[EpssRow] | None:
    url = f"https://epss.empiricalsecurity.com/epss_scores-{target_date.isoformat()}.csv.gz"
    request = Request(url, headers={"User-Agent": "SemperVigil/0.1"})
    try:
        with urlopen(request, timeout=60) as response:
            raw = response.read()
    except (HTTPError, URLError):
        return None
    try:
        text = gzip.decompress(raw).decode("utf-8")
    except Exception:
        return None
    filtered = "\n".join(line for line in text.splitlines() if not line.startswith("#"))
    if not filtered.strip():
        return []
    reader = csv.DictReader(io.StringIO(filtered))
    rows: list[EpssRow] = []
    for item in reader:
        if not isinstance(item, dict):
            continue
        cve_id = str(item.get("cve") or "").strip()
        if not cve_id:
            continue
        try:
            epss_score = float(item.get("epss") or 0)
            epss_percentile = float(item.get("percentile") or 0)
        except (TypeError, ValueError):
            continue
        rows.append(
            EpssRow(
                cve_id=cve_id,
                epss_score=epss_score,
                epss_percentile=epss_percentile,
                epss_date=str(item.get("date") or target_date.isoformat()).strip(),
            )
        )
    return rows


def _apply_epss_rows(conn, rows: list[EpssRow], *, checked_at: str) -> int:
    if not rows:
        return 0
    conn.executemany(
        """
        UPDATE cves
        SET epss_score = %s,
            epss_percentile = %s,
            epss_date = %s,
            epss_checked_at = %s
        WHERE cve_id = %s
        """,
        [
            (
                row.epss_score,
                row.epss_percentile,
                row.epss_date,
                checked_at,
                row.cve_id,
            )
            for row in rows
        ],
    )
    conn.commit()
    return len(rows)
