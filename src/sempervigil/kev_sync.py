from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .storage import get_setting, set_setting, upsert_cve_kev_entries, prune_cve_kev_entries
from .utils import log_event, utc_now_iso

KEV_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
DEFAULT_MAX_AGE_MINUTES = 360
DEFAULT_TIMEOUT_SECONDS = 20


def _fetch_kev_payload(url: str, timeout_seconds: int) -> dict[str, Any]:
    headers = {"User-Agent": "SemperVigil/1.0"}
    request = Request(url, headers=headers)
    with urlopen(request, timeout=timeout_seconds) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8"))


def _parse_kev_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    entries = []
    for item in payload.get("vulnerabilities") or []:
        cve_id = str(item.get("cveID") or "").strip()
        if not cve_id:
            continue
        entries.append(
            {
                "cve_id": cve_id,
                "added_at": item.get("dateAdded") or "",
                "due_date": item.get("dueDate") or "",
                "vendor_project": item.get("vendorProject") or "",
                "product": item.get("product") or "",
                "vulnerability_name": item.get("vulnerabilityName") or "",
                "short_description": item.get("shortDescription") or "",
                "required_action": item.get("requiredAction") or "",
                "ransomware_use": item.get("knownRansomwareCampaignUse") or "",
                "notes": item.get("notes") or "",
                "raw_json": json.dumps(item, sort_keys=True),
            }
        )
    return entries


def ensure_kev_cache(
    conn,
    logger: logging.Logger | None = None,
    *,
    max_age_minutes: int = DEFAULT_MAX_AGE_MINUTES,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    url: str = KEV_URL,
) -> dict[str, object]:
    last_sync_raw = get_setting(conn, "kev.last_sync_at", None)
    if isinstance(last_sync_raw, str):
        try:
            last_sync_dt = datetime.fromisoformat(last_sync_raw.replace("Z", "+00:00"))
        except Exception:
            last_sync_dt = None
    else:
        last_sync_dt = None
    now = datetime.now(tz=timezone.utc)
    if last_sync_dt and now - last_sync_dt < timedelta(minutes=max_age_minutes):
        return {"status": "fresh", "last_sync_at": last_sync_raw}
    try:
        payload = _fetch_kev_payload(url, timeout_seconds)
    except (HTTPError, URLError, TimeoutError) as exc:
        if logger:
            log_event(logger, logging.WARNING, "kev_sync_failed", error=str(exc))
        return {"status": "error", "error": str(exc)}
    entries = _parse_kev_entries(payload)
    sync_at = utc_now_iso()
    upserted = upsert_cve_kev_entries(conn, entries, sync_at=sync_at)
    pruned = prune_cve_kev_entries(conn, sync_at=sync_at)
    set_setting(conn, "kev.last_sync_at", sync_at)
    set_setting(conn, "kev.last_sync_count", str(len(entries)))
    if upserted or pruned:
        set_setting(conn, "kev.last_changed_at", sync_at)
    if logger:
        log_event(
            logger,
            logging.INFO,
            "kev_sync_ok",
            count=len(entries),
            upserted=upserted,
            pruned=pruned,
        )
    return {
        "status": "ok",
        "count": len(entries),
        "upserted": upserted,
        "pruned": pruned,
        "last_sync_at": sync_at,
    }
