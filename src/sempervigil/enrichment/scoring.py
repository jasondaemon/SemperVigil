from __future__ import annotations

import os
import re
from datetime import datetime, timezone

from ..normalize import normalize_name

_CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)


def _parse_csv(value: str | None) -> set[str]:
    if not value:
        return set()
    return {item.strip().lower() for item in value.split(",") if item.strip()}


def _parse_iso_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _extract_entities(title: str) -> list[str]:
    tokens = re.split(r"\W+", title or "")
    entities = []
    for token in tokens:
        if len(token) < 3:
            continue
        norm = normalize_name(token)
        if not norm or norm in {"the", "and", "for", "with", "from"}:
            continue
        entities.append(token)
    return entities[:5]


def score_web_result(event: dict[str, object], result: dict[str, object]) -> tuple[int, dict[str, int]]:
    allowlist = _parse_csv(os.getenv("SV_ENRICH_DOMAIN_ALLOWLIST"))
    blocklist = _parse_csv(os.getenv("SV_ENRICH_DOMAIN_BLOCKLIST"))
    min_score = 0
    max_score = 100
    score = 0
    reasons: dict[str, int] = {}

    domain = (result.get("domain") or "").lower()
    title = (result.get("title") or "")
    snippet = (result.get("snippet") or "")
    combined = f"{title} {snippet}".lower()
    published_at = _parse_iso_date(result.get("published_at") if isinstance(result.get("published_at"), str) else None)

    if domain in blocklist:
        return -999, {"blocked_domain": -999}
    if domain in allowlist:
        score += 25
        reasons["allowlist_domain"] = 25
    if domain.endswith(".gov") or domain.endswith(".mil"):
        score += 10
        reasons["gov_domain"] = 10
    if "github.com" in domain or "nvd.nist.gov" in domain:
        score += 5
        reasons["reference_domain"] = 5

    if published_at:
        age_days = (datetime.now(timezone.utc) - published_at).days
        if age_days <= 14:
            score += 10
            reasons["fresh_14d"] = 10
        elif age_days <= 60:
            score += 5
            reasons["fresh_60d"] = 5

    keyword_map = {
        "breach": 15,
        "compromised": 15,
        "intrusion": 15,
        "incident": 15,
        "ransomware": 15,
        "extortion": 15,
        "leak": 15,
        "campaign": 10,
        "espionage": 10,
        "apt": 10,
        "advisory": 5,
        "patch": 5,
        "update": 5,
        "poc": 5,
        "exploit": 5,
    }
    for key, points in keyword_map.items():
        if key in combined:
            score += points
            reasons[f"keyword:{key}"] = points

    entities = _extract_entities(str(event.get("title") or ""))
    for entity in entities:
        token = entity.lower()
        if token and token in title.lower():
            score += 10
            reasons["entity_title"] = 10
            break
        if token and token in combined:
            score += 5
            reasons["entity_snippet"] = 5
            break

    event_cves = set(event.get("cves") or [])
    if not event_cves and isinstance(event.get("items"), dict):
        for cve in event["items"].get("cves", []):
            cve_id = cve.get("cve_id")
            if cve_id:
                event_cves.add(cve_id)
    if event_cves:
        if any(cve.lower() in combined for cve in event_cves):
            score += 8
            reasons["cve_match"] = 8

    penalties = {
        "daily summary": -10,
        "weekly roundup": -10,
        "newsletter": -10,
        "top 10": -8,
        "tools list": -8,
    }
    for key, points in penalties.items():
        if key in combined:
            score += points
            reasons[f"penalty:{key}"] = points

    if "/tag/" in (result.get("url") or "") or "/category/" in (result.get("url") or ""):
        score -= 8
        reasons["category_index"] = -8

    score = max(min(score, max_score), min_score)
    return score, reasons
