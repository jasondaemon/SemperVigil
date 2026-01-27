from __future__ import annotations

from ..normalize import normalize_name


def build_event_enrich_query(event: dict[str, object]) -> str:
    title = str(event.get("title") or "").strip()
    kind = str(event.get("kind") or "").strip().lower()
    entity = _extract_primary_entity(title) or title
    keyword_bundle = {
        "breach": "(breach OR compromised OR intrusion OR incident)",
        "ransomware": "(ransomware OR extortion OR leak)",
        "campaign": "(campaign OR APT OR espionage)",
        "exploit": "(exploit OR vulnerability OR PoC)",
        "vuln": "(vulnerability OR advisory OR patch)",
    }.get(kind, "")
    parts = []
    if entity:
        parts.append(f"\"{entity}\"")
    if keyword_bundle:
        parts.append(keyword_bundle)
    if kind == "cve_cluster":
        parts.append(title)
    cves = _extract_cves(event)
    if cves and kind != "cve_cluster":
        parts.append(" OR ".join(sorted(cves)))
    return " ".join(part for part in parts if part).strip()


def _extract_primary_entity(title: str) -> str | None:
    tokens = [t for t in title.replace("—", " ").replace("-", " ").split() if len(t) > 2]
    if not tokens:
        return None
    token = tokens[0]
    if normalize_name(token) in {"the", "and", "for", "with"}:
        return tokens[1] if len(tokens) > 1 else None
    return token


def _extract_cves(event: dict[str, object]) -> set[str]:
    cves = set()
    items = event.get("items") if isinstance(event.get("items"), dict) else {}
    for cve in items.get("cves", []):
        cve_id = cve.get("cve_id")
        if cve_id:
            cves.add(str(cve_id))
    return cves
