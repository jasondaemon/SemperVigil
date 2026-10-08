from __future__ import annotations

from ..normalize import normalize_name


def build_event_enrich_query(event: dict[str, object], *, incident_identity=None) -> str:
    title = str(event.get("title") or "").strip()
    kind = str(event.get("kind") or "").strip().lower()
    entity = str(event.get("entity") or "").strip() or _extract_primary_entity(title) or title
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
    if incident_identity:
        # Caller verifies this identity against explicit policy or qualified
        # immutable sources. Generated enrichment summaries are never authority.
        if incident_identity['event_id'] != event.get('id') or incident_identity['entity'] != entity:
            raise ValueError('event_report_initial_query_identity_mismatch')
        parts.extend('"'+term+'"' for term in incident_identity['query_terms'])
        parts.append(str(incident_identity['incident_year']))
    return " ".join(part for part in parts if part).strip()


def _extract_primary_entity(title: str) -> str | None:
    tokens = [t.strip(".,:;!?()[]{}\"'") for t in title.replace("—", " ").split()]
    tokens = [token for token in tokens if len(token) > 2]
    if not tokens:
        return None
    ignored = {
        "the", "and", "for", "with", "from", "north", "south", "korean", "russian",
        "chinese", "iranian", "hackers", "hacker", "group", "attackers", "infected",
        "compromised", "breached", "worldwide", "devices", "campaign",
    }
    candidates = [token for token in tokens if normalize_name(token) not in ignored]
    if not candidates:
        return None
    mixed_case = [token for token in candidates if any(char.isupper() for char in token[1:])]
    if mixed_case:
        return mixed_case[0]
    acronyms = [token for token in candidates if token.isupper() and len(token) >= 3]
    return (acronyms or candidates)[0]


def _extract_cves(event: dict[str, object]) -> set[str]:
    cves = set()
    items = event.get("items") if isinstance(event.get("items"), dict) else {}
    for cve in items.get("cves", []):
        cve_id = cve.get("cve_id")
        if cve_id:
            cves.add(str(cve_id))
    return cves
