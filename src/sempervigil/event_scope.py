"""Exact source anchors for private scope proposals, never semantic approval."""
import json

from .event_review import _json, validate_packet
from .investigation import _version

WORKFLOW = "event-scope-proposal-v1"
MAX_BYTES = 6000
ROLES = {"entity", "affected_system", "attack_mechanism", "reported_reference"}


def propose(packet: dict, *, article_id: int, start: int, end: int, focus: list[dict]) -> dict:
    packet = validate_packet(json.dumps(packet).encode())
    if type(article_id) is not int:
        raise ValueError("invalid_scope_article")
    document = next((d for d in packet["documents"] if d["article_id"] == article_id), None)
    if document is None:
        raise ValueError("scope_source_unavailable")
    text = document["text"]
    if (type(start) is not int or type(end) is not int
            or not 0 <= start < end <= len(text) or not 35 <= end - start <= 1200):
        raise ValueError("invalid_scope_anchor_span")
    if type(focus) is not list or not 2 <= len(focus) <= 4:
        raise ValueError("scope_focus_required")
    fields = []
    roles = set()
    for field in focus:
        if type(field) is not dict or field.keys() != {"role", "start", "end"}:
            raise ValueError("invalid_scope_focus")
        role, left, right = field["role"], field["start"], field["end"]
        if (type(role) is not str or role not in ROLES or role in roles
                or type(left) is not int or type(right) is not int
                or not start <= left < right <= end or right - left > 160
                or not text[left:right].strip()):
            raise ValueError("invalid_scope_focus")
        roles.add(role)
        fields.append({**field, "quote": text[left:right]})
    if "entity" not in roles:
        raise ValueError("scope_entity_required")
    entity = next(f["quote"].strip().casefold() for f in fields if f["role"] == "entity")
    if all(f["role"] == "entity" or f["quote"].strip().casefold() == entity for f in fields):
        raise ValueError("company_only_scope")
    payload = {"workflow": WORKFLOW, "event_id": packet["event"]["id"],
               "anchor": {"article_id": article_id, "document_version": _version(document),
                          "start": start, "end": end, "quote": text[start:end],
                          "source_title": document["title"], "url": document["url"]},
               "focus": sorted(fields, key=lambda f: f["role"]),
               "scope_status": "proposal_only", "public_eligible": False}
    result = {**payload, "scope_version": _version(payload)}
    if len(json.dumps(result, ensure_ascii=True).encode()) > MAX_BYTES:
        raise ValueError("scope_too_large")
    return result


def declaration(scope: dict, event_id: str) -> dict:
    """Bound admission metadata only; the worker must verify source provenance."""
    value = _json(json.dumps(scope, ensure_ascii=True).encode(), MAX_BYTES)
    if value.keys() != {"workflow", "event_id", "anchor", "focus", "scope_status", "public_eligible", "scope_version"}:
        raise ValueError("invalid_scope_contract")
    if (value["workflow"] != WORKFLOW or value["event_id"] != event_id
            or value["scope_status"] != "proposal_only" or value["public_eligible"] is not False
            or value["scope_version"] != _version({k: v for k, v in value.items() if k != "scope_version"})):
        raise ValueError("invalid_scope_contract")
    anchor, focus = value["anchor"], value["focus"]
    if (type(anchor) is not dict or anchor.keys() != {
            "article_id", "document_version", "start", "end", "quote", "source_title", "url"}
            or type(focus) is not list or any(type(f) is not dict or f.keys() != {
                "role", "start", "end", "quote"} for f in focus)):
        raise ValueError("invalid_scope_contract")
    return value


def validate(scope: dict, packet: dict) -> dict:
    value = declaration(scope, packet["event"]["id"])
    anchor, focus = value["anchor"], value["focus"]
    canonical = propose(packet, article_id=anchor["article_id"], start=anchor["start"], end=anchor["end"],
                        focus=[{k: f[k] for k in ("role", "start", "end")} for f in focus])
    if canonical != value:
        raise ValueError("stale_or_modified_scope")
    return canonical


def model_context(scope: dict, packet: dict) -> dict:
    value = validate(scope, packet)
    return {"anchor": {key: value["anchor"][key] for key in ("source_title", "url", "quote")},
            "focus": [{"role": f["role"], "quote": f["quote"]} for f in value["focus"]],
            "scope_status": "proposal_only"}
