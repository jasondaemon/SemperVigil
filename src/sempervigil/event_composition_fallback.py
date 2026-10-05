"""Deterministic, evidence-extractive fallback for failed composition atoms."""
import json
import re

from .investigation import _version

WORKFLOW = "event-composition-extractive-fallback-v1"
_STOP = {"the", "and", "that", "with", "from", "this", "were", "was", "for",
         "are", "but", "not", "have", "has", "had", "their", "its", "into"}


def _tokens(value):
    return set(re.findall(r"[a-z0-9]+", value.lower())) - _STOP


def _entities(value):
    return {x.lower() for x in re.findall(r"\b[A-Z][A-Za-z0-9$.-]{2,}\b", value)
            if x.lower() not in {"the", "a", "an"}}


def _destination(fact):
    mapping = {"attack_vector": "attack_vector", "attack_path": "attack_path",
               "impact": "impact", "response_recovery": "response_recovery",
               "mitigation": "mitigations", "attribution": "attribution",
               "open_question": "open_questions", "context": "response_recovery"}
    return next((mapping[x] for x in fact.get("sections", []) if x in mapping), None)


def _candidates(item, reason, facts):
    query, entities = _tokens(str(item.get("text") or "") + " " + reason), _entities(item["text"])
    ranked = []
    for fact in facts:
        destination, statement = _destination(fact), str(fact.get("statement") or "")
        if not destination:
            continue
        fact_entities = _entities(statement)
        if entities and fact_entities and not (entities & fact_entities):
            continue
        overlap = len(query & _tokens(statement))
        if overlap >= 3:
            ranked.append((overlap, statement, destination, fact))
    ranked.sort(key=lambda row: (row[0], row[1]), reverse=True)
    return [{"destination": row[2], "fact": row[3]} for row in ranked[:1]]


def build(composition_id, composition, ledger_revision, decision):
    from . import event_composition
    from .event_composition_repair import _items
    if composition.get("fallback"):
        raise ValueError("event_composition_fallback_already_applied")
    failures = {row["id"]: row for row in decision.get("audits", [])
                if row.get("verdict") != "supported"}
    if not failures:
        raise ValueError("event_composition_fallback_failures_missing")
    active, aliases = event_composition._active_facts(ledger_revision["ledger"])
    alias_by_id = {fact["fact_id"]: alias for alias, fact in aliases.items()}
    output = {section: [] for section in event_composition.GENERATED_SECTIONS}
    lineage = []
    for item_id, section, item in _items(composition):
        failure = failures.get(item_id)
        if not failure:
            output[section].append({"text": item["text"],
                                    "fact_refs": [alias_by_id[x] for x in item["fact_ids"]],
                                    "claim_type": item.get("claim_type", "sourced_finding"),
                                    "confidence": item.get("confidence")})
            continue
        selected = _candidates(item, failure.get("reason", ""), active)
        if not selected:
            lineage.append({"source_item_id": item_id, "operation": "drop",
                            "offending_clause": item["text"],
                            "audit_reason": failure.get("reason", "")})
            continue
        for index, choice in enumerate(selected, 1):
            fact, destination = choice["fact"], choice["destination"]
            output[destination].append({"text": fact["statement"],
                                        "fact_refs": [alias_by_id[fact["fact_id"]]],
                                        "claim_type": "sourced_finding", "confidence": None})
            lineage.append({"source_item_id": item_id,
                            "proposition_id": f"{item_id}.fallback.{index}",
                            "operation": "relocate" if destination != section else "replace",
                            "source_section": section, "destination_section": destination,
                            "fact_ids": [fact["fact_id"]],
                            "offending_clause": item["text"],
                            "audit_reason": failure.get("reason", "")})
    from .event_composition_repair import _deduplicate
    record = event_composition.validate(json.dumps(_deduplicate(output)).encode(), ledger_revision,
        composition["generation_version"], require_detail_coverage=False)
    fallback = {"workflow": WORKFLOW, "source_composition_id": composition_id,
                "source_audit_request_version": decision.get("request_version"),
                "lineage": lineage}
    record["fallback"] = {**fallback, "version": _version(fallback)}
    return record, record["fallback"]
