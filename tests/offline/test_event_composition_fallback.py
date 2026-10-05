import pytest

from sempervigil import event_composition
from sempervigil import event_composition_fallback as fallback

pytestmark = pytest.mark.offline


def test_overview_extractively_covers_every_required_dimension():
    facts = [
        {"fact_id": "f1", "statement": "Acme detected the intrusion.",
         "sections": ["attack_vector"]},
        {"fact_id": "f2", "statement": "The actor accessed customer records.",
         "sections": ["impact"]},
        {"fact_id": "f3", "statement": "Acme contained the affected systems.",
         "sections": ["response_recovery"]},
    ]
    aliases = {f"F0{index}": fact for index, fact in enumerate(facts, 1)}

    rows = fallback._overview(facts, aliases)

    assert len(rows) == 3
    assert {row["fact_refs"][0] for row in rows} == {"F01", "F02", "F03"}
    assert all(row["claim_type"] == "sourced_finding" for row in rows)


def material(event_kind="breach"):
    facts = [
        {"fact_id": "scope", "statement": "Acme reported a security event.",
         "kind": "reported_fact", "date_text": None, "date_role": "none",
         "sections": ["context"]},
        {"fact_id": "wrong", "statement": "Acme said a reviewed sample appeared authentic.",
         "kind": "reported_fact", "date_text": None, "date_role": "none",
         "sections": ["impact"]},
        {"fact_id": "portal", "statement": "Acme reported its customer portal remained unavailable.",
         "kind": "reported_fact", "date_text": None, "date_role": "none",
         "sections": ["response_recovery"]},
    ]
    ledger = {"public_eligible": False, "title": "Acme event", "kind": event_kind,
              "facts": facts, "superseded_fact_ids": [], "conflict_fact_ids": []}
    sections = {section: [] for section in event_composition.SECTIONS}
    sections["overview"] = [
        {"text": "Acme reported a security event.",
         "fact_ids": ["scope", "portal"],
         "claim_type": "sourced_finding", "confidence": None},
        {"text": "Acme said a reviewed sample appeared authentic.",
         "fact_ids": ["wrong"], "claim_type": "sourced_finding", "confidence": None},
    ]
    sections["impact"] = [{"text": "Acme's customer portal remained unavailable.",
                            "fact_ids": ["wrong"], "claim_type": "sourced_finding",
                            "confidence": None}]
    composition = {"workflow": event_composition.WORKFLOW,
                   "ledger_revision_id": "elr_1", "ledger_id": "eld_1",
                   "generation_version": "a" * 64, "request_version": "b" * 64,
                   "status": "held", "public_eligible": False,
                   "section_policy": event_composition.SECTION_POLICY,
                   "change": {}, "sections": sections}
    revision = {"revision_id": "elr_1", "ledger_id": "eld_1", "status": "accepted",
                "lineage_current": True, "ledger": ledger, "change": {}}
    decision = {"request_version": "audit-1", "audits": [
        {"id": "C01", "verdict": "supported", "reason": "supported"},
        {"id": "C02", "verdict": "supported", "reason": "supported"},
        {"id": "C03", "verdict": "unsupported",
         "reason": "The cited sample does not support portal unavailability."}]}
    return composition, revision, decision


@pytest.mark.parametrize("kind", [
    "breach", "ransomware", "vulnerability", "supply_chain", "campaign",
    "law_enforcement", "initial_report",
])
def test_fallback_relocates_exact_fact_across_event_types(kind):
    composition, revision, decision = material(kind)
    record, lineage = fallback.build("elc_1", composition, revision, decision)
    assert record["sections"]["impact"] == []
    assert record["sections"]["response_recovery"][-1]["fact_ids"] == ["portal"]
    operation = next(row for row in lineage["lineage"] if row["source_item_id"] == "C03")
    assert operation["operation"] == "relocate"
    assert operation["destination_section"] == "response_recovery"
    assert record["derivation"]["parent"]["composition_id"] == "elc_1"
    assert record["generation_version"] != composition["generation_version"]
    assert record["request_version"] != composition["request_version"]
    replay, _ = fallback.build("elc_1", composition, revision, decision)
    assert replay == record


def test_fallback_drops_invented_absence_when_no_fact_matches():
    composition, revision, decision = material()
    composition["sections"]["impact"][0]["text"] = (
        "Acme did not identify every participant or confirm operations had ended.")
    record, lineage = fallback.build("elc_1", composition, revision, decision)
    operation = next(row for row in lineage["lineage"] if row["source_item_id"] == "C03")
    assert operation["operation"] == "drop"
    assert record["sections"]["impact"] == []


def test_entity_filter_excludes_unrelated_same_event_actor():
    composition, revision, decision = material("law_enforcement")
    revision["ledger"]["facts"].append({
        "fact_id": "other", "statement": "Beta's operator was arrested after portal activity.",
        "kind": "reported_fact", "date_text": None, "date_role": "none",
        "sections": ["attribution"]})
    record, lineage = fallback.build("elc_1", composition, revision, decision)
    assert all("other" not in row.get("fact_ids", []) for row in lineage["lineage"])
    assert all("other" not in item["fact_ids"]
               for rows in record["sections"].values() for item in rows)


def test_extract_keeps_former_antecedent_verbatim():
    composition, revision, decision = material()
    fact = {"fact_id": "scope_people",
            "statement": "The actor claimed personal information of agents and applicants, as well as medical information of the former.",
            "kind": "reported_fact", "date_text": None, "date_role": "none",
            "sections": ["impact"]}
    revision["ledger"]["facts"].append(fact)
    composition["sections"]["impact"][0] = {
        "text": "The actor claimed medical information of former employees.",
        "fact_ids": ["wrong"], "claim_type": "sourced_finding", "confidence": None}
    decision["audits"][2]["reason"] = "Former was resolved to the wrong population."
    record, _ = fallback.build("elc_1", composition, revision, decision)
    texts = [item["text"] for item in record["sections"]["impact"]]
    assert fact["statement"] in texts
    assert all("former employees" not in text for text in texts)
