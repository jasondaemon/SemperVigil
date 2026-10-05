import copy

import jsonschema
import pytest

from sempervigil import event_report_contract as contract
from sempervigil.event_source_reports import meaningful_change

pytestmark = pytest.mark.offline


def packet():
    return {"sources":[{"id":"S1","article_id":1,"text":"Acme said certain patient records may be affected. The company rotated credentials.",
        "content_hash":"first"}],"previous_report":{"untrusted":"Attackers stole every record."},
        "previous_evidence_hashes":[],"previous_cited_article_ids":[]}


def report(kind="breach"):
    return {"title":"Acme incident","kind":kind,"items":[
        {"id":"P01","section":"overview","text":"Acme reported possible patient-record exposure.",
         "claim_type":"finding","confidence":None,"rationale":"","date_label":"","date_sort":None,
         "citations":[{"source_id":"S1","quote":"certain patient records may be affected"}]},
        {"id":"P02","section":"response_recovery","text":"The company rotated credentials.",
         "claim_type":"finding","confidence":None,"rationale":"","date_label":"","date_sort":None,
         "citations":[{"source_id":"S1","quote":"The company rotated credentials."}]}]}


@pytest.mark.parametrize("kind",["breach","compromise","ransomware","vulnerability","campaign","law_enforcement"])
def test_source_span_contract_across_types(kind):
    spans = contract.validate(report(kind),packet())
    cite = spans["P01"][0]
    assert packet()["sources"][0]["text"][cite["start"]:cite["end"]]==cite["quote"]


def test_previous_report_is_never_a_citation_source():
    value = report();value["items"][0]["citations"][0]["quote"]="Attackers stole every record."
    with pytest.raises(ValueError,match="quote_not_in_source"):contract.validate(value,packet())


@pytest.mark.parametrize("claim_type,confidence",[("finding","high"),("assessment",None)])
def test_schema_itself_rejects_incompatible_claim_metadata(claim_type,confidence):
    value = report();value["items"][0].update(claim_type=claim_type,confidence=confidence)
    with pytest.raises(jsonschema.ValidationError):contract.validate(value,packet())


def test_justified_assessment_has_premises_rationale_and_confidence():
    value = report();value["items"][1].update(claim_type="assessment",confidence="moderate",
        text="Credential rotation may limit use of compromised credentials.",
        rationale="Assessment based on the reported credential rotation; its effectiveness is unconfirmed.")
    assert contract.validate(value,packet())


def test_material_corrections_and_exact_duplicate_novelty():
    old = {**packet(),"evidence_version":"a"}
    new = copy.deepcopy(old);new["evidence_version"]="b"
    new["sources"].append({**old["sources"][0],"article_id":2,"content_hash":"second"})
    assert meaningful_change(old,new) is False
    new["sources"][0]["content_hash"]="correction"
    assert meaningful_change(old,new) is True


def test_new_source_navigation_is_not_incident_novelty():
    old = {**packet(),"evidence_version":"a"}
    new = copy.deepcopy(old);new["evidence_version"]="b"
    new["sources"].append({**old["sources"][0],"article_id":2,"content_hash":"second",
        "text":old["sources"][0]["text"]+" Related: An unrelated new exploit. Daily Briefing Newsletter Sign up."})
    assert meaningful_change(old,new) is False
    new["sources"][-1]["text"] = old["sources"][0]["text"]+" Acme now confirms 200 affected patients. Related: Other news."
    assert meaningful_change(old,new) is True


def test_cohort_configuration_is_explicit_and_validated(monkeypatch):
    from sempervigil.event_source_reports import cohort_configuration
    monkeypatch.delenv("SV_EVENT_SOURCE_REPORT_COHORT_ID",raising=False)
    monkeypatch.delenv("SV_EVENT_SOURCE_REPORT_COHORT_TOKENS",raising=False)
    assert cohort_configuration() is None
    monkeypatch.setenv("SV_EVENT_SOURCE_REPORT_COHORT_ID","proof-test")
    with pytest.raises(ValueError,match="cohort_invalid"):cohort_configuration()
    monkeypatch.setenv("SV_EVENT_SOURCE_REPORT_COHORT_TOKENS","24000")
    assert cohort_configuration()=={"id":"proof-test","limit":24000}


def test_large_context_holds_without_silent_truncation():
    value = packet()
    with pytest.raises(ValueError,match="context_over_budget"):contract.context(value,max_tokens=1)
    assert "may be affected" in value["sources"][0]["text"]


def test_review_cannot_approve_while_listing_substantive_errors():
    value = {"ready":True,"issues":[{"item_id":"P01","reason":"Lost the material uncertainty qualifier.","source_ids":["S1"]}]}
    with pytest.raises(ValueError,match="review_inconsistent"):contract.validate_review(value,report(),packet())


def test_section_date_metadata_is_valid_but_semantics_require_whole_review():
    value=report()
    value["items"][0].update(date_label="As reported October 1",date_sort="2026-10-01")
    assert contract.validate(value,packet())
    value["items"][0]["date_sort"]="not a date"
    with pytest.raises(ValueError):contract.validate(value,packet())


def test_generator_upgrade_does_not_make_existing_coverage_new_evidence():
    p = packet()
    p["previous_report"] = {"items":[{"text":"The company rotated credentials."}]}
    result = contract.context(contract.update_context(p,"generator_upgrade",p))
    assert result["evidence_delta"] == {"baseline":"known","new":[],"changed":[],"removed":[]}
    assert result["update_reason"] == "generator_upgrade"
    assert "not new event facts" in result["previous_report_coverage"]
    assert result["previous_report"] == p["previous_report"]


def test_unknown_baseline_never_labels_all_sources_new():
    result = contract.update_context(packet(),"evidence_change")
    assert result["evidence_delta"]["baseline"] == "unknown"
    assert result["evidence_delta"]["new"] == []


def test_evidence_delta_distinguishes_new_corrected_and_removed_sources():
    old = packet()
    new = copy.deepcopy(old)
    new["sources"][0]["content_hash"] = "corrected"
    new["sources"].append({"id":"S2","article_id":2,"content_hash":"new","text":"New disclosure."})
    delta = contract.update_context(new,"evidence_change",old)["evidence_delta"]
    assert delta["new"] == ["S2"] and delta["changed"] == ["S1"]
    assert contract.update_context({**old,"sources":[]},"evidence_change",old)["evidence_delta"]["removed"] == ["S1"]


def test_unsupported_temporal_connector_requires_substantive_review_not_lexical_ban():
    value = report()
    value["items"][1]["text"] = "After the patient records were affected, the company subsequently rotated credentials."
    # Exact citations do not prove ordering; the whole-report review is a separate gate.
    assert contract.validate(value,packet())
    issue = {"item_id":"P02","reason":"The source does not establish the asserted sequence.","source_ids":["S1"]}
    assert contract.validate_review({"ready":False,"issues":[issue]},value,packet())["ready"] is False
    with pytest.raises(ValueError,match="review_inconsistent"):
        contract.validate_review({"ready":True,"issues":[issue]},value,packet())


def test_generator_metadata_projection_is_exact_deletion_with_explicit_lineage():
    p = contract.context(contract.update_context(packet(),"generator_upgrade",packet()))
    raw = report()
    raw["items"].append({**raw["items"][1],"id":"P03","section":"what_changed","text":"The credential rotation is new evidence."})
    review = {"ready":True,"issues":[]}
    before = copy.deepcopy(raw)
    derivative = contract.publication_projection(raw,review,p)
    assert derivative["report"] == report() and raw == before
    assert derivative["lineage"]["removed_item_ids"] == ["P03"]
    assert derivative["lineage"]["notice"] == contract.GENERATOR_NOTICE
    assert derivative["lineage"]["raw_report_version"] != derivative["lineage"]["report_version"]
    assert "not endorsed" in derivative["lineage"]["review_scope"]
    assert "what_changed" not in contract.generation_schema(["S1"],p)["properties"]["items"]["items"]["anyOf"][0]["properties"]["section"]["enum"]


def test_projection_never_excludes_substantive_review_issues_or_evidence_updates():
    p = contract.context(contract.update_context(packet(),"generator_upgrade",packet()))
    issue = {"item_id":"P01","reason":"Unsupported material overview statement.","source_ids":["S1"]}
    with pytest.raises(ValueError,match="retained_review_issues"):
        contract.publication_projection(report(),{"ready":False,"issues":[issue]},p)
    for context in (contract.update_context(packet(),"evidence_change",packet()),
                    contract.update_context(packet(),"generator_upgrade")):
        with pytest.raises(ValueError,match="requires_unchanged_evidence"):
            contract.publication_projection(report(),{"ready":True,"issues":[]},contract.context(context))


def test_source_report_scope_is_enforced(monkeypatch):
    from sempervigil.event_source_reports import check_scope
    monkeypatch.setenv("SV_EVENT_SOURCE_REPORT_EVENT_IDS","evt_sample")
    check_scope("evt_sample")
    with pytest.raises(PermissionError,match="outside_scope"):
        check_scope("evt_other")


def test_chart_renders_default_disabled_report_flag_and_scope():
    from pathlib import Path
    root=Path(__file__).resolve().parents[2]
    template=(root/"deploy/helm/sempervigil/templates/configmap-env.yaml").read_text()
    assert 'SV_EVENT_SOURCE_REPORT_ENABLED: {{ .Values.env.SV_EVENT_SOURCE_REPORT_ENABLED' in template
    assert 'SV_EVENT_SOURCE_REPORT_EVENT_IDS: {{ .Values.env.SV_EVENT_SOURCE_REPORT_EVENT_IDS' in template
    assert 'SV_EVENT_SOURCE_REPORT_ENABLED: "0"' in (root/"deploy/helm/sempervigil/values.yaml").read_text()
