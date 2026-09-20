import copy

import pytest

from sempervigil.event_composition_publication import (
    PUBLIC_WORKFLOW,
    QUALIFICATION_WORKFLOW,
    event_identity,
    validate_bundle,
)
from sempervigil.event_render import _canonical_sources, index_entry, render
from sempervigil.investigation import _version

pytestmark = pytest.mark.offline


def _bundle():
    ledger_id = "eld_" + "a" * 64
    fact = {"fact_id": "fact-one", "candidate_id": "candidate-one",
            "evidence_revision_id": "evidence-one", "article_id": 7,
            "statement": "WaterPlum infected devices.", "kind": "observation",
            "date_text": "2026-09-18", "date_role": "incident",
            "exact_passages": [{"id": "passage-one", "start": 0, "end": 12,
                                "text": "Exact source"}],
            "sections": ["timeline", "attack_path", "impact"]}
    ledger = {"workflow": "accepted-evidence-event-ledger-v1", "ledger_id": ledger_id,
              "title": "WaterPlum campaign compromised 30,000 devices", "kind": "campaign",
              "sources": [{"candidate_id": "candidate-one", "evidence_revision_id": "evidence-one",
                           "article_id": 7, "title": "Source", "kind": "campaign"}],
              "facts": [fact], "superseded_fact_ids": [], "conflict_fact_ids": [],
              "public_eligible": False}
    ledger_record = {"ledger": ledger,
                     "change": {"kind": "initial", "added_fact_ids": ["fact-one"],
                                "supersedes_fact_ids": [], "conflict_fact_ids": []},
                     "predecessor_revision_id": None}
    ledger_revision_id = "elr_" + _version(ledger_record)
    composition = {"workflow": "event-ledger-composition-v4", "ledger_id": ledger_id,
                   "ledger_revision_id": ledger_revision_id, "generation_version": "b" * 64,
                   "request_version": "c" * 64,
                   "sections": {"overview": [{"text": "WaterPlum compromised devices <script>.",
                                                "fact_ids": ["fact-one"]}],
                                "attack_vector": [{"text": "The campaign used a malware infection path.",
                                                   "fact_ids": ["fact-one"]}],
                                "attack_path": [],
                                "timeline": [{"text": "Researchers reported the campaign.",
                                              "fact_ids": ["fact-one"], "date_text": "2026-09-18"}],
                                "impact": [{"text": "The reporting described infected devices.",
                                            "fact_ids": ["fact-one"]}],
                                "response_recovery": [], "mitigations": [], "attribution": [],
                                "open_questions": []},
                   "change": ledger_record["change"], "status": "unreviewed",
                   "public_eligible": False}
    composition_id = "elc_" + _version(composition)
    event_id = event_identity(ledger_id)
    qualification = {"workflow": QUALIFICATION_WORKFLOW, "event_id": event_id,
                     "ledger_revision_id": ledger_revision_id, "composition_id": composition_id,
                     "reviewer": {"kind": "human", "id": "test", "version": "d" * 64},
                     "reviewed_at": "2026-09-19T00:00:00+00:00"}
    bundle = {"workflow": PUBLIC_WORKFLOW, "event_id": event_id,
              "ledger_revision_id": ledger_revision_id, "composition_id": composition_id,
              "ledger_record": ledger_record, "composition": composition,
              "sources": [{"candidate_id": "candidate-one", "evidence_revision_id": "evidence-one",
                           "article_id": 7, "title": "Primary report", "url": "https://example.test/report",
                           "brief_day": "2026-09-18"}],
              "qualification": qualification, "predecessor": None}
    return event_id, bundle


def test_composition_bundle_renders_reproducible_page_and_index():
    event_id, bundle = _bundle()
    bundle["composition"]["sections"] = dict(
        sorted(bundle["composition"]["sections"].items())
    )
    bundle["composition_id"] = "elc_" + _version(bundle["composition"])
    bundle["qualification"]["composition_id"] = bundle["composition_id"]
    revision = _version({"workflow": PUBLIC_WORKFLOW, "bundle": bundle})
    projection = validate_bundle(bundle, event_id=event_id, expected_revision=revision)
    assert projection["revision_id"] == revision
    metadata, page = render(bundle, event_id=event_id, expected_revision=revision)
    assert metadata["title"].startswith("WaterPlum")
    assert "Attack vector" in page and "2026-09-18" in page
    assert page.index("<h2>Overview</h2>") < page.index("<h2>Attack vector</h2>")
    assert "<script>" not in page and "&lt;script&gt;" in page
    assert "https://example.test/report" in page
    assert "2026-09-18 - Primary report - example.test" in page
    entry = index_entry(bundle, event_id=event_id, expected_revision=revision)
    assert entry["status"] == "source_backed_event"
    assert entry["counts"]["articles"] == 1


def test_composition_bundle_rejects_section_and_revision_tampering():
    event_id, bundle = _bundle()
    revision = _version({"workflow": PUBLIC_WORKFLOW, "bundle": bundle})
    bad = copy.deepcopy(bundle)
    bad["composition"]["sections"]["mitigations"] = [
        {"text": "Unsupported advice", "fact_ids": ["fact-one"]}
    ]
    bad["composition_id"] = "elc_" + _version(bad["composition"])
    bad["qualification"]["composition_id"] = bad["composition_id"]
    with pytest.raises(ValueError, match="citation_invalid"):
        validate_bundle(bad, event_id=event_id)
    with pytest.raises(ValueError, match="pointer_mismatch"):
        validate_bundle(bundle, event_id=event_id, expected_revision="0" * 64)


def test_canonical_sources_collapse_legacy_trailing_slash_duplicates():
    sources = [
        {"article_id": 7, "url": "https://example.test/report/", "title": "Original"},
        {"article_id": 8, "url": "https://example.test/report", "title": "Search duplicate"},
        {"article_id": 9, "url": "https://independent.test/report", "title": "Independent"},
    ]
    unique, numbers = _canonical_sources(sources)
    assert [source["article_id"] for source in unique] == [7, 9]
    assert numbers == {7: 1, 8: 1, 9: 2}
