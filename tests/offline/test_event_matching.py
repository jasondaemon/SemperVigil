"""Synthetic multi-document contracts, not semantic/model quality evaluation."""
from dataclasses import replace

import pytest

from sempervigil.event_evidence import Citation, Claim, evidence_version, validate_claims
from sempervigil.event_matching import (
    IncidentIdentity, IncidentReference, SourceDocument, extract_passage, match_incident,
)

pytestmark = pytest.mark.offline
MAY = IncidentReference("example-advisories", "incident-may")
JUNE = IncidentReference("example-advisories", "incident-june")
CANDIDATES = (
    IncidentIdentity("acme-may", "org-acme", frozenset({MAY})),
    IncidentIdentity("acme-june", "org-acme", frozenset({JUNE})),
)
DOCUMENT = SourceDocument("roundup", "wire-origin", "https://example.org/news",
                          "Acme: May incident.\nBeta: separate incident.")


def test_roundup_scope_and_claim_offsets():
    passage = extract_passage(DOCUMENT, start=0, end=19, incident_id="acme-may")
    evidence = passage.evidence
    assert evidence.text == "Acme: May incident."
    assert passage.document_id == DOCUMENT.id
    assert evidence.url == DOCUMENT.url
    snapshot = {evidence.id: evidence}
    claim = Claim("claim", "acme-may", evidence.text, "asserted",
                  (Citation(evidence.id, 0, len(evidence.text), evidence.text),))
    assert validate_claims((claim,), incident_id="acme-may", evidence=snapshot,
                           expected_version=evidence_version(snapshot)) == ()
    wrong = replace(claim, citations=(Citation(evidence.id, 20, 43, "Beta: separate incident."),))
    assert "invalid_span" in validate_claims((wrong,), incident_id="acme-may",
                                            evidence=snapshot, expected_version=evidence_version(snapshot))


@pytest.mark.parametrize("start,end", [(-1, 3), (0, 999), (3, 3), (4, 2), (False, 3), (0, 1.5)])
def test_invalid_passage_offsets(start, end):
    with pytest.raises(ValueError, match="invalid_span"):
        extract_passage(DOCUMENT, start=start, end=end, incident_id="acme-may")


@pytest.mark.parametrize("field,value,error", [
    ("id", "", "missing_provenance"), ("origin_id", " ", "missing_provenance"),
    ("url", "javascript:alert(1)", "unsafe_source_url"),
    ("url", "https://user:secret@example.org/", "unsafe_source_url"),
    ("url", "https://[", "unsafe_source_url"),
    ("text", " " * 50, "empty_passage"),
])
def test_invalid_document(field, value, error):
    with pytest.raises(ValueError, match=error):
        extract_passage(replace(DOCUMENT, **{field: value}), start=0, end=19, incident_id="acme-may")


def test_snapshot_identity_unicode_and_syndication():
    original = replace(DOCUMENT, text="Caf\u00e9 \U0001f512 incident")
    first = extract_passage(original, start=0, end=6, incident_id="acme-may")
    assert first.evidence.text == "Caf\u00e9 \U0001f512"
    assert first == extract_passage(original, start=0, end=6, incident_id="acme-may")
    mirror = extract_passage(replace(original, id="mirror", url="https://example.net/copy"),
                             start=0, end=6, incident_id="acme-may")
    assert mirror.evidence.id != first.evidence.id
    assert mirror.evidence.origin_id == first.evidence.origin_id
    changed = extract_passage(replace(original, text=original.text + " updated"),
                              start=0, end=6, incident_id="acme-may")
    assert changed.document_version != first.document_version
    assert changed.evidence.id != first.evidence.id


@pytest.mark.parametrize("references,entity,expected,reason", [
    ({MAY}, "org-acme", "acme-may", "unique_reference"),
    ({JUNE}, "org-acme", "acme-june", "unique_reference"),
    ({MAY, JUNE}, "org-acme", None, "ambiguous_reference"),
    (set(), "org-acme", None, "insufficient_evidence"),
    ({MAY}, "org-beta", None, "entity_conflict"),
    ({IncidentReference("other-authority", MAY.value)}, "org-acme", None, "no_reference_match"),
    ({IncidentReference("example-advisories", "unknown")}, "org-acme", None, "no_reference_match"),
    ({IncidentReference("", "unknown")}, "org-acme", None, "invalid_input"),
    ({MAY}, "", None, "invalid_input"),
])
def test_matching(references, entity, expected, reason):
    for candidates in (CANDIDATES, tuple(reversed(CANDIDATES))):
        decision = match_incident(entity_id=entity, references=frozenset(references), candidates=candidates)
        assert (decision.incident_id, decision.reason) == (expected, reason)


def test_duplicate_and_conflicting_registry_abstain():
    assert match_incident(entity_id="org-acme", references=frozenset({MAY}),
                          candidates=CANDIDATES + CANDIDATES).reason == "invalid_candidates"
    conflict = IncidentIdentity("other", "org-acme", frozenset({MAY}))
    assert match_incident(entity_id="org-acme", references=frozenset({MAY}),
                          candidates=CANDIDATES + (conflict,)).reason == "ambiguous_reference"


def test_multiple_documents_same_incident_preserve_origins():
    documents = (replace(DOCUMENT, text="Acme: May incident."), replace(DOCUMENT, id="followup", origin_id="independent",
                                   url="https://example.net/followup", text="Acme: update in June."))
    passages = [extract_passage(doc, start=0, end=len(doc.text), incident_id="acme-may")
                for doc in documents]
    assert len({p.evidence.origin_id for p in passages}) == 2
    # Publication month and company display names are not matching inputs.
    assert match_incident(entity_id="org-acme", references=frozenset({MAY}),
                          candidates=CANDIDATES).incident_id == "acme-may"
