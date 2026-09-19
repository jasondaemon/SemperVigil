"""Real-source excerpt seed; review labels are provisional, not a semantic engine."""
import json
from pathlib import Path
import re

import pytest

from sempervigil.event_evidence import Citation, Claim, Evidence, evidence_version, validate_claims

pytestmark = pytest.mark.offline
CORPUS = json.loads((Path(__file__).parents[1] / "fixtures/events/odido_review.json").read_text())


def test_corpus_provenance_and_review_boundary():
    assert CORPUS["review_status"] == "assistant_reviewed_provisional"
    documents = CORPUS["documents"]
    assert len({d["id"] for d in documents}) == len(documents) == 4
    excerpt_ids = []
    for document in documents:
        assert re.fullmatch(r"[0-9a-f]{64}", document["text_sha256"])
        assert document["url"].startswith("https://")
        for excerpt in document["excerpts"]:
            excerpt_ids.append(excerpt["id"])
            assert 0 <= excerpt["start"] < excerpt["end"] <= document["text_length"]
            assert excerpt["end"] - excerpt["start"] == len(excerpt["text"])
        # Deliberately small excerpts, not redistributed full source articles.
        assert sum(len(e["text"].split()) for e in document["excerpts"]) <= 25
    assert len(excerpt_ids) == len(set(excerpt_ids))
    cases = CORPUS["cases"]
    assert len({c["id"] for c in cases}) == len(cases) == 12
    assert {c["expected_review"] for c in cases} == {"supported", "reject"}


@pytest.mark.parametrize("case", CORPUS["cases"], ids=lambda c: c["id"])
def test_review_cases_against_structural_validator(case):
    evidence = {}
    for document in CORPUS["documents"]:
        for excerpt in document["excerpts"]:
            # This names a snapshot only; it does not assert source independence.
            evidence[excerpt["id"]] = Evidence(
                excerpt["id"], excerpt["scope"], "snapshot:" + document["text_sha256"],
                document["url"], excerpt["text"],
            )
    citations = tuple(Citation(key, 0, len(evidence[key].text), evidence[key].text)
                      for key in case["citations"])
    claim = Claim(case["id"], CORPUS["incident_id"], case["statement"], case["status"],
                  citations, case.get("date_role", "incident"),
                  case.get("date_precision", "unknown"), case.get("date_value"))
    errors = validate_claims((claim,), incident_id=CORPUS["incident_id"], evidence=evidence,
                            expected_version=evidence_version(evidence))
    assert list(errors) == case["structural_errors"]
    assert case["rationale"].strip()
    if case["expected_review"] == "reject" and not errors:
        # These are known semantic gaps, not successful publication validations.
        assert case["required_gate"] not in {"none", "trusted_scope"}


def test_seed_exposes_semantic_blind_spots():
    cases = CORPUS["cases"]
    assert sum(c["expected_review"] == "supported" for c in cases) == 3
    assert sum(bool(c["structural_errors"]) for c in cases) == 3
    assert sum(c["expected_review"] == "reject" and not c["structural_errors"]
               for c in cases) == 6
