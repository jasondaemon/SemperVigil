import json
from html import escape

import pytest

from sempervigil import event_assessment as assessment, event_review as review
from test_event_review import database, get_packet, resign

pytestmark = pytest.mark.offline


def test_model_reading_retains_citations_without_approving_choices(database):
    packet = get_packet(database)
    packet["documents"][0]["text"] = (
        "Acme disclosed <script>bad()</script> in a source quotation. "
        "Acme patched a different issue unrelated to this incident. "
        "Acme has not confirmed the record count for this incident.")
    resign(packet)
    ids = list(assessment.request_for(packet)["mapping"])
    assert len(ids) == 3
    choices = [("include", "same_incident"), ("exclude", "different_incident"),
               ("hold", "insufficient_context")]
    result = assessment.validate_response(json.dumps({"decisions": [
        {"id": identity, "decision": choice, "reason": reason}
        for identity, (choice, reason) in zip(ids, choices)]}).encode(), packet)
    page = review.render(packet, assessment=result)
    fragment = page.split('id="model-reading">', 1)[1].split("</details>", 1)[0]
    passages = review.draft(packet)["passages"]
    assert escape(passages[0]["quote"]) in fragment
    assert passages[1]["quote"] not in fragment and passages[2]["quote"] not in fragment
    assert "https://example.org/story" in fragment and "Stored feed date: 2026-09-01" in fragment
    assert "not an approved report" in fragment and "3 passages assessed" in fragment
    assert "1 unverified passage</summary>" in fragment and "<script>" not in fragment
    assert passages[1]["quote"] in page
    assert 'value="include" selected' not in page


def test_empty_model_selection_does_not_invent_a_report(database):
    packet = get_packet(database)
    result = assessment.validate_response(json.dumps({"decisions": [
        {"id": identity, "decision": "hold", "reason": "insufficient_context"}
        for identity in assessment.request_for(packet)["mapping"]]}).encode(), packet)
    page = review.render(packet, assessment=result)
    assert "No account of the incident is inferred" in page
    assert "0 unverified passages" in page


def test_extractive_only_review_has_no_model_draft(database):
    page = review.render(get_packet(database))
    assert 'id="model-reading"' not in page
    assert "@@MODELDRAFT@@" not in page
