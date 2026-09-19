import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sempervigil import event_deconstruction as draft, event_review_jobs as jobs, worker, admin
from sempervigil.services import ai_service
from test_event_review import database, get_packet, resign
from test_event_scope import proposal
from test_event_assessment import configured

pytestmark = pytest.mark.offline


def response(packet):
    return {"claims": [{"section": "overview", "statement": "Acme reported a contact-system breach.",
                        "status": "asserted", "quote": packet["documents"][0]["text"],
                        "date_role": "incident", "date_precision": "unknown", "date_value": None}]}


def test_request_is_full_source_bounded_and_timestamp_independent(database):
    packet = get_packet(database)
    scope = proposal(packet)
    first = draft.request_for(packet, scope, 1)
    assert json.loads(first["input"])["source"]["text"] == packet["documents"][0]["text"]
    packet["event"]["updated_at"] = "2026-09-19"
    resign(packet)
    assert draft.request_for(packet, scope, 1) == first
    assert len((first["system"] + first["input"]).encode()) <= draft.MAX_INPUT_BYTES


def test_exact_claim_evidence_and_private_status(database):
    packet = get_packet(database)
    result = draft.validate_response(json.dumps(response(packet)).encode(), packet, proposal(packet), 1)
    row = result["claims"][0]
    assert row["start"] == 0 and row["end"] == len(packet["documents"][0]["text"])
    assert row["date_value"] is None
    assert result["public_eligible"] is False and result["status"] == "unreviewed"
    assert "No dated milestones extracted" in draft.render(result)


@pytest.mark.parametrize("key,value", [
    ("section", "other"), ("statement", ""), ("statement", []), ("status", "confirmed"),
    ("quote", "made up"), ("date_value", "2026-04-01"), ("date_precision", "hour"),
    ("date_role", "publication"), ("section", []), ("date_value", {}),
])
def test_reject_invalid_claim(database, key, value):
    packet = get_packet(database)
    candidate = response(packet)
    candidate["claims"][0][key] = value
    with pytest.raises(ValueError):
        draft.validate_response(json.dumps(candidate).encode(), packet, proposal(packet), 1)


def test_empty_claims_is_abstention_not_permission(database):
    packet = get_packet(database)
    result = draft.validate_response(b'{"claims":[]}', packet, proposal(packet), 1)
    assert not result["claims"] and result["public_eligible"] is False


def test_duplicate_and_oversized_json_rejected(database):
    packet = get_packet(database)
    candidate = response(packet)
    candidate["claims"] *= 2
    for raw in (json.dumps(candidate).encode(), b'{"claims":[],"claims":[]}', b' ' * 16001):
        with pytest.raises(ValueError):
            draft.validate_response(raw, packet, proposal(packet), 1)


def test_changed_anchor_and_over_budget_fail_before_inference(database, tmp_path):
    packet = get_packet(database)
    scope = proposal(packet)
    packet["documents"][0]["text"] += " changed"
    resign(packet)
    complete = Mock()
    with pytest.raises(ValueError, match="stale_or_modified_scope"):
        draft.save(packet, scope, 1, complete, tmp_path)
    scope = proposal(packet)
    packet["documents"].append({**packet["documents"][0], "article_id": 2, "text": "x" * 16000})
    resign(packet)
    with pytest.raises(ValueError, match="over_budget"):
        draft.save(packet, scope, 2, complete, tmp_path)
    complete.assert_not_called()


def test_semantic_falsehood_is_not_misrepresented_as_validated_truth(database):
    packet = get_packet(database)
    candidate = response(packet)
    candidate["claims"][0]["statement"] = "Acme has fully recovered."
    result = draft.validate_response(json.dumps(candidate).encode(), packet, proposal(packet), 1)
    assert result["status"] == "unreviewed" and result["public_eligible"] is False
    assert "do not establish truth" in draft.render(result)


def test_markup_is_escaped(database):
    packet = get_packet(database)
    candidate = response(packet)
    candidate["claims"][0]["statement"] = '<script>alert("x")</script>'
    result = draft.validate_response(json.dumps(candidate).encode(), packet, proposal(packet), 1)
    assert "<script>" not in draft.render(result)
    assert "&lt;script&gt;" in draft.render(result)


def test_disabled_admission_before_connection(monkeypatch, database):
    packet = get_packet(database)
    monkeypatch.setenv("SV_EVENT_REVIEW_ENABLED", "1")
    monkeypatch.setenv("SV_EVENT_REVIEW_SCOPE_ENABLED", "1")
    monkeypatch.delenv("SV_EVENT_DECONSTRUCTION_ENABLED", raising=False)
    conn = Mock()
    with pytest.raises(PermissionError, match="deconstruction_disabled"):
        jobs.submit(conn, event_id="event", aliases=["Acme"], scope=proposal(packet),
                    article_id=1, deconstruct=True)
    conn.assert_not_called()


def test_real_private_dispatch_and_existing_artifact_reader(monkeypatch, database, tmp_path):
    packet = get_packet(database)
    scope = proposal(packet)
    profile = configured(monkeypatch)
    profile["params"]["max_input_chars"] = 15000
    for name in ("SV_EVENT_REVIEW_ENABLED", "SV_EVENT_REVIEW_SCOPE_ENABLED", "SV_EVENT_DECONSTRUCTION_ENABLED"):
        monkeypatch.setenv(name, "1")
    monkeypatch.setenv("SV_EVENT_DECONSTRUCTION_PROFILE_ID", "deconstruction-profile")
    monkeypatch.setenv("SV_EVENT_REVIEW_DIR", str(tmp_path / "private"))
    monkeypatch.setenv("SV_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("SV_DB_URL", "unused")
    monkeypatch.setattr(ai_service, "get_prompt", lambda *a: {
        "system_template": draft.SYSTEM_PROMPT, "user_template": "{{input}}"})
    model = Mock(return_value={"parsed": response(packet), "schema_valid": True})
    monkeypatch.setattr(worker, "run_profile", model)
    monkeypatch.setattr(jobs, "snapshot", lambda *a, **k: packet)
    monkeypatch.setattr(worker, "_log_job_claimed", lambda *a: None)
    monkeypatch.setattr(worker, "is_job_canceled", lambda *a: False)
    def forbidden(*a, **k): pytest.fail("private extraction must not publish")
    for name in ("mark_build_dirty", "update_event_report", "_handle_event_report_llm"):
        monkeypatch.setattr(worker, name, forbidden)
    payload = jobs.payload_for("event", ["Acme"], scope=scope, article_id=1, deconstruct=True)
    job = SimpleNamespace(id="draft-job", job_type=jobs.JOB_TYPE, payload=payload)
    result = worker.run_claimed_job(None, None, job, logging.getLogger("test"))
    model.assert_called_once()
    assert model.call_args.args[1] == "deconstruction-profile"
    job.status, job.result = "succeeded", result
    assert b"Attack progression" in jobs.read_artifact(job)
    assert result["deconstruction"]["claims"] == 1
    assert "private_revision" not in result
    with pytest.raises(ValueError, match="private_revision_unavailable"):
        jobs.read_revision(job)
    assert admin.EventPrivateReviewRequest(aliases=["Acme"], deconstruct=True).deconstruct


@pytest.mark.parametrize("options", [{}, {"article_id": 1}, {"paired": True, "article_id": 1}])
def test_incomplete_or_mixed_modes_rejected(database, options):
    packet = get_packet(database)
    if options.get("paired"):
        options = {**options, "scope": proposal(packet)}
    with pytest.raises(ValueError):
        jobs.payload_for("event", ["Acme"], deconstruct=True, **options)
