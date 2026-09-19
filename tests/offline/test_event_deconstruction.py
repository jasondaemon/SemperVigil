import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sempervigil import event_deconstruction as draft, event_review_jobs as jobs, worker, admin
from sempervigil.services import ai_service
from sempervigil.llm import router
from test_event_review import database, get_packet, resign
from test_event_scope import proposal
from test_event_assessment import configured

pytestmark = pytest.mark.offline


def test_constrained_format_quotes_are_exact_and_dates_have_enums():
    text = 'Context.ai was compromised. "Recovery is partial." Another claim.'
    fields = draft.response_format(text)["json_schema"]["schema"]["properties"]["claims"]["items"]["properties"]
    assert all(q in text for q in fields["quote"]["enum"])
    assert 'Context.ai was compromised.' in fields["quote"]["enum"]
    assert fields["date_precision"]["enum"] == ["unknown", "year", "month", "day"]
    assert "incident" not in fields["section"]["enum"]


def test_generation_schema_rejects_null_with_month_precision(database):
    import jsonschema
    packet = get_packet(database)
    schema = draft.response_format(packet["documents"][0]["text"])["json_schema"]["schema"]
    value = response(packet)
    jsonschema.validate(value, schema)
    value["claims"][0]["date_precision"] = "month"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(value, schema)


def test_transport_applies_schema_only_to_private_local_mode(monkeypatch):
    call = Mock(return_value={"choices": [{"message": {"content": '{"claims":[]}'}}]})
    monkeypatch.setattr(router, "_http_request", call)
    context = {"stage": "event_review_private", "event_deconstruction_source": "Acme reported a breach."}
    router._call_provider("openai_compatible", "http://localhost", None, "ollama/test", [], {}, {}, context)
    assert call.call_args.args[3]["response_format"] == draft.response_format(context["event_deconstruction_source"])
    for stage, model in (("other", "ollama/test"), ("event_review_private", "gpt-test")):
        with pytest.raises(ValueError, match="unsupported_private_deconstruction"):
            router._call_provider("openai_compatible", "http://localhost", None, model, [], {}, {},
                                  {**context, "stage": stage})


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
    again = worker.run_claimed_job(None, None, job, logging.getLogger("test"))
    assert again["model_cache_hit"] is True
    assert again["artifact"] == result["artifact"]
    model.assert_called_once()


@pytest.mark.parametrize("options", [{}, {"article_id": 1}, {"paired": True, "article_id": 1}])
def test_incomplete_or_mixed_modes_rejected(database, options):
    packet = get_packet(database)
    if options.get("paired"):
        options = {**options, "scope": proposal(packet)}
    with pytest.raises(ValueError):
        jobs.payload_for("event", ["Acme"], deconstruct=True, **options)


def source_result(packet, scope, article_id):
    text = next(d["text"] for d in packet["documents"] if d["article_id"] == article_id)
    value = response(packet)
    value["claims"][0]["quote"] = text
    value["claims"][0]["statement"] = text
    result = draft.validate_response(json.dumps(value).encode(), packet, scope, article_id)
    return {**result, "generation_version": "a" * 64}


def test_incremental_compilation_replaces_changed_source_not_old_prose(database):
    packet = get_packet(database)
    scope = proposal(packet)
    first = source_result(packet, scope, 1)
    old = draft.compile_report(packet, scope, [first], "a" * 64)
    packet["documents"].append({**packet["documents"][0], "article_id": 2,
                                "text": "Acme says recovery is partial."})
    resign(packet)
    second = source_result(packet, scope, 2)
    newer = draft.compile_report(packet, scope, [second, first], "a" * 64)
    assert newer == draft.compile_report(packet, scope, [first, second], "a" * 64)
    delta = draft.changes(old, newer)
    assert len(delta["added"]) == 1 and len(delta["retained"]) == 1 and not delta["withdrawn"]
    packet["documents"][1]["text"] = "Acme corrected the restoration claim."
    resign(packet)
    with pytest.raises(ValueError):
        draft.compile_report(packet, scope, [first, second], "a" * 64)
    corrected = source_result(packet, scope, 2)
    latest = draft.compile_report(packet, scope, [first, corrected], "a" * 64)
    delta = draft.changes(newer, latest)
    assert len(delta["withdrawn"]) == len(delta["added"]) == len(delta["retained"]) == 1
    assert "recovery is partial" not in draft.render(latest)


def test_compilation_coverage_and_timestamp_stability(database):
    packet = get_packet(database)
    scope = proposal(packet)
    first = source_result(packet, scope, 1)
    packet["documents"].extend([{**packet["documents"][0], "article_id": 2},
                                {**packet["documents"][0], "article_id": 3, "text": "x" * 16000},
                                {**packet["documents"][0], "article_id": 4, "text": "x" * 601}])
    resign(packet)
    report = draft.compile_report(packet, scope, [first], "a" * 64)
    assert report["coverage"]["pending"] == [2]
    assert report["coverage"]["over_budget"] == [3]
    assert report["coverage"]["unextractable"] == [4]
    packet["event"]["updated_at"] = "2026-09-20"
    resign(packet)
    assert draft.compile_report(packet, scope, [first], "a" * 64) == report
    assert report["public_eligible"] is False


@pytest.mark.parametrize("change", ["generation", "id", "url", "start", "statement"])
def test_compilation_rejects_tampered_source_receipt(database, change):
    packet = get_packet(database)
    scope = proposal(packet)
    result = source_result(packet, scope, 1)
    if change == "generation":
        result["generation_version"] = "b" * 64
    else:
        result["claims"][0][change] = 7 if change == "start" else "tampered"
    with pytest.raises(ValueError):
        draft.compile_report(packet, scope, [result], "a" * 64)


def test_cache_reuses_unchanged_source_despite_other_source_changes(database, tmp_path):
    packet = get_packet(database)
    scope = proposal(packet)
    complete = Mock(return_value=response(packet))
    complete.cache_identity = "a" * 64
    _, first = draft.save(packet, scope, 1, complete, tmp_path)
    assert not first["cache_hit"]
    packet["documents"].append({**packet["documents"][0], "article_id": 2})
    resign(packet)
    _, second = draft.save(packet, scope, 1, complete, tmp_path)
    assert second["cache_hit"] and second["coverage"]["pending"] == [2]
    complete.assert_called_once()
    complete.cache_identity = "b" * 64
    _, third = draft.save(packet, scope, 1, complete, tmp_path)
    assert not third["cache_hit"] and complete.call_count == 2


def test_cache_symlink_and_tampering_fail_without_inference(database, tmp_path):
    packet = get_packet(database)
    scope = proposal(packet)
    complete = Mock(return_value=response(packet))
    complete.cache_identity = "a" * 64
    draft.save(packet, scope, 1, complete, tmp_path)
    cached = next((tmp_path / "deconstruction-cache").glob('*.json'))
    value = json.loads(cached.read_text())
    value["claims"][0]["url"] = "https://wrong.example/"
    cached.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        draft.save(packet, scope, 1, complete, tmp_path)
    cached.unlink()
    cached.symlink_to(tmp_path / "missing")
    with pytest.raises(OSError):
        draft.save(packet, scope, 1, complete, tmp_path)
    complete.assert_called_once()
