import copy
import json
from unittest.mock import Mock

import pytest

from sempervigil import event_claim_support as support, event_deconstruction as draft
from test_event_review import database, get_packet, resign
from test_event_scope import proposal
from test_event_deconstruction import response

pytestmark = pytest.mark.offline


def setup(database):
    packet = get_packet(database)
    scope = proposal(packet)
    source = draft.validate_response(json.dumps(response(packet)).encode(), packet, scope, 1)
    source["generation_version"] = "a" * 64
    return packet, scope, source


def answer(value="supported"):
    return {"audits": [{"id": "c1", **{d: value for d in support.DIMENSIONS}}]}


def test_full_source_and_exact_claim_bound_request(database):
    packet, scope, source = setup(database)
    request = support.request_for(packet, scope, source)
    data = json.loads(request["input"])
    assert data["source"]["text"] == packet["documents"][0]["text"]
    assert data["claims"][0]["quote"] == source["claims"][0]["quote"]
    assert request["mapping"] == {"c1": source["claims"][0]["id"]}
    assert len((request["system"] + request["input"] + json.dumps(request["response_format"])).encode()) <= 15000
    serialized = json.dumps(request["response_format"])
    assert '"pattern"' not in serialized and '"anyOf"' not in serialized
    import jsonschema
    jsonschema.validate(answer(), request["response_format"]["json_schema"]["schema"])


@pytest.mark.parametrize("dimension", support.DIMENSIONS)
@pytest.mark.parametrize("verdict,decision", [("unsupported", "reject"), ("uncertain", "hold")])
def test_one_failed_dimension_prevents_model_supported(database, dimension, verdict, decision):
    packet, scope, source = setup(database)
    raw = answer()
    raw["audits"][0][dimension] = verdict
    result = support.validate_response(json.dumps(raw).encode(), packet, scope, source)
    assert result["suggestions"][0]["decision"] == decision
    assert result["public_eligible"] is False


def test_all_model_passes_never_authorize_publication(database):
    packet, scope, source = setup(database)
    result = support.validate_response(json.dumps(answer()).encode(), packet, scope, source)
    assert result["suggestions"][0]["decision"] == "model_supported"
    assert result["status"] == "model_suggestions_only"
    assert result["public_eligible"] is False
    assert "approval" not in result and "receipt" not in result


@pytest.mark.parametrize("bad", [
    {"audits": []}, {"audits": answer()["audits"] * 2},
    {"audits": [{**answer()["audits"][0], "id": "c9"}]},
    {"audits": [{**answer()["audits"][0], "entailment": None}]},
    {"audits": [{**answer()["audits"][0], "entailment": "confirmed"}]},
    {"audits": [{**answer()["audits"][0], "repair": "new statement"}]},
    {"audits": [{"id": "c1"}]}, {"audits": {}, "approved": True},
])
def test_incomplete_invented_or_repaired_results_rejected(database, bad):
    packet, scope, source = setup(database)
    with pytest.raises(ValueError):
        support.validate_response(json.dumps(bad).encode(), packet, scope, source)


def test_duplicate_json_keys_rejected(database):
    packet, scope, source = setup(database)
    with pytest.raises(ValueError):
        support.validate_response(b'{"audits":[],"audits":[]}', packet, scope, source)


def test_reuse_no_call_and_separate_checker_identity(database, tmp_path):
    packet, scope, source = setup(database)
    complete = Mock(return_value=answer())
    complete.cache_identity = "b" * 64
    first, hit = support.assess(packet, scope, source, complete, tmp_path)
    assert hit is False
    second, hit = support.assess(packet, scope, source, complete, tmp_path)
    assert second == first and hit is True
    assert complete.call_count == 1
    packet["event"]["updated_at"] = "2026-09-20"
    resign(packet)
    assert support.assess(packet, scope, source, complete, tmp_path) == (first, True)
    complete.cache_identity = "c" * 64
    support.assess(packet, scope, source, complete, tmp_path)
    assert complete.call_count == 2


def test_empty_extraction_abstains_without_model(database, tmp_path):
    packet, scope, source = setup(database)
    source = draft.validate_response(b'{"claims":[]}', packet, scope, 1)
    source["generation_version"] = "a" * 64
    complete = Mock()
    complete.cache_identity = "b" * 64
    result, _ = support.assess(packet, scope, source, complete, tmp_path)
    assert result["suggestions"] == [] and result["public_eligible"] is False
    complete.assert_not_called()


def test_changed_claim_or_source_rejected_before_inference(database, tmp_path):
    packet, scope, source = setup(database)
    complete = Mock()
    complete.cache_identity = "b" * 64
    changed = copy.deepcopy(source)
    changed["claims"][0]["statement"] = "Everyone recovered."
    with pytest.raises(ValueError):
        support.assess(packet, scope, changed, complete, tmp_path)
    packet["documents"][0]["text"] += " Correction."
    resign(packet)
    with pytest.raises(ValueError):
        support.assess(packet, scope, source, complete, tmp_path)
    complete.assert_not_called()


def test_changed_canonical_claim_has_new_audit_request(database):
    packet, scope, source = setup(database)
    old = support.request_for(packet, scope, source)
    candidate = response(packet)
    candidate["claims"][0]["statement"] = "Acme has fully recovered."
    changed = draft.validate_response(json.dumps(candidate).encode(), packet, scope, 1)
    changed["generation_version"] = "a" * 64
    assert support.request_for(packet, scope, changed)["request_version"] != old["request_version"]


def test_cache_tamper_fails_without_retry(database, tmp_path):
    packet, scope, source = setup(database)
    complete = Mock(return_value=answer())
    complete.cache_identity = "b" * 64
    support.assess(packet, scope, source, complete, tmp_path)
    path = next((tmp_path / "claim-support-cache").glob('*.json'))
    cached = json.loads(path.read_text())
    cached["public_eligible"] = True
    path.write_text(json.dumps(cached))
    with pytest.raises(ValueError, match="stale_or_modified"):
        support.assess(packet, scope, source, complete, tmp_path)
    assert complete.call_count == 1


def test_no_truncation_when_support_input_exceeds_budget(database, monkeypatch, tmp_path):
    packet, scope, source = setup(database)
    complete = Mock()
    complete.cache_identity = "b" * 64
    monkeypatch.setattr(support, "SYSTEM_PROMPT", "x" * 15001)
    with pytest.raises(ValueError, match="support_source_over_budget"):
        support.assess(packet, scope, source, complete, tmp_path)
    complete.assert_not_called()


def test_private_render_escapes_model_and_preserves_original(database, tmp_path):
    packet, scope, source = setup(database)
    candidate = response(packet)
    candidate["claims"][0]["statement"] = '<script>bad()</script>'
    source = draft.validate_response(json.dumps(candidate).encode(), packet, scope, 1)
    source["generation_version"] = "a" * 64
    before = copy.deepcopy(source)
    complete = Mock(return_value=answer("unsupported"))
    complete.cache_identity = "b" * 64
    result, _ = support.assess(packet, scope, source, complete, tmp_path)
    html = support.render(result, packet, scope, source)
    assert '<script>' not in html and '&lt;script&gt;' in html
    assert "reject" in html and "nothing has been repaired or published" in html
    assert source == before


def test_symlink_cache_refused_before_inference(database, tmp_path):
    packet, scope, source = setup(database)
    target = tmp_path / 'other'
    target.mkdir()
    (tmp_path / 'claim-support-cache').symlink_to(target, target_is_directory=True)
    complete = Mock(return_value=answer())
    complete.cache_identity = "b" * 64
    with pytest.raises(ValueError, match="symlink"):
        support.assess(packet, scope, source, complete, tmp_path)
    complete.assert_not_called()


def test_audit_reject_takes_precedence_over_uncertainty(database):
    packet, scope, source = setup(database)
    raw = answer("uncertain")
    raw["audits"][0]["entailment"] = "unsupported"
    result = support.validate_response(json.dumps(raw).encode(), packet, scope, source)
    assert result["suggestions"][0]["decision"] == "reject"
