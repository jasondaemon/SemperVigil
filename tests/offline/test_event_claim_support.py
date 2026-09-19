import copy
import json
from unittest.mock import Mock
from types import SimpleNamespace
import logging

import pytest

from sempervigil import event_claim_support as support, event_deconstruction as draft
from test_event_review import database, get_packet, resign
from test_event_scope import proposal
from test_event_deconstruction import response
from test_event_assessment import configured
from sempervigil import event_review_jobs as jobs, worker, admin
from sempervigil.services import ai_service
from sempervigil.llm import router

pytestmark = pytest.mark.offline


def setup(database):
    packet = get_packet(database)
    scope = proposal(packet)
    source = draft.validate_response(json.dumps(response(packet)).encode(), packet, scope, 1)
    source["generation_version"] = "a" * 64
    return packet, scope, source


def answer(value="supported"):
    return {"audits": [{"id": "c1", "quotation": value,
                       "context": "supported" if value == "supported" else "not_assessed"}]}


def phase_answer(value="supported"):
    return {"reason": "The supplied wording supports the comparison.", "verdict": value}


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
    jsonschema.validate(phase_answer(), request["response_format"]["json_schema"]["schema"])


@pytest.mark.parametrize("dimension", [d for d in support.DIMENSIONS if d != 'date'])
@pytest.mark.parametrize("verdict,decision", [("unsupported", "reject"), ("uncertain", "hold")])
def test_one_failed_dimension_prevents_model_supported(database, dimension, verdict, decision):
    packet, scope, source = setup(database)
    raw = answer()
    raw["audits"][0][dimension] = verdict
    if dimension == 'quotation':
        raw['audits'][0]['context'] = 'not_assessed'
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
    complete = Mock(return_value=phase_answer())
    complete.cache_identity = "b" * 64
    first, hit = support.assess(packet, scope, source, complete, tmp_path)
    assert hit is False
    second, hit = support.assess(packet, scope, source, complete, tmp_path)
    assert second == first and hit is True
    assert complete.call_count == 2
    packet["event"]["updated_at"] = "2026-09-20"
    resign(packet)
    assert support.assess(packet, scope, source, complete, tmp_path) == (first, True)
    complete.cache_identity = "c" * 64
    support.assess(packet, scope, source, complete, tmp_path)
    assert complete.call_count == 4


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
    complete = Mock(return_value=phase_answer())
    complete.cache_identity = "b" * 64
    support.assess(packet, scope, source, complete, tmp_path)
    path = next((tmp_path / "claim-support-cache").glob('*.json'))
    cached = json.loads(path.read_text())
    cached["public_eligible"] = True
    path.write_text(json.dumps(cached))
    with pytest.raises(ValueError, match="stale_or_modified"):
        support.assess(packet, scope, source, complete, tmp_path)
    assert complete.call_count == 2


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
    complete = Mock(return_value=phase_answer("unsupported"))
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
    complete = Mock(return_value=phase_answer())
    complete.cache_identity = "b" * 64
    with pytest.raises(ValueError, match="symlink"):
        support.assess(packet, scope, source, complete, tmp_path)
    complete.assert_not_called()


def test_audit_reject_takes_precedence_over_uncertainty(database):
    packet, scope, source = setup(database)
    raw = answer("unsupported")
    result = support.validate_response(json.dumps(raw).encode(), packet, scope, source)
    assert result["suggestions"][0]["decision"] == "reject"


def test_quote_phase_cannot_borrow_support_from_article_or_metadata(database):
    packet, scope, source = setup(database)
    claim_id = source['claims'][0]['id']
    quote = support.phase_request(packet, scope, source, claim_id, 'quotation')
    context = support.phase_request(packet, scope, source, claim_id, 'context')
    data = json.loads(quote['input'])
    assert set(data) == {'phase','citation','statement','date_role','date_value'}
    assert data['date_value'] is None
    assert json.loads(context['input'])['source']['text'] == packet['documents'][0]['text']
    assert quote['request_version'] != context['request_version']


def test_serial_single_claim_calls_and_partial_cache_resume(database, tmp_path):
    packet, scope, source = setup(database)
    candidate = response(packet)
    candidate['claims'].append({**candidate['claims'][0], 'statement': 'Acme reported an incident.'})
    source = draft.validate_response(json.dumps(candidate).encode(), packet, scope, 1)
    source['generation_version'] = 'a' * 64
    complete = Mock(side_effect=[phase_answer(), RuntimeError('interrupted')])
    complete.cache_identity = 'b' * 64
    with pytest.raises(RuntimeError):
        support.assess(packet, scope, source, complete, tmp_path)
    for call in complete.call_args_list:
        assert 'statement' in json.loads(call.args[0]['input'])
    complete.side_effect = None
    complete.return_value = phase_answer()
    result, hit = support.assess(packet, scope, source, complete, tmp_path)
    assert not hit and complete.call_count == 5 and len(result['suggestions']) == 2
    assert support.assess(packet, scope, source, complete, tmp_path)[1]
    assert complete.call_count == 5


def test_queued_audit_uses_existing_worker_viewer_and_cache(database, monkeypatch, tmp_path):
    packet, scope, source = setup(database)
    root = tmp_path / 'private'
    extract = Mock(return_value=response(packet))
    extract.cache_identity = 'a' * 64
    draft.save(packet, scope, 1, extract, root)
    key = next((root / 'deconstruction-cache').glob('*.json')).stem
    profile = configured(monkeypatch)
    profile['params']['max_input_chars'] = 15000
    for flag in ('SV_EVENT_REVIEW_ENABLED', 'SV_EVENT_REVIEW_SCOPE_ENABLED', 'SV_EVENT_CLAIM_SUPPORT_ENABLED'):
        monkeypatch.setenv(flag, '1')
    monkeypatch.setenv('SV_EVENT_CLAIM_SUPPORT_PROFILE_ID', 'support-profile')
    monkeypatch.setenv('SV_EVENT_REVIEW_DIR', str(root))
    monkeypatch.setenv('SV_LOG_DIR', str(tmp_path))
    monkeypatch.setenv('SV_DB_URL', 'unused')
    monkeypatch.setattr(ai_service, 'get_prompt', lambda *a: {
        'system_template': support.SYSTEM_PROMPT, 'user_template': '{{input}}'})
    model = Mock(return_value={'parsed': phase_answer(), 'schema_valid': True})
    monkeypatch.setattr(worker, 'run_profile', model)
    monkeypatch.setattr(jobs, 'snapshot', lambda *a, **kw: packet)
    monkeypatch.setattr(worker, '_log_job_claimed', lambda *a: None)
    monkeypatch.setattr(worker, 'is_job_canceled', lambda *a: False)
    def forbidden(*a, **kw): pytest.fail('audit must not publish')
    for name in ('mark_build_dirty', 'update_event_report', '_handle_event_report_llm'):
        monkeypatch.setattr(worker, name, forbidden)
    payload = jobs.payload_for('event', ['Acme'], scope=scope, article_id=1, audit_source=key)
    job = SimpleNamespace(id='support-job', job_type=jobs.JOB_TYPE, payload=payload)
    result = worker.run_claimed_job(None, None, job, logging.getLogger('test'))
    assert model.call_args.args[1] == 'support-profile'
    assert model.call_args.kwargs['context']['event_claim_support_phase'] == 'context'
    job.status, job.result = 'succeeded', result
    assert b'Private claim support audit' in jobs.read_artifact(job)
    assert not result['public_eligible']
    again = worker.run_claimed_job(None, None, job, logging.getLogger('test'))
    assert again['model_cache_hit'] and again['artifact'] == result['artifact']
    assert model.call_count == 2
    assert admin.EventPrivateReviewRequest(aliases=['Acme'], audit_source=key).audit_source == key


def test_disabled_audit_admission_before_db(database, monkeypatch):
    packet, scope, source = setup(database)
    monkeypatch.setenv('SV_EVENT_REVIEW_ENABLED', '1')
    monkeypatch.setenv('SV_EVENT_REVIEW_SCOPE_ENABLED', '1')
    monkeypatch.delenv('SV_EVENT_CLAIM_SUPPORT_ENABLED', raising=False)
    conn = Mock()
    with pytest.raises(PermissionError, match='support_disabled'):
        jobs.submit(conn, event_id='event', aliases=['Acme'], scope=scope, article_id=1, audit_source='a'*64)
    conn.assert_not_called()


@pytest.mark.parametrize('key', ['../x', '', 'a'*65, 12])
def test_audit_rejects_non_digest_source(database, key):
    packet, scope, source = setup(database)
    with pytest.raises(ValueError):
        jobs.payload_for('event', ['Acme'], scope=scope, article_id=1, audit_source=key)


def test_audit_schema_local_private_only(monkeypatch):
    call = Mock(return_value={'choices': [{'message': {'content': '{"audits":[]}'}}]})
    monkeypatch.setattr(router, '_http_request', call)
    context = {'stage': 'event_review_private', 'event_claim_support_phase': 'quotation'}
    router._call_provider('openai_compatible', 'http://localhost', None, 'ollama/test', [], {}, {}, context)
    assert call.call_args.args[3]['response_format'] == support.response_format()
    for change in ({'stage': 'other'}, {'event_claim_support_phase': 'bad'}, {'event_assessment_ids': ['p1']}):
        with pytest.raises(ValueError, match='unsupported_private_support'):
            router._call_provider('openai_compatible', 'http://localhost', None, 'ollama/test', [], {}, {}, {**context, **change})


@pytest.mark.parametrize('verdict', ['unsupported', 'uncertain'])
def test_quote_failure_never_calls_context_or_counts_as_supported(database, tmp_path, verdict):
    packet, scope, source = setup(database)
    complete = Mock(return_value=phase_answer(verdict))
    complete.cache_identity = 'b' * 64
    result, _ = support.assess(packet, scope, source, complete, tmp_path)
    complete.assert_called_once()
    assert result['suggestions'][0]['dimensions']['context'] == 'not_assessed'
    assert result['suggestions'][0]['decision'] != 'model_supported'


@pytest.mark.parametrize('bad', [None, {}, {'reason':'', 'verdict':'supported'},
    {'reason':'x'*241,'verdict':'supported'}, {'reason':'ok','verdict':'confirmed'},
    {'reason':'ok','verdict':'supported','approved':True}])
def test_phase_validation_fails_closed(bad):
    with pytest.raises(ValueError):
        support.validate_phase(json.dumps(bad).encode())


def test_render_explains_cached_reasons_and_coverage_without_inference(database, tmp_path):
    packet, scope, source = setup(database)
    complete = Mock(return_value={'verdict': 'unsupported', 'reason': '<script>not evidence</script>'})
    complete.cache_identity = 'b' * 64
    result, _ = support.assess(packet, scope, source, complete, tmp_path)
    page = support.render(result, packet, scope, source, cache_root=tmp_path)
    assert 'quotation reason: &lt;script&gt;not evidence&lt;/script&gt;' in page
    assert '<script>' not in page
    assert 'Draft coverage for this source' in page
    assert 'Original source context: available' in page
    assert 'not evidence supplied to the quotation check' in page
    assert 'context reason:' not in page
    assert 'What happened: 1 extracted; 0 model-supported' in page
    assert 'Response and recovery: 0 extracted' in page
    complete.assert_called_once()


@pytest.mark.parametrize('change', ['missing', 'verdict', 'generation', 'request'])
def test_render_rejects_missing_or_mismatched_phase_receipts(database, tmp_path, change):
    packet, scope, source = setup(database)
    complete = Mock(return_value=phase_answer('unsupported'))
    complete.cache_identity = 'b' * 64
    result, _ = support.assess(packet, scope, source, complete, tmp_path)
    path = next((tmp_path / 'claim-support-cache').glob('*.json'))
    cached = json.loads(path.read_text())
    if change == 'missing':
        path.unlink()
    else:
        if change == 'verdict': cached['result']['verdict'] = 'supported'
        if change == 'generation': cached['generation_version'] = 'c' * 64
        if change == 'request': cached['request_version'] = 'd' * 64
        path.write_text(json.dumps(cached))
    with pytest.raises(ValueError, match='support_phase_receipt_'):
        support.render(result, packet, scope, source, cache_root=tmp_path)
    complete.assert_called_once()
