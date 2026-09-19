import copy
import json
import logging
from types import SimpleNamespace
from unittest.mock import Mock

import jsonschema
import pytest

from sempervigil import event_assessment as assessment, event_assessment_cache as cache
from sempervigil import event_review_jobs as jobs, worker, admin
from sempervigil.llm import router
from sempervigil.services import ai_service
from test_event_review import database, get_packet, resign
from test_event_scope import proposal
from test_source_assessment import packet_with_sources
from test_event_assessment import configured

pytestmark = pytest.mark.offline


def test_complete_pair_exact_quotes_and_unchanged_default(database):
    packet = packet_with_sources(database)
    scope = proposal(packet)
    normal = assessment.request_for(packet, scope=scope, article_id=2)
    pair = assessment.request_for(packet, scope=scope, article_id=2, paired=True)
    data = json.loads(pair['input'])
    assert data['sources'] == [{'article_id': d['article_id'], 'text': d['text']} for d in packet['documents']]
    assert data['candidate_source_id'] == 2 and data['scope_source_id'] == 1
    assert data['items'][0]['quote'] in packet['documents'][1]['text']
    assert pair['mapping'] == normal['mapping'] and pair['omitted_passages'] == normal['omitted_passages']
    assert pair['workflow'] == assessment.PAIR_WORKFLOW
    assert pair['request_version'] != normal['request_version']
    assert assessment.request_for(packet, scope=scope, article_id=2, paired=False) == normal


def test_over_budget_full_pair_refused_before_inference(database):
    packet = packet_with_sources(database)
    scope = proposal(packet)
    packet['documents'][1]['text'] += ' Additional context.' * 900
    resign(packet)
    complete = Mock(side_effect=AssertionError('must not infer'))
    with pytest.raises(ValueError, match='assessment_pair_over_budget'):
        assessment.assess(packet, complete, scope=scope, article_id=2, paired=True)
    complete.assert_not_called()


@pytest.mark.parametrize('value', [True, 1, 'yes'])
def test_pair_requires_explicit_valid_source_and_flag(database, value):
    packet = get_packet(database)
    with pytest.raises(ValueError):
        assessment.request_for(packet, scope=proposal(packet), paired=value)


def test_pair_cache_identity_and_timestamp_rebinding(database, tmp_path):
    packet = packet_with_sources(database)
    scope = proposal(packet)
    complete = Mock(return_value={'decisions': [{'id':'p1','decision':'hold','reason':'insufficient_context'}]})
    complete.cache_identity = 'd' * 64
    plain, _ = cache.reuse(packet, complete, tmp_path, scope=scope, article_id=2)
    pair, hit = cache.reuse(packet, complete, tmp_path, scope=scope, article_id=2, paired=True)
    assert not hit and pair['request_version'] != plain['request_version']
    packet['event']['updated_at'] = '2026-09-20'
    resign(packet)
    rebound, hit = cache.reuse(packet, complete, tmp_path, scope=scope, article_id=2, paired=True)
    assert hit and rebound['paired'] is True and complete.call_count == 2
    assert assessment.validate_assessment(rebound, packet) == rebound


@pytest.mark.parametrize('ids', [[], ['p2'], ['p1','p1'], ['p1','p3'], ['p1']*5, 'p1', [True]])
def test_format_rejects_unbounded_or_forged_ids(ids):
    with pytest.raises(ValueError): assessment.response_format(ids)


def test_generation_schema_rejects_observed_key_typo_and_extra_fields():
    schema = assessment.response_format(['p1'])['json_schema']['schema']
    good = {'decisions':[{'id':'p1','decision':'include','reason':'same_incident'}]}
    jsonschema.validate(good, schema)
    for change in ('typo', 'extra', 'missing', 'enum', 'count'):
        bad = copy.deepcopy(good)
        row = bad['decisions'][0]
        if change == 'typo': row['id:'] = row.pop('id')
        if change == 'extra': row['approved'] = True
        if change == 'missing': del row['reason']
        if change == 'enum': row['reason'] = 'unvalidated prose'
        if change == 'count': bad['decisions'] *= 2
        with pytest.raises(jsonschema.ValidationError): jsonschema.validate(bad, schema)


def test_schema_transport_is_scoped_and_ordinary_calls_unchanged(monkeypatch):
    transport = Mock(return_value={'choices':[{'message':{'content':'{}'}}]})
    monkeypatch.setattr(router, '_http_request', transport)
    monkeypatch.setattr(router, '_use_openai_background', lambda *a: False)
    args = ('openai_compatible','http://example.test/v1',None,'ollama/test',[],{}, {})
    router._call_provider(*args, context={'stage':'event_review_private','event_assessment_ids':['p1'],
                                        'json_response_format_enabled':True})
    assert transport.call_args.args[3]['response_format'] == assessment.response_format(['p1'])
    router._call_provider(*args, context={'stage':'article_summary'})
    assert 'response_format' not in transport.call_args.args[3]
    with pytest.raises(ValueError, match='unsupported_private_assessment_format'):
        router._call_provider(*args, context={'stage':'article_summary','event_assessment_ids':['p1']})
    assert transport.call_count == 2


def test_pair_admission_default_disabled_before_connection(database, monkeypatch):
    monkeypatch.setenv('SV_EVENT_REVIEW_ENABLED','1')
    monkeypatch.setenv('SV_EVENT_REVIEW_SCOPE_ENABLED','1')
    monkeypatch.delenv('SV_EVENT_REVIEW_PAIR_ENABLED',raising=False)
    connect = Mock(side_effect=AssertionError('must not connect'))
    with pytest.raises(PermissionError, match='private_pair_disabled'):
        jobs.submit(connect,event_id='event',aliases=['Acme'],scope=proposal(get_packet(database)),article_id=1,paired=True)
    connect.assert_not_called()


def test_pair_guard_one_call_schema_context_and_distinct_generation(monkeypatch, database):
    profile = configured(monkeypatch)
    profile['params']['max_input_chars'] = 15000
    monkeypatch.setenv('SV_EVENT_REVIEW_SCOPE_ENABLED','1')
    monkeypatch.setenv('SV_EVENT_REVIEW_PAIR_ENABLED','1')
    monkeypatch.setenv('SV_EVENT_REVIEW_PAIR_PROFILE_ID','pair-profile')
    monkeypatch.setattr(ai_service,'get_prompt',lambda *a:{
        'system_template':assessment.SCOPED_SYSTEM_PROMPT,'user_template':'{{input}}'})
    job = SimpleNamespace(id='pair-job',payload={'scope':{},'article_id':1,'paired':True})
    complete = worker._private_review_completion(None,job,logging.getLogger())
    run = Mock(return_value={'parsed':{'decisions':[{'id':'p1','decision':'hold','reason':'insufficient_context'}]},'schema_valid':True})
    monkeypatch.setattr(worker,'run_profile',run)
    packet = get_packet(database)
    result = assessment.assess(packet,complete,scope=proposal(packet),article_id=1,paired=True)
    assert result['paired'] and not result['public_eligible']
    assert run.call_args.kwargs['context']['event_assessment_ids'] == ['p1']
    assert run.call_args.args[1] == 'pair-profile'
    run.assert_called_once()
    profile['params']['max_tokens'] = 1536
    with pytest.raises(ValueError, match='private_review_profile_budget'):
        worker._private_review_completion(None,job,logging.getLogger())


def test_api_pair_flag_is_strict():
    for value in ('true',1):
        with pytest.raises(ValueError):admin.EventPrivateReviewRequest(aliases=['Acme'],paired=value)


def test_paired_api_to_private_worker_keeps_full_evidence(database, monkeypatch, tmp_path):
    packet = packet_with_sources(database)
    scope = proposal(packet)
    for key in ('SV_EVENT_REVIEW_ENABLED','SV_EVENT_REVIEW_SCOPE_ENABLED','SV_EVENT_REVIEW_PAIR_ENABLED'):
        monkeypatch.setenv(key,'1')
    monkeypatch.setenv('SV_LOG_DIR',str(tmp_path))
    monkeypatch.setenv('SV_EVENT_REVIEW_DIR',str(tmp_path/'private'))
    monkeypatch.setenv('SV_DB_URL','unused-test')
    monkeypatch.setenv('SV_ADMIN_TOKEN','test-only')
    submit = Mock(return_value='job')
    monkeypatch.setattr(jobs,'submit',submit)
    admin.api_event_private_review('event',admin.EventPrivateReviewRequest(
        aliases=['Acme'],scope=scope,article_id=2,paired=True))
    assert submit.call_args.kwargs['paired'] is True
    monkeypatch.setattr(jobs,'snapshot',lambda *a,**k:packet)
    complete = Mock(return_value={'decisions':[{'id':'p1','decision':'hold','reason':'insufficient_context'}]})
    complete.cache_identity = 'e'*64
    payload = jobs.payload_for('event',['Acme'],scope=scope,article_id=2,paired=True)
    result = jobs.run(payload,complete=complete)
    assert result['assessment_summary']['workflow'] == assessment.PAIR_WORKFLOW
    assert result['documents'] == 2 and result['passages'] == 2 and not result['public_eligible']
    assert jobs.run(payload,complete=complete)['model_cache_hit']
    complete.assert_called_once()
