import copy
import json

import pytest

from sempervigil import article_evidence as evidence

pytestmark = pytest.mark.offline
GEN = 'a' * 64
ARTICLE = {'id': 7, 'title': 'Acme incident update', 'content_text':
           'Acme said unprotected tokens may have been exposed. '
           'Acme advised customers to rotate tokens. Recovery remains unconfirmed.'}


def context():
    return {'facts': [{'passage_ids': ['p001'],
                      'statement': 'Acme said unprotected tokens may have been exposed.',
                      'kind': 'allegation', 'date_text': None,
                      'date_role': 'none'}]}


def summary():
    return {'summary_sentences': [{'text': 'Acme said unprotected tokens may have been exposed.',
                                  'fact_ids': ['f1']}], 'bullets': []}


def encode(data): return json.dumps(data).encode()


def test_private_pipeline_retains_existing_feed_shapes_without_mutating_article():
    before = copy.deepcopy(ARTICLE)
    result = evidence.preview(ARTICLE, encode(context()), encode(summary()), GEN, GEN)
    assert ARTICLE == before
    assert not result['public_eligible']
    assert result['evidence']['status'] == result['summary']['status'] == 'unreviewed'
    assert result['feed_preview'] == {'summary': summary()['summary_sentences'][0]['text'],
                                      'summary_bullets': []}
    fact = result['evidence']['facts'][0]
    passage = fact['evidence_passages'][0]
    assert passage['id'] == 'p001'
    assert ARTICLE['content_text'][passage['start']:passage['end']] == passage['text']
    assert fact['date_text'] is None


def test_generation_order_and_schema_use_passage_ids_without_quote_copying():
    schema = evidence.context_schema()
    fields = schema['properties']['facts']['items']['properties']
    assert next(iter(fields)) == 'passage_ids'
    assert 'evidence_quote' not in fields
    request = evidence.context_request(ARTICLE, GEN)
    payload = json.loads(request['input'])
    assert 'text' not in payload['source']
    assert ''.join(p['text'] for p in payload['passages']) == ARTICLE['content_text']


@pytest.mark.parametrize('change', [{'content_text': ''}, {'content_text': None},
                                  {'id': True}, {'id': -1}, {'title': ''}])
def test_missing_source_never_falls_back_to_headline_or_summary(change):
    with pytest.raises(ValueError, match='full_source_required'):
        evidence.context_request({**ARTICLE, 'summary': 'Fallback', **change}, GEN)


def test_budget_includes_prompt_schema_and_full_source_without_truncation():
    with pytest.raises(ValueError, match='over_budget'):
        evidence.context_request({**ARTICLE, 'content_text': '\u00e9' * 5000}, GEN)


@pytest.mark.parametrize('passage_ids', [['p999'], [], ['p001', 'p001']])
def test_unknown_empty_or_duplicate_passage_ids_fail(passage_ids):
    data = context(); data['facts'][0]['passage_ids'] = passage_ids
    with pytest.raises(ValueError, match='unknown_passage|invalid_shape'):
        evidence.validate_context(encode(data), ARTICLE, GEN)


def test_duplicate_source_paragraphs_have_distinct_exact_passages_without_editing_source():
    article = {**ARTICLE, 'content_text': ARTICLE['content_text'] + '\n\n' + ARTICLE['content_text']}
    before = copy.deepcopy(article)
    data = context(); data['facts'][0]['passage_ids'] = ['p001', 'p002']
    result = evidence.validate_context(encode(data), article, GEN)
    fact = result['facts'][0]
    assert [p['id'] for p in fact['evidence_passages']] == ['p001', 'p002']
    for passage in fact['evidence_passages']:
        assert article['content_text'][passage['start']:passage['end']] == passage['text']
    assert article == before
    assert not result['public_eligible']
    assert evidence.validate_context_record(result, article) == result
    evidence.summary_request(article, result, GEN)


def test_selected_passages_are_canonicalized_in_source_order():
    article = {**ARTICLE, 'content_text': 'First fact.\n\nSecond fact.'}
    data = context(); data['facts'][0]['passage_ids'] = ['p002', 'p001']
    result = evidence.validate_context(encode(data), article, GEN)
    assert result['facts'][0]['passage_ids'] == ['p001', 'p002']
    assert [p['text'] for p in result['facts'][0]['evidence_passages']] == [
        'First fact.', 'Second fact.']


def test_summary_request_deduplicates_passage_text_across_facts():
    data = context(); data['facts'].append({**data['facts'][0],
        'statement': 'Customers were advised to rotate tokens.', 'kind': 'recommendation'})
    result = evidence.validate_context(encode(data), ARTICLE, GEN)
    request = evidence.summary_request(ARTICLE, result, GEN)
    payload = json.loads(request['input'])
    assert [p['id'] for p in payload['passages']] == ['p001']
    assert [f['id'] for f in payload['facts']] == ['f1', 'f2']
    assert all('evidence_passages' not in fact for fact in payload['facts'])


def test_selected_passage_does_not_prove_statement_semantics():
    article = {**ARTICLE, 'content_text': 'Acme said no.\n\nAcme said yes.'}
    data = context()
    data['facts'][0].update(passage_ids=['p001'], statement='Acme certainly said yes.')
    result = evidence.validate_context(encode(data), article, GEN)
    assert result['facts'][0]['evidence_passages'][0]['text'] == 'Acme said no.'
    assert result['status'] == 'unreviewed'
    assert not result['public_eligible']


def test_long_paragraphs_split_at_source_boundaries_with_stable_offsets():
    article = {**ARTICLE, 'content_text': ('First sentence. ' * 80).strip()}
    first = evidence.passages_for(article)
    second = evidence.passages_for({**article, 'updated_at': 'ignored'})
    assert first == second and len(first) > 1
    assert all(len(p['text']) <= evidence.MAX_PASSAGE_CHARS for p in first)
    assert [p['id'] for p in first] == [f'p{i:03d}' for i in range(1, len(first)+1)]
    assert all(article['content_text'][p['start']:p['end']] == p['text'] for p in first)


@pytest.mark.parametrize('change', ['remove', 'move', 'workflow'])
def test_modified_passage_record_or_old_contract_is_rejected(change):
    article = {**ARTICLE, 'content_text': ARTICLE['content_text']}
    result = evidence.validate_context(encode(context()), article, GEN)
    if change == 'remove': result['facts'][0]['evidence_passages'].pop()
    elif change == 'move': result['facts'][0]['evidence_passages'][0]['start'] += 1
    else: result['workflow'] = 'article-evidence-v3'
    with pytest.raises(ValueError, match='stale_or_modified'):
        evidence.summary_request(article, result, GEN)


def test_unsupported_date_metadata_is_removed_without_discarding_fact():
    article = {**ARTICLE, 'content_text': 'Incident occurred May 1.\n\nUpdate published May 2.'}
    data = context(); data['facts'][0].update(
        passage_ids=['p001'], date_text='May 2', date_role='incident')
    result = evidence.validate_context(encode(data), article, GEN)
    assert result['facts'][0]['date_text'] is None
    assert result['facts'][0]['date_role'] == 'none'

    data['facts'][0]['passage_ids'] = ['p001', 'p002']
    result = evidence.validate_context(encode(data), article, GEN)
    assert result['facts'][0]['date_text'] == 'May 2'


def test_relative_incident_date_is_not_normalized_from_publication_metadata():
    article = {**ARTICLE, 'content_text': 'Acme said recovery started yesterday.', 'published_at': '2026-09-19'}
    data = context(); data['facts'][0].update(statement=article['content_text'],
        kind='reported_fact', date_text='yesterday', date_role='incident')
    result = evidence.validate_context(encode(data), article, GEN)
    assert result['facts'][0]['date_text'] == 'yesterday'
    assert '2026-09-18' not in json.dumps(result)


def test_date_role_without_explicit_date_is_removed():
    data = context(); data['facts'][0]['date_role'] = 'incident'
    result = evidence.validate_context(encode(data), ARTICLE, GEN)
    assert result['facts'][0]['date_text'] is None
    assert result['facts'][0]['date_role'] == 'none'


def test_context_prompt_states_exact_root_and_field_contract():
    prompt = evidence.CONTEXT_PROMPT
    assert "exactly one JSON object" in prompt
    assert "exactly one key named facts" in prompt
    assert "Use statement, never fact or fact_text" in prompt
    assert '"date_text":null,"date_role":"none"' in prompt
    assert "at most 32 objects" in prompt
    assert evidence.context_schema()["properties"]["facts"]["maxItems"] == evidence.MAX_FACTS


def test_context_fact_count_remains_bounded():
    data = context()
    data["facts"] *= evidence.MAX_FACTS + 1
    with pytest.raises(ValueError, match="invalid_shape"):
        evidence.validate_context(encode(data), ARTICLE, GEN)


def test_summary_prompt_states_exact_root_and_count_contract():
    prompt = evidence.SUMMARY_PROMPT
    words = " ".join(prompt.split())
    assert "exactly one JSON object" in prompt
    assert "exactly summary_sentences and" in prompt
    assert "at most four objects" in words
    assert "at most seven objects" in words
    assert '"fact_ids":["f1"]' in prompt


def test_empty_context_abstains_before_summary_request():
    result = evidence.validate_context(encode({'facts': []}), ARTICLE, GEN)
    with pytest.raises(ValueError, match='no_facts'):
        evidence.summary_request(ARTICLE, result, GEN)


def test_unknown_fact_reference_and_blank_summary_rejected():
    result = evidence.validate_context(encode(context()), ARTICLE, GEN)
    for change in ({'fact_ids': ['f9']}, {'text': ' '}):
        data = summary(); data['summary_sentences'][0].update(change)
        with pytest.raises(ValueError, match='unknown_evidence'):
            evidence.validate_summary(encode(data), ARTICLE, result, GEN)
    with pytest.raises(ValueError, match='abstained'):
        evidence.validate_summary(encode({'summary_sentences': [], 'bullets': []}), ARTICLE, result, GEN)


@pytest.mark.parametrize('field,value', [('id', 'fake'), ('statement', 'Changed'),
                                       ('date_role', 'publication')])
def test_modified_records_fail_before_summary(field, value):
    result = evidence.validate_context(encode(context()), ARTICLE, GEN)
    result['facts'][0][field] = value
    with pytest.raises(ValueError): evidence.summary_request(ARTICLE, result, GEN)


def test_source_or_prompt_identity_changes_request_but_timestamp_does_not():
    request = evidence.context_request(ARTICLE, GEN)
    assert evidence.context_request({**ARTICLE, 'updated_at': 'later'}, GEN) == request
    assert evidence.context_request(ARTICLE, 'b' * 64) != request
    result = evidence.validate_context(encode(context()), ARTICLE, GEN)
    with pytest.raises(ValueError, match='stale_or_modified'):
        evidence.summary_request({**ARTICLE, 'content_text': ARTICLE['content_text'] + ' New detail.'}, result, GEN)


def test_duplicate_facts_and_extra_fields_rejected():
    data = context(); data['facts'] *= 2
    with pytest.raises(ValueError, match='duplicate_fact'):
        evidence.validate_context(encode(data), ARTICLE, GEN)
    data = context(); data['approved'] = True
    with pytest.raises(ValueError, match='invalid_shape'):
        evidence.validate_context(encode(data), ARTICLE, GEN)


def test_exact_references_do_not_prove_semantics_or_publication_authority():
    data = context(); data['facts'][0]['statement'] = 'All protected tokens were certainly stolen.'
    result = evidence.validate_context(encode(data), ARTICLE, GEN)
    assert result['status'] == 'unreviewed' and result['public_eligible'] is False
    assert result['facts'][0]['statement'] == data['facts'][0]['statement']
    # The false claim remains inspectable; no structural test pretends to approve it.
