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
    return {'facts': [{'evidence_quote': 'Acme said unprotected tokens may have been exposed.',
                      'statement': 'Acme said unprotected tokens may have been exposed.',
                      'kind': 'allegation', 'attribution_quote': 'Acme said',
                      'uncertainty_quote': 'may have been exposed', 'date_quote': None,
                      'date_role': 'none'}], 'uncertainties': ['Recovery remains unconfirmed.']}


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
    assert ARTICLE['content_text'][fact['start']:fact['end']] == fact['evidence_quote']
    assert fact['date_quote'] is None


def test_generation_order_and_schema_use_no_sentence_enum_regex_or_conditionals():
    schema = evidence.context_schema()
    assert next(iter(schema['properties']['facts']['items']['properties'])) == 'evidence_quote'
    for forbidden in ('pattern', 'anyOf', 'oneOf', '$ref'):
        assert forbidden not in json.dumps(schema)
    request = evidence.context_request(ARTICLE, GEN)
    assert json.loads(request['input'])['source']['text'] == ARTICLE['content_text']


@pytest.mark.parametrize('change', [{'content_text': ''}, {'content_text': None},
                                  {'id': True}, {'id': -1}, {'title': ''}])
def test_missing_source_never_falls_back_to_headline_or_summary(change):
    with pytest.raises(ValueError, match='full_source_required'):
        evidence.context_request({**ARTICLE, 'summary': 'Fallback', **change}, GEN)


def test_budget_includes_prompt_schema_and_full_source_without_truncation():
    with pytest.raises(ValueError, match='over_budget'):
        evidence.context_request({**ARTICLE, 'content_text': '\u00e9' * 5000}, GEN)


@pytest.mark.parametrize('quote', ['not in source', ' ', 'Acme'])
def test_absent_empty_or_ambiguous_quote_fails(quote):
    data = context(); data['facts'][0]['evidence_quote'] = quote
    with pytest.raises(ValueError, match='quote_not_unique'):
        evidence.validate_context(encode(data), ARTICLE, GEN)


@pytest.mark.parametrize('field', ['attribution_quote', 'uncertainty_quote', 'date_quote'])
def test_qualifiers_cannot_come_from_other_passages(field):
    data = context(); data['facts'][0][field] = 'Recovery remains unconfirmed.'
    with pytest.raises(ValueError, match='qualifier_not_in_quote'):
        evidence.validate_context(encode(data), ARTICLE, GEN)


def test_attribution_required_for_allegation():
    data = context(); data['facts'][0].update(attribution_quote=None, uncertainty_quote=None)
    with pytest.raises(ValueError, match='allegation_unattributed'):
        evidence.validate_context(encode(data), ARTICLE, GEN)


def test_relative_incident_date_is_not_normalized_from_publication_metadata():
    article = {**ARTICLE, 'content_text': 'Acme said recovery started yesterday.', 'published_at': '2026-09-19'}
    data = context(); data['facts'][0].update(evidence_quote=article['content_text'],
        statement=article['content_text'], kind='reported_fact', uncertainty_quote=None,
        date_quote='yesterday', date_role='incident')
    result = evidence.validate_context(encode(data), article, GEN)
    assert result['facts'][0]['date_quote'] == 'yesterday'
    assert '2026-09-18' not in json.dumps(result)


def test_date_role_requires_explicit_date_wording():
    data = context(); data['facts'][0]['date_role'] = 'incident'
    with pytest.raises(ValueError, match='date_role_mismatch'):
        evidence.validate_context(encode(data), ARTICLE, GEN)


def test_empty_context_abstains_before_summary_request():
    result = evidence.validate_context(encode({'facts': [], 'uncertainties': []}), ARTICLE, GEN)
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


@pytest.mark.parametrize('field,value', [('start', 1), ('id', 'fake'), ('statement', 'Changed'),
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
