import copy
import sqlite3

import pytest

from sempervigil.article_enrichment import validated_output
from sempervigil.storage import record_article_enrichment_error

pytestmark = pytest.mark.offline


def context():
    return {**{key: [] for key in ('facts', 'numbers', 'iocs', 'cves', 'timeline', 'uncertainties')},
            'entities': {key: [] for key in ('orgs', 'people', 'products', 'vendors', 'threat_actors', 'countries')}}


def test_valid_payload_preserved_without_coercion_or_added_public_fields():
    data = {'summary': 'The vendor reports a limited exposure.', 'bullets': ['Recovery remains unconfirmed.'],
            'tags': ['incident'], 'cves': [], 'entities': ['Acme'], 'nist_family': 'IR'}
    before = copy.deepcopy(data)
    assert validated_output({'schema_valid': True, 'parsed': data}, 'summary') == before
    assert data == before
    assert validated_output({'schema_valid': True, 'parsed': context()}, 'context') == context()


@pytest.mark.parametrize('key', ['facts', 'numbers', 'iocs', 'cves', 'timeline', 'uncertainties'])
@pytest.mark.parametrize('value', [[42], [None], [''], [True], {}, None])
def test_context_rejects_invalid_list_fields(key, value):
    data = context(); data[key] = value
    with pytest.raises(ValueError, match='article_context_list_invalid'):
        validated_output({'schema_valid': True, 'parsed': data}, 'context')


@pytest.mark.parametrize('value', [None, [], {}, {'orgs': ['Acme']}, {**context()['entities'], 'people': [7]}])
def test_context_entities_required(value):
    data = context(); data['entities'] = value
    with pytest.raises(ValueError, match='article_context_entities_invalid'):
        validated_output({'schema_valid': True, 'parsed': data}, 'context')


@pytest.mark.parametrize('value', [None, '', '   ', 4, [], {}])
def test_summary_requires_real_text(value):
    with pytest.raises(ValueError, match='article_summary_text_required'):
        validated_output({'schema_valid': True, 'parsed': {'summary': value}}, 'summary')


@pytest.mark.parametrize('key', ['bullets', 'entities', 'cves', 'tags'])
def test_summary_rejects_invalid_optional_lists(key):
    with pytest.raises(ValueError, match='article_summary_list_invalid'):
        validated_output({'schema_valid': True, 'parsed': {'summary': 'Text', key: [7]}}, 'summary')


@pytest.mark.parametrize('kind', ['summary', 'context'])
def test_error_updates_preserve_stored_payload_model_and_generation_time(kind):
    raw = sqlite3.connect(':memory:')
    raw.execute('CREATE TABLE articles (id INTEGER, summary_error TEXT, context_error TEXT, summary_llm TEXT, context_llm TEXT, summary_model TEXT, context_model TEXT, summary_generated_at TEXT, context_generated_at TEXT, updated_at TEXT)')
    raw.execute("INSERT INTO articles VALUES (7,NULL,NULL,'old summary','old context','sm','cm','st','ct','ut')")
    class Connection:
        def execute(self, sql, args): return raw.execute(sql.replace('%s', '?'), args)
        def commit(self): raw.commit()
    before = raw.execute('SELECT * FROM articles').fetchone()
    record_article_enrichment_error(Connection(), 7, kind=kind, error='schema_failed')
    after = raw.execute('SELECT * FROM articles').fetchone()
    expected = list(before); expected[1 if kind == 'summary' else 2] = 'schema_failed'
    assert after == tuple(expected)
    with pytest.raises(ValueError):
        record_article_enrichment_error(Connection(), 7, kind='invalid', error='x')
    assert raw.execute('SELECT * FROM articles').fetchone() == after
    raw.close()
