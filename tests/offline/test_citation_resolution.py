import pytest
from sempervigil.event_report_contract import resolve_citation

pytestmark = pytest.mark.offline


def test_unique_original_span_and_mapping():
    source = {"id": "S1", "text": 'They contacted employees to access company systems," Astrana said.'}
    generated = 'They contacted employees to access company systems.'
    result = resolve_citation(source, generated)
    assert result['quote'] == 'They contacted employees to access company systems,"'
    assert source['text'][result['start']:result['end']] == result['quote']
    assert result['normalization']['generated_quote'] == generated
    assert len(result['passage_anchor']) == 64
    assert result == resolve_citation(source, generated)


def test_whitespace_and_exact():
    source = {"id": "S1", "text": 'Alpha  Beta\nGamma.'}
    assert resolve_citation(source, 'Alpha Beta Gamma.')['quote'] == source['text']
    assert 'normalization' not in resolve_citation(source, source['text'])


@pytest.mark.parametrize('original,generated', [
    ('Access was not obtained.', 'Access was obtained.'),
    ('Affected 1.5 million people.', 'Affected 1,5 million people.'),
    ('Affected -5 accounts.', 'Affected 5 accounts.'),
    ('Astrana systems were accessed.', 'Microsoft systems were accessed.'),
    ("They don't authorize access.", 'They dont authorize access.'),
    ('ACME-1 systems were accessed.', 'ACME 1 systems were accessed.'),
    ('One passage here. One passage here!', 'One passage here?'),
])
def test_material_or_ambiguous_changes_hold(original, generated):
    with pytest.raises(ValueError, match='quote_not_in_source'):
        resolve_citation({'id': 'S1', 'text': original}, generated)
