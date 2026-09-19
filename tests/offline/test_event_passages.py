import pytest

from sempervigil.event_passages import citation_context

pytestmark = pytest.mark.offline


def context(text, quote):
    start = text.index(quote)
    return citation_context(text, start, start + len(quote), quote)


def test_previous_sentence_retained_without_resolving_pronoun():
    text = 'Earlier reporting. Acme notified affected customers. They should rotate keys. Recovery continues. Unrelated tail.'
    window = context(text, 'They should rotate keys.')
    assert window['text'] == 'Acme notified affected customers. They should rotate keys. Recovery continues. '
    assert window['text'] == text[window['start']:window['end']]
    assert not window['public_eligible']
    assert 'verdict' not in window


def test_roundup_paragraphs_are_not_borrowed():
    text = 'Another company recovered.\n\nThey should rotate keys.\n\nSeparate attack occurred.'
    assert context(text, 'They should rotate keys.')['text'] == 'They should rotate keys.'


def test_unicode_offsets_and_whitespace_are_preserved():
    text = 'Zo\u00eb notified users.\n\u201cThey should rotate keys.\u201d\nRecovery continues.'
    window = context(text, '\u201cThey should rotate keys.\u201d')
    assert window['text'] == text
    assert window['citation_start'] == text.index('\u201c')


def test_large_context_is_omitted_not_clipped():
    text = '\u00e9' * 1300 + '. They should rotate keys.'
    window = context(text, 'They should rotate keys.')
    assert window['status'] == 'context_over_budget'
    assert window['text'] == 'They should rotate keys.'


def test_cross_paragraph_citation_is_not_expanded():
    text = 'Before. First part.\n\nSecond part. After.'
    quote = 'First part.\n\nSecond part.'
    window = context(text, quote)
    assert window['status'] == 'cross_paragraph_quote'
    assert window['text'] == quote


def test_repeated_quote_is_bound_by_offsets():
    text = 'One. Repeat.\n\nTwo. Repeat.'
    start = text.rindex('Repeat.')
    assert citation_context(text, start, start + 7, 'Repeat.')['text'] == 'Two. Repeat.'


@pytest.mark.parametrize('args', [('text', True, 4, 'text'), ('text', -1, 4, 'text'),
                                 ('text', 0, 9, 'text'), ('text', 0, 4, 'fake'),
                                 ('text', 0, 0, ''), (None, 0, 4, 'text')])
def test_invalid_citations_fail_closed(args):
    with pytest.raises(ValueError, match='invalid_passage_citation'):
        citation_context(*args)


def test_source_change_outside_window_changes_binding():
    a = context('Quote.\n\nOther.', 'Quote.')
    b = context('Quote.\n\nChanged.', 'Quote.')
    assert a['text'] == b['text']
    assert a['source_version'] != b['source_version']
