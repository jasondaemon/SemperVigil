import pytest
from sempervigil.event_report_contract_v2 import resolve_citation
pytestmark = pytest.mark.offline

@pytest.mark.parametrize('body,quote',[
 ("Earlier reporting said the attack's purpose and impact remain unclear at this stage.", "The attack's purpose and impact remain unclear at this stage."),
 ("Autonomy was claimed, but the full extent of human direction remains unestablished.", "The full extent of human direction remains unestablished."),
 ("They say the impact remains unclear; further work continues.", "The impact remains unclear.")])
def test_sentence_extraction_preserves_original_span_and_records_mapping(body,quote):
 result=resolve_citation({'id':'S1','text':body},quote)
 assert result['quote']==body[result['start']:result['end']]
 assert result['quote'].startswith('the ')
 assert result['normalization']['policy']=='unique-initial-function-word-capitalization-v1'
 assert result['normalization']['generated_quote']==quote
 assert 'whole-context review required' in result['normalization']['meaning_validation']

@pytest.mark.parametrize('body,quote',[
 ('the impact remains unclear. the impact remains unclear.', 'The impact remains unclear.'),
 ('the impact remains unclear.', 'The Impact remains unclear.'),
 ('the impact remains unclear.', 'The impact is confirmed.'),
 ('acme reported a breach.', 'Acme reported a breach.'),
 ('us systems were affected.', 'US systems were affected.'),
 ('the impact was not confirmed.', 'The impact was confirmed.'),
 ('the version was 1.2.', 'The version was 12.'),
 ("the agent's activity was reported.", 'The agents activity was reported.')])
def test_no_ambiguity_case_folding_word_changes_or_punctuation_conflation(body,quote):
 with pytest.raises(ValueError,match='quote_not_in_source'):
  resolve_citation({'id':'S1','text':body},quote)

def test_exact_locator_remains_exact():
 span=resolve_citation({'id':'S1','text':'The impact remains unclear.'},'The impact remains unclear.')
 assert 'normalization' not in span
