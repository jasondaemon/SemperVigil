import pytest
from sempervigil import event_report_contract as x

pytestmark=pytest.mark.offline


@pytest.mark.parametrize('source,unsupported,qualified',[
 ('Actors spoofed the support number and contacted staff.',
  'Actors controlled the support telephone number and staff authorized access.',
  'The reported signal was caller-ID presentation, not established control of the number; employee actions are unreported.'),
 ('Materiality was determined October 2. The filing was signed October 3. Detection date was not reported.',
  'Detection occurred before October 2.',
  'Detection date unreported; October 2 is materiality and October 3 is signature date.'),
])
def test_signal_control_and_source_date_fixtures(source,unsupported,qualified):
    # These annotated semantic fixtures document expectations; the model/manual
    # review evaluates meaning, not a production keyword/fact-extraction heuristic.
    assert unsupported!=qualified
    report={'items':[{'id':'P01','text':qualified}]}
    packet={'sources':[{'id':'S1','text':source}],'review_contract':x.REVIEW_CONTRACT}
    issue={'item_id':'P01','reason':'Do not upgrade reported signal or source milestone to an unreported mechanism or incident date.','source_ids':['S1']}
    assert not x.validate_review({'ready':False,'issues':[issue],'locator_warnings':[]},report,packet)['ready']
    assert x.validate_review({'ready':True,'issues':[],'locator_warnings':[]},report,packet)['ready']
