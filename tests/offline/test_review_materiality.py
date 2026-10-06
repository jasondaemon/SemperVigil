"""Contract/accounting tests, not claims of hosted model semantic accuracy."""
import copy
import json
from pathlib import Path
import jsonschema
import pytest
from sempervigil import event_report_contract_v2 as contract

pytestmark = pytest.mark.offline


def test_retained_control_context_and_nonmaterial_editorial_feedback():
    fixture = json.loads((Path(__file__).parents[1]/'fixtures/whole_report_epistemic/C12-approved-clean.json').read_text())
    data = fixture['evaluation_data']
    source = next(s for s in data['evidence']['sources'] if s['id']=='S36617')
    item = next(i for i in data['report']['items'] if i['id']=='P110')
    assert 'update to version 7 or take it offline as soon as possible' in source['text']
    assert 'take the instance offline immediately' in item['text']
    warning = {'item_id':'P110','reason':'Prefer the source urgency wording; the action and qualifications remain the same in this context.','source_ids':['S36617']}
    packet = {**data['evidence'],'review_contract':contract.REVIEW_CONTRACT}
    review = {'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[warning]}
    assert contract.validate_review(review,data['report'],packet)['ready']
    # This is a calibrated expected decision. The actual hosted false-ready
    # status remains false in its immutable triplet journal, never rewritten.
    assert contract.REVIEW_MATERIALITY_RULES in contract.REVIEWER


def test_material_claims_block_even_with_editorial_feedback():
    report={'items':[{'id':'P01'}]}
    packet={'sources':[{'id':'S1'}],'review_contract':contract.REVIEW_CONTRACT}
    issue={'item_id':'P01','reason':'Unreported response speed is an unsupported material premise.','source_ids':['S1']}
    review={'ready':False,'issues':[issue],'locator_warnings':[],'editorial_warnings':[]}
    assert not contract.validate_review(review,report,packet)['ready']
    with pytest.raises(ValueError,match='inconsistent'):
        contract.validate_review({**review,'ready':True},report,packet)
    for phrase in ('unsupported mechanism or response speed','unjustified universal or causal','materially misleads'):
        assert phrase in contract.REVIEW_MATERIALITY_RULES


@pytest.mark.parametrize('change',['item','source','missing_editorial'])
def test_editorial_feedback_remains_bound_and_required_for_new_contract(change):
    report={'items':[{'id':'P01'}]};packet={'sources':[{'id':'S1'}],'review_contract':contract.REVIEW_CONTRACT}
    warning={'item_id':'P01','reason':'A nonblocking wording preference.','source_ids':['S1']}
    value={'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[warning]}
    if change=='item':warning['item_id']='P99'
    if change=='source':warning['source_ids']=['S99']
    if change=='missing_editorial':value.pop('editorial_warnings')
    with pytest.raises(jsonschema.ValidationError):contract.validate_review(value,report,packet)


def test_saved_prior_review_is_not_reclassified_or_given_new_fields():
    packet={'sources':[{'id':'S1'}],'review_contract':contract.PRIOR_REVIEW_CONTRACT}
    old={'ready':False,'issues':[{'item_id':'P01','reason':'Retained old reviewer decision, including a fidelity hold.','source_ids':['S1']}],'locator_warnings':[]}
    retained=copy.deepcopy(old)
    assert contract.validate_review(old,{'items':[{'id':'P01'}]},packet)==retained
    assert 'editorial_warnings' not in old
    schema=contract.review_schema({'items':[{'id':'P01'}]},['S1'],editorial=False)
    assert 'editorial_warnings' not in schema['properties']
