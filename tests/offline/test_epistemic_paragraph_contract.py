"""Structural/fixture checks only: no inference that models obey the new prompts."""
import copy,json
from pathlib import Path
import jsonschema,pytest
from sempervigil import event_report_contract_v2 as contract
from sempervigil.attack_catalog import validate_report
from sempervigil.attack_catalog_runtime import catalog

pytestmark=pytest.mark.offline
ROOT=Path(__file__).resolve().parents[1]/'fixtures'
CASES=json.loads((ROOT/'epistemic_paragraph_cases.json').read_text())

def report(case,variant):
 target=copy.deepcopy(case[variant]['item']);overview=copy.deepcopy(target)
 overview.update(id='P01',section='overview',claim_type='finding',confidence=None,rationale='',text=case['sources'][0]['text'])
 return {'title':'Synthetic '+case['case_id'],'kind':case['kind'],'items':[overview,target]}

@pytest.mark.parametrize('case',CASES,ids=lambda c:c['case_id'])
def test_developed_qualified_paragraphs_and_guidance_are_structurally_supported(case):
 value=report(case,'positive')
 assert len(value['items'][1]['text'].split('.'))>1
 spans,resolved=validate_report(value,{'sources':case['sources'],'attack_reference':{'catalog':catalog().identity,'candidates':[]}},catalog(),contract_override=contract)
 assert spans['P02'] and not any(resolved.values())
 review={'ready':True,'issues':[],'locator_warnings':[]}
 assert contract.validate_review(review,value,{'sources':case['sources']})['ready']

@pytest.mark.parametrize('case',CASES,ids=lambda c:c['case_id'])
def test_semantic_negatives_expose_structural_limit_and_exact_issue_binding(case):
 value=report(case,'negative')
 # A syntactically valid false claim still passes mechanical validation.
 # Human-authored expected issue is NOT a model prediction.
 assert validate_report(value,{'sources':case['sources'],'attack_reference':{'catalog':catalog().identity,'candidates':[]}},catalog(),contract_override=contract)
 review={'ready':False,'issues':[{'item_id':'P02','reason':case['negative']['expected_issue'],'source_ids':['S1']}],'locator_warnings':[]}
 assert contract.validate_review(review,value,{'sources':case['sources']})['ready'] is False
 review['issues'][0]['item_id']='P99'
 with pytest.raises(jsonschema.ValidationError):contract.validate_review(review,value,{'sources':case['sources']})


def test_finding_cannot_take_assessment_metadata_and_guidance_can_keep_premises():
 value=report(CASES[3],'positive');assert value['items'][1]['claim_type']=='assessment'
 value['items'][1]['claim_type']='finding'
 for item in value['items']:item.pop('attack_mappings')
 with pytest.raises(jsonschema.ValidationError):contract.validate(value,{'sources':CASES[3]['sources']})


def test_retained_actual_response_replay_preserves_known_false_ready():
 retained=json.loads((ROOT/'retained_semantic_batch_replay.json').read_text());packet=retained['challenge'];actual={}
 for result in retained['response']['results']:
  case=next(c for c in packet['cases'] if c['case_id']==result['case_id'])
  review={k:v for k,v in result.items() if k!='case_id'}
  contract.validate_review(review,{'items':[case['target_item']]},packet)
  actual[result['case_id']]=review['ready']
 assert actual=={'C1':True,'C2':False,'C3':True,'C4':True}
 assert [c for c in actual if actual[c]!=retained['expected_ready'][c]]==['C1','C3']
 assert retained['usage']['total_tokens']==9843
