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
 target=copy.deepcopy(case[variant]['item']);context=[]
 for index,premise in enumerate(case['neighboring_context'] or [{'id':'P01','text':case['sources'][0]['text']}]):
  overview=copy.deepcopy(target)
  overview.update(id=premise['id'],section='overview' if index==0 else 'attack_vector',
                  claim_type='finding',confidence=None,rationale='',text=premise['text'])
  context.append(overview)
 return {'title':'Synthetic '+case['case_id'],'kind':case['kind'],'items':context+[target]}

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
 review={'ready':False,'issues':[{'item_id':'P02','reason':case['negative']['expected_issue'],'source_ids':[source['id'] for source in case['sources']]}],'locator_warnings':[]}
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



def test_optional_mapping_schema_uses_explicit_v2_contract_without_changing_legacy():
 from sempervigil.attack_catalog import generation_schema
 from sempervigil import event_report_contract as legacy
 legacy_schema=generation_schema(['S1'],[],{})
 v2_schema=generation_schema(['S1'],[],{},contract_override=contract)
 branches=v2_schema['properties']['items']['items']['anyOf']
 assert branches[0]['properties']['text']['description']==contract.schema(['S1'])['properties']['items']['items']['anyOf'][0]['properties']['text']['description']
 assert 'description' not in legacy_schema['properties']['items']['items']['anyOf'][0]['properties']['text']
 assert branches[1]['properties']['claim_type']['enum']==['assessment','intelligence_gap']



def test_distinct_exact_whole_report_fixture_integrity_and_scope():
 import hashlib
 from sempervigil.investigation import _version
 root=ROOT/'whole_report_epistemic';manifest=json.loads((root/'manifest.json').read_text());reports={}
 for name,entry in manifest['fixtures'].items():
  file=root/(name+'.json');assert hashlib.sha256(file.read_bytes()).hexdigest()==entry['file_sha256']
  fixture=json.loads(file.read_text());data=fixture['evaluation_data']
  assert _version(data)==entry['model_data_sha256'] and _version(data['report'])==entry['report_sha256']
  assert not fixture['synthetic']
  assert len(data['report']['items'])==entry['item_count']
  reports[name]={i['id']:i for i in data['report']['items']}
 n13=reports['N13-original-projected'];n14=reports['N14-editorial-split'];control=reports['C12-approved-clean']
 assert 'P115' not in n13 and n14['P115']['text'] in n13['P110']['text']
 assert n13['P110']['text']!=n14['P110']['text']
 assert control['P110']==n14['P110'] and 'P113' not in control and 'P115' not in control
 assert [x['fixture_id'] for x in manifest['proposed_comparison']]==['N13-original-projected','N13-original-projected','C12-approved-clean']
 assert manifest['total_exact_proposed_reservation']==sum(request['reservation'] for request in manifest['proposed_comparison'])
 assert manifest['total_exact_proposed_reservation']<=60000 and manifest['paid_calls']==0



def test_reversed_neighboring_context_is_preserved_in_fixture_report():
 case=next(c for c in CASES if c['case_id']=='advisory')
 value=report(case,'negative')
 assert value['items'][0]['text']==case['neighboring_context'][0]['text']
 assert value['items'][0]['text'].index('Beta')<value['items'][0]['text'].index('Alpha')


def test_fixture_labels_disclose_assistant_provenance_not_model_detection():
 assert all(c['label_provenance']['author']=='Codex assistant, offline construction' for c in CASES)
 assert all(c['label_provenance']['independently_adjudicated'] is False for c in CASES)
 assert len({c['positive']['item']['rationale'] for c in CASES if c['positive']['item']['claim_type']=='assessment'})==sum(c['positive']['item']['claim_type']=='assessment' for c in CASES)
