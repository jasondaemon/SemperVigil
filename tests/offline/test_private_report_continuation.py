import copy,json
import pytest
from sempervigil import event_report_contract as c
from sempervigil.attack_catalog import project_optional_mappings
from sempervigil.private_report_continuation import ContinuationExecutor
from sempervigil.private_report_canary import Executor,identity
from test_attack_catalog import catalog,mapping
from test_private_report_canary import packet as base_packet,report as base_report
pytestmark=pytest.mark.offline

def materials():
 cat=catalog();p=base_packet();p['sources'].append({**p['sources'][0],'id':'S2','article_id':2});t=cat.lookup('T1566.004');p['attack_reference']={'catalog':cat.identity,'candidates':[{k:t[k] for k in ('id','name','url','parent_id','tactics','definition','object_version')}]}
 r=base_report()
 for i in r['items']:i['attack_mappings']=[]
 r['items'][1]['section']='attack_path';r['items'][1]['attack_mappings']=[{**mapping(),'source_ids':['S2']}]
 return cat,p,r

def setup(tmp_path):
 cat,p,r=materials();writer_payload={'model':'gpt-5.6-sol','reasoning_effort':'none','max_completion_tokens':6000,'messages':[{'role':'system','content':'writer'},{'role':'user','content':c.encode(p)}]}
 parent={'workflow':'private-manifest-bound-report-canary-v1','publication':False,'database_transaction':'READ ONLY','max_calls':2,'ceiling':32000,'writer_system_sha256':identity('writer'),'review_system_sha256':identity('review'),'writer_request_sha256':identity(writer_payload),'packet_sha256':identity(p)};parent['id']='canary_'+identity(parent)[:24]
 e=Executor(parent,tmp_path,complete=lambda _: {'choices':[{'finish_reason':'stop','message':{'content':c.encode(r)}}],'usage':{'total_tokens':200}});e.call('writer',writer_payload)
 report,evidence,spans,resolved,removed=project_optional_mappings(r,p,cat)
 payload={'model':'gpt-5.6-luna','reasoning_effort':'low','max_completion_tokens':2400,'messages':[{'role':'system','content':'review'},{'role':'user','content':c.encode({'evidence':evidence,'report':report,'citation_provenance':spans})}],'response_format':{'type':'json_schema','json_schema':{'name':'event_source_report','strict':True,'schema':c.review_schema(report,['S1','S2'])}}}
 m={'workflow':'private-report-continuation-v1','publication':False,'database_transaction':'READ ONLY','max_calls':2,'ceiling':32000,'parent_manifest_sha256':identity(parent),'parent_id':parent['id'],'parent_writer_journal_sha256':identity(json.loads((e.root/'writer.json').read_text())),'review_request_sha256':identity(payload),'projection_sha256':identity({'report':report,'evidence':evidence,'spans':spans,'removed':removed})};m['id']='continuation_'+identity(m)[:24]
 return cat,p,r,parent,m,payload,e

def test_projection_drops_only_invalid_optional_mapping_and_preserves_sources():
 cat,p,r=materials();before=copy.deepcopy(r);report,evidence,spans,resolved,removed=project_optional_mappings(r,p,cat)
 assert r==before and evidence['sources']==p['sources']
 assert removed[0]['reason']=='attack_mapping_evidence_binding_invalid'
 for original,projected in zip(r['items'],report['items']):
  assert {k:v for k,v in original.items() if k!='attack_mappings'}=={k:v for k,v in projected.items() if k!='attack_mappings'}
 assert report['items'][1]['attack_mappings']==[] and evidence['attack_reference']['candidates']==[]


def test_valid_mapping_and_its_full_definition_remain():
 cat,p,r=materials();r['items'][1]['attack_mappings'][0]['source_ids']=['S1']
 report,evidence,_,_,removed=project_optional_mappings(r,p,cat)
 assert report==r and evidence==p and removed==[]


def test_claim_citation_failure_is_never_projected_away():
 cat,p,r=materials();r['items'][0]['citations'][0]['quote']='Invented evidence appears nowhere.'
 with pytest.raises(ValueError,match='quote_not_in_source'):project_optional_mappings(r,p,cat)


def test_continuation_journals_one_review_and_blocks_all_replays(tmp_path):
 cat,p,r,parent,m,payload,e=setup(tmp_path);calls=[]
 ex=ContinuationExecutor(parent,m,tmp_path,complete=lambda v:(calls.append(v) or {'usage':{'total_tokens':10},'choices':[{'message':{'content':'unparsed'}}]}),catalog=cat,contract=c)
 ex.call(payload,p);j=json.loads((e.root/'review.json').read_text());assert j['usage']=={'total_tokens':10} and j['continuation_id']==m['id'] and len(calls)==1
 with pytest.raises(ValueError,match='reservation_limit'):ex.call(payload,p)

@pytest.mark.parametrize('field',['report','evidence','spans','cap','parent','schema'])
def test_changed_material_blocks_before_transport(tmp_path,field):
 cat,p,r,parent,m,payload,e=setup(tmp_path)
 if field=='parent':
  j=json.loads((e.root/'writer.json').read_text());j['usage']['total_tokens']=201;(e.root/'writer.json').write_text(json.dumps(j))
 elif field=='cap':payload['max_completion_tokens']=2401
 elif field=='schema':payload['response_format']['json_schema']['schema']={}
 else:
  data=json.loads(payload['messages'][1]['content'])
  if field=='report':data['report']['title']='changed'
  if field=='evidence':data['evidence']['sources'][0]['text']='changed'
  if field=='spans':data['citation_provenance']={}
  payload['messages'][1]['content']=c.encode(data)
 ex=ContinuationExecutor(parent,m,tmp_path,complete=lambda _:pytest.fail('transport'),catalog=cat,contract=c)
 with pytest.raises(ValueError):ex.call(payload,p)


def test_budget_and_unknown_transport_hold_without_retry(tmp_path):
 cat,p,r,parent,m,payload,e=setup(tmp_path)
 def fail(_):raise TimeoutError()
 ex=ContinuationExecutor(parent,m,tmp_path,complete=fail,catalog=cat,contract=c)
 with pytest.raises(TimeoutError):ex.call(payload,p)
 assert json.loads((e.root/'review.json').read_text())['status']=='unknown_transport'
 with pytest.raises(ValueError,match='reservation_limit'):ex.call(payload,p)


def test_parent_reserved_capacity_is_not_recredited_from_actual_usage(tmp_path):
 cat,p,r,parent,m,payload,e=setup(tmp_path)
 j=json.loads((e.root/'writer.json').read_text());j['reservation']=31999;(e.root/'writer.json').write_text(json.dumps(j))
 m['parent_writer_journal_sha256']=identity(j);m.pop('id');m['id']='continuation_'+identity(m)[:24]
 ex=ContinuationExecutor(parent,m,tmp_path,complete=lambda _:pytest.fail('transport'),catalog=cat,contract=c)
 with pytest.raises(ValueError,match='reservation_limit'):ex.call(payload,p)


def test_unused_definition_tampering_cannot_be_hidden_by_projection():
 cat,p,r=materials();p['attack_reference']['candidates'][0]['definition']='made up'
 with pytest.raises(ValueError,match='definition_mismatch'):project_optional_mappings(r,p,cat)
