import json,copy
import pytest
from sempervigil.private_report_canary import Executor,identity
from sempervigil import event_report_contract as c
pytestmark=pytest.mark.offline


def packet():return {'sources':[{'id':'S1','article_id':1,'content_hash':'h','text':'Acme said certain patient records may be affected. The company rotated credentials.'}]}


def report():
    def row(i,text,quote):return {'id':i,'section':'overview' if i=='P01' else 'response_recovery','text':text,'date_label':'','date_sort':None,'claim_type':'finding','confidence':None,'rationale':'','citations':[{'source_id':'S1','quote':quote}]}
    return {'title':'Acme incident','kind':'breach','items':[row('P01','Acme reported possible patient exposure.','certain patient records may be affected'),row('P02','The company rotated credentials.','The company rotated credentials.')]}


def payload(phase):
    writer=phase=='writer';data=packet() if writer else {'evidence':packet(),'report':report(),'citation_provenance':c.validate(report(),packet())}
    return {'model':'gpt-5.6-sol' if writer else 'gpt-5.6-luna','reasoning_effort':'none' if writer else 'low','max_completion_tokens':6000 if writer else 2400,
            'messages':[{'role':'system','content':phase},{'role':'user','content':c.encode(data)}],
            'response_format':{'type':'json_schema','json_schema':{'name':'event_source_report','strict':True,'schema':{} if writer else c.review_schema(report(),['S1'])}}}


def manifest():
    m={'workflow':'private-manifest-bound-report-canary-v1','publication':False,'database_transaction':'READ ONLY','max_calls':2,'ceiling':32000,
       'writer_system_sha256':identity('writer'),'review_system_sha256':identity('review'),'writer_request_sha256':identity(payload('writer')),'packet_sha256':identity(packet())}
    return {**m,'id':'canary_'+identity(m)[:24]}


def response():return {'choices':[{'finish_reason':'stop','message':{'content':c.encode(report())}}],'usage':{'total_tokens':200}}


def test_two_call_journal_persists_raw_body_usage_and_blocks_replay(tmp_path):
    calls=[];executor=Executor(manifest(),tmp_path,complete=lambda p:(calls.append(p) or response()))
    executor.call('writer',payload('writer'))
    journal=json.loads((executor.root/'writer.json').read_text());assert journal['response']==response() and journal['usage']=={'total_tokens':200}
    with pytest.raises(FileExistsError):Executor(manifest(),tmp_path,complete=lambda p:pytest.fail('replayed')).call('writer',payload('writer'))
    executor.call('review',payload('review'));assert len(calls)==2
    with pytest.raises(ValueError,match='reservation_limit'):executor.call('review',payload('review'))


def test_unknown_transport_is_durable_and_never_retried(tmp_path):
    def failure(p):raise TimeoutError('unknown outcome')
    e=Executor(manifest(),tmp_path,complete=failure)
    with pytest.raises(TimeoutError):e.call('writer',payload('writer'))
    assert json.loads((e.root/'writer.json').read_text())['status']=='unknown_transport'
    with pytest.raises(ValueError,match='incomplete'):e.call('review',payload('review'))
    with pytest.raises(FileExistsError):e.call('writer',payload('writer'))


@pytest.mark.parametrize('mutation',['packet','model','prompt','cap','report','provenance','schema','extra_data'])
def test_manifest_and_review_bindings_block_before_transport(tmp_path,mutation):
    e=Executor(manifest(),tmp_path,complete=lambda p:response());p=payload('writer')
    if mutation in ('report','provenance','schema','extra_data'):
        e.call('writer',p);p=payload('review');data=json.loads(p['messages'][1]['content'])
        if mutation=='report':data['report']['title']='Changed'
        if mutation=='provenance':data['citation_provenance']={}
        if mutation=='schema':p['response_format']['json_schema']['schema']={}
        if mutation=='extra_data':data['secret']='not permitted'
        p['messages'][1]['content']=c.encode(data)
    elif mutation=='packet':p['messages'][1]['content']=c.encode({'unbound':'input'})
    elif mutation=='model':p['model']='different-model'
    elif mutation=='cap':p['max_completion_tokens']=6001
    elif mutation=='prompt':p['messages'][0]['content']='Changed prompt'
    e.complete=lambda p:pytest.fail('transport before validation')
    with pytest.raises(ValueError):e.call('review' if mutation in ('report','provenance','schema','extra_data') else 'writer',p)


def test_invalid_manifest_cannot_expand_budget_or_publish(tmp_path):
    for key,value in [('publication',True),('ceiling',32001),('max_calls',3),('database_transaction','READ WRITE')]:
        m=manifest();m[key]=value
        with pytest.raises(ValueError):Executor(m,tmp_path,complete=lambda p:response())
