import json
from types import SimpleNamespace
import pytest
from sempervigil import event_source_reports as r

pytestmark = pytest.mark.offline


def setup(monkeypatch, phases=('writer',)):
    report = {'title':'Incident', 'kind':'breach', 'items':[
        {'id':f'P0{i}', 'section':section, 'text':f'Item {i}',
         'claim_type':'finding','confidence':None,'rationale':'','date_label':'','date_sort':None,
         'citations':[{'source_id':'S1','quote':'Company reported access.'}]}
        for i,section in [(1,'overview'),(2,'impact')]]}
    body = json.dumps(report)
    rows = [(phase,'completed',json.dumps({'choices':[{'finish_reason':'stop','message':{'content':body}}]}),1000) for phase in phases]
    writes=[]
    def execute(sql, args):
        if 'SELECT reason' in sql:
            return SimpleNamespace(fetchone=lambda:('event_report_quote_not_in_source',))
        if 'SELECT phase' in sql:
            return SimpleNamespace(fetchall=lambda:rows)
        writes.append((sql,args))
    conn=SimpleNamespace(execute=execute,commit=lambda:None)
    monkeypatch.setattr(r,'enabled',lambda:True)
    monkeypatch.setattr(r,'check_scope',lambda _:None)
    monkeypatch.setattr(r,'_fresh',lambda *_:None)
    monkeypatch.setattr(r,'_load',lambda *_:{'event_id':'e','status':'held','reserved_tokens':0,
        'review':None,'report':None,'snapshot':{'sources':[{'id':'S1','text':'Company reported access,"'}]}})
    return conn,report,writes


def test_preserved_review_never_rewrites_or_accepts(monkeypatch):
    conn,report,writes=setup(monkeypatch)
    def call(c,rid,phase,system,data,schema,**kwargs):
        assert phase=='review' and data['report']==report
        assert data['citation_provenance']['P01'][0]['quote']=='Company reported access,"'
        return {'ready':True,'issues':[]}
    monkeypatch.setattr(r,'call',call)
    result=r.review_preserved(conn,'esr_test')
    assert result['report']==report and result['status']=='held'
    assert len(writes)==1 and writes[0][1][-2]=='operator_review_required'
    assert 'status=' not in writes[0][0]


@pytest.mark.parametrize('phases',[(),('writer','review'),('review',)])
def test_preserved_review_refuses_replay(monkeypatch, phases):
    conn,_,writes=setup(monkeypatch,phases)
    monkeypatch.setattr(r,'call',lambda *_args,**_kwargs:pytest.fail('No paid call permitted'))
    with pytest.raises(ValueError,match='not_eligible'):
        r.review_preserved(conn,'esr_test')
    assert not writes
