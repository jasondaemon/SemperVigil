import pytest
import jsonschema
from sempervigil import event_report_contract as x

pytestmark=pytest.mark.offline


@pytest.mark.parametrize('kind,source,claim',[
 ('breach','Company detected activity. It believes records were accessed; the scope is unknown.','The company believes records were accessed; scope remains unknown.'),
 ('vulnerability','A flaw was disclosed. No exploitation was observed by the vendor.','The vendor reported no observed exploitation, not proof that exploitation never occurred.'),
 ('campaign','Several messages were analyzed. Attribution remains unconfirmed.','Attribution remains unconfirmed.'),
 ('law_enforcement','Authorities announced arrests. The allegations have not been adjudicated.','Authorities announced arrests; the allegations remain unadjudicated.'),
])
def test_heldout_types_supported_outside_locator_is_nonblocking(kind,source,claim):
    # Annotated fixtures test the review contract, not a lexical factual validator.
    item={'id':'P01','section':'overview','text':claim,'claim_type':'finding','confidence':None,
          'rationale':'','date_label':'Undated','date_sort':None,
          'citations':[{'source_id':'S1','quote':source.split('. ')[0]+'.'}]}
    report={'title':'Fixture','kind':kind,'items':[item,{**item,'id':'P02','section':'open_questions','text':'A distinct intelligence limitation remains.'}]}
    packet={'sources':[{'id':'S1','text':source}],'review_contract':x.REVIEW_CONTRACT}
    x.validate(report,packet)
    value={'ready':True,'issues':[],'locator_warnings':[{'item_id':'P01',
      'reason':'The supporting qualification occurs later in the complete cited source.','source_ids':['S1']}]}
    assert x.validate_review(value,report,packet)['ready']
    bad={**value,'issues':[{'item_id':'P01','reason':'A material qualification was lost in the report.','source_ids':['S1']}]}
    with pytest.raises(ValueError,match='inconsistent'):x.validate_review(bad,report,packet)
    with pytest.raises(jsonschema.ValidationError):x.validate_review({'ready':True,'issues':[]},report,packet)


def test_legacy_issue_not_reclassified():
    report={'items':[{'id':'P01'}]};packet={'sources':[{'id':'S1'}]}
    old={'ready':False,'issues':[{'item_id':'P01','reason':'Selected quotation omits a supported fact.','source_ids':['S1']}]}
    assert x.validate_review(old,report,packet)==old


def test_writer_override_does_not_change_fixed_reviewer(monkeypatch):
    import json
    from types import SimpleNamespace
    from sempervigil import event_source_reports as r
    from sempervigil.services import ai_service
    monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_WRITER_MODEL','gpt-5.6-sol')
    monkeypatch.setattr(r,'configuration',lambda _:({'model_name':'gpt-5.6-sol'},{},'version'))
    monkeypatch.setattr(r,'_fresh',lambda *_:None)
    monkeypatch.setattr(r,'_reserve_cohort',lambda *_:None)
    monkeypatch.setattr(r,'_load',lambda *_:{'snapshot':{},'charged_tokens':0,'reserved_tokens':0,'budget_tokens':24000})
    monkeypatch.setattr(ai_service,'get_model',lambda _,mid:{'model_name':'gpt-5.6-luna'})
    def execute(sql,args=()):
        if 'SELECT m.id' in sql:row=('fixed-reviewer',)
        elif 'SELECT tokens' in sql:row=None
        else:row=(0,)
        return SimpleNamespace(fetchone=lambda:row)
    conn=SimpleNamespace(execute=execute,commit=lambda:None)
    seen=[]
    def complete(payload):
        seen.append(payload['model'])
        return {'choices':[{'finish_reason':'stop','message':{'content':json.dumps({'value':1})}}],
                'usage':{'total_tokens':1}}
    for phase in ('writer','review'):
        r.call(conn,'esr_test',phase,'system',{},x.object_schema({'value':{'type':'integer'}}),complete=complete)
    assert seen==['gpt-5.6-sol','gpt-5.6-luna']
