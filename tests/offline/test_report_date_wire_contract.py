"""Keep provider date strings aligned with native calendar validation."""
import jsonschema,pytest
from sempervigil import event_report_contract_v2 as contract
pytestmark=pytest.mark.offline

def report(value):
    item={'id':'P01','section':'overview','text':'Acme reported an investigation in September 2026.',
          'claim_type':'finding','confidence':None,'rationale':'','date_label':'September 2026',
          'date_sort':value,'citations':[{'source_id':'S1','quote':'Acme reported an investigation'}]}
    return {'title':'Date precision fixture','kind':'breach','items':[item,{**item,'id':'P02','section':'open_questions','text':'The exact action day is unreported.'}]}

@pytest.mark.parametrize('value',['2026','2026-09','2026/09/21','2026-9-21','2026-09-21T10:00:00Z',''])
def test_incomplete_or_noncanonical_date_is_rejected_in_provider_schema(value):
    with pytest.raises(jsonschema.ValidationError):jsonschema.validate(report(value),contract.schema(['S1']))

def test_partial_precision_is_preserved_without_inventing_a_day():
    value=report(None);packet={'sources':[{'id':'S1','text':'Acme reported an investigation in September 2026.'}]}
    assert contract.validate(value,packet)
    assert value['items'][0]['date_label']=='September 2026' and value['items'][0]['date_sort'] is None
    assert 'do not invent a day' in contract.WRITER

def test_full_day_format_and_invalid_calendar_day_have_distinct_checks():
    value=report('2026-09-21');jsonschema.validate(value,contract.schema(['S1']))
    value['items'][0]['date_sort']='2026-02-30'
    jsonschema.validate(value,contract.schema(['S1']))
    with pytest.raises(ValueError):contract.validate(value,{'sources':[{'id':'S1','text':'Acme reported an investigation in September 2026.'}]})
