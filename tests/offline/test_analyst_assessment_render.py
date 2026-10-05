import pytest
from sempervigil import event_source_report_render as render

pytestmark=pytest.mark.offline


def test_explicit_analyst_section_renders_with_type_confidence_and_rationale(monkeypatch):
    monkeypatch.setattr(render,'resolve',lambda *_args,**_kwargs:({},{}))
    bundle={'sources':[{'id':'S1','url':'https://example.org/source'}],'report':{'items':[
      {'id':'P01','section':'analyst_assessment','claim_type':'assessment','confidence':'moderate',
       'date_label':'Assessment cutoff','text':'A bounded analyst inference.','rationale':'A supported premise with explicit limits.',
       'citations':[{'source_id':'S1'}]}]}}
    _,html=render.render(bundle,event_id='e',expected_revision='r')
    assert '<h2>Analyst assessment</h2>' in html
    assert 'Assessment · moderate confidence' in html
    assert 'A supported premise with explicit limits.' in html
