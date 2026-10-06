import pytest
from sempervigil import event_source_report_render as render

pytestmark=pytest.mark.offline


def test_analyst_section_preserves_type_without_publishing_process_metadata(monkeypatch):
    monkeypatch.setattr(render,'resolve',lambda *_args,**_kwargs:({},{}))
    bundle={'sources':[{'id':'S1','url':'https://example.org/source'}],'report':{'items':[
      {'id':'P01','section':'analyst_assessment','claim_type':'assessment','confidence':'moderate',
       'date_label':'Assessment cutoff','text':'A bounded analyst inference.','rationale':'A supported premise with explicit limits.',
       'citations':[{'source_id':'S1'}]}]}}
    _,html=render.render(bundle,event_id='e',expected_revision='r')
    assert '<h2>Analyst assessment</h2>' in html
    assert 'event-claim-state' not in html
    assert 'confidence' not in html
    assert 'A supported premise with explicit limits.' not in html
    assert bundle['report']['items'][0]['confidence'] == 'moderate'
    assert bundle['report']['items'][0]['rationale'] == 'A supported premise with explicit limits.'


@pytest.mark.parametrize("claim_type,label,visible", [
    ("assessment", "Analyst assessment as of October 5, 2026", False),
    ("intelligence_gap", "Outstanding as of October 5, 2026", False),
    ("intelligence_gap", "As of October 2, 2026", False),
    ("finding", "Status as of September 23, 2026", True),
    ("finding", "As reported September 23, 2026", True),
    ("finding", "September 22, 2026", True),
    ("assessment", "Incident date unreported", True),
])
def test_report_process_dates_are_distinct_from_claim_temporal_qualifications(monkeypatch, claim_type, label, visible):
    import copy
    monkeypatch.setattr(render, "resolve", lambda *_args, **_kwargs: ({}, {}))
    bundle = {"sources":[{"id":"S1","url":"https://example.org/source"}], "report":{"items":[
        {"id":"P01", "section":"attack_path", "claim_type":claim_type, "confidence":"high",
         "date_label":label, "text":"The method is unconfirmed; the company believes information was accessed.",
         "rationale":"Internal reasoning.", "citations":[{"source_id":"S1"}]}]}}
    original = copy.deepcopy(bundle)
    _, html = render.render(bundle, event_id="e", expected_revision="r")
    assert (label in html) is visible
    assert "The method is unconfirmed; the company believes information was accessed." in html
    assert 'href="https://example.org/source"' in html
    assert "confidence" not in html and "Internal reasoning." not in html
    assert bundle == original
