"""Offline editorial examples: contract/gate checks, not measured model judgments."""
import copy
import json
from pathlib import Path

import jsonschema
import pytest
from sempervigil import event_report_contract as contract
from sempervigil import event_source_reports as reports

pytestmark = pytest.mark.offline
CASES = json.loads((Path(__file__).parents[1] / 'fixtures/event_editorial_reports.json').read_text())


@pytest.mark.parametrize('case', CASES, ids=lambda c: c['name'])
def test_evidence_proportional_reports_keep_exact_source_provenance(case):
    spans = contract.validate(case['report'], case['packet'])
    sources = {s['id']: s['text'] for s in case['packet']['sources']}
    for citations in spans.values():
        for cite in citations:
            assert sources[cite['source_id']][cite['start']:cite['end']] == cite['quote']
    review = dict(ready=True, issues=[], locator_warnings=[])
    assert contract.validate_review(review, case['report'], case['packet'])['ready']


@pytest.mark.parametrize('case', CASES, ids=lambda c: c['name'])
def test_paraphrase_only_detail_is_a_substantive_review_hold(case):
    report = copy.deepcopy(case['report'])
    detail = next(i for i in report['items'] if i['id'] == 'P02')
    detail['text'] = case['redundant_text']
    # No lexical heuristic pretends to judge meaning. This is a hand-labelled
    # review response demonstrating how semantic findings must hold publication.
    assert contract.validate(report, case['packet'])
    review = dict(ready=False, issues=[case['expected_substantive_issue']], locator_warnings=[])
    assert not contract.validate_review(review, report, case['packet'])['ready']
    review['ready'] = True
    with pytest.raises(ValueError, match='review_inconsistent'):
        contract.validate_review(review, report, case['packet'])


@pytest.mark.parametrize('case', CASES, ids=lambda c: c['name'])
def test_exact_cross_section_duplicate_remains_invalid(case):
    report = copy.deepcopy(case['report'])
    next(i for i in report['items'] if i['id'] == 'P02')['text'] = report['items'][0]['text']
    with pytest.raises(ValueError, match='duplicate_prose'):
        contract.validate(report, case['packet'])


def test_brief_recap_with_added_detail_is_not_a_word_overlap_failure():
    case = next(c for c in CASES if c['name'] == 'documented_intrusion')
    assert 'stolen VPN credential' in case['report']['items'][0]['text']
    assert 'stolen VPN credential' in case['report']['items'][-1]['text']
    assert contract.validate(case['report'], case['packet'])
    assert 'Permit brief repetition that anchors additional detail or chronology' in contract.REVIEWER
    assert 'lexical similarity alone is not grounds for rejection' in contract.REVIEWER


def test_instruction_contract_has_no_contradictory_short_overview_requirement():
    assert 'connected, developed paragraphs' in contract.WRITER
    assert 'coherent paragraph and provenance\nunit' in contract.WRITER
    assert 'Split items when epistemic type' in contract.WRITER
    assert 'paragraph, heading or word quotas' in contract.WRITER
    assert 'paraphrase alone is not substantive coverage' in contract.WRITER
    assert 'concise executive overview' not in contract.WRITER
    assert 'overview is concise' not in contract.WRITER
    assert 'not a heading count or word\ntarget' in contract.REVIEWER
    for requirement in ('COMPLETE CITED SOURCES', 'Locator warnings never determine ready',
                        'previous report is not evidence'):
        assert requirement in contract.REVIEWER
    assert 'null date_sort' in contract.WRITER


def test_epistemic_types_still_cannot_mix_finding_and_assessment_metadata():
    case = next(c for c in CASES if c['name'] == 'documented_intrusion')
    report = copy.deepcopy(case['report'])
    report['items'][1]['claim_type'] = 'finding'
    with pytest.raises(jsonschema.ValidationError):
        contract.validate(report, case['packet'])


def test_source_ids_and_qualified_dates_remain_guarded():
    case = copy.deepcopy(CASES[0])
    case['report']['items'][0]['citations'][0]['source_id'] = 'previous-report'
    with pytest.raises(jsonschema.ValidationError):
        contract.validate(case['report'], case['packet'])
    assert all(i['date_sort'] is None for i in CASES[0]['report']['items'])
    assert 'believes' in CASES[0]['report']['items'][0]['text']
    assert 'not been adjudicated' in CASES[3]['report']['items'][0]['text']


def test_evolving_report_new_source_is_older_evidence_not_later_event():
    case = CASES[4]
    assert case['packet']['evidence_delta']['new'] == ['S2']
    changed = case['report']['items'][1]
    assert changed['citations'][0]['source_id'] == 'S2'
    assert 'not a later incident development' in changed['text']


def test_prompt_change_changes_generator_identity_and_stale_work_holds(monkeypatch):
    from sempervigil.services import ai_service
    from sempervigil import event_source_report_pilot
    class Conn:
        def execute(self, *args): return self
        def fetchone(self): return (1, 2)
    monkeypatch.setattr(event_source_report_pilot, 'policy', lambda: None)
    monkeypatch.setattr(ai_service, 'get_provider', lambda *args: {'id':1, 'base_url':'https://example.invalid'})
    monkeypatch.setattr(ai_service, 'get_model', lambda *args: {'id':2})
    monkeypatch.setattr(reports, 'phase_settings', lambda: {})
    conn = Conn()
    before = reports.configuration(conn)[2]
    monkeypatch.setattr(contract, 'WRITER', contract.WRITER + '\nOffline identity example.')
    after = reports.configuration(conn)[2]
    assert after != before
    monkeypatch.setattr(reports, 'snapshot', lambda *args: {'source_version':'same'})
    monkeypatch.setattr(reports, 'previous', lambda *args: ('prior', None))
    with pytest.raises(ValueError, match='configuration_changed'):
        reports._fresh(conn, dict(snapshot={}, event_id='test', source_version='same', predecessor='prior', generator_version=before))
