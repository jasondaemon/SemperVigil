import copy
import pytest
from test_private_report_continuation import materials
from sempervigil.attack_catalog import project_optional_mappings
from sempervigil.event_report_editorial import prepare, verify, validate_manual_review, CHECKS

pytestmark = pytest.mark.offline


def fixture():
    cat, packet, raw = materials()
    report, evidence, *_ = project_optional_mappings(raw, packet, cat)
    item = report['items'][0]
    item['text'] = 'The company reported an intrusion. Operators should evaluate the exposed service.'
    finding = copy.deepcopy(item)
    finding['text'] = 'The company reported an intrusion.'
    assessment = copy.deepcopy(item)
    assessment.update(id='P99', text='Operators should evaluate the exposed service.',
                      section='analyst_assessment', claim_type='assessment', confidence='moderate',
                      rationale='Synthetic premise supports evaluation; this is not an exploitability finding.',
                      date_label='Undated analyst assessment', date_sort=None)
    operations = [{'item_id': item['id'], 'parts': [finding, assessment]}]
    review = {'ready': True, 'issues': [], 'locator_warnings': []}
    return cat, report, evidence, review, operations


def proposal():
    cat, report, evidence, review, ops = fixture()
    a = prepare(report, evidence, review, ops, editor='editor', catalog=cat)
    return cat, evidence, a


def receipt(a):
    return {'workflow': 'event-report-independent-editorial-review-v1', 'ready': True,
            'reviewer': 'independent reviewer', 'source_review': 'SYNTHETIC receipt, not live approval.',
            **{k:a[k] for k in ['report_version', 'evidence_version', 'original_report_version', 'original_review_version']},
            'checks': {k:True for k in CHECKS}}


def test_split_preserves_original_and_binds_separate_review():
    cat, report, evidence, review, ops = fixture()
    before = copy.deepcopy((report, evidence, review))
    a = prepare(report, evidence, review, ops, editor='editor', catalog=cat)
    assert (report, evidence, review) == before
    assert a['report']['items'][2:] == report['items'][1:]
    assert a['report_version'] != a['original_report_version']
    assert a['automated_review_scope'] == 'original_report_only'
    assert a['editorial_review_status'] == 'pending_independent_review'
    assert verify(a, evidence, catalog=cat) == a
    assert validate_manual_review(a, receipt(a), evidence, catalog=cat)['report_version'] == a['report_version']


@pytest.mark.parametrize('change', ['prose', 'citation', 'mapping', 'id'])
def test_rejects_added_facts_quotes_mappings_and_id_collisions(change):
    cat, report, evidence, review, ops = fixture()
    if change == 'prose':ops[0]['parts'][1]['text'] += ' Root access was confirmed.'
    if change == 'citation':ops[0]['parts'][1]['citations'][0]['quote'] += ' Invented.'
    if change == 'mapping':ops[0]['parts'][1]['attack_mappings'] = [{'technique_id':'T1068'}]
    if change == 'id':ops[0]['parts'][1]['id'] = report['items'][1]['id']
    with pytest.raises(ValueError):prepare(report, evidence, review, ops, editor='editor', catalog=cat)


@pytest.mark.parametrize('change', ['self_review', 'old_content', 'false_check', 'not_ready', 'old_model_review'])
def test_independent_receipt_requires_derivative_binding_and_all_checks(change):
    cat, evidence, a = proposal();r = receipt(a)
    if change == 'self_review':r['reviewer'] = a['editor']
    if change == 'old_content':r['report_version'] = a['original_report_version']
    if change == 'false_check':r['checks']['whole_derivative_reviewed'] = False
    if change == 'not_ready':r['ready'] = False
    if change == 'old_model_review':r = a['original_review']
    with pytest.raises(ValueError, match='independent_review_required'):
        validate_manual_review(a, r, evidence, catalog=cat)


def test_artifact_or_complete_evidence_tampering_holds():
    cat, evidence, a = proposal();a['report']['items'][0]['confidence'] = 'high'
    with pytest.raises(ValueError, match='derivative_integrity'):verify(a, evidence, catalog=cat)
    cat, evidence, a = proposal();evidence['sources'][0]['text'] += ' New fact.'
    with pytest.raises(ValueError, match='derivative_integrity'):verify(a, evidence, catalog=cat)
