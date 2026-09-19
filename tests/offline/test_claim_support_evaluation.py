import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.offline
spec = importlib.util.spec_from_file_location('support_eval', Path(__file__).resolve().parents[2] / 'tools/evaluate-claim-support.py')
evaluation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evaluation)


def expected():
    return {'event_id':'event','cases':[
        {'id':'a','expected':'model_supported','reason':'supported reporting'},
        {'id':'b','expected':'reject','reason':'wrong citation'}]}


def jobs(a='model_supported', b='reject'):
    return [{'id':'job','status':'succeeded','result':{'claim_support':{
        'event_id':'event','public_eligible':False,'suggestions':[
            {'claim_id':'a','decision':a},{'claim_id':'b','decision':b}]}}}]


def test_matching_decisions_pass():
    assert evaluation.evaluate(expected(), jobs())['passed']


def test_holding_everything_is_not_a_pass():
    result = evaluation.evaluate(expected(), jobs('hold','hold'))
    assert not result['passed'] and len(result['mismatches']) == 2


def test_missing_failed_or_unexpected_jobs_cannot_pass():
    assert not evaluation.evaluate(expected(), [])['passed']
    assert not evaluation.evaluate(expected(), [{'id':'job','status':'failed'}])['passed']
    extra = jobs()
    extra[0]['result']['claim_support']['suggestions'].append({'claim_id':'c','decision':'reject'})
    assert not evaluation.evaluate(expected(), extra)['passed']


def test_duplicate_results_cannot_inflate_accuracy():
    with pytest.raises(ValueError, match='duplicate_observed'):
        evaluation.evaluate(expected(), jobs() * 2)
