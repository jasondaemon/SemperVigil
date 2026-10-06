"""Document current editorial capabilities/limits without changing live prompts."""
import copy

import pytest

from sempervigil import event_report_contract as contract
from test_event_source_reports import packet, report

pytestmark = pytest.mark.offline


def test_connected_multisentence_overview_is_allowed():
    value = report()
    value['items'][0]['text'] = ('Acme reported possible patient-record exposure. '
        'The scope remains qualified: certain records may be affected. '
        'The company reported credential rotation as a response measure.')
    assert contract.validate(value, packet())


def test_multiple_overview_paragraph_items_are_allowed():
    value = report()
    second = copy.deepcopy(value['items'][1])
    second.update(id='P03', section='overview')
    value['items'] = [value['items'][0], second,
        {**value['items'][1], 'text': 'The reported response included rotating credentials.'}]
    assert contract.validate(value, packet())


def test_exact_overview_repetition_is_rejected_across_sections():
    value = report()
    value['items'][1]['text'] = value['items'][0]['text']
    with pytest.raises(ValueError, match='duplicate_prose'):
        contract.validate(value, packet())


def test_paraphrased_repetition_is_not_mechanically_detected():
    value = report()
    value['items'][1]['text'] = 'Possible exposure of patient records was reported by Acme.'
    # A lexical validator cannot establish section novelty; whole-report review must.
    assert contract.validate(value, packet())


def test_live_prompts_already_require_nonrepetitive_detail_and_qualified_dates():
    assert 'not recycle overview sentences' in contract.WRITER
    assert 'serious repetition' in contract.REVIEWER
    assert 'null date_sort' in contract.WRITER
