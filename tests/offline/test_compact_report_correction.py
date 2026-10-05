import copy,json
import pytest
from sempervigil import event_report_contract as x,event_source_reports as r
from sempervigil.event_source_report_executor import JournaledExecutor

pytestmark=pytest.mark.offline


def test_patch_only_replaces_flagged_items():
    report={'title':'Title','kind':'breach','items':[{'id':'P01','text':'fixed'},{'id':'P02','text':'old'}]}
    original=copy.deepcopy(report)
    revised=x.apply_correction(report,{'items':[{'id':'P02','text':'new'}]},{'P02'})
    assert report==original and revised['items'][0]==report['items'][0]
    assert revised['items'][1]['text']=='new'
    with pytest.raises(ValueError,match='scope_changed'):
        x.apply_correction(report,{'items':[{'id':'P01','text':'new'}]},{'P02'})


def test_final_two_phase_adapter_journals_and_refuses_third(monkeypatch,tmp_path):
    monkeypatch.setattr(r,'ready_client',lambda _:None)
    calls=[]
    def complete(payload):calls.append(payload);return {'usage':{'total_tokens':3}}
    executor=JournaledExecutor(object(),'esr_final',tmp_path,ceiling=22000,
                             phases=('correction','verification'),complete=complete)
    executor({'max_completion_tokens':1600,'messages':[{'content':'correction'}]})
    executor({'max_completion_tokens':1600,'messages':[{'content':'verification'}]})
    receipts=[json.loads(path.read_text()) for path in (tmp_path/'esr_final').glob('*.json')]
    assert {v['phase'] for v in receipts}=={'correction','verification'}
    assert all(v['status']=='completed' for v in receipts)
    with pytest.raises(ValueError,match='call_limit'):
        executor({'max_completion_tokens':1600})
    replay=JournaledExecutor(object(),'esr_final',tmp_path,phases=('correction','verification'),complete=complete)
    with pytest.raises(ValueError,match='not_replayed'):
        replay({'max_completion_tokens':1600,'messages':[{'content':'correction'}]})
    assert len(calls)==2
