import json
import pytest
from sempervigil import event_report_final_editor as editor,event_report_contract_v2 as contract
pytestmark=pytest.mark.offline


def profile():
    return {'workflow':editor.WORKFLOW,'model':'gpt-5.6-sol','reasoning_effort':'high','max_completion_tokens':12000,'context_overrides':{'evt_fbi':32768}}


def test_single_event_context_override_retains_complete_bodies():
    packet={'event_id':'evt_fbi','sources':[{'id':'S1','article_id':1,'content_hash':'h','text':'complete original body'}],'final_editor':profile()}
    measured=contract.tokens(contract.encode({**packet,'coverage':{'mode':'complete','omitted_source_ids':[]}}))
    assert contract.context(packet,max_tokens=measured)['sources']==packet['sources']
    with pytest.raises(ValueError,match='over_budget'):contract.context(packet,max_tokens=measured-1)
    assert contract.context(packet)['coverage']=={'mode':'complete','omitted_source_ids':[]}


@pytest.mark.parametrize('change',[{'context_overrides':{'evt_fbi':200001}},{'context_overrides':{'evt_fbi':True}},{'max_completion_tokens':0},{'workflow':'old'},{'model':''},{'reasoning_effort':'unknown'}])
def test_invalid_explicit_policy_fails_closed(monkeypatch,change):
    monkeypatch.setenv(editor.CONFIG_ENV,json.dumps({**profile(),**change}))
    with pytest.raises(ValueError):editor.configuration()


def test_editor_refuses_source_omission():
    with pytest.raises(ValueError,match='complete_context_required'):editor.editor_input({'coverage':{'mode':'delta_with_prior_evidence','omitted_source_ids':['S2']}},{},{})


def test_default_disabled_and_scoped_configuration(monkeypatch):
    monkeypatch.delenv(editor.CONFIG_ENV,raising=False);assert editor.configuration() is None
    monkeypatch.setenv(editor.CONFIG_ENV,json.dumps(profile()));assert editor.configuration()==profile()
    assert editor.configuration()['context_overrides'].get('evt_other',24000)==24000
