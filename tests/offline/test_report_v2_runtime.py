import json,hashlib
from pathlib import Path
import pytest
from sempervigil import attack_catalog_runtime as runtime
from sempervigil import event_report_contract_v2 as contract
from sempervigil.attack_catalog import project_optional_mappings
from test_private_report_continuation import materials
pytestmark=pytest.mark.offline

def test_default_off_never_loads_catalog(monkeypatch):
 monkeypatch.delenv('SV_EVENT_REPORT_V2_ENABLED',raising=False)
 monkeypatch.setattr(runtime,'catalog',lambda *a,**k:pytest.fail('catalog loaded'))
 assert runtime.selected('evt_test') is False


def test_scope_requires_explicit_list_and_disallows_active_pilot(monkeypatch):
 from sempervigil import event_source_report_pilot as pilot
 monkeypatch.setenv('SV_EVENT_REPORT_V2_ENABLED','1');monkeypatch.delenv('SV_EVENT_REPORT_V2_EVENT_IDS',raising=False)
 with pytest.raises(ValueError,match='scope_required'):runtime.scope()
 monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test');monkeypatch.setattr(pilot,'policy',lambda:{'events':['evt_test']})
 with pytest.raises(ValueError,match='pilot_overlap'):runtime.scope()
 monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_other');assert runtime.scope()=={'evt_other'}


def test_full_source_retrieval_finds_late_named_behavior_and_exact_budget():
 sources=[{'id':'S1','title':'Incident investigation','text':('No details were available. '*100)+'Investigators reported password spraying against several accounts.'}]
 r=runtime.reference(sources)
 assert 'T1110.003' in {t['id'] for t in r['candidates']}
 assert r['retrieval']['searched_source_ids']==['S1']
 assert contract.tokens(json.dumps(r,ensure_ascii=False,separators=(',',':')))<=3200
 for t in r['candidates']:assert t['definition']==runtime.catalog().lookup(t['id'])['definition']


@pytest.mark.parametrize('domain',['enterprise','mobile','ics'])
def test_packaged_catalog_identity_and_historical_pin(domain):
 c=runtime.catalog(domain);assert c.identity['release']=='19.2'
 assert runtime.catalog_for(c.identity).identity==c.identity
 with pytest.raises(ValueError,match='digest'):runtime.catalog(domain,'19.2','0'*64)


def test_invalid_mapping_cannot_leave_explicit_taxonomy_claim():
 cat,p,r=materials();r['items'][1]['text']='The source establishes T1566.004.'
 with pytest.raises(ValueError,match='leave_taxonomy_claim'):project_optional_mappings(r,p,cat,contract_override=contract)


def test_scope_generation_remains_default_off(monkeypatch):
 from sempervigil import event_source_reports_v2 as reports
 monkeypatch.setenv('SV_EVENT_REPORT_V2_ENABLED','1');monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test')
 monkeypatch.delenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED',raising=False)
 with pytest.raises(PermissionError,match='generation_disabled'):reports.submit(None,'evt_test')
