"""Policy validation and semantic regression expectations; no model accuracy claims."""
import json
from datetime import datetime,timedelta,timezone
import pytest
from sempervigil import event_report_v2_policy as policy,event_report_contract_v2 as contract
pytestmark=pytest.mark.offline


def configure(monkeypatch):
 now=datetime.now(timezone.utc)
 monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','1')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_ENABLED','1')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_test')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','1')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_ID','v2-test')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_TOKENS','64000')
 p={'starts_at':(now-timedelta(minutes=1)).isoformat(),'expires_at':(now+timedelta(hours=1)).isoformat(),'max_runs':2,'max_concurrent':1,'run_tokens':32000,'debounce_seconds':300,'generator_version':'b'*64}
 monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p));return p


def test_dormant_policy_does_not_read_catalog(monkeypatch):
 monkeypatch.delenv('SV_EVENT_REPORT_V2_AUTONOMOUS',raising=False)
 assert policy.policy() is None


@pytest.mark.parametrize('change',['expiry','naive','pin','bool','missing','extra','capacity','concurrency','cohort'])
def test_fail_closed_configuration(monkeypatch,change):
 p=configure(monkeypatch)
 if change=='expiry':p['expires_at']=p['starts_at']
 if change=='naive':p['expires_at']='2026-10-07T12:00:00'
 if change=='pin':p['generator_version']='latest'
 if change=='bool':p['max_runs']=True
 if change=='missing':p.pop('expires_at')
 if change=='extra':p['backfill']=True
 if change=='capacity':p['run_tokens']=65000
 if change=='concurrency':p['max_concurrent']=3
 if change=='cohort':monkeypatch.delenv('SV_EVENT_REPORT_V2_COHORT_TOKENS')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY',json.dumps(p))
 with pytest.raises(ValueError):policy.policy()


def test_expiry_and_rollback_disable(monkeypatch):
 p=configure(monkeypatch);active=policy.policy();policy.active(active)
 monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','0')
 with pytest.raises(ValueError,match='generation_disabled'):policy.active(active)
 monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','1')
 active['expires_at']=active['starts_at']
 with pytest.raises(ValueError,match='inactive'):policy.active(active)


def test_epistemic_contract_is_general_and_paragraph_coherent():
 assert contract.EPISTEMIC_PARAGRAPH_RULES in contract.WRITER
 assert contract.REVIEW_DECISION_CHECKLIST in contract.REVIEWER
 assert all(identifier not in contract.WRITER+contract.REVIEWER for identifier in ('P110:', 'P113:', 'P115:', 'CVE-2026-102489', 'CVE-2026-102490'))
 # Coverage is not proof that any model follows these instructions.


def test_legacy_pilot_cohort_cannot_be_reused(monkeypatch):
 configure(monkeypatch)
 from sempervigil import event_source_report_pilot
 monkeypatch.setattr(event_source_report_pilot,'policy',lambda:{'id':'v2-test','events':['evt_other']})
 with pytest.raises(ValueError,match='pilot_cohort_overlap'):policy.policy()


def test_policy_code_is_generation_pinned(monkeypatch):
 from pathlib import Path
 from sempervigil import event_source_reports_v2 as reports
 from sempervigil.services import ai_service
 class Cursor:
  def fetchone(self):return ('provider','model')
 class Connection:
  def execute(self,*args):return Cursor()
 monkeypatch.setattr(ai_service,'get_provider',lambda *a:{'id':'provider','base_url':'https://example.org'})
 monkeypatch.setattr(ai_service,'get_model',lambda *a:{'id':'model'})
 first=reports.configuration(Connection())[2];original=Path.read_bytes
 monkeypatch.setattr(Path,'read_bytes',lambda p:original(p)+(b' changed lifecycle policy' if p.name=='event_report_v2_policy.py' else b''))
 assert reports.configuration(Connection())[2]!=first


def legacy_configuration(monkeypatch):
 from sempervigil import event_source_report_pilot
 now=datetime.now(timezone.utc)
 p={'id':'legacy-live','limit':64000,'events':['evt_ms','evt_ast'],'starts_at':(now-timedelta(minutes=1)).isoformat(),'expires_at':(now+timedelta(hours=1)).isoformat(),'max_runs':2,'max_concurrent':1,'run_tokens':32000}
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_PILOT_POLICY',json.dumps(p))
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_ID',p['id'])
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_COHORT_TOKENS',str(p['limit']))
 monkeypatch.setenv('SV_EVENT_SOURCE_REPORT_EVENT_IDS',','.join(p['events']))
 return event_source_report_pilot.policy()


def test_distinct_policies_coexist_without_shared_environment_or_identity(monkeypatch):
 from sempervigil import event_source_report_pilot as legacy
 from sempervigil import event_source_reports_v2 as reports
 configure(monkeypatch);old=legacy_configuration(monkeypatch)
 v2=policy.policy();policy.active(v2);legacy.active(old)
 assert v2['id']=='v2-test' and v2['events']==['evt_test']
 assert old['id']=='legacy-live' and set(old['events'])=={'evt_ms','evt_ast'}
 assert reports.cohort_configuration()=={'id':'v2-test','limit':64000}
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_TOKENS','32000')
 assert policy.policy()['limit']==32000 and legacy.policy()==old
 monkeypatch.delenv('SV_EVENT_REPORT_V2_COHORT_ID')
 with pytest.raises(ValueError,match='v2_cohort_invalid'):policy.policy()
 assert legacy.policy()==old


def test_v2_never_inherits_legacy_cohort_or_overlapping_scope(monkeypatch):
 configure(monkeypatch);old=legacy_configuration(monkeypatch)
 monkeypatch.delenv('SV_EVENT_REPORT_V2_COHORT_ID')
 monkeypatch.delenv('SV_EVENT_REPORT_V2_COHORT_TOKENS')
 with pytest.raises(ValueError,match='policy_required'):policy.policy()
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_ID',old['id'])
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_TOKENS','64000')
 with pytest.raises(ValueError,match='pilot_cohort_overlap'):policy.policy()
 monkeypatch.setenv('SV_EVENT_REPORT_V2_COHORT_ID','v2-test')
 monkeypatch.setenv('SV_EVENT_REPORT_V2_EVENT_IDS','evt_ms')
 with pytest.raises(ValueError,match='active_pilot_overlap'):policy.policy()
