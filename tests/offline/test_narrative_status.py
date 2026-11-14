"""The dedicated narrative monitor is authenticated and performs only reads."""
import pytest
from fastapi.testclient import TestClient
from sempervigil import admin,storage,event_report_v2_policy as policy
pytestmark=pytest.mark.offline

def test_narrative_monitor_requires_configured_auth(monkeypatch):
    monkeypatch.delenv('SV_ADMIN_TOKEN',raising=False)
    monkeypatch.setattr(admin,'_get_conn',lambda:pytest.fail('unauthorized database read'))
    assert TestClient(admin.app).get('/admin/api/events/evt_case/narrative-status').status_code==403

def test_narrative_monitor_rejects_missing_credential(monkeypatch):
    monkeypatch.setenv('SV_ADMIN_TOKEN','synthetic-test-token')
    monkeypatch.setattr(admin,'_get_conn',lambda:pytest.fail('unauthorized database read'))
    assert TestClient(admin.app).get('/admin/api/events/evt_case/narrative-status').status_code==401

def test_narrative_monitor_rejects_outside_scope(monkeypatch):
    monkeypatch.setenv('SV_ADMIN_TOKEN','synthetic-test-token');monkeypatch.setattr(policy,'policy',lambda:{'events':['evt_other']})
    monkeypatch.setattr(admin,'_get_conn',lambda:pytest.fail('outside-scope database read'))
    assert TestClient(admin.app).get('/admin/api/events/evt_case/narrative-status',headers={'X-Admin-Token':'synthetic-test-token'}).status_code==404

def test_narrative_monitor_does_not_generate_write_or_invent_rates(monkeypatch):
    monkeypatch.setenv('SV_ADMIN_TOKEN','synthetic-test-token');monkeypatch.setattr(policy,'policy',lambda:{'id':'synthetic-cohort','events':['evt_case']});monkeypatch.setattr(policy,'active',lambda p:None)
    queries=[]
    class Conn:
        closed=False
        def execute(self,q,args=None):
            assert q.startswith(('SET TRANSACTION READ ONLY','SELECT run_id'))
            queries.append(q);return self
        def fetchall(self):return []
        def close(self):self.closed=True
    conn=Conn();monkeypatch.setattr(admin,'_get_conn',lambda:conn);monkeypatch.setattr(storage,'get_setting',lambda c,k,d:{'known_actual_tokens':0,'rates':None})
    r=TestClient(admin.app).get('/admin/api/events/evt_case/narrative-status',headers={'X-Admin-Token':'synthetic-test-token'})
    assert r.status_code==200 and r.json()['active'] and r.json()['rates'] is None and r.json()['estimated_usd'] is None
    assert r.json()['updates']==[] and len(queries)==2 and conn.closed
