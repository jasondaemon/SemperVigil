import json
from datetime import datetime, timedelta, timezone
import pytest
from sempervigil import event_reassessment_automation as scheduler
from sempervigil import event_report_v2_policy as policy, event_report_initial as initial

pytestmark = pytest.mark.offline


def identity():
    return {'event_id': 'evt_test', 'entity': 'Acme', 'system': 'legacy EHR',
            'incident_window': 'January 2025; discovery February 2025',
            'query_terms': ['legacy', 'EHR'], 'incident_year': 2025,
            'source_anchors': [{'source_id': 'S1', 'quote': 'Acme reported access to legacy EHR data in January 2025.'}]}


def configure(monkeypatch):
    now = datetime.now(timezone.utc)
    p = {'starts_at': (now-timedelta(minutes=1)).isoformat(),
         'expires_at': (now+timedelta(hours=12)).isoformat(), 'max_runs': 1,
         'max_concurrent': 1, 'run_tokens': 70000, 'debounce_seconds': 300,
         'generator_version': 'b'*64, 'admission_kind': 'initial_report',
         'incident_identity': identity()}
    for k, v in {'AUTONOMOUS': '1', 'ENABLED': '1', 'EVENT_IDS': 'evt_test',
                 'GENERATION_ENABLED': '1', 'COHORT_ID': 'initial-fixture',
                 'COHORT_TOKENS': '70000', 'POLICY': json.dumps(p)}.items():
        monkeypatch.setenv('SV_EVENT_REPORT_V2_'+k, v)
    monkeypatch.setenv('SV_EVENT_REPORT_V2_FINAL_EDITOR_CONFIG', json.dumps({
        'workflow': 'whole-source-final-editor-narrative-v1', 'model': 'fixture-editor',
        'reasoning_effort': 'high', 'max_completion_tokens': 12000, 'context_overrides': {}}))
    return p


def test_initial_authority_is_explicit_single_event_and_editor(monkeypatch):
    p = configure(monkeypatch)
    assert policy.policy()['admission_kind'] == 'initial_report'
    p['max_runs'] = 2
    monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY', json.dumps(p))
    with pytest.raises(ValueError, match='bounded_editor'): policy.policy()


def test_initial_management_does_not_break_unselected_legacy_summary(monkeypatch):
    configure(monkeypatch)
    from sempervigil import worker
    monkeypatch.setattr(worker, 'get_event', lambda *_: {'id': 'evt_other'})
    assert worker._handle_event_report_llm(None, None, {'event_id': 'evt_test'}, None)['reason'] == 'bounded_initial_narrative_managed'
    monkeypatch.setenv('SV_EVENT_REPORT_V2_POLICY', '{}')
    # A bad selected policy holds its own case, while other summaries continue.
    assert worker._handle_event_report_llm(None, None, {'event_id': 'evt_other'}, None)['reason'] == 'missing_event_version'
    with pytest.raises(ValueError): worker._handle_event_report_llm(None, None, {'event_id': 'evt_test'}, None)


def test_initial_report_rejects_invented_revision_changes(monkeypatch):
    from sempervigil import event_report_contract_v2 as contract
    from test_event_source_reports import packet, report
    import jsonschema
    p = packet(); p['update_reason'] = 'initial_report'
    r = report(); r['items'][0]['section'] = 'what_changed'
    for item in r['items']: item['attack_mappings'] = []
    with pytest.raises(jsonschema.ValidationError): contract.validate(r, p)


@pytest.mark.parametrize('change', ['pointer', 'withdrawn_history', 'entity', 'source_quote'])
def test_initial_draft_rejects_wrong_identity_and_publication_history(monkeypatch, change):
    p = configure(monkeypatch); p = policy.policy()
    class Conn:
        def execute(self, sql, args):
            if 'SELECT entity' in sql:
                row = ('Other' if change == 'entity' else 'Acme', 'active', 'confirmed', 'draft')
            elif 'event_public_pointers' in sql: row = ('rev',) if change == 'pointer' else None
            else: row = (1,) if change == 'withdrawn_history' else None
            return type('R', (), {'fetchone': lambda _: row})()
    monkeypatch.setattr('sempervigil.event_source_reports_v2.snapshot', lambda *_: {
        'sources': [{'id': 'S1', 'text': 'Different incident.' if change == 'source_quote' else identity()['source_anchors'][0]['quote']}]})
    with pytest.raises(ValueError): initial.require_draft(Conn(), p, 'evt_test')


def test_paused_scope_calls_no_recovery_or_advancement(monkeypatch):
    monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_ENABLED', '1')
    monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_EVENT_IDS', '[]')
    monkeypatch.setattr(scheduler, '_resume_audited_fallback', lambda _: pytest.fail('global recovery'))
    assert scheduler.tick(object()) == []


def test_scoped_scheduler_skips_global_recovery_and_rotates_pending_checks(monkeypatch):
    monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_ENABLED', '1')
    monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_EVENT_IDS', '["evt_allowed"]')
    monkeypatch.setattr(scheduler, '_resume_curator_version_hold', lambda _: pytest.fail('global recovery'))
    checked = []
    monkeypatch.setattr('sempervigil.storage.set_setting', lambda c, k, v: checked.append(k))
    class Conn:
        def execute(self, sql, args):
            assert args == (['evt_allowed'], ['evt_allowed'])
            assert 'checked.updated_at NULLS FIRST' in sql
            return type('R', (), {'fetchall': lambda _: [('evt_allowed',)]})()
    monkeypatch.setattr(scheduler, 'advance', lambda *_: {'status': 'pending'})
    assert scheduler.tick(Conn()) == [{'status': 'pending'}]
    assert checked == [scheduler.CHECKED_PREFIX+'evt_allowed']


def test_terminal_recovery_and_held_active_case_do_not_stop_other_work(monkeypatch):
    monkeypatch.setenv('SV_EVENT_REASSESSMENT_AUTOMATION_ENABLED', '1')
    monkeypatch.delenv('SV_EVENT_REASSESSMENT_AUTOMATION_EVENT_IDS', raising=False)
    monkeypatch.setattr('sempervigil.storage.set_setting', lambda *_: None)
    for name in ['_resume_curator_version_hold', '_resume_detail_filter_hold', '_resume_transient_composition_hold']:
        monkeypatch.setattr(scheduler, name, lambda _: None)
    monkeypatch.setattr(scheduler, '_resume_audited_fallback', lambda _: {'status': 'held', 'reason': 'nonconvergent'})
    class Conn:
        def execute(self, *_):
            return type('R', (), {'fetchall': lambda _: [('evt_held',), ('evt_viable',), ('evt_later',)]})()
    seen = []
    def advance(c, e):
        seen.append(e); return {'status': 'held' if e == 'evt_held' else 'queued', 'event_id': e}
    monkeypatch.setattr(scheduler, 'advance', advance)
    result = scheduler.tick(Conn())
    assert [r['status'] for r in result] == ['held', 'held', 'queued']
    assert seen == ['evt_held', 'evt_viable']


def test_asos_july_customer_credential_stuffing_is_not_october_employee_incident():
    from sempervigil.worker import _draft_event_in_scope
    july = {'entity': 'ASOS', 'incident_date': '2026-07-28'}
    assert not _draft_event_in_scope(july, entity='ASOS', incident_date='2026-10-06', window_days=14)
    assert _draft_event_in_scope({'entity': 'ASOS', 'incident_date': '2026-10-06'},
                               entity='ASOS', incident_date='2026-10-08', window_days=14)


def test_enrichment_retains_every_filter_outcome_without_extra_fetch(monkeypatch):
    from sempervigil import worker
    event = {'id': 'evt_test', 'kind': 'breach', 'entity': 'Acme', 'title': 'Acme breach'}
    monkeypatch.setattr(worker, 'get_event', lambda *_: event)
    monkeypatch.setattr(initial, 'research_identity', lambda *_: None)
    monkeypatch.setattr(worker, 'searxng_search', lambda *a, **k: [
        {'title': 'No URL'}, {'url': 'https://example.org/low', 'title': 'Low'},
        {'url': 'https://user:secret@example.org/good?token=private', 'title': 'Good'}])
    monkeypatch.setattr(worker, 'score_web_result', lambda e, i: (10 if i['title'] == 'Low' else 30, {'fixture': 1}))
    monkeypatch.setattr(worker, 'upsert_event_web_source', lambda *_: 'source')
    monkeypatch.setenv('SV_EVENT_ENRICH_AUTO_FETCH', '0')
    monkeypatch.setattr(worker, 'enqueue_job', lambda *_a, **_k: pytest.fail('No extra paid/fetch job'))
    result = worker._handle_enrich_event_from_web(object(), object(), {'event_id': 'evt_test'}, object())
    assert result['saved'] == 1
    decisions = result['diagnostics']['results']
    assert [d['reason'] for d in decisions] == ['missing_url', 'below_min_score', 'score_admitted']
    assert decisions[-1]['url'] == 'https://example.org/good'
